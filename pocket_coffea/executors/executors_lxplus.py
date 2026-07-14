import os
import sys
import socket
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_manual_jobs import (
    ExecutorFactoryManualABC,
    INNER_RUN_OPTIONS_FILENAME,
    ensure_job_sh_forwards_inner_yaml,
    ensure_sub_transfers_inner_yaml,
    write_inner_run_options,
)
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from pocket_coffea.utils.network import check_port
from pocket_coffea.parameters.dask_env import setup_dask
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.utils.rucio import get_xrootd_sites_map
from pocket_coffea.utils.site_rewrite import (
    GLOBAL_XROOTD_REDIRECTOR,
    rewrite_fileset_blocklist,
    rewrite_fileset_to_redirector,
)
import pocket_coffea
import cloudpickle
import yaml
import json
from copy import deepcopy
import multiprocessing as mp
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

_PREPARE_CFG = None
_PREPARE_JOBS_DIR = None
_PREPARE_OUTPUTDIR = None


def _prepare_job_config_worker(config, split, i, jobs_dir, outputdir):
    partial_config = config.clone()
    partial_config.set_filesets_manually(split)
    outputfile = f"{os.path.abspath(outputdir)}/output_job_{i}.coffea"
    config_file = f"{jobs_dir}/config_job_{i}.pkl"
    with open(config_file, "wb") as f:
        cloudpickle.dump(partial_config, f)
    return i, config_file, split, outputfile

def _prepare_job_config_init(config, jobs_dir, outputdir):
    global _PREPARE_CFG, _PREPARE_JOBS_DIR, _PREPARE_OUTPUTDIR
    _PREPARE_CFG = config
    _PREPARE_JOBS_DIR = jobs_dir
    _PREPARE_OUTPUTDIR = outputdir

def _prepare_job_config_worker_indexed(item):
    i, split = item
    return _prepare_job_config_worker(_PREPARE_CFG, split, i, _PREPARE_JOBS_DIR, _PREPARE_OUTPUTDIR)


CONDOR_GRACEFUL_SHUTDOWN_SECONDS = 5 * 60


def build_condor_shutdown_policy():
    """Return the submit attributes used for the five-minute timeout warning."""
    return {
        "periodic_remove": (
            "(JobStatus == 2) && (JobCurrentStartDate =!= UNDEFINED) && "
            f"((time() - JobCurrentStartDate) > "
            f"(MaxRuntime - {CONDOR_GRACEFUL_SHUTDOWN_SECONDS}))"
        ),
        "want_graceful_removal": True,
        "job_max_vacate_time": CONDOR_GRACEFUL_SHUTDOWN_SECONDS,
        "kill_sig": 15,
    }


def build_condor_job_state(per_job_chunksize, queue, cpus, memory):
    return {
        str(i): {
            "queue": queue,
            "chunksize": chunksize,
            "base_cpus": cpus,
            "base_memory": memory,
            "request_cpus": cpus,
            "request_memory": memory,
            "resources_scaled": False,
            "resubmissions": 0,
        }
        for i, chunksize in enumerate(per_job_chunksize)
    }


def get_worker_env(run_options,x509_path,exec_name="dask"):
    env_worker = [
        'export XRD_RUNFORKHANDLER=1',
        'export MALLOC_TRIM_THRESHOLD_=0'        ,
        ]
    if exec_name == "dask":
        env_worker.append('ulimit -u unlimited')

    if not run_options['ignore-grid-certificate']:
        env_worker.append(f'export X509_USER_PROXY={x509_path}')
    
    # Adding list of custom setup commands from user defined run options
    if run_options.get("custom-setup-commands", None):
        env_worker += run_options["custom-setup-commands"]

    # Now checking for conda environment  conda-env:true
    if run_options.get("conda-env", False):
        env_worker.append(f'export PATH={os.environ["CONDA_PREFIX"]}/bin:$PATH')
        if "CONDA_ROOT_PREFIX" in os.environ:
            env_worker.append(f"{os.environ['CONDA_ROOT_PREFIX']} activate {os.environ['CONDA_DEFAULT_ENV']}")
        elif "MAMBA_ROOT_PREFIX" in os.environ:
            env_worker.append(f"{os.environ['MAMBA_ROOT_PREFIX']} activate {os.environ['CONDA_DEFAULT_ENV']}")
        else:
            raise Exception("CONDA prefix not found in env! Something is wrong with your conda installation if you want to use conda in the dask cluster.")

    # if local-virtual-env: true the dask job is configured to pickup
    # the local virtual environment. 
    if run_options.get("local-virtualenv", False):
        env_worker.append(f"source {sys.prefix}/bin/activate")
        if exec_name == "dask":
            env_worker.append(f"export PYTHONPATH={sys.prefix}/lib:$PYTHONPATH")
        elif exec_name == "condor":
            if os.getenv("PYTHONPATH"):
                pythonpath = os.getenv("PYTHONPATH")
            else:
                pythonpath = "/".join(pocket_coffea.__file__.split("/")[:-2])
            env_worker.append(f"export PYTHONPATH={pythonpath}")

    return env_worker

class DaskExecutorFactory(ExecutorFactoryABC):

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options, **kwargs)
        
    def setup(self):
        ''' Start the DASK cluster here'''
        self.setup_proxyfile()
        # Setup dask general options from parameters/dask_env.py
        import dask.config
        from distributed import Client
        setup_dask(dask.config)

        # Spin up a HTCondor cluster for dask using dask_lxplus
        from dask_lxplus import CernCluster
        if "lxplus" not in socket.gethostname():
                raise Exception("Trying to run with dask/lxplus not at CERN! Please try different runner options")

        n_port = self.run_options.get("dask-scheduler-port", 8786)
        while not check_port(n_port):
            print(f">> Port {n_port} is already occupied, trying port {n_port + 1}...")
            n_port += 1
        print(">> Creating dask-lxplus cluster transmitting on port:", n_port)
        # Creating a CERN Cluster, special configuration for dask-on-lxplus
        log_folder = "condor_log"
        self.dask_cluster = CernCluster(
                cores=self.run_options['cores-per-worker'],
                memory=self.run_options['mem-per-worker'],
                disk=self.run_options['disk-per-worker'],
                image_type="singularity",
                worker_image=self.run_options["worker-image"],
                death_timeout=self.run_options["death-timeout"],
                scheduler_options={"port": n_port, "host": socket.gethostname()},
                log_directory = f"{self.outputdir}/{log_folder}",
                # shared_temp_directory="/tmp"
                job_extra={
                    "log": f"{self.outputdir}/{log_folder}/dask_job_output.log",
                    "output": f"{self.outputdir}/{log_folder}/dask_job_output.out",
                    "error": f"{self.outputdir}/{log_folder}/dask_job_output.err",
                    "should_transfer_files": "Yes", #
                    "when_to_transfer_output": "ON_EXIT",
                    "+JobFlavour": f'"{self.run_options["queue"]}"'
                },
                env_extra=get_worker_env(self.run_options,self.x509_path,"dask"),
            )

        #Cluster adaptive number of jobs only if requested
        print(">> Sending out jobs")
        self.dask_cluster.adapt(minimum=1 if self.run_options["adaptive"]
                                else self.run_options['scaleout'],
                      maximum=self.run_options['scaleout'])
        
        self.dask_client = Client(self.dask_cluster)
        print(">> Waiting for the first job to start...")
        self.dask_client.wait_for_workers(1)
        print(">> You can connect to the Dask viewer at http://localhost:8787")

        # if self.run_options["performance-report"]:
        #     self.performance_report_path = os.path.join(self.outputdir, f"{log_folder}/dask-report.html")
        #     print(f"Saving performance report to {self.performance_report_path}")
        #     self.performance_report(filename=performance_report_path):

        
    def get(self):
        return coffea_processor.dask_executor(**self.customized_args())

    def customized_args(self):
        args = super().customized_args()
        # in the futures executor Nworkers == N scaleout
        args["client"] = self.dask_client
        args["treereduction"] = self.run_options["tree-reduction"]
        args["retries"] = self.run_options["retries"]
        return args

    def close(self):
        self.dask_client.close()
        self.dask_cluster.close()

#--------------------------------------------------------------------
# Manual jobs executor

def build_job_script(
    env_extras,
    abs_jobdir_path,
    abs_output_path,
    copy_command,
    runnercmd,
    inner_yaml_basename,
    split_by_category,
    cores_per_worker,
):
    '''Build the per-job HTCondor wrapper (job.sh) as a string.

    Extracted from ExecutorFactoryCondorCERN.submit_jobs so the generated script can be
    unit-tested without a live condor submission. The job id is passed to the wrapper as
    ``$1`` and captured once into ``$JOBID`` so it is available inside ``run_with_retries``
    (where bare ``$1`` refers to the *function's* first argument — the command string —
    not the job id, which used to corrupt the flag-file names on copy failure).

    In split-by-category mode the split runs inside the job-local ``output/`` scratch dir
    (never the shared final dir) and every step is exit-checked, so a split/copy failure
    marks the job ``.failed`` instead of silently reporting ``.done``.
    '''
    if split_by_category:
        splitcommands = f'''
    cd output || {{ echo 'cd output failed'; rm $JOBDIR/job_$JOBID.running; touch $JOBDIR/job_$JOBID.failed; exit 1; }}
    split-output output_all.coffea -b category -o output.coffea || {{ echo 'split-output failed'; rm $JOBDIR/job_$JOBID.running; touch $JOBDIR/job_$JOBID.failed; exit 1; }}
    rm -f output_all.coffea
    for f in *.coffea; do
        run_with_retries "{copy_command} $f {abs_output_path}/${{f%.coffea}}_job_$JOBID.coffea"
    done
    cd ..
'''
    else:
        splitcommands = f'run_with_retries "{copy_command} output/output_all.coffea {abs_output_path}/output_job_$JOBID.coffea"'

    script = f"""#!/bin/bash
{env_extras}

JOBDIR={abs_jobdir_path}
JOBID="$1"
MAX_XROOTD_REWRITES=10
cleanup_started=0

cleanup() {{
    trap - TERM INT
    if [ "$cleanup_started" -eq 1 ]; then
        return
    fi
    cleanup_started=1
    echo "Termination signal received."
    rm -f "$JOBDIR/job_$JOBID.running" "$JOBDIR/job_$JOBID.idle"
    touch "$JOBDIR/job_$JOBID.timeout"
    echo "Marked job as timeout."
    if [[ -n "${{child:-}}" ]]; then
        kill -TERM "$child" 2>/dev/null || true
        wait "$child" 2>/dev/null || true
    fi
    exit 0
}}

trap cleanup TERM INT

run_with_retries() {{
    local cmd="$*"
    for i in {{1..10}}; do
        eval "$cmd" &
        child=$!
        wait "$child"
        command_status=$?
        child=""
        [ $command_status -eq 0 ] && return 0
        sleep 10
    done
    echo "$cmd failed after 10 attempts."
    rm -f $JOBDIR/job_$JOBID.running
    touch $JOBDIR/job_$JOBID.failed
    exit 1
}}

rm -f $JOBDIR/job_$JOBID.idle

echo "Starting job $JOBID"
touch $JOBDIR/job_$JOBID.running

attempt=0
job_succeeded=0
failed_xrootd_sites="$_CONDOR_SCRATCH_DIR/failed_xrootd_sites.txt"
while true; do
    attempt_log="$_CONDOR_SCRATCH_DIR/job_attempt_${{attempt}}.out"
    echo "Runner attempt $attempt"
    {runnercmd} --cfg $2 -o output EXECUTOR --chunksize $3 --custom-run-options {inner_yaml_basename} > "$attempt_log" 2>&1 &
    child=$!
    wait "$child"
    runner_status=$?
    child=""
    cat "$attempt_log"
    if [ $runner_status -eq 0 ]; then
        job_succeeded=1
        break
    fi
    if [ $attempt -ge $MAX_XROOTD_REWRITES ]; then
        echo "Reached the maximum number of XRootD recovery attempts ($MAX_XROOTD_REWRITES)."
        break
    fi
    python -m pocket_coffea.scripts.rewrite_xrootd_site --config "$2" --log "$attempt_log" --failed-sites-file "$failed_xrootd_sites" &
    child=$!
    wait "$child"
    rewrite_status=$?
    child=""
    if [ $rewrite_status -eq 0 ]; then
        echo "XRootD site rewrite changed the config. Rerunning job $JOBID."
        attempt=$((attempt + 1))
        continue
    fi
    break
done

if [ $job_succeeded -eq 1 ]; then
    echo 'Job successful'
    {splitcommands}
    rm $JOBDIR/job_$JOBID.running
    touch $JOBDIR/job_$JOBID.done
else
    echo 'Job failed'
    rm $JOBDIR/job_$JOBID.running
    touch $JOBDIR/job_$JOBID.failed
fi
echo 'Done'
"""

    if int(cores_per_worker) > 1:
        script = script.replace("EXECUTOR", f"--executor futures --scaleout {cores_per_worker}")
    else:
        script = script.replace("EXECUTOR", "--executor iterative")
    return script


class ExecutorFactoryCondorCERN(ExecutorFactoryManualABC):
    def get(self):
        pass

    def prepare_jobs(self, splits):
        config_files = [ ]
        jobs_config = {
            "job_name": self.job_name,
            "job_dir": os.path.abspath(self.jobs_dir),
            "output_dir": os.path.abspath(self.outputdir),
            "split_by_category": self.run_options["split-by-category"],
            "config_pkl_total": f"{os.path.abspath(self.outputdir)}/configurator.pkl",
            "jobs_list": {}
        }
        # Disabling the postprocessing
        self.config.do_postprocessing = False
        # Splitting the filesets, creating a new configuration for each and pickling it.
        # Parallelised with multiprocessing.Pool to speed up large submission batches.
        total_splits = len(splits)
        config_files = [None] * total_splits
        console = Console(stderr=True)
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[cyan]Preparing config pkls"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            refresh_per_second=10,
            transient=False,
        )
        work_items = [(i, split) for i, split in enumerate(splits)]
        with progress:
            task1 = progress.add_task("prepare_pkls", total=total_splits)
            ctx = mp.get_context("fork")
            with ctx.Pool(
                processes=8,
                initializer=_prepare_job_config_init,
                initargs=(self.config, self.jobs_dir, self.outputdir),
            ) as pool:
                for i, config_file, split, output_file in pool.imap_unordered(
                    _prepare_job_config_worker_indexed, work_items, chunksize=1
                ):
                    config_files[i] = config_file
                    jobs_config["jobs_list"][f"job_{i}"] = {
                        "filesets": split,
                        "config_file": config_file,
                        "output_file": output_file,
                    }
                    progress.update(task1, advance=1)

        yaml.dump(jobs_config, open(f"{self.jobs_dir}/jobs_config.yaml", "w"))
        # save the configuration
        return config_files

    def submit_jobs(self, jobs_config):
        '''Prepare job config and script and submit the jobs to the cluster'''
        
        abs_output_path = os.path.abspath(self.outputdir)
        abs_jobdir_path = os.path.abspath(self.jobs_dir)
        os.makedirs(f"{self.jobs_dir}/logs", exist_ok=True)
        
        env_extras_list=get_worker_env(self.run_options,self.x509_path,"condor")
        env_extras= "\n".join(env_extras_list)

        if os.getenv("PYTHONPATH"):
            pythonpath = os.getenv("PYTHONPATH")
        else:
            pythonpath = "/".join(pocket_coffea.__file__.split("/")[:-2])        

        copy_command = "cp"
        eos_prefix = self.run_options["eos-prefix"]
        if abs_output_path.startswith("/eos/"):
            abs_output_path = eos_prefix + abs_output_path
        if abs_output_path.startswith(eos_prefix):
            copy_command = "xrdcp -f"

        # Handle columns
        columncommand = ""
        if len(self.config.columns) > 0 and "dump_columns_as_arrays_per_chunk" in self.config.workflow_options:
            column_out_dir = self.config.workflow_options["dump_columns_as_arrays_per_chunk"]
            if not os.path.isabs(column_out_dir) and not column_out_dir.startswith(eos_prefix):
                # If the config contains an absolute path, then the
                # parquets are written directly to disk (e.g. eos.)
                # This may be unstable, but not much to do at the executor level.
                # Otherwise, check that the directory exists and then copy the directory to outputdir
                columncommand = f'[ -d "{column_out_dir}" ] && run_with_retries "{copy_command} -r {column_out_dir} {abs_output_path}"'

        runnerpath = f"{pythonpath}/pocket_coffea/scripts/runner.py"
        if os.path.isfile(runnerpath):
            runnercmd = "python " + runnerpath
        else:
            runnercmd = "pocket-coffea run"

        # Persist the inner-relevant run-option overrides (e.g. skip-bad-files)
        # so the *inner* pocket-coffea call running on the worker can honour
        # them via --custom-run-options. The YAML is whitelisted to keep
        # outer-only keys (worker-image, queue, cores-per-worker, ...) from
        # leaking through.
        inner_yaml_path = write_inner_run_options(self.jobs_dir, self.run_options)
        inner_yaml_basename = os.path.basename(inner_yaml_path)

        # Build the per-job wrapper (extracted into build_job_script so it is unit-testable).
        script = build_job_script(
            env_extras=env_extras,
            abs_jobdir_path=abs_jobdir_path,
            abs_output_path=abs_output_path,
            copy_command=copy_command,
            runnercmd=runnercmd,
            inner_yaml_basename=inner_yaml_basename,
            split_by_category=self.run_options["split-by-category"],
            cores_per_worker=self.run_options["cores-per-worker"],
        )
        with open(f"{self.jobs_dir}/job.sh", "w") as f:
            f.write(script)

        # Resolve per-job chunksize. Accepts a scalar (legacy) or a per-sample dict.
        # See ExecutorFactoryManualABC._resolve_chunksize_for_job.
        chunksize_cfg = self.run_options['chunksize']
        self._validate_chunksize_keys(chunksize_cfg, self.filesets)
        per_job_chunksize = [self._resolve_chunksize_for_job(chunksize_cfg, split)
                             for split in self._splits]
        if len(set(per_job_chunksize)) > 1:
            print(f"[chunksize] Per-job chunksize varies (min={min(per_job_chunksize)}, "
                  f"max={max(per_job_chunksize)}); HTCondor `queue chunksize from arglist.txt` "
                  f"will inject the right value per job.")

        # Writing the jid file as the htcondor python submission does not work in the singularity
        sub = {
            'Executable': "job.sh",
            'Error': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).out",
            'Output': f"{abs_jobdir_path}/logs/job_$(ClusterId).$(ProcId).out",
            'Log': f"{abs_jobdir_path}/logs/job_$(ClusterId).log",
            'MY.SendCredential': True,
            'MY.SingularityImage': f'"{self.run_options["worker-image"]}"',
            '+JobFlavour': f'"{self.run_options["queue"]}"',
            'RequestCpus' : self.run_options['cores-per-worker'],
            'RequestMemory' : f"{self.run_options['mem-per-worker']}",
            'arguments': f"$(ProcId) config_job_$(ProcId).pkl $(chunksize) {self.run_options['cores-per-worker']}",
            'should_transfer_files':'YES',
            'when_to_transfer_output' : 'ON_EXIT',
            'transfer_output_files' : '',
            'transfer_input_files' : f"{abs_jobdir_path}/config_job_$(ProcId).pkl,{self.x509_path},{abs_jobdir_path}/job.sh,{abs_jobdir_path}/{inner_yaml_basename}",
            'on_exit_remove': '(ExitBySignal == False) && (ExitCode == 0)',
            'max_retries' : self.run_options["retries"],
            'requirements' : 'Machine =!= LastRemoteHost'
        }
        sub.update(build_condor_shutdown_policy())

        # HTCondor inline `queue <var> from ( ... )` form: one job per item, with
        # the per-job value made available as $(chunksize) in the arguments line.
        # Single condor_submit jobs_all.sub submits everything; the items are
        # embedded directly in the sub file (no separate arglist file to manage).
        with open(f"{self.jobs_dir}/jobs_all.sub", "w") as f:
            for k, v in sub.items():
                f.write(f"{k} = {v}\n")
            f.write("queue chunksize from (\n")
            for cs in per_job_chunksize:
                f.write(f"  {cs}\n")
            f.write(")\n")

        job_state = build_condor_job_state(
            per_job_chunksize,
            self.run_options["queue"],
            self.run_options["cores-per-worker"],
            self.run_options["mem-per-worker"],
        )
        with open(f"{self.jobs_dir}/job_state.json", "w") as f:
            json.dump(job_state, f, indent=2, sort_keys=True)

        # check-jobs appends one queue row per failed job to this template.
        resubmit_sub = dict(sub)
        resubmit_sub.update({
            "Error": f"{abs_jobdir_path}/logs/job_$(ClusterId).$(PROC).out",
            "Output": f"{abs_jobdir_path}/logs/job_$(ClusterId).$(PROC).out",
            "Log": f"{abs_jobdir_path}/logs/job_$(ClusterId).$(PROC).log",
            "+JobFlavour": '"$(QUEUE)"',
            "RequestCpus": "$(CPUS)",
            "RequestMemory": "$(MEMORY)",
            "arguments": "$(PROC) config_job_$(PROC).pkl $(CHUNKSIZE) $(CPUS)",
            "transfer_input_files": (
                f"{abs_jobdir_path}/config_job_$(PROC).pkl,{self.x509_path},"
                f"{abs_jobdir_path}/job.sh,{abs_jobdir_path}/{inner_yaml_basename}"
            ),
        })
        with open(f"{self.jobs_dir}/resubmit.sub", "w") as f:
            for k, v in resubmit_sub.items():
                f.write(f"{k} = {v}\n")

        print(f"Created global resubmission state for {len(jobs_config)} jobs.")
        for i, _ in enumerate(jobs_config):
            # Let's also create a .idle file to indicate the the job is in idle
            with open(f"{self.jobs_dir}/job_{i}.idle", "w") as f:
                f.write("")

        dry_run = self.run_options.get("dry-run", False)
        if dry_run:
            print(f"Dry run, not submitting jobs. You can find all files: {abs_jobdir_path}")
            return
        else:
            print("Submitting jobs")
            os.system(f"cd {abs_jobdir_path} && condor_submit jobs_all.sub")


    def recreate_jobs(self, jobs_to_recreate):
        raise RuntimeError(
            "--recreate-jobs has been removed; use check-jobs --resubmit instead."
        )

        # Read the jobs config
        if not os.path.exists(f"{self.jobs_dir}/jobs_config.yaml"):
            print("No jobs_config.yaml found. Exiting.")
            exit(1)
        with open(f"{self.jobs_dir}/jobs_config.yaml") as f:
            jobs_config = yaml.safe_load(f)

        # (Re)materialise the inner run-options YAML so any --skip-bad-files /
        # --custom-run-options-style overrides from this outer call reach the
        # resubmitted jobs. Also idempotently patch the wrapper script so an
        # existing jobs_dir (pre-feature) starts forwarding the YAML.
        abs_jobdir_path = os.path.abspath(self.jobs_dir)
        write_inner_run_options(self.jobs_dir, self.run_options)
        if ensure_job_sh_forwards_inner_yaml(f"{self.jobs_dir}/job.sh"):
            print(f"[recreate-jobs] Patched {self.jobs_dir}/job.sh to forward "
                  f"{INNER_RUN_OPTIONS_FILENAME} to the inner pocket-coffea run.")

        # Flag-file state lists. Computed unconditionally: they drive `auto` selection
        # AND are consulted below (whether to clear .failed/.running, whether to bump the
        # queue) for explicit `--recreate-jobs <ids>` resubmissions too. Previously they
        # were defined only inside the `auto` branch, so any explicit id list raised
        # NameError on `job in runningjobs`.
        failedjobs = [f.replace(".failed","") for f in os.listdir(self.jobs_dir) if f.endswith(".failed")]
        runningjobs = [f.replace(".running","") for f in os.listdir(self.jobs_dir) if f.endswith(".running")]
        idlejobs = [f.replace(".idle","") for f in os.listdir(self.jobs_dir) if f.endswith(".idle")]

        if jobs_to_recreate == "auto":
            jobs_to_recreate = failedjobs + runningjobs + idlejobs
            if len(jobs_to_recreate) == 0:
                print(f"Could not automatically find *.failed/*.running/*.idle/*.timeout files in {self.jobs_dir}. All jobs probably succeeded! Exiting.")
                exit()
            jobs_to_recreate = ','.join(jobs_to_recreate)

        jobs_to_redo = []
        for j in jobs_to_recreate.split(","):
            if not j.startswith("job_"):
                jobs_to_redo.append(f"job_{j}")
            else:
                jobs_to_redo.append(j)
        print(f"Recreating jobs: {jobs_to_redo}")

        # Parse blocklist-sites: accept comma-separated string or list, mirroring build_datasets
        blocklist_raw = self.run_options.get("blocklist-sites", None) or []
        if isinstance(blocklist_raw, str):
            blocklist_sites = {s for s in blocklist_raw.split(",") if s}
        else:
            blocklist_sites = set(blocklist_raw)
        if blocklist_sites:
            print(f"Blocklisting sites at recreate time: {sorted(blocklist_sites)}")

        # --use-redirector takes precedence over the per-site blocklist rewrite —
        # it's a "no Rucio, just put everything on the global xrootd redirector"
        # one-shot. When both are set we honour the redirector and warn.
        use_redirector = bool(self.run_options.get("use-redirector", False))
        if use_redirector:
            print(f"[recreate-jobs] --use-redirector: rewriting every file to "
                  f"{GLOBAL_XROOTD_REDIRECTOR} without per-site Rucio lookups.")
            if blocklist_sites:
                print("[recreate-jobs] WARNING: --blocklist-sites is set but --use-redirector "
                      "overrides it (no per-site Rucio resolution happens).")

        sitemap = get_xrootd_sites_map() if blocklist_sites and not use_redirector else None
        # Open a single rucio client up front so blocklist rewrites reuse the same lookup session.
        rucio_client = None
        if blocklist_sites and not use_redirector:
            try:
                from pocket_coffea.utils.rucio import get_rucio_client
                rucio_client = get_rucio_client()
            except Exception as e:
                print(f"WARNING: could not open a rucio client ({e}); replica lookups will fail.")

        # Optional queue rewrite: if --recreate-queue is set, every resubmitted
        # .sub file is rewritten to use that HTCondor +JobFlavour. Validate up front.
        recreate_queue = self.run_options.get("recreate-queue", None)
        if recreate_queue is not None and recreate_queue not in queues:
            print(f"WARNING: recreate-queue={recreate_queue!r} is not in the known "
                  f"HTCondor queue list {queues}. Proceeding anyway — your value will "
                  f"be written verbatim into the .sub file.")

        for job in jobs_to_redo:
            # Check if the job is in the list of jobs to recreate
            if job not in jobs_config["jobs_list"]:
                print(f"Job {job} not found in the list of jobs")
                continue

            current_fileset = jobs_config["jobs_list"][job]["filesets"]
            new_fileset = deepcopy(current_fileset)
            modified = False

            if use_redirector:
                # One-shot: every file in this job is pointed at the global
                # xrootd redirector. No Rucio lookup, no per-site logic.
                new_fileset = rewrite_fileset_to_redirector(new_fileset)
                modified = True
            elif blocklist_sites:
                new_fileset = rewrite_fileset_blocklist(new_fileset, sitemap, blocklist_sites,
                                                        rucio_client=rucio_client)
                modified = True

            if modified:
                # Update files in the configurator
                config = cloudpickle.load(open(f"{self.jobs_dir}/config_{job}.pkl", "rb"))
                config.set_filesets_manually(new_fileset)
                cloudpickle.dump(config, open(f"{self.jobs_dir}/config_{job}.pkl", "wb"))

            sub_file = f"{self.jobs_dir}/{job}.sub"
            if not os.path.exists(sub_file):
                resubmit_template_path = f"{self.jobs_dir}/resubmit.sub"
                state_file_path = f"{self.jobs_dir}/job_state.json"
                if os.path.exists(resubmit_template_path) and os.path.exists(state_file_path):
                    with open(resubmit_template_path) as f:
                        template = f.read()
                    with open(state_file_path) as f:
                        state_all = json.load(f)
                    job_num = job.split("_", 1)[1]
                    state = state_all.get(job_num)
                    if state:
                        # Replace template variables
                        sub_content = template
                        sub_content = sub_content.replace("$(PROC)", job_num)
                        sub_content = sub_content.replace("$(QUEUE)", state["queue"])
                        sub_content = sub_content.replace("$(CHUNKSIZE)", str(state["chunksize"]))
                        sub_content = sub_content.replace("$(CPUS)", str(state["request_cpus"]))
                        sub_content = sub_content.replace("$(MEMORY)", str(state["request_memory"]))

                        with open(sub_file, "w") as f:
                            f.write(sub_content)
                            f.write("\nqueue\n")

            # Idempotently make sure this job's .sub ships the inner-run-options
            # YAML so the wrapper can pass it to the inner pocket-coffea run.
            ensure_sub_transfers_inner_yaml(sub_file, abs_jobdir_path)

            # Optional explicit queue rewrite from --recreate-queue (runs before the
            # implicit timeout-bump so the explicit value wins if both apply)
            if recreate_queue is not None:
                set_queue(sub_file, job, recreate_queue)
            # If the job failed due to timeout, increase the timelimit
            elif job in runningjobs:
                update_queue(sub_file, job)

            # Resubmit the job
            dry_run = self.run_options.get("dry-run", False)
            if dry_run:
                print(f"Dry run, not resubmitting  {job}")
            else:
                if job in failedjobs:
                    os.system(f"rm {self.jobs_dir}/{job}.failed")
                elif job in runningjobs:
                    os.system(f"rm {self.jobs_dir}/{job}.running")
                elif job in timeoutjobs:
                    os.system(f"rm {self.jobs_dir}/{job}.timeout")
                os.system(f"touch {self.jobs_dir}/{job}.idle")
                os.system(f"cd {self.jobs_dir} && condor_submit {job}.sub")
                print(f"Resubmitted {job}")

# rewrite_fileset_blocklist and GLOBAL_XROOTD_REDIRECTOR are imported above
# from pocket_coffea.utils.site_rewrite (kept dependency-free for unit testing).

def update_queue(subfile,job):
    with open(subfile, 'r') as fl:
        lines = fl.readlines()

    with open(subfile, 'w') as fl:
        for ln in lines:
            if ln.startswith("+JobFlavour"):
                q = ln.split('"')[-2]
                idx = queues.index(q)
                if idx + 1 < len(queues):
                    newq = queues[idx+1]
                    print(f"Bumping {job} from {q} to {newq}")
                else:
                    print(f"{job}: Already at the highest queue! Cannot bump up.")
                    newq = q
                fl.write(f'+JobFlavour = "{newq}"\n')
            else:
                fl.write(ln)


def set_queue(subfile, job, new_queue):
    """Rewrite the +JobFlavour line of `subfile` to `new_queue` (no-op if already set)."""
    with open(subfile, 'r') as fl:
        lines = fl.readlines()
    changed = False
    with open(subfile, 'w') as fl:
        for ln in lines:
            if ln.startswith("+JobFlavour"):
                old = ln.split('"')[-2]
                if old != new_queue:
                    print(f"[queue] {job}: {old} -> {new_queue}")
                    changed = True
                fl.write(f'+JobFlavour = "{new_queue}"\n')
            else:
                fl.write(ln)
    return changed

queues = [
    "espresso",
    "microcentury",
    "longlunch",
    "workday",
    "tomorrow",
    "testmatch",
    "nextweek"
]
                
def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    elif executor_name == "condor":
        return ExecutorFactoryCondorCERN(**kwargs)
