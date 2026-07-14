'''Simple script that checks the status of the jobs submitted by runner on condor.

The status of the jobs can be checked by looking at the file in the jobs folder.

- job_x.idle: The job is waiting to be executed
- job_x.running: The job is running
- job_x.done: The job has finished
- job_x.failed: The job has failed
- job_x.timeout: The job hit the Condor time limit

    where x is the job id.
'''

import os
import yaml
import cloudpickle
import click
import glob
import json
from copy import deepcopy
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
import time
import re
from collections import Counter
from pocket_coffea.utils.job_progress import (
    aggregate_by_group,
    load_job_to_group_map,
    render_progress_bar,
)
from pocket_coffea.utils.network import get_proxy_path
from pocket_coffea.utils.rucio import get_xrootd_sites_map, get_rucio_client
from pocket_coffea.utils.site_rewrite import (
    find_other_file,
    rewrite_fileset_blocklist,
    rewrite_fileset_to_redirector,
    GLOBAL_XROOTD_REDIRECTOR,
)
from pocket_coffea.utils.htcondor_queue import bump_queue, set_queue

queues = [
    "espresso",
    "microcentury",
    "longlunch",
    "workday",
    "tomorrow",
    "testmatch",
    "nextweek"
]



def get_tables(tot_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs=None, details=False):
    timeout_jobs = timeout_jobs or []
    failed_or_timeout = failed_jobs + [job for job in timeout_jobs if job not in failed_jobs]
    # Summary table
    table1 = Table(title="Job Summary")
    table1.add_column("Total jobs", style="cyan", no_wrap=True)
    table1.add_column("Idle jobs", style="blue", no_wrap=True)
    table1.add_column("Running jobs", style="magenta", no_wrap=True)
    table1.add_column("Done jobs", style="green", no_wrap=True)
    table1.add_column("Failed jobs", style="red", no_wrap=True)
    table1.add_row(str(len(tot_jobs)),
                  str(len(idle_jobs)),
                  str(len(running_jobs)),
                  str(len(done_jobs)),
                  str(len(failed_or_timeout)))
    # Create a table to display the status
    if details:
        table2 = Table(title="Job Status")
        table2.add_column("Job ID", style="cyan", no_wrap=True)
        table2.add_column("Submitted", style="blue", no_wrap=True)
        table2.add_column("Running", style="magenta", no_wrap=True)
        table2.add_column("Done", style="green", no_wrap=True)
        table2.add_column("Failed", style="red", no_wrap=True)
        for job in tot_jobs:
            table2.add_row(job,
                          "X" if job in idle_jobs else "",
                          "X" if job in running_jobs else "",
                          "X" if job in done_jobs else "",
                          "X" if job in failed_or_timeout else "")
    else:
        table2 = None
    return table1, table2


# Layout setup
def create_layout(with_progress=False):
    """Two-column layout. The left column carries the summary table (and
    the per-group progress table when `with_progress` is True) and gets
    twice the width of the log panel on the right, since that's where the
    interesting content lives."""
    layout = Layout()
    layout.split_row(
        Layout(name="left", ratio=2),
        Layout(name="right", ratio=1),
    )
    if with_progress:
        # Fixed-height summary panel so it doesn't grow at the expense of the
        # per-group table; 9 rows covers the Panel border + Table title +
        # header row + data row + a bit of padding. Bumped from 7 to fit
        # everything without cropping the bottom of the table.
        layout["left"].split_column(
            Layout(name="summary", size=9),
            Layout(name="progress"),
        )
    return layout

def check_jobs_logs(jobs_folder):
     # Idle jobs
    idle_jobs = [ a.split("/")[-1][:-5] for a in glob.glob(f"{jobs_folder}/job_*.idle")]
    # Running jobs
    running_jobs = [a.split("/")[-1][:-8] for a in glob.glob(f"{jobs_folder}/job_*.running")]
    # Done jobs
    done_jobs = [ a.split("/")[-1][:-5] for a in glob.glob(f"{jobs_folder}/job_*.done")]
    # Failed jobs
    failed_jobs = [ a.split("/")[-1][:-7] for a in glob.glob(f"{jobs_folder}/job_*.failed")]
    # Timeout jobs are converted to failed jobs by the monitor, after queue bump handling.
    timeout_jobs = [ a.split("/")[-1][:-8] for a in glob.glob(f"{jobs_folder}/job_*.timeout")]
    return idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs


def get_progress_table(group_counts, label, multi_sample_overlap=False, bar_width=30):
    """Build a rich Table showing per-group progress, sorted by % done
    ascending so straggling groups surface at the top. Includes a stacked
    coloured progress bar column (done / running / idle / failed)."""
    title = f"Progress by {label}"
    if multi_sample_overlap:
        title += "  [dim](jobs touching multiple samples are counted under each)[/]"
    table = Table(title=title)
    table.add_column(label.capitalize(), style="cyan", no_wrap=True)
    table.add_column("Total", justify="right")
    table.add_column("Idle", justify="right", style="blue")
    table.add_column("Running", justify="right", style="magenta")
    table.add_column("Done", justify="right", style="green")
    table.add_column("Failed", justify="right", style="red")
    table.add_column("Progress", justify="left", no_wrap=True)
    table.add_column("% Done", justify="right")

    rows = sorted(group_counts.items(), key=lambda kv: (kv[1]["pct_done"], kv[0]))
    for name, counts in rows:
        pct = f"{counts['pct_done']:.1f}%" if counts["total"] else "n/a"
        pct_style = "green" if counts["pct_done"] >= 99.5 else (
            "yellow" if counts["pct_done"] >= 50 else "red"
        )
        table.add_row(
            name,
            str(counts["total"]),
            str(counts["idle"]),
            str(counts["running"]),
            str(counts["done"]),
            str(counts["failed"]),
            render_progress_bar(counts, width=bar_width),
            f"[{pct_style}]{pct}[/]",
        )
    return table

def update_blacklist(xrootdfaillist, blacklist_threshold):
    sitepathlist = [i.split("/store/")[0] for i in xrootdfaillist]
    failedsitecounter = Counter(sitepathlist)
    blacklist_sites = []
    for site,fails in failedsitecounter.items():
        if fails > blacklist_threshold:
            blacklist_sites.append(site)
    return blacklist_sites

def condor_rm_job(job):
    """condor_rm any still-queued/running HTCondor instance of `job` (a
    ``job_<n>`` name) so a recreated job's old instance can't keep running and
    double-write its output.

    The instance is matched on the per-job ``config_job_<n>.pkl`` that appears
    in the job's condor ``Arguments`` (unique per job index, so ``job_1`` is not
    confused with ``job_10``). Returns the ``condor_rm`` output, or ``""`` if
    nothing matched.
    """
    idx = job.split("_")[-1]
    constraint = f'regexp("config_job_{idx}\\.pkl", Arguments)'
    try:
        return os.popen(f"condor_rm -constraint '{constraint}'").read().strip()
    except Exception as e:
        return f"condor_rm failed: {e}"


def recreate_jobs_oneshot(jobs_folder, jobs_to_recreate, *, use_redirector=False,
                          blocklist_sites=None, recreate_queue=None,
                          skip_bad_files=False, queue_shift=1, remove_running=False,
                          dry_run=False):
    """One-shot recreate/resubmit of a chosen set of manual jobs.

    Ported from the manual-job executors' old ``--recreate-jobs`` path so the
    functionality lives in one place. Operates purely on the jobs_dir on-disk
    contract (``jobs_config.yaml`` + ``config_job_i.pkl`` + ``job_i.sub`` +
    the flag files), and — unlike the reactive ``--resubmit`` loop — can act on
    failed **and** running/idle jobs, e.g. to move everything off a blocklisted
    site or onto the global xrootd redirector mid-run.

    `jobs_to_recreate` is ``"auto"`` (scan ``*.failed``/``*.running``/``*.idle``
    flag files) or a comma list (``0,1,3`` or ``job_0,job_3``).

    When `remove_running` is set, each recreated job that is still queued in
    HTCondor (running/idle) is ``condor_rm``'d before resubmission so the old
    instance can't keep running and double-write its output.
    """
    jobs_folder = Path(jobs_folder)
    jobs_config_path = jobs_folder / "jobs_config.yaml"
    if not jobs_config_path.exists():
        rprint(f"[red]No jobs_config.yaml found in {jobs_folder}. Cannot recreate jobs.[/]")
        return
    with open(jobs_config_path) as f:
        jobs_config = yaml.safe_load(f)

    blocklist_sites = set(blocklist_sites or [])
    abs_jobdir = os.path.abspath(jobs_folder)

    # (Re)materialise the inner run-options YAML so a --skip-bad-files override
    # from this recreate call reaches the resubmitted jobs, idempotently
    # patching the wrapper/sub of an existing (possibly pre-feature) jobs_dir.
    ensure_sub_transfers = None
    if skip_bad_files:
        from pocket_coffea.executors.executors_manual_jobs import (
            write_inner_run_options,
            ensure_job_sh_forwards_inner_yaml,
            ensure_sub_transfers_inner_yaml,
            INNER_RUN_OPTIONS_FILENAME,
        )
        ensure_sub_transfers = ensure_sub_transfers_inner_yaml
        write_inner_run_options(str(jobs_folder), {"skip-bad-files": True})
        if ensure_job_sh_forwards_inner_yaml(f"{jobs_folder}/job.sh"):
            rprint(f"[recreate] Patched {jobs_folder}/job.sh to forward "
                   f"{INNER_RUN_OPTIONS_FILENAME} to the inner pocket-coffea run.")

    # Resolve the selector to a job list, and record which selected jobs are
    # currently failed/running (needed for the flag-flip and queue-bump below).
    if jobs_to_recreate == "auto":
        failedjobs = [f[:-len(".failed")] for f in os.listdir(jobs_folder) if f.endswith(".failed")]
        runningjobs = [f[:-len(".running")] for f in os.listdir(jobs_folder) if f.endswith(".running")]
        idlejobs = [f[:-len(".idle")] for f in os.listdir(jobs_folder) if f.endswith(".idle")]
        jobs_to_redo = failedjobs + runningjobs + idlejobs
        if not jobs_to_redo:
            rprint(f"[green]No *.failed/*.running/*.idle jobs found in {jobs_folder}; "
                   f"nothing to recreate.[/]")
            return
    else:
        jobs_to_redo = []
        for j in jobs_to_recreate.split(","):
            j = j.strip()
            if not j:
                continue
            jobs_to_redo.append(j if j.startswith("job_") else f"job_{j}")
        # Derive current flag states from disk (the old executor path left these
        # undefined for explicit lists, crashing on `job in runningjobs`).
        failedjobs = [j for j in jobs_to_redo if (jobs_folder / f"{j}.failed").exists()]
        runningjobs = [j for j in jobs_to_redo if (jobs_folder / f"{j}.running").exists()]
        idlejobs = [j for j in jobs_to_redo if (jobs_folder / f"{j}.idle").exists()]
    rprint(f"Recreating jobs: {jobs_to_redo}")

    # Optionally kill the still-queued (running/idle) HTCondor instance of each
    # recreated job so it can't keep running and double-write its output.
    if remove_running:
        queued = [j for j in jobs_to_redo if j in set(runningjobs) | set(idlejobs)]
        if queued:
            rprint(f"[recreate] Removing still-queued condor instances of: {queued}")
        for j in queued:
            if dry_run:
                rprint(f"[dim]Dry run, not running condor_rm for {j}[/]")
                continue
            out = condor_rm_job(j)
            rprint(f"[recreate] condor_rm {j}: {out or 'no matching queued job'}")

    # Jobs that failed due to an XRootD error get a per-file alternate-site lookup.
    xrootd_err_logs = os.popen(f"grep -il {jobs_folder}/logs/*.err -e 'XRootD error'").read().split()
    xrootd_fail_jobs = ["job_" + f.split("/")[-1].split(".")[-2] for f in xrootd_err_logs]
    if xrootd_err_logs:
        backupdir = f"{jobs_folder}/logs/processed"
        os.makedirs(backupdir, exist_ok=True)
        os.system(f"mv {' '.join(xrootd_err_logs)} {backupdir}")

    sitemap = get_xrootd_sites_map()
    if blocklist_sites:
        rprint(f"Blocklisting sites at recreate time: {sorted(blocklist_sites)}")

    # --use-redirector takes precedence over the per-site blocklist rewrite:
    # a "no Rucio, everything on the global xrootd redirector" one-shot.
    if use_redirector:
        rprint(f"[recreate] --use-redirector: rewriting every file to "
               f"{GLOBAL_XROOTD_REDIRECTOR} without per-site Rucio lookups.")
        if blocklist_sites:
            rprint("[recreate] WARNING: --blocklist-sites is set but --use-redirector "
                   "overrides it (no per-site Rucio resolution happens).")

    rucio_client = None
    if (xrootd_fail_jobs or blocklist_sites) and not use_redirector:
        try:
            rucio_client = get_rucio_client()
        except Exception as e:
            rprint(f"[yellow]WARNING: could not open a rucio client ({e}); "
                   f"replica lookups will fail.[/]")

    if recreate_queue is not None and recreate_queue not in queues:
        rprint(f"[yellow]WARNING: recreate-queue={recreate_queue!r} is not in the known "
               f"HTCondor queue list {queues}; writing your value verbatim.[/]")

    for job in jobs_to_redo:
        if job not in jobs_config["jobs_list"]:
            rprint(f"[yellow]Job {job} not found in jobs_config.yaml; skipping.[/]")
            continue

        # Source the ORIGINAL fileset from jobs_config.yaml (from-scratch) so
        # repeated recreates don't compound rewrites.
        new_fileset = deepcopy(jobs_config["jobs_list"][job]["filesets"])
        modified = False

        if use_redirector:
            new_fileset = rewrite_fileset_to_redirector(new_fileset)
            modified = True
        else:
            if job in xrootd_fail_jobs:
                rprint(f"Replacing input files in {job} since it failed due to an XRootD error.")
                for sample, dct in new_fileset.items():
                    dct['files'] = [
                        find_other_file(fl, sitemap, blocklist=blocklist_sites,
                                        rucio_client=rucio_client)
                        for fl in dct['files']
                    ]
                modified = True
            if blocklist_sites:
                new_fileset = rewrite_fileset_blocklist(new_fileset, sitemap, blocklist_sites,
                                                        rucio_client=rucio_client)
                modified = True

        if modified:
            cfgfile = f"{jobs_folder}/config_{job}.pkl"
            config = cloudpickle.load(open(cfgfile, "rb"))
            config.set_filesets_manually(new_fileset)
            cloudpickle.dump(config, open(cfgfile, "wb"))

        if skip_bad_files and ensure_sub_transfers is not None:
            ensure_sub_transfers(f"{jobs_folder}/{job}.sub", abs_jobdir)

        # Explicit queue override wins over the implicit timeout bump.
        selected_queue = None
        if recreate_queue is not None:
            set_queue(f"{jobs_folder}/{job}.sub", recreate_queue, job)
            selected_queue = recreate_queue
        elif job in runningjobs:
            selected_queue = bump_queue(f"{jobs_folder}/{job}.sub", queue_shift)

        if selected_queue is not None:
            sync_dynamic_queue(jobs_folder, job, selected_queue)

        if dry_run:
            rprint(f"[dim]Dry run, not resubmitting {job}[/]")
            continue
        if job in failedjobs:
            os.system(f"rm {jobs_folder}/{job}.failed")
        elif job in runningjobs:
            os.system(f"rm {jobs_folder}/{job}.running")
        os.system(f"touch {jobs_folder}/{job}.idle")
        os.system(f"cd {jobs_folder} && condor_submit {job}.sub")
        rprint(f"[green]Resubmitted {job}[/]")



def load_job_state(state_file):
    with open(state_file) as f:
        return json.load(f)


def save_job_state(state_file, job_state):
    with open(state_file, "w") as f:
        json.dump(job_state, f, indent=2, sort_keys=True)


def sync_dynamic_queue(jobs_folder, job_name, queue):
    """Keep proactive recreation and dynamic reactive retries on one queue."""
    state_file = Path(jobs_folder) / "job_state.json"
    if not state_file.exists():
        return False
    state = load_job_state(state_file)
    job_num = job_name.split("_", 1)[1]
    if job_num not in state:
        return False
    state[job_num]["queue"] = queue
    save_job_state(state_file, state)
    return True


def setup_proxyfile():
    _x509_localpath = get_proxy_path()
    x509_path = os.environ["HOME"] + f'/{_x509_localpath.split("/")[-1]}'
    if _x509_localpath != x509_path:
        print("Copying proxy file to $HOME.")
        os.system(f"scp {_x509_localpath} {x509_path}")
    os.environ["X509_USER_PROXY"] = x509_path


def scale_memory(memory, factor):
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([A-Za-z]*)\s*", str(memory))
    if not match:
        raise ValueError(f"Cannot scale RequestMemory value: {memory!r}")
    value = float(match.group(1)) * factor
    return f"{value:g}{match.group(2)}"


def bump_jobqueue(job_num, job_state=None, state_file=None, shift=1, ncpu=1):
    """Bump a dynamic job state, or a legacy per-job submit file.

    The one-argument form is retained for check-jobs' historic public helper
    API and delegates to the shared submit-file implementation from PR #541.
    """
    if state_file is None:
        return bump_queue(job_num, shift)
    if job_state is None:
        return bump_queue(Path(state_file).parent / f"job_{job_num}.sub", shift)
    state = job_state[str(job_num)]
    current_queue = state["queue"]
    state["queue"] = queues[min(queues.index(current_queue) + shift, len(queues) - 1)]
    if not state["resources_scaled"]:
        state["request_cpus"] = int(state["base_cpus"]) * ncpu
        state["request_memory"] = scale_memory(state["base_memory"], ncpu)
        state["resources_scaled"] = True
    save_job_state(state_file, job_state)
    return state["queue"]


def should_shift_for_refailure(job_num, job_state, job_name, shifted_jobs):
    if job_state is None:
        return job_name not in shifted_jobs
    state = job_state.get(str(job_num), {})
    return state.get("resubmissions", 0) >= 1 and job_name not in shifted_jobs


def is_xrootd_exhaustion_log(out_text):
    markers = (
        "XRootD failure found at root://xrootd-cms.infn.it",
        "Reached the maximum number of XRootD recovery attempts",
    )
    return any(marker in out_text for marker in markers)


def submit_resubmit_jobs(jobs_folder, job_nums, job_state, state_file, log_text):
    job_nums = list(dict.fromkeys(str(job_num) for job_num in job_nums))
    if job_state is None or not (Path(jobs_folder) / "resubmit.sub").exists():
        succeeded = True
        for job_num in job_nums:
            output = os.popen(
                f"cd {jobs_folder} && condor_submit job_{job_num}.sub", "r"
            ).read()
            log_text.append(output.strip())
            if "job(s) submitted to cluster" not in output:
                succeeded = False
                continue
            os.system(f"rm -f {jobs_folder}/job_{job_num}.failed")
            os.system(f"touch {jobs_folder}/job_{job_num}.idle")
        return succeeded

    job_nums = [job_num for job_num in job_nums if job_num in job_state]
    if not job_nums:
        return False

    with open(f"{jobs_folder}/resubmit.sub") as f:
        template = f.read()
    with open(f"{jobs_folder}/resubmit_now.sub", "w") as f:
        f.write(template)
        f.write("\nqueue PROC, QUEUE, CHUNKSIZE, CPUS, MEMORY from (\n")
        for job_num in job_nums:
            state = job_state[job_num]
            f.write(
                f"  {job_num} {state['queue']} {state['chunksize']} "
                f"{state['request_cpus']} {state['request_memory']}\n"
            )
        f.write(")\n")

    resubmit_log = os.popen(
        f"cd {jobs_folder} && condor_submit resubmit_now.sub", "r"
    ).read()
    resubmit_succeeded = "job(s) submitted to cluster" in resubmit_log
    log_text.append(resubmit_log.strip())
    if resubmit_succeeded:
        for job_num in job_nums:
            os.system(f"rm -f {jobs_folder}/job_{job_num}.failed")
            os.system(f"touch {jobs_folder}/job_{job_num}.idle")
            job_state[job_num]["resubmissions"] += 1
        save_job_state(state_file, job_state)
        log_text.append(f"[red]Resubmitted {len(job_nums)} failed jobs to condor[/]")
    else:
        log_text.append(f"[red]Failed to resubmit {len(job_nums)} failed jobs to condor[/]")
    return resubmit_succeeded


def latest_job_out(jobs_folder, job_name):
    job_num = job_name.split('_')[1]
    candidates = glob.glob(f"{jobs_folder}/logs/job_*.{job_num}.out")
    if not candidates:
        return None
    return sorted(candidates, key=os.path.getmtime)[-1]


def convert_timeout_jobs(jobs_folder, timeout_jobs, running_jobs, idle_jobs, failed_jobs,
                         queue_shift, ncpu, job_state, state_file, log_text,
                         shifted_jobs=None):
    shifted_jobs = shifted_jobs if shifted_jobs is not None else set()
    converted = 0
    for job in list(timeout_jobs):
        if job in running_jobs:
            running_jobs.remove(job)
            os.system(f"rm -f {jobs_folder}/{job}.running")
        if job in idle_jobs:
            idle_jobs.remove(job)
            os.system(f"rm -f {jobs_folder}/{job}.idle")

        os.system(f"rm -f {jobs_folder}/{job}.timeout")
        if job not in failed_jobs:
            failed_jobs.append(job)
        os.system(f"touch {jobs_folder}/{job}.failed")

        job_num = job.split("_", 1)[1]
        next_jf = bump_jobqueue(job_num, job_state, state_file, queue_shift, ncpu)
        shifted_jobs.add(job)
        log_text.append(f"{job} reached the Condor time limit. Marked as failed and bumped to longer condor queue: {next_jf}.")
        converted += 1
    return converted

@click.command()
@click.option("-j", "--jobs-folder", type=str, help="Folder containing the jobs", required=True)
@click.option("-d","--details", is_flag=True, help="Show the details of the jobs")
@click.option("-r","--resubmit", is_flag=True, help="Resubmit the failed jobs")
@click.option("-m","--max-resubmit", type=int, help="Maximum number of resubmission", default=4)
@click.option("-b", "--blacklist-threshold", type=int, default=3,
              help="Retained compatibility threshold for XRootD site failures.")
@click.option("-q","--queue-shift", type=int, help="How many queues to bump to if a job is removed due to time limit? E.g. 1 = bump to next queue, 2 = bump to next-to-next queue", default=1)
@click.option("-n", "--ncpu", type=click.IntRange(min=1), default=1, show_default=True,
              help="CPU and memory multiplier applied once when a job's queue is shifted")
@click.option("--by", "group_by", type=click.Choice(["sample", "dataset", "none"]),
              default="sample",
              help="Show a per-group progress table below the summary. Requires "
                   "jobs_config.yaml in the jobs folder (created by manual-job "
                   "executors). Pass 'none' to disable. Default: sample.")
@click.option("--recreate", type=str, default=None,
              help="One-shot proactive recreate/resubmit: 'auto' or a comma-separated job list.")
@click.option("--once", is_flag=True, default=False,
              help="Run one monitor/resubmit iteration and exit.")
@click.option("--use-redirector", is_flag=True, default=False,
              help="Use the global XRootD redirector for recreation or as retry fallback.")
@click.option("--blocklist-sites", type=str, default=None,
              help="Comma-separated CMS sites or XRootD prefixes to avoid.")
@click.option("--recreate-queue", type=str, default=None,
              help="Force recreated jobs to this HTCondor queue.")
@click.option("--skip-bad-files", is_flag=True, default=False,
              help="Enable Coffea skip-bad-files for recreated jobs.")
@click.option("--remove-running", is_flag=True, default=False,
              help="Remove queued/running Condor instances before recreation.")
def check_jobs(jobs_folder, details, resubmit, max_resubmit, blacklist_threshold,
               queue_shift, ncpu,
               group_by, recreate, once, use_redirector, blocklist_sites,
               recreate_queue, skip_bad_files, remove_running):
    # check if the user passed the parent folder
    subdirs = os.listdir(jobs_folder)
    if len(subdirs) == 1 and subdirs[0] == "job":
        jobs_folder = os.path.join(jobs_folder,"job")

    jobs_folder = Path(jobs_folder)

    explicit_blocklist = {
        site.strip() for site in blocklist_sites.split(",") if site.strip()
    } if blocklist_sites else set()
    if recreate is not None:
        recreate_jobs_oneshot(
            jobs_folder,
            recreate,
            use_redirector=use_redirector,
            blocklist_sites=explicit_blocklist,
            recreate_queue=recreate_queue,
            skip_bad_files=skip_bad_files,
            queue_shift=queue_shift,
            remove_running=remove_running,
        )
        if not resubmit:
            return

    state_file = jobs_folder / "job_state.json"
    job_state = load_job_state(state_file) if state_file.exists() else None
    if job_state is not None:
        tot_jobs = [f"job_{job_num}" for job_num in job_state]
    else:
        tot_jobs = [Path(path).stem for path in glob.glob(f"{jobs_folder}/job_*.sub")]
    # Redo everything every 5 sec
    console = Console()

    failed_jobs_stats = {}
    tot_done = 0

    # Load the per-job sample/dataset map if available and the user opted in.
    group_to_jobs = None
    group_label = None
    multi_sample_overlap = False
    if group_by != "none":
        sample_to_jobs, dataset_to_jobs = load_job_to_group_map(jobs_folder)
        if sample_to_jobs is None:
            rprint(f"[yellow]No jobs_config.yaml in {jobs_folder}; per-group progress "
                   f"table disabled.[/]")
        else:
            if group_by == "sample":
                group_to_jobs = sample_to_jobs
                group_label = "sample"
            else:
                group_to_jobs = dataset_to_jobs
                group_label = "dataset"
            # Detect uniform-split overlap: any job appearing under more than one group.
            all_jobs_listed = [j for jobs in group_to_jobs.values() for j in jobs]
            multi_sample_overlap = len(all_jobs_listed) != len(set(all_jobs_listed))

    if resubmit:
        try:
            setup_proxyfile()
        except Exception as exc:
            rprint(f"[red]{exc}[/]")
            raise SystemExit(1)

    os.makedirs(f"{jobs_folder}/logs/processedlogs", exist_ok=True)
    
    # Main loop
    show_progress = group_to_jobs is not None
    layout = create_layout(with_progress=show_progress)
    log_text = []
    idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs = check_jobs_logs(jobs_folder)
    initially_shifted_jobs = set()
    convert_timeout_jobs(
        jobs_folder, timeout_jobs, running_jobs, idle_jobs, failed_jobs,
        queue_shift, ncpu, job_state, state_file, log_text, initially_shifted_jobs,
    )
    tables = get_tables(tot_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs, details=details)
    if show_progress:
        layout["summary"].update(Panel(tables[0], title="Job Status"))
        gc = aggregate_by_group(group_to_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs)
        layout["progress"].update(Panel(
            get_progress_table(gc, group_label, multi_sample_overlap=multi_sample_overlap)))
    else:
        layout["left"].update(Panel(tables[0], title="Job Status"))
    layout["right"].update(Panel("No logs yet", title="Log"))
    
    definitive_failed = []
    step = 0
    
    with Live(layout, refresh_per_second=1/5, console=console):  # Refresh rate
        try:
            while True:
                step += 1
                idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs = check_jobs_logs(jobs_folder)
                shifted_jobs = initially_shifted_jobs
                initially_shifted_jobs = set()
                convert_timeout_jobs(
                    jobs_folder, timeout_jobs, running_jobs, idle_jobs, failed_jobs,
                    queue_shift, ncpu, job_state, state_file, log_text, shifted_jobs,
                )
                tables = get_tables(tot_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs, details=details)
                # Update the left panel(s) with fresh tables
                if show_progress:
                    layout["summary"].update(Panel(tables[0], title="Job Status"))
                    gc = aggregate_by_group(group_to_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs)
                    layout["progress"].update(Panel(
                        get_progress_table(gc, group_label, multi_sample_overlap=multi_sample_overlap),
                        title=f"Progress by {group_label}"))
                else:
                    layout["left"].update(Panel(tables[0], title="Job Status"))

                resubmit_now = []
                # Checking failed jobs
                if len(failed_jobs) > 0:
                    if len(failed_jobs) > len(definitive_failed) and not resubmit:
                        log_text.append("[red]Failed jobs found. Check the details below. Use --resubmit to resubmit the failed jobs[/]")
                    for failed_job in failed_jobs:
                        if failed_job in failed_jobs_stats:
                            if failed_job not in definitive_failed:
                                failed_jobs_stats[failed_job] += 1
                        else:
                            failed_jobs_stats[failed_job] = 1

                        if not failed_job in definitive_failed:
                            out_text = ""
                            out_file = latest_job_out(jobs_folder, failed_job)
                            if out_file:
                                with open(out_file) as f:
                                    c = f.readlines()
                                out_text = "".join(c)
                                log_text.append( f"[b]Job {failed_job} failed[/] {failed_jobs_stats[failed_job]} times. Last output:")
                                log_text.append("\t"+ "".join(c[-3:]))
                            else:
                                log_text.append( f"Error in job {failed_job}: No .out file found")

                            if resubmit and failed_jobs_stats[failed_job] <= max_resubmit:
                                failed_job_num = failed_job.split("_", 1)[1]
                                xrootd_exhausted = is_xrootd_exhaustion_log(out_text)
                                if xrootd_exhausted:
                                    log_text.append(
                                        f"{failed_job} exhausted XRootD recovery. "
                                        "Resubmitting the original AFS config without queue changes."
                                    )
                                if (not xrootd_exhausted) and should_shift_for_refailure(
                                        failed_job_num, job_state, failed_job, shifted_jobs):
                                    next_jf = bump_jobqueue(
                                        failed_job_num, job_state, state_file,
                                        queue_shift, ncpu,
                                    )
                                    shifted_jobs.add(failed_job)
                                    log_text.append(
                                        f"{failed_job} failed again. Bumped to longer "
                                        f"condor queue: {next_jf}."
                                    )
                                resubmit_now.append(failed_job_num)
                            else:
                                # Add it to the list of jobs that are definitely failed
                                definitive_failed.append(failed_job)

                if resubmit_now:
                    if submit_resubmit_jobs(
                        jobs_folder, resubmit_now, job_state, state_file, log_text
                    ):
                        for job_num in set(resubmit_now):
                            job = f"job_{job_num}"
                            if job in failed_jobs:
                                failed_jobs.remove(job)
                            if job not in idle_jobs:
                                idle_jobs.append(job)
                   
                if len(log_text):
                    if len(log_text) > 20:
                        log_text = log_text[-20:]
                    layout["right"].update(Panel("\n".join(log_text), title="Log"))

                if len(tot_jobs) == len(done_jobs) + len(failed_jobs):
                    rprint("[green]All jobs are completed[/]")
                    rprint(f"Now merge outputs with [yellow]merge-outputs -jc {jobs_folder}[/].")
                    break
                if once:
                    break
                time.sleep(5)
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            rprint(f"[red]check-jobs stopped on an unexpected error:[/] {exc!r}")
            raise

if __name__ == "__main__":
    check_jobs()
