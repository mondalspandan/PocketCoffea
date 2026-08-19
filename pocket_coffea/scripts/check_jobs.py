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
import fcntl
import socket
import subprocess
import sys
import uuid
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
from functools import wraps
from pocket_coffea.utils.job_progress import (
    aggregate_by_group,
    load_job_to_group_map,
    render_progress_bar,
)
from pocket_coffea.utils.network import get_proxy_path
from pocket_coffea.utils.rucio import get_xrootd_sites_map, get_rucio_client
from pocket_coffea.utils.site_rewrite import (
    extract_failed_url,
    find_other_file,
    normalize_rse,
    rewrite_fileset_blocklist,
    rewrite_fileset_to_redirector,
    GLOBAL_XROOTD_REDIRECTOR,
)
from pocket_coffea.utils.htcondor_queue import QUEUES, bump_queue, set_queue

LOCK_FILENAME = ".check_jobs.lock"
JOB_MARKERS = ("idle", "running", "done", "failed", "timeout")
CONDOR_REMOVAL_TIMEOUT = 10.0
RESOURCE_SCALED_MARKER = "# check-jobs-resources-scaled"


def _resolve_jobs_folder(jobs_folder):
    jobs_folder = Path(jobs_folder)
    if len(os.listdir(jobs_folder)) == 1 and (jobs_folder / "job").is_dir():
        jobs_folder = jobs_folder / "job"
    return jobs_folder


def _new_lock_info():
    return {
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "start_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "command_line": " ".join(shlex_quote(arg) for arg in sys.argv),
        "session_id": uuid.uuid4().hex,
    }


def shlex_quote(value):
    """Quote one argv item without requiring Python 3.8's shlex.join."""
    import shlex
    return shlex.quote(value)


def _write_lock(path, info, exclusive):
    flags = os.O_WRONLY | os.O_CREAT
    if exclusive:
        flags |= os.O_EXCL
    fd = os.open(path, flags, 0o644)
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(info, handle, sort_keys=True)
            handle.write("\n")
    except Exception:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise


def acquire_check_jobs_lock(jobs_folder):
    """Atomically create or explicitly override the per-jobs-dir lock."""
    path = Path(jobs_folder) / LOCK_FILENAME
    info = _new_lock_info()
    try:
        _write_lock(path, info, exclusive=True)
        return info
    except FileExistsError:
        try:
            fd = os.open(path, os.O_RDWR)
        except FileNotFoundError:
            return acquire_check_jobs_lock(jobs_folder)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            with os.fdopen(os.dup(fd)) as handle:
                try:
                    existing = json.load(handle)
                except ValueError:
                    existing = {}
            rprint(
                f"[yellow]check-jobs is already running on "
                f"{existing.get('hostname', 'unknown host')} "
                f"(PID {existing.get('pid', 'unknown')}).[/]"
            )
            rprint("Use that existing session or stop it first.")
            if not click.confirm("Proceed anyway despite the risk?", default=False):
                return None

            temporary = path.with_name(f"{path.name}.{info['session_id']}.tmp")
            try:
                _write_lock(temporary, info, exclusive=True)
                os.replace(temporary, path)
            finally:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
            return info
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


def release_check_jobs_lock(jobs_folder, session_id):
    path = Path(jobs_folder) / LOCK_FILENAME
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return False
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            if os.fstat(fd).st_ino != os.stat(path).st_ino:
                return False
            with os.fdopen(os.dup(fd)) as handle:
                try:
                    info = json.load(handle)
                except ValueError:
                    return False
            if info.get("session_id") != session_id:
                return False
            path.unlink()
            return True
        except FileNotFoundError:
            return False
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _with_check_jobs_lock(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        jobs_folder = _resolve_jobs_folder(kwargs["jobs_folder"])
        lock = acquire_check_jobs_lock(jobs_folder)
        if lock is None:
            return None
        kwargs["jobs_folder"] = jobs_folder
        try:
            return function(*args, **kwargs)
        finally:
            release_check_jobs_lock(jobs_folder, lock["session_id"])
    return wrapped



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


_CONDOR_ABORT_RE = re.compile(
    r"^\s*009 \((\d+)\.(\d+)\.\d+\)\s+(\d\d/\d\d \d\d:\d\d:\d\d)"
)


def recover_condor_log_failures(jobs_folder, log_offsets):
    """Recover current event-009 removals into the normal job markers."""
    jobs_folder = Path(jobs_folder)
    active = {}
    for marker in ("idle", "running"):
        for marker_path in jobs_folder.glob(f"job_*.{marker}"):
            job = marker_path.name.rsplit(".", 1)[0]
            if any((jobs_folder / f"{job}.{terminal}").exists()
                   for terminal in ("done", "failed", "timeout")):
                continue
            active[job] = max(active.get(job, 0), marker_path.stat().st_mtime)

    recovered = {}
    for log_path in (jobs_folder / "logs").glob("job_*.log"):
        key = str(log_path)
        offset = log_offsets.get(key, 0)
        if offset > log_path.stat().st_size:
            offset = 0
        with log_path.open(errors="replace") as handle:
            handle.seek(offset)
            while True:
                line_start = handle.tell()
                line = handle.readline()
                if not line:
                    break
                match = _CONDOR_ABORT_RE.match(line)
                if not match:
                    continue
                reason = handle.readline()
                if not reason:
                    handle.seek(line_start)
                    break

                stem_parts = log_path.stem.split(".", 1)
                job = (f"job_{stem_parts[1]}" if len(stem_parts) == 2
                       else f"job_{match.group(2)}")
                if job not in active:
                    continue
                marker_time = active[job]
                marker_year = time.localtime(marker_time).tm_year
                event_base = time.mktime(time.strptime(
                    f"{marker_year}/{match.group(3)}", "%Y/%m/%d %H:%M:%S"
                ))
                event_time = min(
                    (event_base - 365 * 24 * 3600, event_base,
                     event_base + 365 * 24 * 3600),
                    key=lambda candidate: abs(candidate - marker_time),
                )
                if event_time >= marker_time and (
                        job not in recovered or event_time > recovered[job][0]):
                    recovered[job] = (event_time, reason)
            log_offsets[key] = handle.tell()

    for job, (_, reason) in recovered.items():
        if any((jobs_folder / f"{job}.{terminal}").exists()
               for terminal in ("done", "failed", "timeout")):
            continue
        clear_job_markers(jobs_folder, job)
        marker = "timeout" if (
            "SYSTEM_PERIODIC_REMOVE" in reason
            and "wall time exceeded" in reason.lower()
        ) else "failed"
        (jobs_folder / f"{job}.{marker}").touch()
    return len(recovered)


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

def condor_rm_job(job):
    """condor_rm any still-queued/running HTCondor instance of `job` (a
    ``job_<n>`` name) so a recreated job's old instance can't keep running and
    double-write its output.

    The instance is matched on the per-job ``config_job_<n>.pkl`` that appears
    in the job's condor ``Args`` ClassAd (unique per job index, so ``job_1`` is not
    confused with ``job_10``). Returns ``(success, output)`` from ``condor_rm``.
    """
    constraint = condor_job_constraint(job)
    try:
        result = subprocess.run(
            ["condor_rm", "-constraint", constraint],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
    except OSError as exc:
        return False, f"condor_rm failed: {exc}"
    return result.returncode == 0, result.stdout.strip()


def condor_job_constraint(job):
    """Return the lxplus ClassAd constraint for one config pickle."""
    idx = job.split("_")[-1]
    return f'regexp("config_job_{idx}\\.pkl", Args)'


def wait_for_condor_job_removal(job, timeout=CONDOR_REMOVAL_TIMEOUT, poll_interval=0.2):
    """Wait until no lxplus job matching ``job`` remains in the queue."""
    constraint = condor_job_constraint(job)
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = subprocess.run(
                ["condor_q", "-constraint", constraint, "-af", "ClusterId"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=False,
            )
        except OSError:
            result = None
        if result is not None and result.returncode == 0 and not result.stdout.strip():
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(poll_interval)


def condor_submit_job(jobs_folder, submit_file):
    try:
        result = subprocess.run(
            ["condor_submit", submit_file],
            cwd=str(jobs_folder),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
    except OSError as exc:
        return False, f"condor_submit failed: {exc}"
    return result.returncode == 0, result.stdout.strip()


def clear_job_markers(jobs_folder, job):
    for marker in JOB_MARKERS:
        try:
            (Path(jobs_folder) / f"{job}.{marker}").unlink()
        except FileNotFoundError:
            pass


def mark_job_idle(jobs_folder, job):
    clear_job_markers(jobs_folder, job)
    (Path(jobs_folder) / f"{job}.idle").touch()


def mark_job_failed(jobs_folder, job):
    clear_job_markers(jobs_folder, job)
    (Path(jobs_folder) / f"{job}.failed").touch()


def recreate_jobs_oneshot(jobs_folder, jobs_to_recreate, *, use_redirector=False,
                          blocklist_sites=None, recreate_queue=None,
                          skip_bad_files=False, queue_shift=1, ncpu=1,
                          remove_running=False,
                          dry_run=False):
    """One-shot recreate/resubmit of a chosen set of manual jobs.

    Ported from the manual-job executors' old ``--recreate-jobs`` path so the
    functionality lives in one place. Operates purely on the jobs_dir on-disk
    contract (``jobs_config.yaml`` + ``config_job_i.pkl`` + ``job_i.sub`` +
    the flag files), and — unlike the reactive ``--resubmit`` loop — can act on
    failed **and** running/idle jobs, e.g. to move everything off a blocklisted
    site or onto the global xrootd redirector mid-run.

    `jobs_to_recreate` is ``"auto"`` (scan ``*.failed``/``*.running``/``*.idle``/``*.timeout``
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
    state_file = jobs_folder / "job_state.json"
    job_state = load_job_state(state_file) if state_file.exists() else None

    blocklist_sites = {normalize_rse(site) for site in (blocklist_sites or [])}
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
        inner_options_path = Path(jobs_folder) / INNER_RUN_OPTIONS_FILENAME
        existing_options = {}
        if inner_options_path.exists():
            with open(inner_options_path) as handle:
                existing_options = yaml.safe_load(handle) or {}
        existing_options["skip-bad-files"] = True
        write_inner_run_options(str(jobs_folder), existing_options)
        if ensure_job_sh_forwards_inner_yaml(f"{jobs_folder}/job.sh"):
            rprint(f"[recreate] Patched {jobs_folder}/job.sh to forward "
                   f"{INNER_RUN_OPTIONS_FILENAME} to the inner pocket-coffea run.")

    # Resolve the selector to a job list, and record which selected jobs are
    # currently failed/running (needed for the flag-flip and queue-bump below).
    if jobs_to_recreate == "auto":
        failedjobs = [f[:-len(".failed")] for f in os.listdir(jobs_folder) if f.endswith(".failed")]
        runningjobs = [f[:-len(".running")] for f in os.listdir(jobs_folder) if f.endswith(".running")]
        idlejobs = [f[:-len(".idle")] for f in os.listdir(jobs_folder) if f.endswith(".idle")]
        timeoutjobs = [f[:-len(".timeout")] for f in os.listdir(jobs_folder) if f.endswith(".timeout")]
        jobs_to_redo = list(dict.fromkeys(failedjobs + runningjobs + idlejobs + timeoutjobs))
        if not jobs_to_redo:
            rprint(f"[green]No *.failed/*.running/*.idle/*.timeout jobs found in {jobs_folder}; "
                   f"nothing to recreate.[/]")
            return
    else:
        jobs_to_redo = []
        for j in jobs_to_recreate.split(","):
            j = j.strip()
            if not j:
                continue
            jobs_to_redo.append(j if j.startswith("job_") else f"job_{j}")
        jobs_to_redo = list(dict.fromkeys(jobs_to_redo))
        # Derive current flag states from disk (the old executor path left these
        # undefined for explicit lists, crashing on `job in runningjobs`).
        failedjobs = [j for j in jobs_to_redo if (jobs_folder / f"{j}.failed").exists()]
        runningjobs = [j for j in jobs_to_redo if (jobs_folder / f"{j}.running").exists()]
        idlejobs = [j for j in jobs_to_redo if (jobs_folder / f"{j}.idle").exists()]
        timeoutjobs = [j for j in jobs_to_redo if (jobs_folder / f"{j}.timeout").exists()]
    rprint(f"Recreating jobs: {jobs_to_redo}")

    # Jobs that failed due to an XRootD error get a per-file alternate-site lookup.
    xrootd_fail_jobs = []
    for job in jobs_to_redo:
        out_file = latest_job_out(jobs_folder, job)
        if out_file and extract_failed_url(Path(out_file).read_text(errors="replace")):
            xrootd_fail_jobs.append(job)

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

    if recreate_queue is not None and recreate_queue not in QUEUES:
        rprint(f"[yellow]WARNING: recreate-queue={recreate_queue!r} is not in the known "
               f"HTCondor queue list {QUEUES}; writing your value verbatim.[/]")

    for job in jobs_to_redo:
        if job not in jobs_config["jobs_list"]:
            rprint(f"[yellow]Job {job} not found in jobs_config.yaml; skipping.[/]")
            continue

        active = job in set(runningjobs) | set(idlejobs)
        if active and not remove_running:
            rprint(f"[yellow]Refusing to recreate active {job}; pass --remove-running "
                   "to remove its existing HTCondor job first.[/]")
            continue
        if active and not dry_run:
            removed, output = condor_rm_job(job)
            rprint(f"[recreate] condor_rm {job}: {output or 'no output'}")
            if not removed:
                rprint(f"[red]Could not remove {job}; leaving markers and skipping resubmission.[/]")
                continue
            if not wait_for_condor_job_removal(job):
                rprint(f"[red]Could not confirm removal of {job}; skipping resubmission.[/]")
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

        # Explicit queue override wins over the implicit timeout queue bump,
        # but timeout resource scaling still applies.
        selected_queue = None
        if job in timeoutjobs:
            selected_queue = escalate_timeout_job(
                jobs_folder, job, job_state, state_file,
                0 if recreate_queue is not None else queue_shift, ncpu,
            )
        if recreate_queue is not None:
            set_queue(f"{jobs_folder}/{job}.sub", recreate_queue, job)
            selected_queue = recreate_queue

        if selected_queue is not None:
            sync_dynamic_queue(jobs_folder, job, selected_queue, job_state, state_file)

        if dry_run:
            rprint(f"[dim]Dry run, not resubmitting {job}[/]")
            continue
        submitted, output = condor_submit_job(jobs_folder, f"{job}.sub")
        if submitted:
            mark_job_idle(jobs_folder, job)
            rprint(f"[green]Resubmitted {job}: {output or 'submitted'}[/]")
        else:
            mark_job_failed(jobs_folder, job)
            rprint(f"[red]Failed to resubmit {job}: {output or 'condor_submit failed'}[/]")



def load_job_state(state_file):
    with open(state_file) as f:
        return json.load(f)


def save_job_state(state_file, job_state):
    with open(state_file, "w") as f:
        json.dump(job_state, f, indent=2, sort_keys=True)


def sync_dynamic_queue(jobs_folder, job_name, queue, job_state=None, state_file=None):
    """Keep proactive recreation and dynamic reactive retries on one queue."""
    state_file = Path(state_file) if state_file is not None else Path(jobs_folder) / "job_state.json"
    if job_state is None:
        if not state_file.exists():
            return False
        job_state = load_job_state(state_file)
    job_num = job_name.split("_", 1)[1]
    if job_num not in job_state:
        return False
    sub_file = Path(jobs_folder) / f"{job_name}.sub"
    if sub_file.exists():
        set_queue(sub_file, queue, job_name)
    job_state[job_num]["queue"] = queue
    save_job_state(state_file, job_state)
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


def update_job_submit_resources(sub_file, request_cpus, request_memory):
    with open(sub_file) as handle:
        lines = handle.readlines()
    with open(sub_file, "w") as handle:
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("RequestCpus"):
                line = f"RequestCpus = {request_cpus}\n"
            elif stripped.startswith("RequestMemory"):
                line = f"RequestMemory = {request_memory}\n"
            elif stripped.startswith("arguments") and "$(CPUS)" not in line:
                line = re.sub(r"\s+\d+\s*$", f" {request_cpus}\n", line)
            handle.write(line)


def scale_submit_resources(sub_file, factor):
    with open(sub_file) as handle:
        content = handle.read()
    if RESOURCE_SCALED_MARKER in content:
        return False
    cpus_match = re.search(r"^RequestCpus\s*=\s*(\d+)\s*$", content, re.MULTILINE)
    memory_match = re.search(r"^RequestMemory\s*=\s*(\S+)\s*$", content, re.MULTILINE)
    if not cpus_match or not memory_match:
        return False
    update_job_submit_resources(
        sub_file,
        int(cpus_match.group(1)) * factor,
        scale_memory(memory_match.group(1), factor),
    )
    with open(sub_file, "a") as handle:
        handle.write(f"{RESOURCE_SCALED_MARKER}\n")
    return True


def escalate_timeout_job(jobs_folder, job, job_state, state_file, queue_shift, ncpu):
    job_num = job.split("_", 1)[1]
    selected_queue = bump_jobqueue(job_num, job_state, state_file, queue_shift, ncpu)
    if job_state is not None and job_num in job_state:
        update_job_submit_resources(
            f"{jobs_folder}/{job}.sub",
            job_state[job_num]["request_cpus"],
            job_state[job_num]["request_memory"],
        )
    elif selected_queue is not None:
        scale_submit_resources(f"{jobs_folder}/{job}.sub", ncpu)
    return selected_queue


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
    state["queue"] = QUEUES[min(QUEUES.index(current_queue) + shift, len(QUEUES) - 1)]
    if not state["resources_scaled"]:
        state["request_cpus"] = int(state["base_cpus"]) * ncpu
        state["request_memory"] = scale_memory(state["base_memory"], ncpu)
        state["resources_scaled"] = True
    sub_file = Path(state_file).parent / f"job_{job_num}.sub"
    if sub_file.exists():
        set_queue(sub_file, state["queue"], f"job_{job_num}")
    save_job_state(state_file, job_state)
    return state["queue"]


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
            submitted, output = condor_submit_job(jobs_folder, f"job_{job_num}.sub")
            log_text.append(output)
            if not submitted:
                mark_job_failed(jobs_folder, f"job_{job_num}")
                succeeded = False
                continue
            mark_job_idle(jobs_folder, f"job_{job_num}")
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

    resubmit_succeeded, resubmit_log = condor_submit_job(jobs_folder, "resubmit_now.sub")
    log_text.append(resubmit_log)
    if resubmit_succeeded:
        for job_num in job_nums:
            mark_job_idle(jobs_folder, f"job_{job_num}")
            job_state[job_num]["resubmissions"] += 1
        save_job_state(state_file, job_state)
        log_text.append(f"[red]Resubmitted {len(job_nums)} failed jobs to condor[/]")
    else:
        log_text.append(f"[red]Failed to resubmit {len(job_nums)} failed jobs to condor[/]")
        for job_num in job_nums:
            mark_job_failed(jobs_folder, f"job_{job_num}")
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
        if job in idle_jobs:
            idle_jobs.remove(job)
        clear_job_markers(jobs_folder, job)
        if job not in failed_jobs:
            failed_jobs.append(job)
        (Path(jobs_folder) / f"{job}.failed").touch()

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
              help="With --recreate, rewrite files through the global XRootD redirector.")
@click.option("--blocklist-sites", type=str, default=None,
              help="With --recreate, comma-separated CMS/Rucio site names to avoid.")
@click.option("--recreate-queue", type=click.Choice(QUEUES), default=None,
              help="With --recreate, force jobs to this HTCondor queue.")
@click.option("--skip-bad-files", is_flag=True, default=False,
              help="With --recreate, enable Coffea skip-bad-files.")
@click.option("--remove-running", is_flag=True, default=False,
              help="With --recreate, remove queued/running Condor instances first.")
@_with_check_jobs_lock
def check_jobs(jobs_folder, details, resubmit, max_resubmit,
               queue_shift, ncpu,
               group_by, recreate, once, use_redirector, blocklist_sites,
               recreate_queue, skip_bad_files, remove_running):
    jobs_folder = _resolve_jobs_folder(jobs_folder)

    recreate_only = []
    if use_redirector:
        recreate_only.append("--use-redirector")
    if blocklist_sites:
        recreate_only.append("--blocklist-sites")
    if recreate_queue is not None:
        recreate_only.append("--recreate-queue")
    if skip_bad_files:
        recreate_only.append("--skip-bad-files")
    if remove_running:
        recreate_only.append("--remove-running")
    if recreate is None and recreate_only:
        raise click.UsageError(
            ", ".join(recreate_only) + " requires --recreate"
        )

    explicit_blocklist = {
        site.strip() for site in blocklist_sites.split(",") if site.strip()
    } if blocklist_sites else set()
    invalid_blocklist = [site for site in explicit_blocklist if site.startswith("root://")]
    if invalid_blocklist:
        raise click.BadParameter(
            "--blocklist-sites accepts CMS/Rucio site names, not XRootD prefixes: "
            + ", ".join(sorted(invalid_blocklist))
        )
    if recreate is not None:
        recreate_jobs_oneshot(
            jobs_folder,
            recreate,
            use_redirector=use_redirector,
            blocklist_sites=explicit_blocklist,
            recreate_queue=recreate_queue,
            skip_bad_files=skip_bad_files,
            queue_shift=queue_shift,
            ncpu=ncpu,
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
    condor_log_offsets = {}
    recover_condor_log_failures(jobs_folder, condor_log_offsets)
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
                recover_condor_log_failures(jobs_folder, condor_log_offsets)
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
                                if "Corrupt input data" in out_text:
                                    log_text.append(
                                        f"[yellow]{failed_job} reported corrupt input data in its .out log.[/]"
                                    )
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
