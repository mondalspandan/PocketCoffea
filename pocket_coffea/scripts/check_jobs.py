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
import shutil
import fcntl
import socket
import subprocess
import sys
import tempfile
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
from pocket_coffea.utils.htcondor_queue import QUEUES, bump_queue, next_queue, set_queue

LOCK_FILENAME = ".check_jobs.lock"
JOB_MARKERS = ("idle", "running", "done", "failed", "timeout")
CONDOR_REMOVAL_TIMEOUT = 10.0
RESOURCE_SCALED_MARKER = "# check-jobs-resources-scaled"
ATTEMPT_STATE_FILENAME = "check_jobs_state.json"


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


def scan_condor_log_failures(jobs_folder, log_offsets):
    """Return current event-009 failures without changing the jobs directory."""
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
                # Condor logs have second precision while marker mtimes do not;
                # equality is ambiguous and is conservatively ignored.
                if event_time > int(marker_time) and (
                        job not in recovered or event_time > recovered[job][0]):
                    recovered[job] = (event_time, reason)
            log_offsets[key] = handle.tell()

    findings = {}
    for job, (_, reason) in recovered.items():
        if any((jobs_folder / f"{job}.{terminal}").exists()
               for terminal in ("done", "failed", "timeout")):
            continue
        findings[job] = "timeout" if (
            "SYSTEM_PERIODIC_REMOVE" in reason
            and "wall time exceeded" in reason.lower()
        ) else "failed"
    return findings


def apply_condor_log_failures(jobs_folder, findings):
    """Materialise scan findings as normal markers during active recovery."""
    for job, marker in findings.items():
        if any((Path(jobs_folder) / f"{job}.{terminal}").exists()
               for terminal in ("done", "failed", "timeout")):
            continue
        clear_job_markers(jobs_folder, job)
        (Path(jobs_folder) / f"{job}.{marker}").touch()


def recover_condor_log_failures(jobs_folder, log_offsets):
    """Compatibility wrapper: scan and apply event-009 failures."""
    findings = scan_condor_log_failures(jobs_folder, log_offsets)
    apply_condor_log_failures(jobs_folder, findings)
    return len(findings)


def merge_inferred_status(idle_jobs, running_jobs, done_jobs, failed_jobs,
                          timeout_jobs, findings):
    """Overlay in-memory Condor-log findings for passive display only."""
    idle_jobs = list(idle_jobs)
    running_jobs = list(running_jobs)
    done_jobs = list(done_jobs)
    failed_jobs = list(failed_jobs)
    timeout_jobs = list(timeout_jobs)
    for job, marker in findings.items():
        if job in done_jobs or job in failed_jobs or job in timeout_jobs:
            continue
        if job in idle_jobs:
            idle_jobs.remove(job)
        if job in running_jobs:
            running_jobs.remove(job)
        target = timeout_jobs if marker == "timeout" else failed_jobs
        if job not in target:
            target.append(job)
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

    Ported from the manual-job executors' old recreation path so the
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
    result = {"requested": [], "submitted": [], "skipped": [], "failed": {}}
    jobs_config_path = jobs_folder / "jobs_config.yaml"
    if not jobs_config_path.exists():
        message = f"No jobs_config.yaml found in {jobs_folder}"
        rprint(f"[red]{message}. Cannot recreate jobs.[/]")
        result["failed"]["jobs_config"] = message
        return result
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
            return result
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
    result["requested"] = list(jobs_to_redo)

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
            message = "job is not present in jobs_config.yaml"
            result["failed"][job] = message
            rprint(f"[red]{job}: {message}[/]")
            continue

        active = job in set(runningjobs) | set(idlejobs)
        if active and not remove_running:
            rprint(f"[yellow]Refusing to recreate active {job}; pass --remove-running "
                   "to remove its existing HTCondor job first.[/]")
            result["skipped"].append(job)
            continue

        # Source the ORIGINAL fileset from jobs_config.yaml (from-scratch) so
        # repeated recreates don't compound rewrites. All rewrite errors are
        # per-job failures and occur before any scheduler removal.
        cfg_tmp = None
        sub_tmp = None
        try:
            new_fileset = deepcopy(jobs_config["jobs_list"][job]["filesets"])
            if use_redirector:
                new_fileset = rewrite_fileset_to_redirector(new_fileset)
            else:
                if job in xrootd_fail_jobs:
                    rprint(f"Replacing input files in {job} since it failed due to an XRootD error.")
                    for sample, dct in new_fileset.items():
                        dct['files'] = [
                            find_other_file(fl, sitemap, blocklist=blocklist_sites,
                                            rucio_client=rucio_client)
                            for fl in dct['files']
                        ]
                if blocklist_sites:
                    new_fileset = rewrite_fileset_blocklist(new_fileset, sitemap, blocklist_sites,
                                                            rucio_client=rucio_client)

            # Prepare every replacement artifact in memory/temporary files. In
            # particular, do not touch the durable pickle or submit file while
            # an active scheduler instance could still be using them.
            cfgfile = jobs_folder / f"config_{job}.pkl"
            config = cloudpickle.load(cfgfile.open("rb"))
            config.set_filesets_manually(new_fileset)
            cfg_tmp = Path(tempfile.mkstemp(
                prefix=f".{cfgfile.name}.", suffix=".tmp", dir=jobs_folder)[1])
            with cfg_tmp.open("wb") as handle:
                cloudpickle.dump(config, handle)

            subfile = jobs_folder / f"{job}.sub"
            sub_text = subfile.read_text()
            job_num = job.split("_", 1)[1]
            candidate_state = None
            if job_state is not None and job_num in job_state:
                candidate_state = deepcopy(job_state[job_num])
                if job in timeoutjobs:
                    candidate_state = _candidate_dynamic_state(job_state, job_num,
                                                               0 if recreate_queue is not None else queue_shift, ncpu)
                if recreate_queue is not None:
                    candidate_state["queue"] = recreate_queue
                final_sub_text = _materialize_submit_text(sub_text, candidate_state)
            else:
                final_sub_text = sub_text
                if job in timeoutjobs:
                    final_sub_text, _ = _candidate_legacy_text(
                        final_sub_text, 0 if recreate_queue is not None else queue_shift, ncpu)
                if recreate_queue is not None:
                    final_sub_text = re.sub(r"^.*\+JobFlavour.*$",
                                            f'+JobFlavour="{recreate_queue}"',
                                            final_sub_text, count=1, flags=re.MULTILINE)

            if skip_bad_files and ensure_sub_transfers is not None:
                sub_tmp = Path(tempfile.mkstemp(
                    prefix=f".{subfile.name}.", suffix=".tmp", dir=jobs_folder)[1])
                sub_tmp.write_text(final_sub_text)
                ensure_sub_transfers(str(sub_tmp), abs_jobdir)
                final_sub_text = sub_tmp.read_text()
                sub_tmp.unlink()

            # This is deliberately before condor_rm: a missing/expired proxy
            # must never kill the old active attempt.
            if not dry_run:
                prepare_proxy_for_jobs(jobs_folder)
            sub_tmp = Path(tempfile.mkstemp(
                prefix=f".{subfile.name}.", suffix=".tmp", dir=jobs_folder)[1])
            sub_tmp.write_text(final_sub_text)

            if active and not dry_run:
                removed, output = condor_rm_job(job)
                rprint(f"[recreate] condor_rm {job}: {output or 'no output'}")
                if not removed:
                    raise RuntimeError("condor_rm failed; old job was left in place")
                if not wait_for_condor_job_removal(job):
                    raise RuntimeError("could not confirm Condor removal")

            os.replace(cfg_tmp, cfgfile)
            os.replace(sub_tmp, subfile)
            if dry_run:
                rprint(f"[dim]Dry run, not resubmitting {job}[/]")
                result["skipped"].append(job)
                continue

            submitted, output = condor_submit_job(jobs_folder, subfile.name)
            if not submitted:
                mark_job_failed(jobs_folder, job)
                result["failed"][job] = output or "condor_submit failed"
                rprint(f"[red]Failed to resubmit {job}: {output or 'condor_submit failed'}[/]")
                continue
            if candidate_state is not None:
                job_state[job_num] = candidate_state
                save_job_state(state_file, job_state)
                materialize_job_submit_state(jobs_folder, job_num, job_state)
            _record_attempt(jobs_folder, job_num, job_state, state_file)
            mark_job_idle(jobs_folder, job)
            result["submitted"].append(job)
            rprint(f"[green]Resubmitted {job}: {output or 'submitted'}[/]")
        except Exception as exc:
            result["failed"][job] = str(exc)
            rprint(f"[red]{job}: {exc}[/]")
            for temporary in jobs_folder.glob(f".{job}.*.tmp"):
                temporary.unlink(missing_ok=True)
            if cfg_tmp is not None:
                cfg_tmp.unlink(missing_ok=True)
            if sub_tmp is not None:
                sub_tmp.unlink(missing_ok=True)

    rprint("Recreate summary:")
    rprint(f"  submitted: {len(result['submitted'])}")
    rprint(f"  skipped:   {len(result['skipped'])}")
    rprint(f"  failed:    {len(result['failed'])}")
    for job, message in result["failed"].items():
        rprint(f"{job}: {message}")
    return result



def load_job_state(state_file):
    with open(state_file) as f:
        return json.load(f)


def save_job_state(state_file, job_state):
    with open(state_file, "w") as f:
        json.dump(job_state, f, indent=2, sort_keys=True)


def _submission_proxy_contract(jobs_folder):
    """Read the durable proxy contract, with a conservative legacy fallback."""
    jobs_folder = Path(jobs_folder)
    jobs_config = jobs_folder / "jobs_config.yaml"
    if jobs_config.exists():
        with jobs_config.open() as handle:
            metadata = (yaml.safe_load(handle) or {}).get("submission")
        if metadata is not None:
            return {
                "requires_grid_certificate": bool(metadata.get("requires_grid_certificate", False)),
                "proxy_transfer_path": metadata.get("proxy_transfer_path"),
                "proxy_source": metadata.get("proxy_source", "default"),
            }

    submit_files = sorted(jobs_folder.glob("job_*.sub")) or [jobs_folder / "resubmit.sub"]
    for submit_file in submit_files:
        if not submit_file.exists():
            continue
        for line in submit_file.read_text().splitlines():
            if not line.startswith("transfer_input_files"):
                continue
            values = line.split("=", 1)[1].strip().split(",")
            candidates = []
            for value in values:
                value = value.strip().strip('"')
                base = os.path.basename(value)
                if (re.fullmatch(r"config_job_.*\.pkl", base)
                        or base == "job.sh" or base == "inner_run_options.yaml"):
                    continue
                candidates.append(value)
            likely = [value for value in candidates
                      if re.search(r"(?:x509|proxy)", os.path.basename(value), re.I)]
            if len(likely) == 1:
                candidate = likely[0]
                default_proxy = os.path.basename(candidate).startswith("x509")
                return {
                    "requires_grid_certificate": True,
                    "proxy_transfer_path": candidate,
                    "proxy_source": "default" if default_proxy else "legacy",
                }
            if len(likely) > 1 or len(candidates) > 1:
                raise RuntimeError(
                    "Cannot unambiguously infer the X509 proxy from this legacy submit file."
                )
            if candidates:
                candidate = candidates[0]
                return {
                    "requires_grid_certificate": True,
                    "proxy_transfer_path": candidate,
                    "proxy_source": "legacy",
                }
            return {
                "requires_grid_certificate": False,
                "proxy_transfer_path": None,
                "proxy_source": None,
            }
    return {
        "requires_grid_certificate": False,
        "proxy_transfer_path": None,
        "proxy_source": None,
    }


def _atomic_copy_proxy(source, target):
    source = os.path.abspath(os.path.expandvars(source))
    target = os.path.abspath(os.path.expandvars(target))
    if source == target:
        if not os.path.exists(target):
            raise RuntimeError(f"Required proxy transfer path does not exist: {target}")
        os.chmod(target, 0o600)
        return
    parent = os.path.dirname(target) or "."
    os.makedirs(parent, exist_ok=True)
    temporary = os.path.join(parent, f".{os.path.basename(target)}.{os.getpid()}.tmp")
    try:
        with open(source, "rb") as src, open(temporary, "wb") as dst:
            shutil.copyfileobj(src, dst)
        os.chmod(temporary, 0o600)
        os.replace(temporary, target)
        os.chmod(target, 0o600)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def prepare_proxy_for_jobs(jobs_folder):
    """Prepare exactly the proxy path recorded by the original submission."""
    contract = _submission_proxy_contract(jobs_folder)
    if not contract["requires_grid_certificate"]:
        return None
    proxy_path = contract.get("proxy_transfer_path")
    if not proxy_path:
        raise RuntimeError("The jobs require a grid certificate but no proxy transfer path was recorded")
    proxy_path = os.path.expandvars(proxy_path)
    source = proxy_path
    if contract.get("proxy_source") == "default":
        source = get_proxy_path()
        os.makedirs(os.path.dirname(proxy_path) or ".", exist_ok=True)
        if os.path.abspath(source) != os.path.abspath(proxy_path):
            _atomic_copy_proxy(source, proxy_path)
    if not os.path.exists(proxy_path):
        raise RuntimeError(f"Required proxy transfer path does not exist: {proxy_path}")
    os.chmod(proxy_path, 0o600)
    os.environ["X509_USER_PROXY"] = proxy_path
    return proxy_path


def _attempt_state_path(jobs_folder):
    return Path(jobs_folder) / ATTEMPT_STATE_FILENAME


def _load_attempt_state(jobs_folder):
    path = _attempt_state_path(jobs_folder)
    if not path.exists():
        return {}
    with path.open() as handle:
        return json.load(handle)


def _save_attempt_state(jobs_folder, state):
    path = _attempt_state_path(jobs_folder)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    with temporary.open("w") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
    os.replace(temporary, path)


def _attempts(jobs_folder, job_num, job_state):
    if job_state is not None and str(job_num) in job_state:
        return int(job_state[str(job_num)].get("resubmissions", 0))
    return int(_load_attempt_state(jobs_folder).get(str(job_num), {}).get("resubmissions", 0))


def _record_attempt(jobs_folder, job_num, job_state, state_file):
    if job_state is not None and str(job_num) in job_state:
        job_state[str(job_num)]["resubmissions"] = _attempts(jobs_folder, job_num, job_state) + 1
        save_job_state(state_file, job_state)
        return
    state = _load_attempt_state(jobs_folder)
    state.setdefault(str(job_num), {})["resubmissions"] = _attempts(jobs_folder, job_num, None) + 1
    _save_attempt_state(jobs_folder, state)


def _materialize_submit_text(text, state):
    lines = []
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if "+JobFlavour" in line and state.get("queue") is not None:
            line = f'+JobFlavour="{state["queue"]}"\n'
        elif stripped.startswith("RequestCpus"):
            line = f"RequestCpus = {state['request_cpus']}\n"
        elif stripped.startswith("RequestMemory"):
            line = f"RequestMemory = {state['request_memory']}\n"
        elif stripped.startswith("arguments") and "$" not in line:
            line = re.sub(
                r"(config_job_[^\s]+\.pkl\s+)\S+(\s+)\S+\s*$",
                rf"\g<1>{state['chunksize']}\g<2>{state['request_cpus']}\n",
                line,
            )
        lines.append(line)
    return "".join(lines)


def materialize_job_submit_state(jobs_folder, job_num, job_state):
    """Rewrite a concrete submit file from the authoritative per-job state."""
    state = job_state[str(job_num)]
    sub_file = Path(jobs_folder) / f"job_{job_num}.sub"
    if not sub_file.exists():
        return False
    sub_file.write_text(_materialize_submit_text(sub_file.read_text(), state))
    return True


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
    job_state[job_num]["queue"] = queue
    save_job_state(state_file, job_state)
    materialize_job_submit_state(jobs_folder, job_num, job_state)
    return True


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


def _candidate_dynamic_state(job_state, job_num, queue_shift, ncpu):
    candidate = deepcopy(job_state[str(job_num)])
    candidate["queue"] = next_queue(candidate["queue"], queue_shift)
    if not candidate.get("resources_scaled", False):
        candidate["request_cpus"] = int(candidate["base_cpus"]) * ncpu
        candidate["request_memory"] = scale_memory(candidate["base_memory"], ncpu)
        candidate["resources_scaled"] = True
    return candidate


def _candidate_legacy_text(sub_text, queue_shift, ncpu):
    queue = None
    for line in sub_text.splitlines():
        if "+JobFlavour" in line:
            queue = line.split("=", 1)[1].strip().replace('"', '')
            break
    candidate_queue = next_queue(queue, queue_shift) if queue is not None else None
    text = sub_text
    if candidate_queue is not None:
        text = re.sub(r"^.*\+JobFlavour.*$", f'+JobFlavour="{candidate_queue}"', text, count=1, flags=re.MULTILINE)
    if RESOURCE_SCALED_MARKER not in text:
        cpus_match = re.search(r"^RequestCpus\s*=\s*(\d+)\s*$", text, re.MULTILINE)
        memory_match = re.search(r"^RequestMemory\s*=\s*(\S+)\s*$", text, re.MULTILINE)
        if cpus_match and memory_match:
            cpus = int(cpus_match.group(1)) * ncpu
            memory = scale_memory(memory_match.group(1), ncpu)
            text = re.sub(r"^RequestCpus\s*=.*$", f"RequestCpus = {cpus}", text, count=1, flags=re.MULTILINE)
            text = re.sub(r"^RequestMemory\s*=.*$", f"RequestMemory = {memory}", text, count=1, flags=re.MULTILINE)
            lines = text.splitlines()
            for i, line in enumerate(lines):
                if line.strip().startswith("arguments") and "$(CPUS)" not in line:
                    lines[i] = re.sub(r"\s+\d+\s*$", f" {cpus}", line)
                    break
            text = "\n".join(lines) + ("\n" if sub_text.endswith("\n") else "")
            text += RESOURCE_SCALED_MARKER + "\n"
    return text, candidate_queue


def escalate_timeout_job(jobs_folder, job, job_state, state_file, queue_shift, ncpu):
    """Compatibility helper that commits a timeout escalation immediately."""
    job_num = job.split("_", 1)[1]
    if job_state is not None:
        candidate = _candidate_dynamic_state(job_state, job_num, queue_shift, ncpu)
        job_state[job_num] = candidate
        save_job_state(state_file, job_state)
        materialize_job_submit_state(jobs_folder, job_num, job_state)
        return candidate["queue"]
    text, queue = _candidate_legacy_text(Path(jobs_folder, f"job_{job}.sub").read_text(), queue_shift, ncpu)
    Path(jobs_folder, f"job_{job}.sub").write_text(text)
    return queue


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
    state["queue"] = next_queue(current_queue, shift)
    if not state["resources_scaled"]:
        state["request_cpus"] = int(state["base_cpus"]) * ncpu
        state["request_memory"] = scale_memory(state["base_memory"], ncpu)
        state["resources_scaled"] = True
    save_job_state(state_file, job_state)
    materialize_job_submit_state(Path(state_file).parent, job_num, job_state)
    return state["queue"]


def is_xrootd_exhaustion_log(out_text):
    markers = (
        "XRootD failure found at root://xrootd-cms.infn.it",
        "Reached the maximum number of XRootD recovery attempts",
    )
    return any(marker in out_text for marker in markers)


def submit_resubmit_jobs(jobs_folder, job_nums, job_state, state_file, log_text,
                         pending_candidates=None):
    job_nums = list(dict.fromkeys(str(job_num) for job_num in job_nums))
    pending_candidates = pending_candidates or {}
    succeeded_jobs = []
    if job_state is None or not (Path(jobs_folder) / "resubmit.sub").exists():
        for job_num in job_nums:
            candidate = pending_candidates.get(f"job_{job_num}", {})
            sub_path = Path(jobs_folder) / f"job_{job_num}.sub"
            temporary = None
            try:
                prepare_proxy_for_jobs(jobs_folder)
                candidate_text = candidate.get("sub")
                if candidate_text is None and candidate.get("state") is not None:
                    candidate_text = _materialize_submit_text(
                        sub_path.read_text(), candidate["state"]
                    )
                if candidate_text is not None:
                    temporary = sub_path.with_name(f".resubmit_{job_num}.{os.getpid()}.sub")
                    temporary.write_text(candidate_text)
                    submit_name = temporary.name
                else:
                    submit_name = sub_path.name
            except Exception as exc:
                log_text.append(f"[red]Could not prepare proxy for job_{job_num}: {exc}[/]")
                continue
            submitted, output = condor_submit_job(jobs_folder, submit_name)
            log_text.append(output)
            if submitted:
                if temporary is not None:
                    os.replace(temporary, sub_path)
                if job_state is not None and candidate.get("state") is not None:
                    job_state[job_num] = candidate["state"]
                    save_job_state(state_file, job_state)
                    materialize_job_submit_state(jobs_folder, job_num, job_state)
                mark_job_idle(jobs_folder, f"job_{job_num}")
                _record_attempt(jobs_folder, job_num, job_state, state_file)
                succeeded_jobs.append(job_num)
            else:
                log_text.append(f"[red]Failed to resubmit job_{job_num}; persistent state unchanged.[/]")
            if temporary is not None and temporary.exists():
                temporary.unlink()
        return succeeded_jobs

    job_nums = [job_num for job_num in job_nums if job_num in job_state]
    if not job_nums:
        return False

    with open(f"{jobs_folder}/resubmit.sub") as f:
        template = f.read()
    with open(f"{jobs_folder}/resubmit_now.sub", "w") as f:
        f.write(template)
        f.write("\nqueue PROC, QUEUE, CHUNKSIZE, CPUS, MEMORY from (\n")
        for job_num in job_nums:
            state = pending_candidates.get(f"job_{job_num}", {}).get("state", job_state[job_num])
            f.write(
                f"  {job_num} {state['queue']} {state['chunksize']} "
                f"{state['request_cpus']} {state['request_memory']}\n"
            )
        f.write(")\n")

    try:
        prepare_proxy_for_jobs(jobs_folder)
    except Exception as exc:
        log_text.append(f"[red]Could not prepare proxy for resubmission: {exc}[/]")
        return succeeded_jobs
    resubmit_succeeded, resubmit_log = condor_submit_job(jobs_folder, "resubmit_now.sub")
    log_text.append(resubmit_log)
    if resubmit_succeeded:
        for job_num in job_nums:
            candidate = pending_candidates.get(f"job_{job_num}", {}).get("state")
            if candidate is not None:
                job_state[job_num] = candidate
            mark_job_idle(jobs_folder, f"job_{job_num}")
            job_state[job_num]["resubmissions"] = int(job_state[job_num].get("resubmissions", 0)) + 1
            materialize_job_submit_state(jobs_folder, job_num, job_state)
            succeeded_jobs.append(job_num)
        save_job_state(state_file, job_state)
        log_text.append(f"[red]Resubmitted {len(job_nums)} failed jobs to condor[/]")
    else:
        log_text.append(f"[red]Failed to resubmit {len(job_nums)} failed jobs to condor[/]")
    return succeeded_jobs


def latest_job_out(jobs_folder, job_name):
    job_num = job_name.split('_')[1]
    candidates = glob.glob(f"{jobs_folder}/logs/job_*.{job_num}.out")
    if not candidates:
        return None
    return sorted(candidates, key=os.path.getmtime)[-1]


def convert_timeout_jobs(jobs_folder, timeout_jobs, running_jobs, idle_jobs, failed_jobs,
                         queue_shift, ncpu, job_state, state_file, log_text,
                         shifted_jobs=None, pending_candidates=None):
    shifted_jobs = shifted_jobs if shifted_jobs is not None else set()
    pending_candidates = pending_candidates if pending_candidates is not None else {}
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
        if job_state is not None and job_num in job_state:
            candidate_state = _candidate_dynamic_state(job_state, job_num, queue_shift, ncpu)
            pending_candidates[job] = {"state": candidate_state}
            next_jf = candidate_state["queue"]
        else:
            current = (Path(jobs_folder) / f"{job}.sub").read_text()
            candidate_sub, next_jf = _candidate_legacy_text(current, queue_shift, ncpu)
            pending_candidates[job] = {"sub": candidate_sub}
        shifted_jobs.add(job)
        log_text.append(f"{job} reached the Condor time limit. Marked as failed and bumped to longer condor queue: {next_jf}.")
        converted += 1
    return converted

@click.command()
@click.option("-j", "--jobs-folder", type=str, help="Folder containing the jobs", required=True)
@click.option("-d","--details", is_flag=True, help="Show the details of the jobs")
@click.option("-r","--resubmit", is_flag=True, help="Resubmit the failed jobs")
@click.option("-m","--max-resubmit", type=int, help="Maximum number of resubmission", default=4)
@click.option("-q","--queue-shift", type=click.IntRange(min=0), help="How many queues to bump to if a job is removed due to time limit? E.g. 1 = bump to next queue, 2 = bump to next-to-next queue", default=1)
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
@click.option("--recreate-queue", type=str, default=None,
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
        recreate_result = recreate_jobs_oneshot(
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
        if recreate_result and recreate_result.get("failed"):
            raise click.exceptions.Exit(1)
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
            prepare_proxy_for_jobs(jobs_folder)
        except Exception as exc:
            rprint(f"[red]{exc}[/]")
            raise SystemExit(1)

    # Main loop
    show_progress = group_to_jobs is not None
    layout = create_layout(with_progress=show_progress)
    log_text = []
    condor_log_offsets = {}
    mutation_enabled = recreate is not None or resubmit
    findings = scan_condor_log_failures(jobs_folder, condor_log_offsets)
    idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs = check_jobs_logs(jobs_folder)
    initially_shifted_jobs = set()
    pending_candidates = {}
    if mutation_enabled:
        apply_condor_log_failures(jobs_folder, findings)
        idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs = check_jobs_logs(jobs_folder)
        convert_timeout_jobs(
            jobs_folder, timeout_jobs, running_jobs, idle_jobs, failed_jobs,
            queue_shift, ncpu, job_state, state_file, log_text, initially_shifted_jobs,
            pending_candidates=pending_candidates,
        )
    else:
        idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs = merge_inferred_status(
            idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs, findings
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
                findings = scan_condor_log_failures(jobs_folder, condor_log_offsets)
                idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs = check_jobs_logs(jobs_folder)
                shifted_jobs = initially_shifted_jobs
                initially_shifted_jobs = set()
                if mutation_enabled:
                    apply_condor_log_failures(jobs_folder, findings)
                    idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs = check_jobs_logs(jobs_folder)
                    convert_timeout_jobs(
                        jobs_folder, timeout_jobs, running_jobs, idle_jobs, failed_jobs,
                        queue_shift, ncpu, job_state, state_file, log_text, shifted_jobs,
                        pending_candidates=pending_candidates,
                    )
                else:
                    idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs = merge_inferred_status(
                        idle_jobs, running_jobs, done_jobs, failed_jobs, timeout_jobs, findings
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

                            failed_job_num = failed_job.split("_", 1)[1]
                            attempts = _attempts(jobs_folder, failed_job_num, job_state)
                            if resubmit and attempts < max_resubmit:
                                xrootd_exhausted = is_xrootd_exhaustion_log(out_text)
                                if xrootd_exhausted:
                                    log_text.append(
                                        f"{failed_job} exhausted XRootD recovery. "
                                        "Resubmitting the original AFS config without queue changes."
                                    )
                                elif attempts >= 1 and failed_job not in timeout_jobs and failed_job not in shifted_jobs:
                                    if failed_job not in pending_candidates:
                                        if job_state is not None and failed_job_num in job_state:
                                            pending_candidates[failed_job] = {
                                                "state": _candidate_dynamic_state(
                                                    job_state, failed_job_num, queue_shift, ncpu
                                                )
                                            }
                                            log_text.append(
                                                f"{failed_job} failed again; preparing an escalation to "
                                                f"{pending_candidates[failed_job]['state']['queue']}."
                                            )
                                        else:
                                            current_sub = (jobs_folder / f"job_{failed_job_num}.sub").read_text()
                                            pending_candidates[failed_job] = {
                                                "sub": _candidate_legacy_text(current_sub, queue_shift, ncpu)[0]
                                            }
                                resubmit_now.append(failed_job_num)
                            else:
                                # Add it to the list of jobs that are definitely failed
                                definitive_failed.append(failed_job)

                if resubmit_now:
                    successful = submit_resubmit_jobs(
                        jobs_folder, resubmit_now, job_state, state_file, log_text,
                        pending_candidates=pending_candidates,
                    )
                    for job_num in set(successful):
                            job = f"job_{job_num}"
                            if job in failed_jobs:
                                failed_jobs.remove(job)
                            if job not in idle_jobs:
                                idle_jobs.append(job)
                            pending_candidates.pop(job, None)
                   
                if len(log_text):
                    if len(log_text) > 20:
                        log_text = log_text[-20:]
                    layout["right"].update(Panel("\n".join(log_text), title="Log"))

                terminal_failed = len(definitive_failed) if resubmit else len(failed_jobs)
                if len(tot_jobs) == len(done_jobs) + terminal_failed:
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
