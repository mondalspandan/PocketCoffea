'''Monitor current-format PocketCoffea manual jobs.'''

import fcntl
import glob
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from copy import deepcopy
from functools import wraps
from pathlib import Path

import click
import cloudpickle
import yaml
from rich import print as rprint
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table

from pocket_coffea.executors.executors_manual_jobs import render_condor_submit
from pocket_coffea.utils.htcondor_queue import next_queue
from pocket_coffea.utils.job_progress import aggregate_by_group, load_job_to_group_map, render_progress_bar
from pocket_coffea.utils.network import get_proxy_path
from pocket_coffea.utils.rucio import get_xrootd_sites_map, get_rucio_client
from pocket_coffea.utils.site_rewrite import (
    GLOBAL_XROOTD_REDIRECTOR, extract_failed_url, find_other_file, normalize_rse,
    rewrite_fileset_blocklist, rewrite_fileset_to_redirector,
)

LOCK_FILENAME = ".check_jobs.lock"
JOB_MARKERS = ("idle", "running", "done", "failed", "timeout")
CONDOR_REMOVAL_TIMEOUT = 10.0
CONTRACT_ERROR = (
    "This jobs directory predates the consolidated check-jobs format.\n"
    "Please resubmit it with the current PocketCoffea version."
)


def _resolve_jobs_folder(jobs_folder):
    folder = Path(jobs_folder)
    if len(os.listdir(folder)) == 1 and (folder / "job").is_dir():
        return folder / "job"
    return folder


def shlex_quote(value):
    import shlex
    return shlex.quote(value)


def _new_lock_info():
    return {
        "hostname": socket.gethostname(), "pid": os.getpid(),
        "start_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "command_line": " ".join(shlex_quote(arg) for arg in sys.argv),
        "session_id": uuid.uuid4().hex,
    }


def acquire_check_jobs_lock(jobs_folder):
    path = Path(jobs_folder) / LOCK_FILENAME
    info = _new_lock_info()
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        with path.open("r+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            existing = json.load(handle)
            rprint(f"[yellow]check-jobs is already running on {existing.get('hostname', 'unknown host')} "
                   f"(PID {existing.get('pid', 'unknown')}).[/]")
            if not click.confirm("Proceed anyway despite the risk?", default=False):
                return None
            handle.seek(0)
            handle.truncate()
            json.dump(info, handle)
            handle.write("\n")
            handle.flush()
            return info
    with os.fdopen(fd, "w") as handle:
        json.dump(info, handle)
        handle.write("\n")
    return info


def release_check_jobs_lock(jobs_folder, session_id):
    path = Path(jobs_folder) / LOCK_FILENAME
    try:
        with path.open("r+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            if json.load(handle).get("session_id") != session_id:
                return False
            path.unlink()
            return True
    except FileNotFoundError:
        return False


def load_current_contract(jobs_folder):
    folder = _resolve_jobs_folder(jobs_folder)
    try:
        metadata = yaml.safe_load((folder / "jobs_config.yaml").read_text()) or {}
        submission = metadata["submission"]
        if submission["format_version"] != 1:
            raise ValueError
        if submission["executor"] not in ("condor@lxplus", "condor@rubin"):
            raise ValueError
        for key in ("requires_grid_certificate", "proxy_transfer_path",
                    "proxy_source", "supports_queue_escalation"):
            if key not in submission:
                raise ValueError
        state_file = folder / "job_state.json"
        state = json.loads(state_file.read_text())
        if not isinstance(state, dict) or not state:
            raise ValueError
        for name in ("job.sh", "inner_run_options.yaml", "resubmit.sub"):
            if not (folder / name).is_file():
                raise ValueError
        for job, values in state.items():
            if not all((folder / f"config_job_{job}.pkl").is_file(),
                       (folder / f"job_{job}.sub").is_file()):
                raise ValueError
            if not all(key in values for key in
                       ("chunksize", "request_cpus", "request_memory", "resubmissions")):
                raise ValueError
            if submission["executor"] == "condor@lxplus" and not all(
                    key in values for key in
                    ("queue", "base_cpus", "base_memory", "resources_scaled")):
                raise ValueError
        return folder, metadata, submission, state, state_file
    except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        raise click.UsageError(CONTRACT_ERROR)


def _with_check_jobs_lock(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        folder = _resolve_jobs_folder(kwargs["jobs_folder"])
        load_current_contract(folder)
        if not (kwargs.get("resubmit") or kwargs.get("recreate") is not None):
            kwargs["jobs_folder"] = folder
            return function(*args, **kwargs)
        lock = acquire_check_jobs_lock(folder)
        if lock is None:
            return None
        kwargs["jobs_folder"] = folder
        try:
            return function(*args, **kwargs)
        finally:
            release_check_jobs_lock(folder, lock["session_id"])
    return wrapped


def check_jobs_logs(jobs_folder):
    folder = Path(jobs_folder)
    return tuple([p.stem for p in folder.glob(f"job_*.{marker}")]
                  for marker in JOB_MARKERS)


_CONDOR_ABORT_RE = re.compile(r"^\s*009 \((\d+)\.(\d+)\.\d+\)\s+(\d\d/\d\d \d\d:\d\d:\d\d)")


def scan_condor_log_failures(jobs_folder, log_offsets):
    folder = Path(jobs_folder)
    active = {}
    for marker in ("idle", "running"):
        for path in folder.glob(f"job_*.{marker}"):
            job = path.stem
            if any((folder / f"{job}.{term}").exists() for term in ("done", "failed", "timeout")):
                continue
            active[job] = path.stat().st_mtime
    recovered = {}
    for path in (folder / "logs").glob("job_*.log"):
        key = str(path)
        offset = log_offsets.get(key, 0)
        if offset > path.stat().st_size:
            offset = 0
        with path.open(errors="replace") as handle:
            handle.seek(offset)
            while True:
                start = handle.tell()
                line = handle.readline()
                if not line:
                    break
                match = _CONDOR_ABORT_RE.match(line)
                if not match:
                    continue
                reason = handle.readline()
                if not reason:
                    handle.seek(start)
                    break
                parts = path.stem.split(".", 1)
                job = f"job_{parts[1]}" if len(parts) == 2 else f"job_{match.group(2)}"
                if job not in active:
                    continue
                year = time.localtime(active[job]).tm_year
                event = time.mktime(time.strptime(
                    f"{year}/{match.group(3)}", "%Y/%m/%d %H:%M:%S"))
                if event > active[job] and (job not in recovered or event > recovered[job][0]):
                    recovered[job] = (event, reason)
            log_offsets[key] = handle.tell()
    return {
        job: ("timeout" if "SYSTEM_PERIODIC_REMOVE" in reason and
              "wall time exceeded" in reason.lower() else "failed")
        for job, (_, reason) in recovered.items()
        if not any((folder / f"{job}.{term}").exists()
                   for term in ("done", "failed", "timeout"))
    }


def clear_job_markers(jobs_folder, job):
    folder = Path(jobs_folder)
    for marker in JOB_MARKERS:
        (folder / f"{job}.{marker}").unlink(missing_ok=True)


def apply_condor_log_failures(jobs_folder, findings):
    for job, marker in findings.items():
        if not any((Path(jobs_folder) / f"{job}.{term}").exists()
                   for term in ("done", "failed", "timeout")):
            clear_job_markers(jobs_folder, job)
            (Path(jobs_folder) / f"{job}.{marker}").touch()


def recover_condor_log_failures(jobs_folder, log_offsets):
    findings = scan_condor_log_failures(jobs_folder, log_offsets)
    apply_condor_log_failures(jobs_folder, findings)
    return len(findings)


def merge_inferred_status(idle, running, done, failed, timeout, findings):
    values = [list(value) for value in (idle, running, done, failed, timeout)]
    idle, running, done, failed, timeout = values
    for job, marker in findings.items():
        if job in done or job in failed or job in timeout:
            continue
        if job in idle:
            idle.remove(job)
        if job in running:
            running.remove(job)
        (timeout if marker == "timeout" else failed).append(job)
    return idle, running, done, failed, timeout


def get_tables(total, idle, running, done, failed, timeout=None, details=False):
    timeout = timeout or []
    failed = failed + [job for job in timeout if job not in failed]
    table = Table(title="Job Summary")
    for title, style in (("Total jobs", "cyan"), ("Idle jobs", "blue"),
                         ("Running jobs", "magenta"), ("Done jobs", "green"),
                         ("Failed jobs", "red")):
        table.add_column(title, style=style)
    table.add_row(str(len(total)), str(len(idle)), str(len(running)),
                  str(len(done)), str(len(failed)))
    detail = None
    if details:
        detail = Table(title="Job Status")
        for title in ("Job ID", "Submitted", "Running", "Done", "Failed"):
            detail.add_column(title)
        for job in total:
            detail.add_row(job, "X" if job in idle else "", "X" if job in running else "",
                           "X" if job in done else "", "X" if job in failed else "")
    return table, detail


def get_progress_table(group_counts, label, multi_sample_overlap=False, bar_width=30):
    table = Table(title=f"Progress by {label}")
    for column in (label.capitalize(), "Total", "Idle", "Running", "Done", "Failed",
                   "Progress", "% Done"):
        table.add_column(column, no_wrap=True)
    for name, counts in sorted(group_counts.items(),
                               key=lambda item: (item[1]["pct_done"], item[0])):
        table.add_row(name, str(counts["total"]), str(counts["idle"]),
                      str(counts["running"]), str(counts["done"]),
                      str(counts["failed"]), render_progress_bar(counts, width=bar_width),
                      f"{counts['pct_done']:.1f}%")
    return table


def condor_rm_job(job):
    idx = job.split("_", 1)[1]
    try:
        result = subprocess.run(
            ["condor_rm", "-constraint", f'regexp("config_job_{idx}\\.pkl", Args)'],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    except OSError as exc:
        return False, str(exc)
    return result.returncode == 0, result.stdout.strip()


def wait_for_condor_job_removal(job, timeout=CONDOR_REMOVAL_TIMEOUT, poll_interval=0.2):
    idx = job.split("_", 1)[1]
    constraint = f'regexp("config_job_{idx}\\.pkl", Args)'
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                ["condor_q", "-constraint", constraint, "-af", "ClusterId"],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
            if result.returncode == 0 and not result.stdout.strip():
                return True
        except OSError:
            pass
        time.sleep(poll_interval)
    return False


def condor_submit_job(jobs_folder, submit_file):
    try:
        result = subprocess.run(
            ["condor_submit", submit_file], cwd=str(jobs_folder),
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    except OSError as exc:
        return False, str(exc)
    return result.returncode == 0, result.stdout.strip()


def mark_job_idle(jobs_folder, job):
    clear_job_markers(jobs_folder, job)
    (Path(jobs_folder) / f"{job}.idle").touch()


def mark_job_failed(jobs_folder, job):
    clear_job_markers(jobs_folder, job)
    (Path(jobs_folder) / f"{job}.failed").touch()


def load_job_state(state_file):
    return json.loads(Path(state_file).read_text())


def save_job_state(state_file, state):
    state_file = Path(state_file)
    temp = state_file.with_name(f".{state_file.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    os.replace(temp, state_file)


def _submission_proxy_contract(jobs_folder):
    return load_current_contract(jobs_folder)[2]


def _atomic_copy_proxy(source, target):
    source, target = os.path.abspath(source), os.path.abspath(target)
    if source == target:
        if not os.path.exists(target):
            raise RuntimeError(f"Required proxy transfer path does not exist: {target}")
        os.chmod(target, 0o600)
        return
    parent = os.path.dirname(target) or "."
    os.makedirs(parent, exist_ok=True)
    temp = os.path.join(parent, f".{os.path.basename(target)}.{os.getpid()}.tmp")
    try:
        with open(source, "rb") as src, open(temp, "wb") as dst:
            shutil.copyfileobj(src, dst)
        os.chmod(temp, 0o600)
        os.replace(temp, target)
        os.chmod(target, 0o600)
    finally:
        Path(temp).unlink(missing_ok=True)


def prepare_proxy_for_jobs(jobs_folder):
    submission = _submission_proxy_contract(jobs_folder)
    if not submission["requires_grid_certificate"]:
        return None
    path = submission["proxy_transfer_path"]
    if not path:
        raise RuntimeError("The jobs require a grid certificate but no proxy transfer path was recorded")
    if submission["proxy_source"] == "default":
        _atomic_copy_proxy(get_proxy_path(), path)
    elif submission["proxy_source"] != "explicit":
        raise RuntimeError(f"Unsupported proxy source {submission['proxy_source']!r}")
    if not os.path.exists(path):
        raise RuntimeError(f"Required proxy transfer path does not exist: {path}")
    os.chmod(path, 0o600)
    os.environ["X509_USER_PROXY"] = path
    return path


def scale_memory(memory, factor):
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([A-Za-z]*)\s*", str(memory))
    if not match:
        raise ValueError(f"Cannot scale RequestMemory value: {memory!r}")
    return f"{float(match.group(1)) * factor:g}{match.group(2)}"


def candidate_state(current, submission, queue_shift, ncpu, escalate):
    candidate = deepcopy(current)
    if not escalate or not submission["supports_queue_escalation"]:
        return candidate
    candidate["queue"] = next_queue(candidate["queue"], queue_shift)
    if not candidate["resources_scaled"]:
        candidate["request_cpus"] = int(candidate["base_cpus"]) * ncpu
        candidate["request_memory"] = scale_memory(candidate["base_memory"], ncpu)
        candidate["resources_scaled"] = True
    return candidate


def render_states(folder, submission, states):
    rows = []
    for job, state in sorted(states.items(), key=lambda item: int(item[0])):
        row = {"PROC": job, "CHUNKSIZE": state["chunksize"],
               "CPUS": state["request_cpus"], "MEMORY": state["request_memory"]}
        if submission["executor"] == "condor@lxplus":
            row["QUEUE"] = state["queue"]
        rows.append(row)
    return render_condor_submit(
        (Path(folder) / "resubmit.sub").read_text(), rows, submission["executor"])


def materialize_job_submit_state(jobs_folder, job_num, state):
    folder, _, submission, _, _ = load_current_contract(jobs_folder)
    (folder / f"job_{job_num}.sub").write_text(
        render_states(folder, submission, {str(job_num): state[str(job_num)]}))
    return True


def is_xrootd_exhaustion_log(out_text):
    return any(marker in out_text for marker in (
        "XRootD failure found at root://xrootd-cms.infn.it",
        "Reached the maximum number of XRootD recovery attempts"))


def latest_job_out(jobs_folder, job):
    job_num = job.split("_", 1)[1]
    paths = glob.glob(f"{jobs_folder}/logs/job_*.{job_num}.out")
    return sorted(paths, key=os.path.getmtime)[-1] if paths else None


def convert_timeout_jobs(jobs_folder, timeout_jobs, running, idle, failed,
                         queue_shift, ncpu, state, state_file, log_text,
                         shifted=None, pending=None):
    _, _, submission, _, _ = load_current_contract(jobs_folder)
    shifted = shifted if shifted is not None else set()
    pending = pending if pending is not None else {}
    for job in list(timeout_jobs):
        if job in running:
            running.remove(job)
        if job in idle:
            idle.remove(job)
        mark_job_failed(jobs_folder, job)
        failed.append(job) if job not in failed else None
        job_num = job.split("_", 1)[1]
        pending[job] = candidate_state(state[job_num], submission, queue_shift, ncpu, True)
        shifted.add(job)
        log_text.append(
            f"{job} reached the Condor time limit; preparing "
            + ("queue/resource escalation." if submission["supports_queue_escalation"]
               else "a retry."))
    return len(timeout_jobs)


def submit_resubmit_jobs(jobs_folder, job_nums, state, state_file, log_text, pending=None):
    folder, _, submission, _, _ = load_current_contract(jobs_folder)
    pending = pending or {}
    states = {job: deepcopy(pending.get(f"job_{job}", state[job])) for job in job_nums}
    (folder / "resubmit_now.sub").write_text(render_states(folder, submission, states))
    try:
        prepare_proxy_for_jobs(folder)
    except Exception as exc:
        log_text.append(f"[red]Could not prepare proxy for resubmission: {exc}[/]")
        return []
    ok, output = condor_submit_job(folder, "resubmit_now.sub")
    if not ok:
        log_text.append(f"[red]Failed to resubmit jobs; state unchanged: {output}[/]")
        return []
    for job, candidate in states.items():
        committed = deepcopy(candidate)
        committed["resubmissions"] = int(state[job]["resubmissions"]) + 1
        state[job] = committed
        materialize_job_submit_state(folder, job, state)
        mark_job_idle(folder, f"job_{job}")
    save_job_state(state_file, state)
    log_text.append(f"[green]Resubmitted {len(states)} failed jobs to condor[/]")
    return list(states)


def recreate_jobs_oneshot(jobs_folder, jobs_to_recreate, *, use_redirector=False,
                          blocklist_sites=None, recreate_queue=None, skip_bad_files=False,
                          queue_shift=1, ncpu=1, remove_running=False, dry_run=False):
    folder, jobs_config, submission, state, state_file = load_current_contract(jobs_folder)
    result = {"requested": [], "submitted": [], "skipped": [], "failed": {}}
    if recreate_queue is not None and submission["executor"] == "condor@rubin":
        raise click.UsageError("--recreate-queue is only supported for condor@lxplus")
    if skip_bad_files:
        options_path = folder / "inner_run_options.yaml"
        options = yaml.safe_load(options_path.read_text()) or {}
        options["skip-bad-files"] = True
        temp = options_path.with_name(f".{options_path.name}.{os.getpid()}.tmp")
        temp.write_text(yaml.safe_dump(options, sort_keys=False))
        os.replace(temp, options_path)

    if jobs_to_recreate == "auto":
        jobs = []
        for marker in ("failed", "running", "idle", "timeout"):
            jobs.extend(p.stem for p in folder.glob(f"job_*.{marker}"))
        jobs = list(dict.fromkeys(jobs))
    else:
        jobs = list(dict.fromkeys(
            item.strip() if item.strip().startswith("job_") else f"job_{item.strip()}"
            for item in jobs_to_recreate.split(",") if item.strip()))
    result["requested"] = jobs
    if not jobs:
        return result

    blocklist = {normalize_rse(site) for site in (blocklist_sites or [])}
    failed_xrootd = []
    for job in jobs:
        output = latest_job_out(folder, job)
        if output and extract_failed_url(Path(output).read_text(errors="replace")):
            failed_xrootd.append(job)
    sitemap = get_xrootd_sites_map() if (failed_xrootd or blocklist) and not use_redirector else None
    client = get_rucio_client() if sitemap is not None else None

    for job in jobs:
        if job not in jobs_config["jobs_list"]:
            result["failed"][job] = "job is not present in jobs_config.yaml"
            continue
        active = (folder / f"{job}.running").exists() or (folder / f"{job}.idle").exists()
        if active and not remove_running:
            result["skipped"].append(job)
            continue
        config_temp = sub_temp = None
        removed = not active
        try:
            fileset = deepcopy(jobs_config["jobs_list"][job]["filesets"])
            if use_redirector:
                fileset = rewrite_fileset_to_redirector(fileset)
            else:
                if job in failed_xrootd:
                    for dataset in fileset.values():
                        dataset["files"] = [
                            find_other_file(file, sitemap, blocklist=blocklist, rucio_client=client)
                            for file in dataset["files"]]
                if blocklist:
                    fileset = rewrite_fileset_blocklist(fileset, sitemap, blocklist, rucio_client=client)
            config_path = folder / f"config_{job}.pkl"
            config = cloudpickle.load(config_path.open("rb"))
            config.set_filesets_manually(fileset)
            config_temp = Path(tempfile.mkstemp(prefix=f".{config_path.name}.", suffix=".tmp", dir=folder)[1])
            with config_temp.open("wb") as handle:
                cloudpickle.dump(config, handle)
            job_num = job.split("_", 1)[1]
            candidate = deepcopy(state[job_num])
            if (folder / f"{job}.timeout").exists():
                candidate = candidate_state(candidate, submission, queue_shift, ncpu, True)
            if recreate_queue is not None:
                candidate["queue"] = recreate_queue
            row = {"PROC": job_num, "CHUNKSIZE": candidate["chunksize"],
                   "CPUS": candidate["request_cpus"], "MEMORY": candidate["request_memory"]}
            if submission["executor"] == "condor@lxplus":
                row["QUEUE"] = candidate["queue"]
            sub_temp = Path(tempfile.mkstemp(prefix=f".{job}.", suffix=".sub", dir=folder)[1])
            sub_temp.write_text(render_condor_submit(
                (folder / "resubmit.sub").read_text(), [row], submission["executor"]))
            prepare_proxy_for_jobs(folder)
            if active and remove_running and not dry_run:
                ok, output = condor_rm_job(job)
                if not ok:
                    raise RuntimeError(f"condor_rm failed: {output}")
                if not wait_for_condor_job_removal(job):
                    raise RuntimeError("could not confirm Condor removal")
                removed = True
            if dry_run:
                result["skipped"].append(job)
                continue
            ok, output = condor_submit_job(folder, sub_temp.name)
            if not ok:
                mark_job_failed(folder, job)
                result["failed"][job] = output or "condor_submit failed"
                continue
            os.replace(config_temp, config_path)
            os.replace(sub_temp, folder / f"{job}.sub")
            candidate["resubmissions"] = int(state[job_num]["resubmissions"]) + 1
            state[job_num] = candidate
            save_job_state(state_file, state)
            mark_job_idle(folder, job)
            result["submitted"].append(job)
        except Exception as exc:
            result["failed"][job] = str(exc)
            if removed:
                mark_job_failed(folder, job)
        finally:
            if config_temp:
                config_temp.unlink(missing_ok=True)
            if sub_temp:
                sub_temp.unlink(missing_ok=True)

    rprint("Recreate summary:")
    rprint(f"  submitted: {len(result['submitted'])}")
    rprint(f"  skipped: {len(result['skipped'])}")
    rprint(f"  failed: {len(result['failed'])}")
    for job, message in result["failed"].items():
        rprint(f"{job}: {message}")
    return result


@click.command()
@click.option("-j", "--jobs-folder", required=True, type=str)
@click.option("-d", "--details", is_flag=True)
@click.option("-r", "--resubmit", is_flag=True)
@click.option("-m", "--max-resubmit", type=int, default=4)
@click.option("-q", "--queue-shift", type=click.IntRange(min=0), default=1)
@click.option("-n", "--ncpu", type=click.IntRange(min=1), default=1)
@click.option("--by", "group_by", type=click.Choice(["sample", "dataset", "none"]), default="sample")
@click.option("--recreate", type=str, default=None)
@click.option("--once", is_flag=True, default=False)
@click.option("--use-redirector", is_flag=True, default=False)
@click.option("--blocklist-sites", type=str, default=None)
@click.option("--recreate-queue", type=str, default=None)
@click.option("--skip-bad-files", is_flag=True, default=False)
@click.option("--remove-running", is_flag=True, default=False)
@_with_check_jobs_lock
def check_jobs(jobs_folder, details, resubmit, max_resubmit, queue_shift, ncpu, group_by,
               recreate, once, use_redirector, blocklist_sites, recreate_queue,
               skip_bad_files, remove_running):
    folder, config, submission, state, state_file = load_current_contract(jobs_folder)
    if recreate is None and any((use_redirector, blocklist_sites, recreate_queue,
                                 skip_bad_files, remove_running)):
        raise click.UsageError("recreate-only options require --recreate")
    if recreate is not None:
        result = recreate_jobs_oneshot(
            folder, recreate, use_redirector=use_redirector,
            blocklist_sites=(blocklist_sites or "").split(","),
            recreate_queue=recreate_queue, skip_bad_files=skip_bad_files,
            queue_shift=queue_shift, ncpu=ncpu, remove_running=remove_running)
        if result["failed"]:
            raise click.exceptions.Exit(1)
        if not resubmit:
            return

    total = [f"job_{job}" for job in state]
    groups = None
    label = None
    overlap = False
    if group_by != "none":
        sample_jobs, dataset_jobs = load_job_to_group_map(folder)
        groups, label = (sample_jobs, "sample") if group_by == "sample" else (dataset_jobs, "dataset")
        if groups:
            listed = [job for jobs in groups.values() for job in jobs]
            overlap = len(listed) != len(set(listed))

    layout = Layout()
    layout.split_row(Layout(name="left", ratio=2), Layout(name="right", ratio=1))
    if groups is not None:
        layout["left"].split_column(Layout(name="summary", size=9), Layout(name="progress"))
    log_text, offsets, pending, shifted, definitive = [], {}, {}, set(), set()

    def refresh(idle, running, done, failed, timeout):
        summary, _ = get_tables(total, idle, running, done, failed, timeout, details)
        if groups is None:
            layout["left"].update(Panel(summary, title="Job Status"))
        else:
            layout["summary"].update(Panel(summary, title="Job Status"))
            layout["progress"].update(Panel(
                get_progress_table(aggregate_by_group(groups, idle, running, done, failed), label, overlap)))
        layout["right"].update(Panel("\n".join(log_text[-20:]) or "No logs yet", title="Log"))

    mutation = bool(resubmit or recreate is not None)
    with Live(layout, refresh_per_second=0.2, console=Console()):
        while True:
            findings = scan_condor_log_failures(folder, offsets)
            idle, running, done, failed, timeout = check_jobs_logs(folder)
            if mutation:
                apply_condor_log_failures(folder, findings)
                idle, running, done, failed, timeout = check_jobs_logs(folder)
                convert_timeout_jobs(folder, timeout, running, idle, failed, queue_shift, ncpu,
                                     state, state_file, log_text, shifted, pending)
            else:
                idle, running, done, failed, timeout = merge_inferred_status(
                    idle, running, done, failed, timeout, findings)
            resubmit_now = []
            if resubmit:
                for job in failed:
                    if job in definitive:
                        continue
                    number = job.split("_", 1)[1]
                    attempts = int(state[number]["resubmissions"])
                    output = latest_job_out(folder, job)
                    out_text = Path(output).read_text(errors="replace") if output else ""
                    if "Corrupt input data" in out_text:
                        log_text.append(f"{job} reported corrupt input data in its .out log.")
                    if attempts >= max_resubmit:
                        definitive.add(job)
                        continue
                    if (attempts >= 1 and job not in timeout and job not in shifted
                            and job not in pending and not is_xrootd_exhaustion_log(out_text)):
                        pending[job] = candidate_state(state[number], submission, queue_shift, ncpu, True)
                    resubmit_now.append(number)
            if resubmit_now:
                successful = submit_resubmit_jobs(folder, resubmit_now, state, state_file, log_text, pending)
                for number in successful:
                    job = f"job_{number}"
                    failed[:] = [value for value in failed if value != job]
                    if job not in idle:
                        idle.append(job)
                    pending.pop(job, None)
                    definitive.discard(job)
            refresh(idle, running, done, failed, timeout)
            terminal_failed = len(definitive) if resubmit else len(failed)
            if len(total) == len(done) + terminal_failed:
                rprint("[green]All jobs are completed[/]")
                rprint(f"Now merge outputs with [yellow]merge-outputs -jc {folder}[/].")
                break
            if once:
                break
            time.sleep(5)


if __name__ == "__main__":
    check_jobs()
