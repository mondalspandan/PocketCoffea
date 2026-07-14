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
import click
import glob
import json
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
import time
import re
from pocket_coffea.utils.job_progress import (
    aggregate_by_group,
    load_job_to_group_map,
    render_progress_bar,
)
from pocket_coffea.utils.network import get_proxy_path

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

def load_job_state(state_file):
    with open(state_file) as f:
        return json.load(f)


def save_job_state(state_file, job_state):
    with open(state_file, "w") as f:
        json.dump(job_state, f, indent=2, sort_keys=True)


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


def bump_jobqueue(job_num, job_state, state_file, shift=1, ncpu=1):
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
@click.option("-q","--queue-shift", type=int, help="How many queues to bump to if a job is removed due to time limit? E.g. 1 = bump to next queue, 2 = bump to next-to-next queue", default=1)
@click.option("-n", "--ncpu", type=click.IntRange(min=1), default=1, show_default=True,
              help="CPU and memory multiplier applied once when a job's queue is shifted")
@click.option("--by", "group_by", type=click.Choice(["sample", "dataset", "none"]),
              default="sample",
              help="Show a per-group progress table below the summary. Requires "
                   "jobs_config.yaml in the jobs folder (created by manual-job "
                   "executors). Pass 'none' to disable. Default: sample.")
def check_jobs(jobs_folder, details, resubmit, max_resubmit, queue_shift, ncpu, group_by):
    # check if the user passed the parent folder
    subdirs = os.listdir(jobs_folder)
    if len(subdirs) == 1 and subdirs[0] == "job":
        jobs_folder = os.path.join(jobs_folder,"job")

    jobs_folder = Path(jobs_folder)
    state_file = f"{jobs_folder}/job_state.json"
    job_state = load_job_state(state_file)
    tot_jobs = [f"job_{job_num}" for job_num in job_state]
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
                time.sleep(5)
        except KeyboardInterrupt:
            pass

if __name__ == "__main__":
    check_jobs()
