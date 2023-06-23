from pathlib import Path
from time import sleep

from condorcmf.scheduler.job import Job


def main():
    # Create a job object
    executable = Path(Path.cwd(), "sleep.bat")
    job = Job(
        executable=executable,
        requests=[("request_cpus", 1)],
        requirements='(Arch=="X86_64") && (OpSys=="Windows") && (DA_CDT_HOST==TRUE)',
        custom_args=["+DACDTJob = true"],
        job_id="sleep",
    )

    # Build the job description and dump associated data to disk
    job.build()

    # Submit the job
    job.submit()

    # Wait for the job to finish
    while not job.finished():
        print(f"Job {job.job_id} not finished yet, status {job.status}")
        sleep(3)

    # Clean up associated log files and cache
    job.clean()


if __name__ == "__main__":
    main()
