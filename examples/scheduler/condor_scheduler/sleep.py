from pathlib import Path
from time import sleep

from condorcmf.scheduler import utils

LOG_DIR = Path("./log")
LOG_DIR.mkdir(exist_ok=True)


def main():
    # Submit a job to the queue
    job_file = "./sleep.sub"
    cluster_id = utils.condor_submit(job_file)
    print(f"Job submitted with ID {cluster_id}")

    # We can get the status of a job using the cluster ID or the log file
    log_file = Path(Path.cwd(), "log/log.log")
    cluster_id_status = utils.get_job_status(cluster_id)
    log_file_status = utils.get_job_status(log_file=log_file)
    print(f"Job status using cluster ID: {cluster_id_status}")
    print(f"Job status using log file: {log_file_status}")

    # Wait for the job to finish
    print("Waiting for job to finish...")
    utils.condor_wait(log_file)

    # Submit and then hold a job
    cluster_id = utils.condor_submit(job_file)
    print(f"Job submitted with ID {cluster_id}")

    # Wait for log file to appear
    while not log_file.exists():
        sleep(1)

    # Wait for job to start then place on hold
    print("Waiting for job to start...")
    utils.wait_all_jobs_match_status([cluster_id], target_status=2, log_dirs=[log_file])

    # Remove job
    print("Removing job...")
    utils.condor_remove(cluster_id)
    sleep(5)
    print(utils.get_job_status(log_file=log_file))


if __name__ == "__main__":
    main()
