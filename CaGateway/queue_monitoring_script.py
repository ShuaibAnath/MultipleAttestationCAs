import time
import logging
import csv
from redis import Redis
from rq import Queue

# Initialize Redis connection and RQ queue
redis_conn = Redis(host='localhost', port=6379, db=0)  # Adjust connection if needed
q = Queue(connection=redis_conn)

# CSV file setup
csv_file_path = 'queue_status.csv'

# Write headers if the file does not exist
def write_csv_header():
    with open(csv_file_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Only write headers if the file is empty
        if csvfile.tell() == 0:
            writer.writerow(['Timestamp', 'Queued', 'Started', 'Deferred', 'Finished', 'Stopped', 'Scheduled', 'Canceled', 'Failed'])

# Initialize logging (optional, can log other events as well)
logging.basicConfig(
    filename='queue_status.log',  # Log file for any errors
    level=logging.DEBUG,  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    datefmt='%Y-%m-%d %H:%M:%S'  # Date format for log entries
)

def log_job_states_to_csv():
    """
    Monitors the queue and logs the number of jobs in various states into a CSV file:
    queued, started, deferred, finished, stopped, scheduled, canceled, and failed.
    """
    while True:
        # Initialize counters for different job states
        queued_count = 0
        started_count = 0
        deferred_count = 0
        finished_count = 0
        stopped_count = 0
        scheduled_count = 0
        canceled_count = 0
        failed_count = 0

        # Fetch all jobs in the queue
        jobs = q.jobs

        # Loop through each job and count its state
        for job in jobs:
            job_status = job.get_status()  # Get the current status of the job

            if job_status == 'queued':
                queued_count += 1
            elif job_status == 'started':
                started_count += 1
            elif job_status == 'deferred':
                deferred_count += 1
            elif job_status == 'finished':
                finished_count += 1
            elif job_status == 'stopped':
                stopped_count += 1
            elif job_status == 'scheduled':
                scheduled_count += 1
            elif job_status == 'canceled':
                canceled_count += 1
            elif job_status == 'failed':
                failed_count += 1

        # Log the data to the CSV file
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')  # Get the current time as a string
        with open(csv_file_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([timestamp, queued_count, started_count, deferred_count, finished_count, stopped_count, scheduled_count, canceled_count, failed_count])

        logging.debug(f"Logged to CSV at {timestamp} - Queued: {queued_count}, Started: {started_count}, "
                      f"Deferred: {deferred_count}, Finished: {finished_count}, Stopped: {stopped_count}, "
                      f"Scheduled: {scheduled_count}, Canceled: {canceled_count}, Failed: {failed_count}")

        # Wait for 0.1 seconds before checking the queue again
        time.sleep(0.1)

if __name__ == "__main__":
    # Initialize the CSV header if the file is empty
    write_csv_header()

    # Start monitoring job states and logging them to CSV
    log_job_states_to_csv()
