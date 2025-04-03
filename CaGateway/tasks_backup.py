import requests
import logging
from random import choice
import time
from redis import Redis
from rq import Queue

# Initialize Redis connection and RQ queue
redis_conn = Redis(host='localhost', port=6379, db=0)
q = Queue(connection=redis_conn)

# Configure logging to write to a file (for general logging)
logging.basicConfig(
    filename='worker_logs.log',  # General log file where the logs will be saved
    level=logging.DEBUG,  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    datefmt='%Y-%m-%d %H:%M:%S'  # Date format for log entries
)


def forward_request(request_data, enqueue_time, arrival_rate, request_id):
     # Get the current length of the queue
    queue_length = len(q)  # Get the length of the queue
    logging.debug(f"Current queue length: {queue_length}")

    wait_time = time.perf_counter() - enqueue_time
    logging.debug(f"Request spent {wait_time:.6f} seconds in the queue")

   # Record the wait time in a text file within the WaitTimes folder
    wait_time_filename = f'WaitTimes/wait_times_arrival_rate_{int(round(float(arrival_rate)))}/wait_time_{request_id}.txt'
    with open(wait_time_filename, 'w') as wait_file:
        wait_file.write(str(wait_time))
    
    method = request_data['method']
    url = request_data['url']
    files = request_data['files']
    form_data = request_data['form_data']
    backend_server = request_data['backend_server']  # Get the backend server from the request data

    logging.debug(f"Forwarding {method} request to {backend_server}{url} with data: {form_data}")
    logging.debug(f"Received request data: {request_data}")

    try:
        # Make the request to the selected backend server
        response = requests.post(f"{backend_server}{url}", files=files, data=form_data)

        # Check the response from the backend
        if response.status_code == 200:
            logging.debug(f"Received successful response from backend: {response.status_code}")
            return {
                'status_code': response.status_code,
                'data': {
                    'message': "Request processed successfully",
                    'file_name': response.headers.get('Content-Disposition')
                }
            }
        else:
            logging.error(f"Error from backend: {response.status_code}")
            return {
                'status_code': response.status_code,
                'data': {'message': 'Backend processing failed'}
            }
    except requests.exceptions.RequestException as e:
        logging.error(f"Error while forwarding request: {e}")
        return {
            'status_code': 500,
            'data': {'message': 'Error while forwarding request to backend'}
        }
