from flask import Flask, request, jsonify
from rq import Queue
from redis import Redis
import logging
import time
from tasks import forward_request
import os
import csv
from datetime import datetime

# Create directories for storing wait times
os.makedirs('WaitTimes', exist_ok=True)  # directory for wait times
os.makedirs('SubmissionResponseDataFogNode', exist_ok=True)  # directory for queue data
os.makedirs('ServerResponses', exist_ok=True)  # directory for Server responses

def create_directory(folder_path, directory_name):
    # Combine the folder path with the new directory name
    new_directory = os.path.join(folder_path, directory_name)
    
    # Check if the directory already exists
    if not os.path.exists(new_directory):
        # Create the new directory
        os.makedirs(new_directory)


def log_to_csv(csv_file_name, time_data, queue_length, request_id, invocation):
    with open(csv_file_name, mode='a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([time_data, queue_length, request_id, invocation])


arrival_rates_list = list(range(5, 501, 5))
for arrival_rate in arrival_rates_list:
    create_directory(folder_path='WaitTimes', directory_name=f'wait_times_arrival_rate_{arrival_rate}')

# Initialize Redis connection and RQ queue
redis_conn = Redis(host='localhost', port=6379, db=0)
q = Queue(connection=redis_conn)

# Initialize Flask application for the load balancer
app = Flask(__name__)

# Set up logging
# Configure logging to write to a file (for general logging)
logging.basicConfig(
    filename='load_balancer.log',  # General log file where the logs will be saved
    level=logging.DEBUG,  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    datefmt='%Y-%m-%d %H:%M:%S'  # Date format for log entries
)

# Backend server details (You can scale this up with more backend servers)
backend_servers = [
    'http://146.230.192.170'#,
    # 'http://146.230.193.218'#,
    # 'http://146.230.192.179'
]

def get_next_backend_server(request_id):
    # Fetch the current backend server index from Redis
    current_server_index = redis_conn.get('current_server_index')
    
    # If the index is None (i.e., this is the first request), initialize it to 0
    if current_server_index is None or request_id == 0:
        current_server_index = 0
        redis_conn.set('current_server_index', current_server_index)  # Set the initial value in Redis
    else:
        current_server_index = int(current_server_index)
    
    # Select the backend server based on the current index
    backend_server = backend_servers[current_server_index]
    
    # Update the server index in Redis for the next round-robin selection
    redis_conn.set('current_server_index', (current_server_index + 1) % len(backend_servers))

    return backend_server


# Route to receive client requests and enqueue them
@app.route("/request_ak_cert", methods=["POST"])
def load_balancer():
    # Read the files into memory as bytes (this will allow serialization)
    csr_file_content = request.files['csr'].read()
    signed_csr_file_content = request.files['signed_csr'].read()

    # Select the backend server using the round-robin method
    if len(backend_servers) > 1:
        backend_server = get_next_backend_server(request_id=request.form['request_id'])
    else:
        backend_server = backend_servers[0]
    
    # Pass the backend server URL to the worker
    request_data = {
        'method': 'POST',
        'url': '/request_ak_cert',
        'files': {
            'csr': csr_file_content,
            'signed_csr': signed_csr_file_content
        },
        'form_data': {
            'request_id': request.form['request_id'],
            'arrival_rate': request.form['arrival_rate']
        },
        'backend_server': backend_server  # Send the selected server to the worker
    }

    # Initialize the CSV files with headers
    arrival_rate = request.form['arrival_rate']
    if not os.path.exists(f'SubmissionResponseDataFogNode/submission_response_queue_data_arrival_rate_{arrival_rate}.csv'):
        with open(f'SubmissionResponseDataFogNode/submission_response_queue_data_arrival_rate_{arrival_rate}.csv', mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['Time', 'Queue Length','Request ID', 'SubmitDispatchDequeued'])

    # Check the current length of the queue
    queue_length = len(q)

    log_to_csv(
        f'SubmissionResponseDataFogNode/submission_response_queue_data_arrival_rate_{arrival_rate}.csv',
        time_data=datetime.now(),
        queue_length=queue_length,
        request_id=request.form['request_id'],
        invocation='Request Submitted'
    )
    
    # If the queue is full (size >= 5), return a 503 response
    if queue_length >= 5:
        logging.warning(f"Queue is full. Current queue size: {queue_length}")
        return jsonify({"error": "Queue is full"}), 503

    # Enqueue the request to be processed in the background
    enqueue_time = time.perf_counter()
    job = q.enqueue(forward_request, request_data, enqueue_time, request.form['arrival_rate'], request.form['request_id'])

    log_to_csv(
        f'SubmissionResponseDataFogNode/submission_response_queue_data_arrival_rate_{arrival_rate}.csv',
        time_data=datetime.now(),
        queue_length=queue_length,
        request_id=request.form['request_id'],
        invocation='Request Enqueued')
    
    # Wait for the job to finish (blocking call)
    while job.get_status(refresh=True) not in ['finished', 'failed']:
        logging.debug(f"Job {job.get_id()} is still in status: {job.get_status()}")
        time.sleep(0.001)  # Wait for 1ms before checking again
            
    # Check if the job finished successfully
    if job.get_status() == 'finished':
        result = job.result
        logging.debug(f"Received result from RQ worker: {result}")
        
        # Retrieve the wait time value from the file
        arrival_rate = request.form['arrival_rate']
        request_id = request.form['request_id']
        wait_time_filename = f'WaitTimes/wait_times_arrival_rate_{arrival_rate}/wait_time_{request_id}.txt'
        try:
            with open(wait_time_filename, 'r') as wait_file:
                wait_time_value = wait_file.read().strip()  # Use .strip() to clean up any extra whitespace or newlines
            result['wait_time'] = wait_time_value  # Add the wait time to the result
            
            response_filename = f'ServerResponses/response_{request_id}.pem'
            with open(response_filename, 'r') as response_file:
                result['certificate']  = response_file.read() 
            
        except FileNotFoundError:
            logging.error(f"Wait time file not found: {wait_time_filename}")
            result['wait_time'] = 'Wait time data not available'

        return jsonify(result), 200
    else:
        # Handle failed job
        logging.error(f"Job {job.get_id()} failed with error: {job.exc_info}")
        return jsonify({"error": "Failed to process the request"}), 500


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)
