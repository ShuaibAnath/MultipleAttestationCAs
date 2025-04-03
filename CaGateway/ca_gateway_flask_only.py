import queue
import threading
import requests
from flask import Flask, request, send_file, jsonify
import os
from datetime import datetime, timezone
import csv

app = Flask(__name__)

QUEUE_SIZE = 5
# FIFO Queue to manage incoming requests
request_queue = queue.Queue(maxsize=QUEUE_SIZE)
request_queue.queue

# Directories for storing files
os.makedirs('SignedCSRs', exist_ok=True)
os.makedirs('CSRs', exist_ok=True)
os.makedirs('ServerResponses', exist_ok=True)
os.makedirs('DispatchResponseDataCA', exist_ok=True)  
os.makedirs('SubmissionResponseDataFogNode', exist_ok=True)  
os.makedirs('WaitTimes', exist_ok=True)  # New directory for wait times

def create_directory(folder_path, directory_name):
    # Combine the folder path with the new directory name
    new_directory = os.path.join(folder_path, directory_name)
    
    # Check if the directory already exists
    if not os.path.exists(new_directory):
        # Create the new directory
        os.makedirs(new_directory)

arrival_rates_list = list(range(5, 501, 5))
for arrival_rate in arrival_rates_list:
    create_directory(folder_path='WaitTimes', directory_name=f'wait_times_arrival_rate_{arrival_rate}')

# Lock to ensure only one request is forwarded to the server at a time
lock = threading.Lock()

# Set the server URLs for load balancing
SERVER_URLS = [
    'http://146.230.192.158:5001/request_ak_cert' , # CA Server 1
    # 'http://146.230.193.218:5001/request_ak_cert' , # CA Server 2
    # 'http://146.230.192.168:5001/request_ak_cert' # CA Server 3
]

# Round-robin counter to select the server
server_counter = 0

# Active requests per server to track if the server is busy
active_requests = [False] * len(SERVER_URLS)

# A dictionary to hold Events for each request to synchronize with the client
request_events = {}


CSV_LOG_FILE = "sub_response_queue_data.csv"
DISPATCH_RECEIVE_FILE = "dispatch_to_ca_and_receive_response_logs.csv"

# Function to write logs to a CSV file
def log_to_csv(csv_file_name,time_data, queue_length, request_id, invocation, num_active_requests):
    with open(csv_file_name, mode='a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([time_data, queue_length, request_id, invocation, num_active_requests])

# Function to write logs to a Dispatch/Reponse CSV file
def log_to_dispatch_response_csv(csv_file_name,time_data, request_id, queue_length, dispatch_response):
    with open(csv_file_name, mode='a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([time_data, request_id, queue_length, dispatch_response])

def check_file_exists(filename):
    if os.path.exists(filename):
        return True
    else:
        return False


def forward_to_server(request_id, files, arrival_rate):
    while all(active_requests):  # Check if all servers are busy
        continue
    # Iterate through the active_requests list to find the first available server
    for i, is_busy in enumerate(active_requests):
        if not is_busy:  # Found a server that is not busy
            server_url = SERVER_URLS[i]

            # Set the active request to True for the selected server (mark it as busy)
            with lock:
                active_requests[i] = True
            # print(f'CA Server {i+1} is not busy, server url= {server_url} and request id {request_id}')
            # Prepare the data to forward to the server
            with open(files['csr'], 'rb') as csrf, open(files['signed_csr'], 'rb') as signedcsrf:
                files_data = {
                    'csr': (files['csr'], csrf),
                    'signed_csr': (files['signed_csr'], signedcsrf)
                }
                data = {'request_id': request_id,'arrival_rate': arrival_rate}

                # Send the files to the server
                try:
                    response = requests.post(server_url, files=files_data, data=data)
                    if response.status_code == 200:
                        # Return both the response content and the start_time in the headers
                        start_time = response.headers.get('X-Start-Time')  # Get start time from headers
                        return response.content, start_time, 200  # Include start_time in the return
                    else:
                        return response.text, None, response.status_code  # In case of failure, return error message
                except requests.exceptions.RequestException as e:
                    return f"Error sending request {request_id}: {e}", None, 500
                finally:
                    # Set the active request to False when done (mark server as free)
                    active_requests[i] = False
            break  # Exit the loop after forwarding the request to the first available server

    else:
        # If no server is available (all are busy)
        return "All servers are busy, try again later", None, 500


# Function to process requests from the queue and forward them to the server
def process_requests():
    while True:
        # Check if the queue is empty before trying to process a request, keep checking
        if request_queue.empty():
            continue

        # If queue is not empty get the first request at the front of the queue 
        request_id, files, enqueue_time, event, arrival_rate = request_queue.get()
        log_to_dispatch_response_csv(csv_file_name=f'DispatchResponseDataCA/dispatch_response_log_arrival_rate_{arrival_rate}.csv',time_data=datetime.now(),
                                     request_id=request_id,
                                     queue_length=(request_queue.qsize()),dispatch_response='DispatchFromQueue')

        with lock:
            print(f"Processing request {request_id}...")

        # Calculate the wait time (in seconds)
        wait_time = datetime.now() - enqueue_time
        wait_time_seconds = wait_time.total_seconds()
        # Record the wait time in a text file within the WaitTimes folder
        wait_time_filename = f'WaitTimes/wait_times_arrival_rate_{int(round(float(arrival_rate)))}/wait_time_{request_id}.txt'
        with open(wait_time_filename, 'w') as wait_file:
            wait_file.write(str(wait_time_seconds))
        
        # Forward the request to one of the servers
        response_content, start_time, response_code = forward_to_server(request_id, files, arrival_rate)
        log_to_dispatch_response_csv(csv_file_name=f'DispatchResponseDataCA/dispatch_response_log_arrival_rate_{arrival_rate}.csv', time_data=datetime.now(),request_id=request_id,
                                     queue_length=request_queue.qsize(), dispatch_response='Response Received From CA')

        # Save the response content to a file 
        response_filename = f'ServerResponses/response_{request_id}.pem'
        with open(response_filename, 'wb') as f:
            f.write(response_content)
   
        # Signal the event that the request has been processed
        event.set()

        # Mark the task as done
        request_queue.task_done()


@app.route('/request_ak_cert', methods=['POST'])
def handle_ak_cert_request():
    # Retrieve the request_id from the form data
    request_id = request.form.get('request_id')
    arrival_rate = request.form.get('arrival_rate')

    print(f'Received request {request_id}')

    submit_time = datetime.now()
    
    # Initialize the CSV files with headers
    if not os.path.exists(f'SubmissionResponseDataFogNode/submission_response_queue_data_arrival_rate_{arrival_rate}.csv'):
        with open(f'SubmissionResponseDataFogNode/submission_response_queue_data_arrival_rate_{arrival_rate}.csv', mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['Time', 'queue_length','Request ID', 'Request or Response','Number of Active Requests'])

    if not os.path.exists(f'DispatchResponseDataCA/dispatch_response_log_arrival_rate_{arrival_rate}.csv'):
        with open(f'DispatchResponseDataCA/dispatch_response_log_arrival_rate_{arrival_rate}.csv', mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['Time','Request ID', 'Queue Length', 'Dispatch/Response'])
    
    # print(f'Queue size at submission of request ID {request_id} : {request_queue.qsize()}')
    log_to_csv(csv_file_name=f'SubmissionResponseDataFogNode/submission_response_queue_data_arrival_rate_{arrival_rate}.csv',
               time_data=submit_time, queue_length=request_queue.qsize(), request_id=request_id,
               invocation='Request Received From Fog Node', num_active_requests=sum(1 for value in active_requests if value is True))

    if request_id is None:
        return 'Request ID is missing', 400
    
    # Get the uploaded message and item files
    csr_file = request.files.get('csr')
    signed_csr_file = request.files.get('signed_csr')

    if not csr_file or not signed_csr_file:
        return 'Both CSR and Signed CSR files are required', 400

    # Save files locally to send to the server later
    csr_filename = f'CSRs/csr_{request_id}.pem'
    signed_csr_filename = f'SignedCSRs/signed_csr_{request_id}.sig'

    csr_file.save(csr_filename)
    signed_csr_file.save(signed_csr_filename)

    # Check if All CA nodes are busy
    if all(active_requests):
        
        if request_queue.full():
            # If  queue is full AND the number of active requests are equal to the number of CA nodes
            return 'Request queue is full, please try again later', 503 
        
        # Otherwise the queue is not full and the incoming request needs to be enqueued
        else:
            # Create an event to synchronize with the response
            result_event = threading.Event()
            enqueue_time = datetime.now()  # Record the time when the request is queued
            # print(f"Queuing request with id {request_id} as all servers are busy")
            request_queue.put((request_id, {'csr': csr_filename, 'signed_csr': signed_csr_filename}, enqueue_time, result_event, arrival_rate))
            # print(f'Queued request with id {request_id}: queue size {request_queue.qsize()} queue state {request_queue}')
            # Wait for the server to process the request (this blocks until the event is set)
            result_event.wait()

    # At least 1 free server, send request using round robin
    elif not all(active_requests):
        # There's at least one available server and the queue is empty
        # Dispatch request to an available server using round-robin
        print(f"Processing request {request_id}...")
        log_to_dispatch_response_csv(csv_file_name=f'DispatchResponseDataCA/dispatch_response_log_arrival_rate_{arrival_rate}.csv', time_data=datetime.now(),request_id=request_id,
                                    queue_length=request_queue.qsize(), dispatch_response='DispatchDirectNoQueueing')
        files =  {'csr': csr_filename, 'signed_csr': signed_csr_filename}
        response_content, start_time, response_code = forward_to_server(request_id, files, arrival_rate)
        log_to_dispatch_response_csv(csv_file_name=f'DispatchResponseDataCA/dispatch_response_log_arrival_rate_{arrival_rate}.csv', time_data=datetime.now(),request_id=request_id,
                                    queue_length=request_queue.qsize(),dispatch_response='ResponseWtihoutQueueing')
        
        # Save the response content to a file 
        response_filename = f'ServerResponses/response_{request_id}.pem'

        if isinstance(response_content, str):
            with open(response_filename, 'w') as f:
                f.write(response_content)
        else:    
            with open(response_filename, 'wb') as f:
                f.write(response_content)
  

    # Once the response is ready, return the response file to the fog node
    response_filename = f'ServerResponses/response_{request_id}.pem'
    try:
        # Forward CA repsonse to the fog node
        response = send_file(response_filename, as_attachment=True)
        finished_time = datetime.now() # Record the time at which response from the CA was received

        # Prepare wait time data
        wait_time_filename = f'WaitTimes/wait_times_arrival_rate_{int(round(float(arrival_rate)))}/wait_time_{request_id}.txt'
        wait_file_exists = check_file_exists(filename=wait_time_filename)
        if wait_file_exists:
            with open(wait_time_filename, 'r') as wait_file:
                wait_time = wait_file.read()
            response.headers['X-Wait-Time'] = wait_time
        
        # log Response Data from CA  
        log_to_csv(csv_file_name=f'SubmissionResponseDataFogNode/submission_response_queue_data_arrival_rate_{arrival_rate}.csv',
            time_data=finished_time, queue_length=request_queue.qsize(), request_id=request_id,
            invocation='Certificate Response Sent to Fog Node', num_active_requests=sum(1 for value in active_requests if value is True))
        
        return response
    
    except Exception as e:
        return f"Error sending the response file: {e}", 500


if __name__ == '__main__':

    # Start a background thread to process requests
    threading.Thread(target=process_requests, daemon=True).start()

    # Run the Flask app for the load balancer
    app.run(debug=True, host='146.230.192.120', port=5000)