import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os
import csv
import time  # Import time to add delay
import pandas as pd
import numpy as np
import sys  # For command-line arguments


ak_certs_dir = 'AKCerts' 
os.makedirs(ak_certs_dir, exist_ok=True)

ak_creds_dir = 'AKCredentials' 
os.makedirs(ak_creds_dir, exist_ok=True)

run_start_dir = 'RunStartTimes' 
os.makedirs(run_start_dir, exist_ok=True)

request_csv_data_dir = 'RequestCsvData/single_ca_100_requests_gunicorn'
os.makedirs(request_csv_data_dir, exist_ok=True)

# Create the RequestArrivalTimes folder if it doesn't exist
request_arrival_times_csvs_dir = 'RequestArrivalTimes'
os.makedirs(request_arrival_times_csvs_dir, exist_ok=True)

success_counter = 0

# Function to write logs to a CSV file
def log_to_csv(csv_file_name, request_id, sent_time, end_time, turnaround_time, wait_time, status_code):
    with open(csv_file_name, mode='a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([request_id, sent_time, end_time, turnaround_time, wait_time, status_code])

def get_submit_times_list(file_name):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_name)

    # Extract the 'Submit Time' column and convert it to a list
    submit_time_list = df['Submit Time'].tolist()

    return submit_time_list


# Set the URL of the load balancer
LOAD_BALANCER_URL = 'http://192.168.18.167/request_ak_cert'

# Define a function to send a single request
def send_request(request_id, run_start_time, request_csv_data_file_path, arrival_rate):
    # Prepare the files to send
    csr_file = f'AKCredentials/key_{request_id+1}/attestation_key_{request_id+1}.csr'
    signed_csr_file = f'AKCredentials/key_{request_id+1}/attestation_key_{request_id+1}_signed.csr'
    sent_time = datetime.now()  # Record time before sending the request

    sent_timestamp = datetime.now()
    print(f"Sending Request ID {request_id}...")
    global success_counter
    # Open the files and prepare the data for the POST request
    with open(csr_file, 'rb') as csrf, open(signed_csr_file, 'rb') as signedcsrf:
        files = {
            'csr': (csr_file, csrf),
            'signed_csr': (signed_csr_file, signedcsrf)
        }
        data = {'request_id': request_id, 'arrival_rate': arrival_rate}
        
        # Send the files to the load balancer
        try:
            response = requests.post(LOAD_BALANCER_URL, files=files, data=data)
            end_time = datetime.now()
            received_timestamp = datetime.now()

            if response.status_code == 200:
                success_counter += 1
                wait_time = response.headers.get('X-Wait-Time', '0.0')
                turnaround_time = (received_timestamp - sent_timestamp).total_seconds()

                print(f'Sent time for request id {request_id} is {(sent_time - run_start_time).total_seconds()}')
                print(f'End Time for request id {request_id} is {(end_time - run_start_time).total_seconds()}')
                print(f'Turnaround Time for request id {request_id} is {turnaround_time}')
                print(f'Wait Time for request id {request_id} is {wait_time}')

                # Log the data to the CSV file relative to run_start_time
                log_to_csv(request_csv_data_file_path ,request_id, (sent_time - run_start_time).total_seconds(),
                            (end_time - run_start_time).total_seconds(),
                            turnaround_time,
                            wait_time, 
                            'completed')

                print(f"Request {request_id} successfully sent and received.")

                # Get the response file and save it in the ServerResponses directory
                response_filename = f"fn_response_request_id_{request_id}_arrival_rate_{arrival_rate}.pem"
                file_path = os.path.join(ak_certs_dir, response_filename)

                # Save the received response file
                with open(file_path, 'wb') as f:
                    f.write(response.content)
            
                print(f"Response for request {request_id} saved as {file_path} with code {response.status_code}")
            
            else:
                print(f"Failed to get response for request_id {request_id}: {response.status_code} Message: {response.text}") 
                # Log error data in the CSV file
                wait_time = response.headers.get('X-Wait-Time', '0.0')
                turnaround_time = (received_timestamp - sent_timestamp).total_seconds()
                log_to_csv(request_csv_data_file_path, request_id,
                           (sent_time - run_start_time).total_seconds(),
                           (end_time - run_start_time).total_seconds(),
                            '0', '-1', 'killed')
               
        except requests.exceptions.RequestException as e:
            print(f"Error sending request {request_id}: {e}")
            end_time = datetime.now()
            log_to_csv(csv_file_name=request_csv_data_file_path, request_id=request_id,
                       sent_time=(sent_time - run_start_time).total_seconds(),
                       end_time=(end_time - run_start_time).total_seconds(),
                       turnaround_time='N/A',wait_time='N/A', status_code=f"Error sending request {request_id}: {e}")

def main():
    # Parse the arrival_rate argument from command line
    if len(sys.argv) != 2:
        print("Usage: python client_script.py <arrival_rate>")
        sys.exit(1)
    
    arrival_rate = int(sys.argv[1])

    # Initialize the CSV file with headers
    csv_file = f'triple_ca_request_statistics_arrival_rate_{arrival_rate}.csv'
    request_csv_data_file_path = os.path.join(request_csv_data_dir, csv_file)
    if not os.path.exists(request_csv_data_file_path):
        with open(request_csv_data_file_path, mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['Request ID', 'Submit Time', 'End Time', 'Turnaround Time', 'Wait Time', 'Status'])

    run_start_time_file=f'run_start_time_arrival_rate_{arrival_rate}.txt'
    run_start_file_path = os.path.join(run_start_dir, run_start_time_file)    
    
    submit_times_list = get_submit_times_list(file_name=f'single_ca_request_stats/single_ca_job_statistics_arrival_rate_{arrival_rate}.csv')

    # Generate a list of unique request IDs for each request
    request_id = 0    
    # Record the time before the requests are sent starts running
    run_start_time = datetime.now()

    # Write the start time to the file
    with open(run_start_file_path, 'w') as file:
        file.write(f"{run_start_time}")
    print(f"Run started at: {run_start_time}")
    
    # Use ThreadPoolExecutor to send requests concurrently until 100 successful responses are received
    with ThreadPoolExecutor(max_workers=100) as executor:
        while request_id < 100:
            if request_id == 0:
                poisson_delay = submit_times_list[request_id]
                time.sleep(submit_times_list[request_id])
            else:
                time.sleep(submit_times_list[request_id] - submit_times_list[request_id-1]) 
                poisson_delay = submit_times_list[request_id] - submit_times_list[request_id-1]

            print(f"Sending request {request_id} after a delay of {poisson_delay} seconds.")
            executor.submit(send_request, request_id, run_start_time, request_csv_data_file_path, arrival_rate)
            request_id += 1

    print(success_counter)

if __name__ == '__main__':
    print("=== Starting client requests ===")
    main()
