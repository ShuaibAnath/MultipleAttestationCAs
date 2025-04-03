import requests
import time
import threading

def send_request(client_url, csr_file, signed_csr_file, request_id, arrival_rate):
    files = {
        'csr': open(csr_file, 'rb'),
        'signed_csr': open(signed_csr_file, 'rb')
    }

    form_data = {
        'request_id': request_id,
        'arrival_rate': arrival_rate
    }

    try:
        response = requests.post(client_url, files=files, data=form_data)
        # Get the response JSON and print it
        response_data = response.json()
        
        # Print the full response
        print(f"Response: {response_data}")
        
        # Extract the wait time and print it
        # if 'wait_time' in response_data:
        #     print(f"Wait Time for Request ID {request_id}: {response_data['wait_time']} seconds")
        # else:
        #     print(f"No wait time data received for Request ID {request_id}")
        # print(f"Response: {response.json()}")
    except Exception as e:
        print(f"Error sending request: {e}")

# Example usage:
client_url = "http://146.230.192.169/request_ak_cert"  # Load balancer's address
csr_file = "AKCredentials/key_1/attestation_key_1.csr"
signed_csr_file = "AKCredentials/key_1/attestation_key_1_signed.csr"
request_id = 0
arrival_rate = 5

# Send multiple requests concurrently with a delay of 0.5 seconds
threads = []
for i in range(100):
    # time.sleep(0.0001)  # Delay between requests
    thread = threading.Thread(target=send_request, args=(client_url, csr_file, signed_csr_file, request_id, arrival_rate))
    threads.append(thread)
    thread.start()
    request_id=i+1

# Wait for all threads to complete
for thread in threads:
    thread.join()

