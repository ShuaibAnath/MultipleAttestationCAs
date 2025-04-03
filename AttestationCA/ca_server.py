from flask import Flask, request, send_file, jsonify
import os
import random
from datetime import datetime , timezone
import subprocess
import time
import csv

app = Flask(__name__)

# Create a directory for storing the uploaded files (if it doesn't exist)
UPLOAD_FOLDER = 'uploads_ca'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

ak_certs_dir = 'AKCerts'
os.makedirs(ak_certs_dir, exist_ok=True)

processing_time_dir = 'ProcessingTimesData'
os.makedirs(processing_time_dir, exist_ok=True)

CSV_LOG_FILE = "processing_time_data.csv"

# Function to write logs to a CSV file
def log_to_csv(csv_file_name,received_time, start_time, end_time, request_id, duration_data):
    with open(csv_file_name, mode='a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([received_time, start_time, end_time, request_id, duration_data])

def verify_signature(bash_script_path, ak_csr_path, rsa_public_key_path, ak_csr_signed_path, ak_csr_digest_bin_filename):
    try:
        subprocess.run([bash_script_path, ak_csr_path, rsa_public_key_path, ak_csr_signed_path, ak_csr_digest_bin_filename], check=True)
        # print(f"Signature verified")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error During Signature Verification: {e}")
        return False

# Function to simulate processing time with random sleep
def process_requests(request_id, files, received_time, arrival_rate):
    # Process the request (generate the response file)
    csr_file, signed_csr_file = files

    # Save the uploaded files
    csr_filename = os.path.join(app.config['UPLOAD_FOLDER'], f'csr_{request_id}.csr')
    signed_csr_filename = os.path.join(app.config['UPLOAD_FOLDER'], f'signed_csr_{request_id}.sig')
    ak_csr_digest_bin_filename = os.path.join(app.config['UPLOAD_FOLDER'], f'ak_csr_{request_id}.digest.bin') 

    # Save the received files
    with open(csr_filename, 'wb') as f:
        f.write(csr_file)
    with open(signed_csr_filename, 'wb') as f:
        f.write(signed_csr_file)

    rsa_pub_key_path = 'RsaPrimaryPublicKeys/rsa_primary_public_key_fog_node.pem'
    verify_signature_script_path='./verify_signature_and_generate_certificate.sh'

    print(f'Starting processing for ID {request_id}')
    start_time = datetime.now() # Record time before verifying the request
    
    verified = verify_signature(bash_script_path=verify_signature_script_path, ak_csr_path=csr_filename,
                        rsa_public_key_path=rsa_pub_key_path, ak_csr_signed_path=signed_csr_filename,
                        ak_csr_digest_bin_filename=ak_csr_digest_bin_filename)

    end_time = datetime.now() # Record time after verifying request

    if verified:
        processing_time = end_time - start_time  # Calculate the processing time
        processing_file_name = os.path.join(processing_time_dir, f'processing_time_data_arrival_rate_{arrival_rate}.csv')
        # Initialize the CSV files with headers
        if not os.path.exists(processing_file_name):
            with open(processing_file_name, mode='w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(['Received-Time','Start-Time','End-Time','Request ID', 'Processing Duration'])
        log_to_csv(csv_file_name=processing_file_name,received_time=received_time, start_time=start_time, end_time=end_time,
        request_id=request_id, duration_data=processing_time.total_seconds())
        # print(f"Signature verified in {processing_time.total_seconds()} seconds")
    
        attestation_key_cert_name = f'AKCerts/attestation_key_cert_{request_id}_arrival_rate_{arrival_rate}.pem'
        # Run the bash script to generate the AK certificate
        subprocess.run(['./generate_ak_cert.sh', csr_filename, attestation_key_cert_name], check=True)

        return attestation_key_cert_name, start_time
    else:
        return 'file_could_not_be_verified.txt', start_time


@app.route('/request_ak_cert', methods=['POST'])
def handle_ak_cert_request():
    received_time = datetime.now()
    # Retrieve the request_id from the data
    request_id = request.form.get('request_id')
    if request_id is None:
        return 'Request ID is missing', 400
    print(f'Received request with ID {request_id}')
    arrival_rate = request.form.get('arrival_rate')
    # Get the uploaded message and item files
    csr_file = request.files.get('csr')
    signed_csr_file = request.files.get('signed_csr')

    if not csr_file or not signed_csr_file:
        return 'Both CSR and Signed CSR files are required', 400

    # Process the request and generate a response file
    response_filename, start_time = process_requests(request_id, (csr_file.read(), signed_csr_file.read()), received_time, arrival_rate)
    if response_filename == 'file_could_not_be_verified.txt':
        return f"Signature verification or certificate generation failed", 503

    try:
        # Prepare the response to include both the file and start_time
        response = send_file(response_filename, as_attachment=True)
        
        # Add custom headers or additional info in the response
        response.headers['X-Start-Time'] = start_time
        
        return response
    except Exception as e:
        return f"Error sending the response file: {e}", 500

if __name__ == '__main__':
    
    # Run the Flask app for the server
    app.run(debug=True, host='146.230.192.168', port=5001)
