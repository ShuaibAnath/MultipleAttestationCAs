#!/bin/bash

# Define the base directory to store AK credentials
AK_CREDENTIALS_DIR="AKCredentials"

# Create the base directory if it doesn't exist
mkdir -p $AK_CREDENTIALS_DIR

# Define common variables for AK creation
EK_KEY_HANDLE="0x81010001"  # EK handle is 0x81010001
SIG_SCHEME="rsapss"
AK_KEY_HANDLE_BASE="0x81010002"  # Base handle, will be incremented for each key
EK_PUB_KEY_PEM_FILE="TpmEndorsementCredentials/EK_PUBLIC_KEY.pem"
AK_CSR_DIGEST_BIN="attestation_key_csr.digest.bin"

# Loop to create 5 attestation keys
for i in {1..3}; do
    # Define a sub-directory for each AK
    AK_SUBDIR="${AK_CREDENTIALS_DIR}/key_${i}"

    # Create the sub-directory for storing attestation files
    mkdir -p $AK_SUBDIR

    # Define unique file names for each AK within its sub-directory
    AK_CTX_FILE="${AK_SUBDIR}/attestation_key_${i}.ctx"
    AK_PUB_KEY_FILE="${AK_SUBDIR}/attestation_key_${i}.pub"
    AK_NAME_FILE="${AK_SUBDIR}/attestation_key_${i}.name"
    AK_PUB_KEY_PEM_FILE="${AK_SUBDIR}/attestation_key_${i}.pem"
    AK_CSR_PEM="${AK_SUBDIR}/attestation_key_${i}.csr"

    # Generate unique AK key handle by converting the index to a valid handle format
    AK_KEY_HANDLE="0x8101000$((i+1))"  # Creating handles like 0x81010002, 0x81010003, ...

    # Run the tpm2_createak command with the variables
    echo "Creating AK $i with handle $AK_KEY_HANDLE..."
    tpm2_createak -C $EK_KEY_HANDLE -c $AK_CTX_FILE -G rsa -g sha256 -s $SIG_SCHEME -u $AK_PUB_KEY_FILE -n $AK_NAME_FILE

    # To evict control of an NV-index, first use the owner hierarchy to take control
    tpm2_evictcontrol -C o -c $AK_KEY_HANDLE # Use the correct handle format
    # Then evict control for newly created AK pair
    tpm2_evictcontrol -C o -c $AK_CTX_FILE $AK_KEY_HANDLE

    # Get AK public key in .pem format for use with OpenSSL
    tpm2_readpublic -c $AK_CTX_FILE -o $AK_PUB_KEY_PEM_FILE -f pem

    # Generate AK CSR
    openssl req -provider tpm2 -provider default -propquery '?provider=tpm2' \
                -new -subj "/C=SA/CN=FogNode${i}" -key handle:$AK_KEY_HANDLE \
                -out $AK_CSR_PEM

    echo "AK $i and related files created in $AK_SUBDIR"
done

echo "All attestation keys and related data have been created successfully."
