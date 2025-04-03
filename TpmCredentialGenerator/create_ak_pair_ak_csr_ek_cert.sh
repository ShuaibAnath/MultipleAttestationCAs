#!/bin/bash

# Define variables for the options
AK_PUB_KEY_PEM_FILE=$1
AK_CSR_PEM=$2 
EK_KEY_HANDLE="0x81010001" # EK handle is 0x81010001 
AK_CTX_FILE="attestation_key.ctx"
AK_PUB_KEY_FILE="attestation_key.pub"
AK_NAME_FILE="attestation_key.name"
SIG_SCHEME="rsapss"
AK_KEY_HANDLE="0x81010002"
EK_PUB_KEY_PEM_FILE="ek_public.pem" 

# Run the tpm2_createak command with the variables
# Create AK under EK hierarchy
tpm2_createak -C $EK_KEY_HANDLE -c $AK_CTX_FILE -G rsa -g sha256 -s $SIG_SCHEME -u $AK_PUB_KEY_FILE  -n $AK_NAME_FILE

# To evict control of an NV-index, first use the owner hierarchy to take control
tpm2_evictcontrol -C o -c $AK_KEY_HANDLE # Example use of nv-index 0x81010002
# Then evict control for newly created AK pair
tpm2_evictcontrol -C o -c $AK_CTX_FILE $AK_KEY_HANDLE

# Get AK public key in .pem format for use with OpenSSL
tpm2_readpublic -c $AK_CTX_FILE -o $AK_PUB_KEY_PEM_FILE -f pem

# Generate AK CSR
openssl req -provider tpm2 -provider default -propquery '?provider=tpm2' \
            -new -subj "/C=SA/CN=FogNode3-0" -key handle:$AK_KEY_HANDLE \
            -out $AK_CSR_PEM