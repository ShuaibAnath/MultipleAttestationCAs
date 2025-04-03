#!/bin/bash

AK_CSR=$1
RSA_PRIMARY_PUBLIC_KEY=$2
SIGNED_CSR=$3
AK_CSR_DIGEST_BIN=$4
# Add variable for AK certificate file name that will be received from python script

# Create digest for primary key signature
openssl dgst -sha256 -binary $AK_CSR > $AK_CSR_DIGEST_BIN

# Verify RSA primary object private key signature
openssl dgst -verify $RSA_PRIMARY_PUBLIC_KEY -keyform pem -sigopt rsa_padding_mode:pss -sha256 -signature $SIGNED_CSR $AK_CSR_DIGEST_BIN
