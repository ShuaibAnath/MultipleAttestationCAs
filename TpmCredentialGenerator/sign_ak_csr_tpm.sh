#!/bin/bash

AK_CSR=$1 
AK_CSR_SIGNED=$2
RSA_PRIMARY_KEY_CONTEXT="rsa.ctx"
RSA_PRIMARY_PUBLIC_KEY="rsa_public_key.pem"
AK_CSR_DIGEST_BIN="ak_csr.digest.bin"

# Commands that have been previously run:
    # tpm2_createprimary -C e -c primary.ctx
    # tpm2_create -G rsa -u rsa.pub -r rsa.priv -C primary.ctx

tpm2_load -C primary.ctx -u rsa.pub -r rsa.priv -c $RSA_PRIMARY_PUBLIC_KEY

# Create digest for primary key signature
openssl dgst -sha256 -binary $AK_CSR > $AK_CSR_DIGEST_BIN

# Sign the AK CSR
tpm2_sign -Q -c $RSA_PRIMARY_KEY_CONTEXT -g sha256 -s rsapss -f plain -o $AK_CSR_SIGNED $AK_CSR_DIGEST_BIN

# Get created RSA primary public key in .pem format for use with OpenSSL
tpm2_readpublic -c $RSA_PRIMARY_KEY_CONTEXT -o $RSA_PRIMARY_PUBLIC_KEY -f pem

openssl dgst -verify $RSA_PRIMARY_PUBLIC_KEY -keyform pem -sigopt rsa_padding_mode:pss -sha256 -signature $AK_CSR_SIGNED $AK_CSR_DIGEST_BIN