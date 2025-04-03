#!/bin/bash

AK_CSR=$1
AK_CERT=$2
# Sign the certificate created previously with the same private key: 
openssl x509 -req -days 365 -in $AK_CSR -signkey CAkeys/ca1_private_key.pem -out $AK_CERT