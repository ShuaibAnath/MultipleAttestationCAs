#!/bin/bash

# Define the base directory to store AK credentials
AK_CREDENTIALS_DIR="AKCredentials"

# Create the base directory if it doesn't exist
mkdir -p $AK_CREDENTIALS_DIR

# Define common variables for AK creation
EK_KEY_HANDLE="0x81010001"  # EK handle is 0x81010001
SIG_SCHEME="rsapss"
AK_KEY_HANDLE_BASE=0x81010002  # Base handle, will be incremented for each key
EK_PUB_KEY_PEM_FILE="ek_public.pem"

# Create an RSA primary key context (this is done once)
# Ensure the necessary files for the primary key exist
PRIMARY_OBJECT_CONTEXT="RsaPrimaryObjectCredentials/primary.ctx"
RSA_PRIMARY_KEY_CONTEXT="RsaPrimaryObjectCredentials/rsa.ctx"
RSA_PRIMARY_PUBLIC_KEY="RsaPrimaryObjectCredentials/rsa_public_key.pem" 
RSA_PUB="RsaPrimaryObjectCredentials/rsa.pub" 
RSA_PRIV="RsaPrimaryObjectCredentials/rsa.priv"

# Loop to create 5 attestation keys, attestation key CSRs, and signed attestation key CSRs
for ((i=0; i<5; i++)); do
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
    AK_CSR_SIGNED="${AK_SUBDIR}/attestation_key_${i}_signed.csr"
    AK_CSR_DIGEST_BIN="${AK_SUBDIR}/attestation_key_csr_${i}.digest.bin"

    # Generate unique AK key handle by converting the index to a valid handle format
    # AK_KEY_HANDLE="0x8101000a"
    # tpm2-tools has issues with this handle because of differences in uppercase and lowercase.
    # Also, generating the hex value to input to tpm2-tools becomes unnecessarily complex, refer to create_multiple_aks_and_csrs.sh for simpler implementation
    AK_KEY_HANDLE="0x$(printf '%08X' $((AK_KEY_HANDLE_BASE + i - 1)) | tr 'A-Z' 'a-z')"
    # AK_KEY_HANDLE="0x$(printf '%08X' $((AK_KEY_HANDLE_BASE + i - 1)))"

    # Step 1: Create AK under EK hierarchy
    echo "Creating AK $i with handle $AK_KEY_HANDLE..."
    tpm2_createak -C $EK_KEY_HANDLE -c $AK_CTX_FILE -G rsa -g sha256 -s $SIG_SCHEME -u $AK_PUB_KEY_FILE -n $AK_NAME_FILE

    # Step 2: To evict control of an NV-index, first use the owner hierarchy to take control
    tpm2_evictcontrol -C o -c $AK_KEY_HANDLE # Use the correct handle format
    # Then evict control for newly created AK pair
    tpm2_evictcontrol -C o -c $AK_CTX_FILE $AK_KEY_HANDLE

    # Step 3: Get AK public key in .pem format for use with OpenSSL
    tpm2_readpublic -c $AK_CTX_FILE -o $AK_PUB_KEY_PEM_FILE -f pem

    # Step 4: Generate AK CSR
    openssl req -provider tpm2 -provider default -propquery '?provider=tpm2' \
                -new -subj "/C=SA/CN=FogNode${i}" -key handle:$AK_KEY_HANDLE \
                -out $AK_CSR_PEM

    # Step 5: Sign the AK CSR using the RSA primary key context
    echo "Signing the CSR for AK $i..."
    tpm2_load -C $PRIMARY_OBJECT_CONTEXT -u $RSA_PUB -r $RSA_PRIV -c $RSA_PRIMARY_KEY_CONTEXT

    # Create digest for primary key signature
    openssl dgst -sha256 -binary $AK_CSR_PEM > $AK_CSR_DIGEST_BIN

    # Sign the AK CSR using the primary RSA key
    tpm2_sign -Q -c $RSA_PRIMARY_KEY_CONTEXT -g sha256 -s rsapss -f plain -o $AK_CSR_SIGNED $AK_CSR_DIGEST_BIN

    # Step 6: Verify the signature
    tpm2_readpublic -c $RSA_PRIMARY_KEY_CONTEXT -o $RSA_PRIMARY_PUBLIC_KEY -f pem
    openssl dgst -verify $RSA_PRIMARY_PUBLIC_KEY -keyform pem -sigopt rsa_padding_mode:pss -sha256 -signature $AK_CSR_SIGNED $AK_CSR_DIGEST_BIN

    # After processing each CSR and signing it, print success
    echo "CSR for AK $i signed and verified successfully. Files saved in $AK_SUBDIR"
done
echo "All attestation keys and related data have been created, signed, and verified successfully."
