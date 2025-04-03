#!/bin/bash

PRIMARY_KEY_CONTEXT="primary.ctx"
ENDORSEMENT_HIERARCHY="e"
RSA_PRIMARY_PUBLIC_KEY="rsa.pub"
RSA_PRIMARY_PRIVATE_KEY="rsa.priv"

# Step 1: Create the Primary Key
tpm2_createprimary -C $ENDORSEMENT_HIERARCHY -g sha256 -G rsa -o $PRIMARY_KEY_CONTEXT

# Step 2: Create an object under the primary object. Public and private portions
tpm2_create -G rsa -u rsa.pub -r rsa.priv -C $PRIMARY_KEY_CONTEXT


