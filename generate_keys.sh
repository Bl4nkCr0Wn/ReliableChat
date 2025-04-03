#!/bin/bash

for i in {1..5}
do
    # Generate private key
    openssl genpkey -algorithm RSA -out private_key_$i.pem -pkeyopt rsa_keygen_bits:2048

    # Extract the public key from the private key
    openssl rsa -pubout -in private_key_$i.pem -out public_key_$i.pem

    echo "Generated key pair $i"
done
