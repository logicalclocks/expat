#!/usr/bin/env bash
set -e

INTERMEDIATE_CA_KEY_PASS=CHANGEME
INTERMEDIATE_CA_CONFIG=/srv/hops/certs-dir/intermediate/openssl-intermediate.cnf
INTERMEDIATE_CA_CRL=/srv/hops/certs-dir/intermediate/crl/intermediate.crl.pem

OPENSSL=/usr/bin/openssl

function _help {
    echo "Revoke certificates"
    echo "Usage: $0 CERTIFICATES_DIRECTORY"
}

if [ $# -ne 1 ]
then
    _help
    exit 2
fi

DIR=$1

files=$(find "$DIR" -name "*.cert.pem")
for f in $files
do
    echo "Revoking $f..."
    $OPENSSL ca -batch -config $INTERMEDIATE_CA_CONFIG -passin pass:$INTERMEDIATE_CA_KEY_PASS -revoke "$f"
    echo "Revoked $f"
done

echo "Updating Certificate Revocation List..."
$OPENSSL ca -batch -config $INTERMEDIATE_CA_CONFIG -gencrl -passin pass:$INTERMEDIATE_CA_KEY_PASS -out "$INTERMEDIATE_CA_CRL"
echo "Updated CRL $INTERMEDIATE_CA_CRL"
