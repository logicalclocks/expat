#!/usr/bin/env bash

if [ $# -ne 4  ]; then
 echo "Usage: $0 IDENTIFIER KEYSTORE_PASSWORD VALIDITY_DAYS CA_PASSWORD"
 exit 1
fi

set -e

#Variables
IDENTIFIER="$1"
KEYSTOREPW="$2"
VALIDITY_DAYS="$3"


CA_PASSWORD="$4"
cd /srv/hops/certs-dir/intermediate/

## 1. Create a user key
openssl genrsa \
      -out private/"${IDENTIFIER}".key.pem 2048

chmod 400 private/"${IDENTIFIER}".key.pem

## 2. Create a user certificate
#       -subj "/C=${COUNTRY_CODE}/ST=${CITY}/L=${ORCID}/O=${ORG}/CN=${IDENTIFIER}/emailAddress=${EMAIL}" \
openssl req -config ../openssl-ca.cnf \
       -subj "/CN=${IDENTIFIER}" \
       -passin pass:"$KEYSTOREPW" -passout pass:"$KEYSTOREPW" \
       -key private/"${IDENTIFIER}".key.pem \
       -new -sha256 -out csr/"${IDENTIFIER}".csr.pem

openssl ca -batch -config openssl-intermediate.cnf \
      -passin pass:"${CA_PASSWORD}" \
      -extensions usr_cert -days ${VALIDITY_DAYS} -notext -md sha256 \
      -in csr/"${IDENTIFIER}".csr.pem \
      -out certs/"${IDENTIFIER}".cert.pem

chmod 444 certs/"${IDENTIFIER}".cert.pem

## 3. Verify the intermediate certificate
# openssl verify -CAfile certs/ca-chain.cert.pem certs/${IDENTIFIER}.cert.pem

## 4. Create bundle of private and public key
openssl pkcs12 -export -in certs/"${IDENTIFIER}".cert.pem -inkey private/"${IDENTIFIER}".key.pem -out cert_and_key.p12 -name "${IDENTIFIER}" -CAfile certs/intermediate.cert.pem -caname intermediate -password pass:"${KEYSTOREPW}"

## 5. Create keystore and import key-pair
keytool -importkeystore -destkeystore "${IDENTIFIER}"__kstore.jks -srckeystore cert_and_key.p12 -srcstoretype PKCS12 -alias "${IDENTIFIER}" -srcstorepass "${KEYSTOREPW}" -deststorepass "${KEYSTOREPW}" -destkeypass "${KEYSTOREPW}"

## 6. Create client and intermediate CA bundle
cat certs/"${IDENTIFIER}".cert.pem certs/intermediate.cert.pem > /tmp/"${IDENTIFIER}"_bundle.pem

## 7. Import bundle to keystore
keytool -importcert -noprompt -keystore "${IDENTIFIER}"__kstore.jks -alias "${IDENTIFIER}" -file /tmp/"${IDENTIFIER}"_bundle.pem -storepass "${KEYSTOREPW}"

## 8. Create truststore and import Hops Root CA
keytool -importcert -noprompt -trustcacerts -alias hops_root_ca -file ../certs/ca.cert.pem -keystore "${IDENTIFIER}"__tstore.jks -deststorepass "${KEYSTOREPW}"

chown -R glassfish:glassfish "${IDENTIFIER}"__kstore.jks "${IDENTIFIER}"__tstore.jks
mv  "${IDENTIFIER}"__kstore.jks /tmp/"${IDENTIFIER}"__kstore.jks
mv  "${IDENTIFIER}"__tstore.jks /tmp/"${IDENTIFIER}"__tstore.jks
rm cert_and_key.p12
rm /tmp/"${IDENTIFIER}"_bundle.pem
