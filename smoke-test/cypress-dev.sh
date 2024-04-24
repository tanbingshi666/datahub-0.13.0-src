#!/bin/bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

if [ "${RUN_QUICKSTART:-true}" == "true" ]; then
    source ./run-quickstart.sh
fi

source venv/bin/activate

export KAFKA_BROKER_CONTAINER="datahub-kafka-broker-1"
export KAFKA_BOOTSTRAP_SERVER="broker:9092"
python -c 'from tests.cypress.integration_test import ingest_data; ingest_data()'

cd tests/cypress
npm install

source "$DIR/set-cypress-creds.sh"

npx cypress open \
   --env "ADMIN_DISPLAYNAME=$CYPRESS_ADMIN_DISPLAYNAME,ADMIN_USERNAME=$CYPRESS_ADMIN_USERNAME,ADMIN_PASSWORD=$CYPRESS_ADMIN_PASSWORD"
