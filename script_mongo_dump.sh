#!/usr/bin/env bash


echo "*******  COMPLEX CARE LAB  *******"
echo "MAPPING HEALTH SYSTEM"
echo "*******  *******  *******  *******"
echo "...."

export COMPOSE_HTTP_TIMEOUT=300000

echo "DUMPING MONGO"

docker-compose -f docker-compose-CeleryExecutor.yml exec mongo mongodump  --username "root" --password "password" --gzip --archive="/data/dump/V01.3.gz"

exit 0