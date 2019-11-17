#!/usr/bin/env bash


echo "*******  COMPLEX CARE LAB  *******"
echo "MAPPING HEALTH SYSTEM"
echo "*******  *******  *******  *******"
echo "...."

#$delete_mongo = 1

#Sets a timeout for docker-compose because mongorestore can take some
export COMPOSE_HTTP_TIMEOUT=300000


echo "CREATING AIRFLOW POOLS"
sleep 5

docker-compose -f docker-compose-CeleryExecutor.yml exec webserver  /entrypoint.sh airflow pool -s POOL_SCA_FILE 10 POOL_SCA_FILE
sleep 5
docker-compose -f docker-compose-CeleryExecutor.yml exec webserver  /entrypoint.sh airflow pool -s POOL_SCA_MEMORY 6 POOL_SCA_MEMORY
sleep 5
docker-compose -f docker-compose-CeleryExecutor.yml exec webserver  /entrypoint.sh airflow pool -s POOL_SCA_MONGO 10 POOL_SCA_MONGO

echo "FINISHED"

echo "SETTING RABBITMQ DEFAULTS"
sleep 5

docker-compose -f docker-compose-CeleryExecutor.yml exec  rabbitmq rabbitmqctl set_disk_free_limit 50MB



exit 0