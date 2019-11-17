#!/usr/bin/env bash


echo "*******  COMPLEX CARE LAB  *******"
echo "MAPPING HEALTH SYSTEM"
echo "*******  *******  *******  *******"
echo "...."

#$delete_mongo = 1

#Sets a timeout for docker-compose because mongorestore can take some
export COMPOSE_HTTP_TIMEOUT=300000

echo "STOPPING RUNNING CONTAINERS"
sleep 3
docker-compose -f docker-compose-CeleryExecutor.yml down

if [ -z  "$MHS_DATALINK" ]
then
echo "MHS_DATALINK NOT DEFINED"
exit -1
else
echo "USING MHS_DATALINK=$MHS_DATALINK"
fi

echo "DELETING AIRFLOW DATA!!!!"
sleep 5
if [ -d "$MHS_DATALINK/airflow" ]
then
rm -R $MHS_DATALINK/postgres/airflow
fi

echo "DELETING MONGO DATA!!!!"
sleep 5
if [ -d "$MHS_DATALINK/mongo/mhs/data" ]
then
rm -R $MHS_DATALINK/mongo/mhs/data
fi

echo "EXECUTING docker-compose"
docker-compose -f docker-compose-CeleryExecutor.yml down
docker-compose -f docker-compose-CeleryExecutor.yml up -d --build

exit 0