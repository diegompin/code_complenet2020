#!/usr/bin/env bash


echo "EXECUTING docker-compose"
docker-compose -f docker-compose-CeleryExecutor.yml down
docker-compose -f docker-compose-CeleryExecutor.yml up -d --build
sleep 30

echo "SETTING RABBITMQ DEFAULTS"

docker-compose -f docker-compose-CeleryExecutor.yml exec  rabbitmq rabbitmqctl set_disk_free_limit 50MB

exit 0