# !/bin/bash

docker rmi -f $(docker images -aq)
docker builder prune
lithops clean
