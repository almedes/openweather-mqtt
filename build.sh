#!/bin/bash
export DOCKER_CLI_EXPERIMENTAL=enabled
docker buildx create --use --name build --node build --driver-opt network=host
docker buildx build --platform linux/amd64 -t $USER/${PWD##*/} --load .
