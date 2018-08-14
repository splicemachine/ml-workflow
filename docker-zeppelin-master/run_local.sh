#!/usr/bin/env bash

docker build -t zeppelin .
docker run -p 8080:8080 -p 54321:54321 --name zeppelin --network mynetwork zeppelin