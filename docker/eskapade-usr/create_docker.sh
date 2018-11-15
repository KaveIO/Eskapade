#! /bin/bash

echo 'Building docker image'
docker build -t kave/eskapade-usr:0.8.2 .
