#! /bin/bash

echo 'Building docker image'
docker build -t kave/eskapade-env:0.8 .
