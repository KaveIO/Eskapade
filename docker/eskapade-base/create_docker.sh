#! /bin/bash

echo 'Building docker image'
docker build -t kave/eskapade-base:0.8.4 .
