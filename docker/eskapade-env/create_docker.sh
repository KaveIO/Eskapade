#! /bin/bash

ESKAPADE_TAR=eskapade.tar.gz

if [ -f ${ESKAPADE_TAR} ]; then
	echo 'Cleaning up old' ${ESKAPADE_TAR}
    	rm -f "${ESKAPADE_TAR}"
fi

echo 'Creating new' ${ESKAPADE_TAR}
cd ../..
COPYFILE_DISABLE=1 tar -czf ${ESKAPADE_TAR} --exclude 'results'             \
	                                    --exclude 'build'               \
			                    --exclude 'python/eskapade/lib' \
			                    --exclude '__pycache__'         \
			                    ./*
cd -
mv ../../${ESKAPADE_TAR} .

echo 'Building docker image'
docker build -t kave/eskapade-env:0.7 .
