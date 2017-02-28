#!/bin/bash

MODULES_ZIP_FILE="${ESKAPADE}/ae_python_modules.zip"
if ! [ -z "${1}" ]; then
    cd "${ESKAPADE}"
    MODULES_ZIP_FILE="$(realpath ${1})"
fi
rm -rf "${MODULES_ZIP_FILE}"

if ! [ -z "${WORKDIRROOT}" ]; then
    echo "Adding WorkDir Python modules to ZIP archive ${MODULES_ZIP_FILE}"
    cd "${WORKDIRROOT}/python"
    zip -q -r "${MODULES_ZIP_FILE}" *.py
    zip -q -r "${MODULES_ZIP_FILE}" */*.py
fi

echo "Adding Python modules to ZIP archive ${MODULES_ZIP_FILE}"
cd "${ESKAPADE}/python"
zip -q -r "${MODULES_ZIP_FILE}" */*.py
