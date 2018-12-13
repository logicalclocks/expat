#!/bin/bash

set -e

if [ $# -ne 4 ] ; then
    echo "Usage: jupyter_migrate.sh install project conda_path conda_user"
    exit 1
fi

OP=$1
PROJECT=$2
CONDA_DIR=$3
CONDA_USER=$4
SPARKMAGIC_DIR=$CONDA_DIR/sparkmagic

if [ "$OP" == "install" ] ; then
    # Install packages
    su "$CONDA_USER" -c "yes | ${CONDA_DIR}/envs/${PROJECT}/bin/pip install --no-cache-dir --upgrade hdfscontents urllib3 requests jupyter pandas"
    if [ $? -ne 0 ] ; then
        exit 2
    fi

    # Install packages to allow users to manage their jupyter extensions
    su "$CONDA_USER" -c "yes | ${CONDA_DIR}/envs/${PROJECT}/bin/pip install --no-cache-dir --upgrade jupyter_contrib_nbextensions jupyter_nbextensions_configurator"
    if [ $? -ne 0 ] ; then
        exit 2
    fi

    su "$CONDA_USER" -c "yes | ${CONDA_DIR}/envs/${PROJECT}/bin/pip install --no-cache-dir --upgrade $SPARKMAGIC_DIR/hdijupyterutils $SPARKMAGIC_DIR/autovizwidget $SPARKMAGIC_DIR/sparkmagic"
    if [ $? -ne 0 ] ; then
        exit 2
    fi

    # Enable kernels
    su "$CONDA_USER" -c "cd ${CONDA_DIR}/envs/${PROJECT}/lib/python*/site-packages && ${CONDA_DIR}/envs/${PROJECT}/bin/jupyter-kernelspec install sparkmagic/kernels/sparkkernel --sys-prefix"
    if [ $? -ne 0 ] ; then
        exit 2
    fi

    su "$CONDA_USER" -c "cd ${CONDA_DIR}/envs/${PROJECT}/lib/python*/site-packages && ${CONDA_DIR}/envs/${PROJECT}/bin/jupyter-kernelspec install sparkmagic/kernels/pysparkkernel --sys-prefix"
    if [ $? -ne 0 ] ; then
        exit 2
    fi

    su "$CONDA_USER" -c "cd ${CONDA_DIR}/envs/${PROJECT}/lib/python*/site-packages && ${CONDA_DIR}/envs/${PROJECT}/bin/jupyter-kernelspec install sparkmagic/kernels/pyspark3kernel --sys-prefix"
    if [ $? -ne 0 ] ; then
        exit 2
    fi

    su "$CONDA_USER" -c "cd ${CONDA_DIR}/envs/${PROJECT}/lib/python*/site-packages && ${CONDA_DIR}/envs/${PROJECT}/bin/jupyter-kernelspec install sparkmagic/kernels/sparkrkernel --sys-prefix"
    if [ $? -ne 0 ] ; then
        exit 2
    fi

    # Enable jupyter notebook extensions
    su "$CONDA_USER" -c "${CONDA_DIR}/envs/${PROJECT}/bin/jupyter contrib nbextension install --sys-prefix"
    su "$CONDA_USER" -c "${CONDA_DIR}/envs/${PROJECT}/bin/jupyter serverextension enable jupyter_nbextensions_configurator --sys-prefix"

elif [ "$OP" == "remove" ] ; then

    su "$CONDA_USER" -c "yes | ${CONDA_DIR}/envs/${PROJECT}/bin/pip uninstall hdfscontents jupyter sparkmagic"

else
    exit -1
fi
