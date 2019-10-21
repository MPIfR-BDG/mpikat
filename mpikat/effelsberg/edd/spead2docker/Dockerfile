FROM nvidia/cuda:8.0-devel-ubuntu16.04

MAINTAINER Ewan Barr "ebarr@mpifr-bonn.mpg.de"

ENV DEBIAN_FRONTEND noninteractive

RUN echo 'deb http://us.archive.ubuntu.com/ubuntu trusty main multiverse' >> /etc/apt/sources.list && \
    mkdir /var/run/sshd && \
    apt-get -y check && \
    apt-get -y update && \
    apt-get install -y apt-utils apt-transport-https software-properties-common python-software-properties && \
    apt-get -y update --fix-missing && \
    apt-get -y upgrade

RUN apt-get --no-install-recommends -y install \
    build-essential \
    autoconf \
    autogen \
    automake \
    autotools-dev \
    ca-certificates \
    cmake \
    csh \
    cvs \
    expect \
    gcc \
    gfortran \
    git \
    gsl-bin \
    hwloc \
    libboost1.58-all-dev \
    libcfitsio-dev \
    libgsl-dev \
    libgsl2 \
    numactl \
    libhwloc-dev \
    libltdl-dev \
    libtool \
    librdmacm-dev \
    libibverbs-dev \
    ntp \
    ntpstat \
    openssl \
    pkg-config \
    python-setuptools \
    python2.7 \
    python2.7-dev \
    wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists && \
    easy_install pip

RUN pip install \
    coloredlogs \
    jinja2 \
    posix_ipc \
    mock

#Download and install Melladnox OFED 4.3 for Ubuntu 16.04
RUN wget http://content.mellanox.com/ofed/MLNX_OFED-4.3-1.0.1.0/MLNX_OFED_LINUX-4.3-1.0.1.0-ubuntu16.04-x86_64.tgz && \
    tar -xzvf MLNX_OFED_LINUX-4.3-1.0.1.0-ubuntu16.04-x86_64.tgz && \
    MLNX_OFED_LINUX-4.3-1.0.1.0-ubuntu16.04-x86_64/mlnxofedinstall --force \
     --user-space-only --without-fw-update --all -q && \
    cd .. && \
    rm -rf MLNX_OFED_LINUX-4.3-1.0.1.0-ubuntu16.04-x86_64 && \
    rm -rf *.tgz

ENV PSRHOME /software/
ENV OSTYPE linux
RUN mkdir -p $PSRHOME
WORKDIR $PSRHOME

RUN pip install -e \
    git+https://github.com/ska-sa/katportalclient.git#egg=katportalclient \
    ipaddress

RUN git clone https://gitlab.mpifr-bonn.mpg.de/wchen/Beamforming.git && \
    cd Beamforming && \
    pip install -e .

RUN git clone https://github.com/ewanbarr/mpikat.git &&\
    cd mpikat && \
    git checkout fbf_control_worker && \
    pip install -e .

ENV PACKAGES /usr/local/cuda-8.0/

RUN pip install spead2 


