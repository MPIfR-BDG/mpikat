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

# Install SPEAD2
WORKDIR $PSRHOME
RUN git clone https://github.com/ska-sa/spead2.git && \
    cd spead2 && \
    ./bootstrap.sh --no-python && \
    ./configure --prefix=/usr/local && \
    make -j 6 && \
    make install && \
    make clean
    ENV PACKAGES $PACKAGES $PSRHOME

# Install PSRDADA
COPY psrdada_cvs_login $PSRHOME
RUN  chmod +x psrdada_cvs_login &&\
    ./psrdada_cvs_login && \
    cvs -z3 -d:pserver:anonymous@psrdada.cvs.sourceforge.net:/cvsroot/psrdada co -P psrdada
ENV PSRDADA_HOME $PSRHOME/psrdada
WORKDIR $PSRDADA_HOME
RUN mkdir build/ && \
    ./bootstrap && \
    ./configure --prefix=$PSRDADA_HOME/build && \
    make && \
    make install && \
    make clean
ENV PATH $PATH:$PSRDADA_HOME/build/bin
ENV PSRDADA_BUILD $PSRDADA_HOME/build/
ENV PACKAGES $PACKAGES $PSRDADA_BUILD

WORKDIR $PSRHOME
RUN git clone https://github.com/ewanbarr/psrdada_cpp.git && \
    cd psrdada_cpp/ &&\
    git checkout fbfuse &&\
    git submodule init &&\
    git submodule update &&\
    mkdir build/ &&\
    cd build/ &&\
    cmake -DENABLE_CUDA=true ../ &&\
    make -j 4 &&\
    make install

#install MKRecv
WORKDIR $PSRHOME
RUN git config --global http.sslverify false && \
    git clone https://gitlab.mpifr-bonn.mpg.de/mhein/mkrecv.git && \
    cd mkrecv && \
    git checkout master && \
    cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DPSRDADA_CPP_INCLUDE_DIR=$PSRHOME/psrdada_cpp/ -DENABLE_CUDA=true && \
    make -j 6 && \
    make install

#install MKSend
WORKDIR $PSRHOME
RUN git config --global http.sslverify false && \
    git clone https://gitlab.mpifr-bonn.mpg.de/mhein/mksend.git && \
    cd mksend && \
    git checkout master && \
    cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DPSRDADA_CPP_INCLUDE_DIR=$PSRHOME/psrdada_cpp/ -DENABLE_CUDA=true && \
    make -j 6 && \
    make install

RUN env | awk '{print "export ",$0}' > $HOME/.profile && \
    echo "source $HOME/.profile" >> $HOME/.bashrc

COPY startup.sh startup.sh
RUN chmod +x startup.sh
COPY ntp.conf /etc/ntp.conf
ENTRYPOINT ["./startup.sh"]