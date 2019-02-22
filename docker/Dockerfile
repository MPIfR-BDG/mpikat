FROM ubuntu:16.04

ENV DEBIAN_FRONTEND noninteractive

RUN echo 'deb http://us.archive.ubuntu.com/ubuntu trusty main multiverse' >> /etc/apt/sources.list && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends \
    git \
    gcc \
    openssl \
    ca-certificates \
	ntp \
	ntpstat \
    python2.7 \
    python2.7-dev \
    python-setuptools && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists && \
    easy_install pip

RUN pip install coloredlogs jinja2 posix_ipc mock 
	
RUN pip install -e \
	git+https://github.com/ska-sa/katportalclient.git#egg=katportalclient \
	ipaddress

RUN git clone https://gitlab.mpifr-bonn.mpg.de/wchen/Beamforming.git && \
	cd Beamforming && \
	pip install -e .

RUN git clone https://github.com/ewanbarr/mpikat.git &&\
	cd mpikat &&\
	pip install -e .

ENV ARSE 2
COPY startup.sh startup.sh
RUN chmod +x startup.sh
COPY ntp.conf /etc/ntp.conf
ENTRYPOINT ["./startup.sh"]
