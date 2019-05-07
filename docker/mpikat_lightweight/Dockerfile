FROM ubuntu:16.04


RUN apt-get update && apt-get install -y     git     build-essential     python2.7     python2.7-dev     python-setuptools     ntp     ntpstat     vim  && rm -rf /var/lib/apt/lists/*


### pip
RUN easy_install pip &&     pip install --upgrade pip


### Python packages
RUN pip install -e    git+https://github.com/ska-sa/katportalclient.git#egg=katportalclient    ipaddress

RUN git clone https://gitlab.mpifr-bonn.mpg.de/wchen/Beamforming.git &&    cd Beamforming &&    pip install -e .

RUN git clone https://github.com/ewanbarr/mpikat.git &&   cd mpikat &&    git checkout fbf_control_worker &&    pip install -e .

# Smart history search with arrow keys
RUN echo "\"\e[A\":history-search-backward" >> ~/.inputrc &&     echo "\"\e[B\":history-search-forward" >> ~/.inputrc

# Configure vim to indent with 4 spaces and behave nicely in general
# https://stackoverflow.com/questions/234564/tab-key-4-spaces-and-auto-indent-after-curly-braces-in-vim
RUN echo "filetype plugin indent on" >> ~/.vimrc &&     echo "set tabstop=4" >> ~/.vimrc &&     echo "set shiftwidth=4" >> ~/.vimrc &&     echo "set expandtab" >> ~/.vimrc &&     echo "set pastetoggle=<F2>" >> ~/.vimrc &&     echo "set hlsearch" >> ~/.vimrc

COPY startup.sh startup.sh
RUN chmod +x startup.sh
COPY ntp.conf /etc/ntp.conf
ENTRYPOINT ["./startup.sh"]
