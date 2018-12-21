#!/usr/bin/env python

# docker run --runtime=nvidia -e NVIDIA_VISIBLE_DEVICES=all -e NVIDIA_DRIVER_CAPABILITIES=all --rm -it --ulimit memlock=40000000000 -v host_dir:doc_dir --net=host xinpingdeng/fold_mode
# docker run --runtime=nvidia tells the container that we will use nvidia/cuda library at runtime;
# --rm means the container will be released once it finishs;
# -i means it is a interactive container;
# -t oallocate a pseudo-TTY;
# as single character can be combined, we use -it instead of -i and -t here;
# --ulimit memlock=XX tell container to use XX bytes locked shared memory, NOTE, here is the size of shared memory used by all docker containers on host machine, not the current one;
# --net=host let container to use host network configuration;
# -e NVIDIA_DRIVER_CAPABILITIES controls which driver libraries/binaries will be mounted inside the container;
# -e NVIDIA_VISIBLE_DEVICES which GPUs will be made accessible inside the container;
# -v maps the host directory with the directory inside container, if the directories do not exist, docker will create them;
# Detail on how to setup nvidia docker image can be found at https://github.com/NVIDIA/nvidia-container-runtime;

import os, argparse

# Read in command line arguments
parser = argparse.ArgumentParser(description='Launch the pipeline to catpure and fold data stream from BMF or from PSRDADA file')
parser.add_argument('-a', '--image', type=str, nargs='+',
                    help='The name of docker image')
parser.add_argument('-b', '--suffix', type=int, nargs='+',
                    help='The suffix of docker container name')
parser.add_argument('-c', '--root', type=int, nargs='+',
                    help='To run the docker as root or not')

args    = parser.parse_args()
suffix  = args.suffix[0]
image   = args.image[0]
root    = args.root[0]
hdir    = '/home/pulsar/'
ddir    = '/beegfs/'
numa    = int(suffix/2.) # only works with 4 beams per GPU node configuration
gpu     = numa
dvolume = '{:s}:{:s}'.format(ddir, ddir)
hvolume = '{:s}:{:s}'.format(hdir, hdir)

if root:
    comline = "docker run --privileged --cap-add=SYS_PTRACE --security-opt seccomp=unconfined -it --rm --runtime=nvidia --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/rdma_cm -e DISPLAY --net=host -v {:s} -v {:s} -v /tmp:/tmp -e NVIDIA_VISIBLE_DEVICES={:d} -e NVIDIA_DRIVER_CAPABILITIES=all --cap-add=IPC_LOCK --ulimit memlock=-1:-1 --cpuset-mems={:d} --name {:s}.{:d} xinpingdeng/{:s}".format(dvolume, hvolume, gpu, numa, image, suffix, image)
else:    
    comline = "docker run --cap-add=SYS_PTRACE --security-opt seccomp=unconfined -it --rm --runtime=nvidia --device=/dev/infiniband/uverbs0 --device=/dev/infiniband/rdma_cm -e DISPLAY --net=host -v {:s} -v {:s} -v /tmp:/tmp -u 50000:50000 -e NVIDIA_VISIBLE_DEVICES={:d} -e NVIDIA_DRIVER_CAPABILITIES=all --cap-add=IPC_LOCK --ulimit memlock=-1:-1 --cpuset-mems={:d} --name {:s}.{:d} xinpingdeng/{:s}".format(dvolume, hvolume, gpu, numa, image, suffix, image)

print comline
print "\nYou are going to a docker container with the name {:s}.{:d}!\n".format(image, suffix)

os.system(comline)
