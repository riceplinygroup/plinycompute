FROM ubuntu:16.04
#FROM ubuntu:latest

# Install
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends apt-utils
RUN apt-get -y upgrade 
RUN apt-get install -y python scons build-essential  clang-3.8   llvm-3.8 flex bison uuid-dev libeigen3-dev   libc++-dev  
RUN apt-get clean 

# Set working directory
WORKDIR /usr/src/pdb

# Copy the pdb files
ADD . /usr/src/pdb

# the following env variables are needed to get scons running 
ENV TERM xterm
ENV HOME /usr/src/pdb


# Build pdb
RUN scons


RUN "echo "worker_1_1:8109"  >  /usr/src/pdb/conf/serverlist"
RUN "echo "worker_2_1:8110"  >>  /usr/src/pdb/conf/serverlist"

