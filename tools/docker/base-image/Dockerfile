FROM ubuntu:17.04

# install stuff
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends apt-utils
RUN apt-get install -y cmake clang llvm flex bison uuid-dev libeigen3-dev libc++-dev
RUN apt-get install -y inetutils-ping
RUN apt-get install -y gdbserver git unzip

# create the working directory
RUN mkdir /pdb
WORKDIR /pdb

# copy the scripts and the cofig
ADD scripts /pdb/scripts
ADD conf /pdb/conf

# build the hpc toolkit
RUN mkdir /hpc
RUN mkdir /build

WORKDIR /build

RUN git clone https://github.com/HPCToolkit/hpctoolkit.git
RUN git clone https://github.com/HPCToolkit/hpctoolkit-externals.git

# compile the external libraries
WORKDIR /build/hpctoolkit-externals

RUN ./configure
RUN make all -j 4
RUN make distclean

# compile the hpc toolkit
WORKDIR /build/hpctoolkit

RUN ./configure --prefix=/hpc --with-externals=/build/hpctoolkit-externals
RUN make
RUN make install

RUN apt-get install -y libsnappy1v5 libsnappy-dev
RUN apt-get install -y libgsl-dev
RUN apt-get install -y python3