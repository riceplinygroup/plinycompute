FROM ubuntu:16.04
#FROM ubuntu:latest

# Install
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends apt-utils
RUN apt-get -y upgrade 
RUN apt-get install -y python scons build-essential clang flex bison uuid-dev 
RUN apt-get clean 


WORKDIR /usr/src/pdb

ADD . /usr/src/pdb

CMD ["scons"]



