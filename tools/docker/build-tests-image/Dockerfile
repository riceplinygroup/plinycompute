FROM dimitrijejankov/pdb:latest

# set the mount volume for pdb
VOLUME /pdb

# go to the pdb directory
WORKDIR /pdb

# add the build test shell script
ADD ./build-tests.sh /

# build the tests
ENTRYPOINT ["./../build-tests.sh"]
