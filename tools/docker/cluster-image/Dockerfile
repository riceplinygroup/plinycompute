FROM dimitrijejankov/pdb:latest

# make the profiling directory
RUN mkdir /profile-info

# go to the pdb directory
WORKDIR /pdb

# add the binaries
ADD pdb-cluster /pdb
ADD pdb-server /pdb
ADD MasterServerTest /pdb
# ADD CatalogServerTests /pdb
# ADD GetListNodesTest /pdb
