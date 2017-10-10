# PDB - A Fast and Large-Scalae Cluster Computing Platform 

## Building PDB

Requirements:  
Software:

scons http://scons.org/
bison
flex
LLVM/clang++3.8


OS: Ubuntu-16, MacOS

Run: scons 

## Run PDB on local

python scripts/startPseudoCluster.py #numThreads #sharedMemPoolSize (MB)


## Cleanup PDB Data and Catalog on local

scripts/cleanupNode.sh



## Run PDB on a Cluster 


Firstly, we need to setup the test suite by following 4 steps. (Those 4 steps only need to be done only once)

Step (1.1) In rice cloud or AWS, find one ubuntu server as your Master, and log in to that server using the 'ubuntu' account; (In future, we shall not be constrained by OS, and we can use the 'pdb' account)

Step (1.2) Download PDB code from svn to the Master server, configure PDB_HOME to be the svn repository. For example, you can:

     - edit ~/.bashrc, and add following to that file: export PDB_HOME=~/PDB/ObjectQueryModel

Step (1.3) Next, configure PDB_INSTALL to be the location that you want PDB to be installed at on the workers.  For example, you might add the following to .basrc:

     export PDB_INSTALL=/disk1/PDB

  Then run following command in shell to make sure these variables are set: source ~/.bashrc

Step (1.4) In rice cloud, find at least one different ubuntu servers as your Slaves, make sure those slaves can be accessed by Master through network and vice versa, and also make sure you have only one PEM file to log on to all slaves. Then add only IPs of those slaves to the file: $PDB_HOME/conf/serverlist. For example, my serverlist looks like following:
10.134.96.184
10.134.96.153  

Step (1.5) On the master server, install the cluster by run:
     
     scripts/install.sh $pem_file/private_key






Secondly, we start the cluster

On the Master server:

Step (2.1)

cd $PDB_HOME
scripts/startMaster.sh $pem_file/private_key

wait for the scripts to return (see something like "master is started!" in the end), and move to  step 2.3:

Step (2.2) : run following command:   
 
cd $PDB_HOME
scripts/startWorkers.sh $pem_file/private_key $MasterIPAddress $ThreadNumber (optional, default is 4)  $SharedMemSize (optional, unit MB, default is 4096)

wait for the scripts to return (see something like "servers are started!" in the end).


Thirdly, you can run test cases

For example:


Ex1. In PDB without Pliny dependency (PLINY_HOME is set to empty)
cd $PDB_HOME
bin/test52  Y Y YourTestingDataSizeInMB (e.g. 1024 to test 1GB data) YourMasterIP


Ex2. In PDB with Pliny dependency (PLINY_HOME is set to pdb-pliny-interface)
cd $PLINY_HOME
./bin/pdb-create -d db1 -s set1 -c
./bin/pliny-add -d db1 -s set1 -c --capacity 0  < ~/maven12-src-edu.json
./bin/pdb-flush -d db1 -s set1 -c
./bin/pliny-query -s set1 -d db1 -o set1_out -f foo.jsonl -c < tests/dataset-4.jsonl

## Stop Cluster
cd $PDB_HOME
scripts/stopWorkers.sh $pem_file/private_key


## Soft Reboot Cluster (restart cluster with all data kept)
cd $PDB_HOME
scripts/stopWorkers.sh $pem_file/private_key
scripts/startMaster.sh $pem_file/private_key
scripts/startWorkers.sh $pem_file/private_key $MasterIPAddress $ThreadNum $SharedMemoryPoolSize


## Upgrade Cluster (for developers and testers upgrade binaries and restart cluster with all data kept)
cd $PDB_HOME
scripts/stopWorkers.sh $pem_file/private_key
scripts/upgrade.sh $pem_file/private_key
scripts/startMaster.sh $pem_file/private_key
scripts/startWorkers.sh $pem_file/private_key $MasterIPAddress $ThreadNum $SharedMemoryPoolSize


## Cleanup Catalog and Storage data
You can cleanup all catalog and storage data by running following command in master

cd $PDB_HOME
scripts/cleanup.sh $pem_file


## Environment Variables:


(1) PDB_SSH_OPTS

by default, it is defined to be "-o StrictHostKeyChecking=no"

(2) PDB_SSH_FOREGROUND

if you define it to non empty like "y" or "yes", it will run as before and bring all output to your ssh terminal;

by default, it is not defined, and it will run in background using nohup, which means it will not be interrupted by ssh.



## Compiling shared libraries

(1) Add your shared library header file and source file like following example:
https://svn.rice.edu/r/software_0/ObjectQueryModel/src/sharedLibraries/headers/ChrisSelection.h
https://svn.rice.edu/r/software_0/ObjectQueryModel/src/sharedLibraries/source/ChrisSelection.cc

Note, it MUST be a pdb :: Object instance, and follow all rules of pdb :: Object (Please search Object Model FAQ in the PDB google group). For example, you must include the ENABLE_DEEP_COPY macro in the public statements. You must include the header file "GetVTable.h", and have the GET_V_TABLE macro in the source file.


(2) Build your shared library.
Now you can build it by adding following to SConstruct(https://svn.rice.edu/r/software_0/ObjectQueryModel/SConstruct):

common_env.SharedLibrary('libraries/libChrisSelection.so', ['build/libraries/ChrisSelection.cc'] + all)

Then add 'libraries/libChrisSelection.so' to "main=common_env.Alias(...)"

In future, shared library should be able to be compiled at client side via a PDB client library.





