# PDB - A Fast and Large-Scalae Cluster Computing Platform 

## Building PDB

Requirements:  scons http://scons.org/

Run: scons 



## Run PDB on a Cluster 


Firstly, we need to setup the test suite by following 4 steps. (Those 4 steps only need to be done only once)

Step (1.1) In rice cloud or AWS, find one ubuntu server as your Master, and log in to that server using the 'ubuntu' account;


Step (1.2) Download PDB code from svn to the Master server, configure PDB_HOME and PLINY_HOME to be the svn repository. For example, you can:
     - edit ~/.bashrc, and add following to that file: export PDB_HOME=~/PDB/ObjectQueryModel

     If you are running PDB from Pliny, you can do following:
     export PLINY_HOME=~/pliny/pdb-pliny-interface
     export PDB_HOME=$PLINY_HOME/pdb

   - run following command in shell: source ~/.bashrc

Step (1.3) In rice cloud, find at least one different ubuntu servers as your Slaves, make sure those slaves can be accessed by Master through network and vice versa, and also make sure you have only one PEM file to log on to all slaves. Then add only IPs of those slaves to the file: $PDB_HOME/conf/serverlist. For example, my serverlist looks like following:
10.134.96.184
10.134.96.153  

Step (1.4) On the master server, install the cluster by run:
     
     scripts/install.sh $pem_file

Secondly, we start the cluster

On the Master server:

Step (2.1)

cd $PDB_HOME
scripts/startMaster.sh YourPEMFilePathToLogInToWorkers

wait for the scripts to return (see something like "master is started!" in the end), and move to  step 2.3:

Step (2.2) : run following command:   
 
cd $PDB_HOME
scripts/startWorkers.sh YourPemFilePathToLogInToWorkers MasterIPAddress ThreadNumber (optional, default is 4)  SharedMemSize (optional, unit MB, default is 4096)

wait for the scripts to return (see something like "servers are started!" in the end).


Thirdly, you can run test cases

For example:

cd $PDB_HOME
bin/test52  Y Y YourTestingDataSizeInMB (e.g. 1024 to test 1GB data) YourMasterIP


## Stop Cluster
cd $PDB_HOME
scripts/stopWorkers.sh $PATH_TO_YOUR_PEM_FILE


## Cleanup Catalog and Storage data
You can cleanup all catalog and storage data by running following command in master

cd $PDB_HOME
scripts/cleanup.sh $PATH_TO_YOUR_PEM_FILE


## Environment Variables:


(1) PDB_SSH_OPTS

by default, it is defined to be "-o StrictHostKeyChecking=no"

(2) PDB_SSH_FOREGROUND

if you define it to non empty like "y" or "yes", it will run as before and bring all output to your ssh terminal;

by default, it is not defined, and it will run in background using nohup (I saw Carlos also used this in his contributed "launchWorkers.sh"), which means it will not be interrupted by ssh.
















## Documentation




## Example Programs





## Running Tests

Testing requires building PDB.





## Configuration








## Contributing








