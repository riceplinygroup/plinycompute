# PDB - A Fast and Large-Scalae Cluster Computing Platform 



## Building PDB

Requirements:  scons http://scons.org/

Run: scons 







## Run PDB on a Cluster 



Firstly, we need to setup the test suite by following 3 steps. (Those 3 steps only need to be done once)

Step (1.1) In rice cloud, find one ubuntu server as your Master, and log in to that server using the 'ubuntu' account;


Step (1.2) Download PDB code from svn to the Master server, configure PDB_HOME to be the svn repository. For example, you can:
     - edit ~/.bashrc, and add following to that file: export PDB_HOME=~/PDB/ObjectQueryModel
   - run following command in shell: source ~/.bashrc

Step (1.3) In rice cloud, find at least one different ubuntu servers as your Slaves, make sure those slaves can be accessed by Master through network and vice versa, and also make sure you have only one PEM file to log on to all slaves. Then add only IPs of those slaves to the file: $PDB_HOME/conf/serverlist. For example, my serverlist looks like following:
10.134.96.184
10.134.96.153  

Secondly, we run the test case.  (You can repeatedly run following steps to repeat the test)

On the Master server:


Step (2.1) : reboot the Master server, open a terminal on Master server, and run following command to reboot workers:

cd $PDB_HOME
scripts/rebootWorkeres.sh YourPemFilePathToLogInToWorkers (e.g. XXXX.pem)

wait for all workers to come back (restarted)

Step (2.2) : open a terminal on Master server, and run following commands:

cd $PDB_HOME
scripts/startMaster.sh YourPEMFilePathToLogInToWorkers

wait for the scripts to return (see something like "master is started!" in the end), and move to  step 2.3:

Step (2.3) : run following command:   
 
cd $PDB_HOME
scripts/startWorkers.sh YourPemFilePathToLogInToWorkers MasterIPAddress ThreadNumber (optional, default is 6)  SharedMemSize (optional, unit MB, default is 12884)

wait for the scripts to return (see something like "servers are started!" in the end), and move to the step 2.4:


Step (2.4) : run following command:  

cd $PDB_HOME
bin/test52  Y Y YourTestingDataSizeInMB (e.g. 1024 to test 1GB data) YourMasterIP
























## Documentation




## Example Programs





## Running Tests

Testing requires building PDB.





## Configuration








## Contributing








