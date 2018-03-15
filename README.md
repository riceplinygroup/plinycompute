# PlinyCompute: A platform for high-performance distributed tool and library development.

## Building PlinyCompute

### Perequisites:

| Name          | Homepage                          | Ubutnu Packages             |
| ------------- |:---------------------------------:| ---------------------------:|
| Snappy        | https://github.com/google/snappy  | libsnappy1v5, libsnappy-dev |
| GSL           | https://www.gnu.org/software/gsl/ | libgsl-dev                  |
| Boost         | http://www.boost.org/             | libboost-dev, libboost-program-options-dev, libboost-filesystem-dev, libboost-system-dev |
| Bison           | https://www.gnu.org/software/bison/ | bison                   |
| Flex            | https://github.com/westes/flex      | flex                    |

### Building PlinyCompute and Build Targets:

1. Clone PlinyCompute from GitHub, issuing the following command:
```bash 
$ git clone https://github.com/riceplinygroup/plinycompute.git
```
This command will download PlinyCompute in a folder named plinycompute. Make sure you are in that directory. In a linux machine the prompt should look something similar to:
```bash 
ubuntu@master:~/plinycompute$
```
Invoke cmake to create the CMakeFiles, by default PlinyCompute is built without debug messages with the following command:
```bash 
$ cmake .
```
However, if you are interested in debugging the application, issue the following command:
```bash 
$ cmake -DUSE_DEBUG:BOOL=ON .
```
Conversely, to turn debugging messages off, issue the following command:
```bash 
$ cmake -DUSE_DEBUG:BOOL=OFF .
```
Depending of what target you want to build, issue the following command, replacing <number-of-jobs> with an integer number (this allows to execute multiple recipes in parallel), and the target with one target from the table below:
```bash 
$ make -j <number-of-jobs> <target>
```
For example, the following command compiles and builds the executable pdb-cluster (by default created in the folder `bin`).
```bash 
$ make -j 4 pdb-cluster
```

This table lists the different make targets that can be built along its description:

| Target                  | Description                                                        |
| ----------------------- | ------------------------------------------------------------------ |
| pdb-cluster             | This target builds the master server that runs on the master node.  |
| pdb-server              | This target builds the worker server that runs on the worker nodes. |
| shared-libraries        | This target builds all the shared libraries.                        |
| unit-tests              | This target builds all unit tests and their possible dependencies.  |
| run-integration-tests   | This target builds all integration tests and their dependencies, then proceeds on running them one by one.  |
| clean-integration-tests | If there happens to be a situation where an integration test would fail, this target will remove them. |
| &lt;TestName&gt;              | This target builds the test named &lt;TestName&gt; and all of the dependencies. For example running **make TestAllSelection** will build the TestAllSelection test. |
| RunLocal&lt;TestName&gt;              | This target builds the test named &lt;TestName&gt; and all of the dependencies, then proceeds on running it. For example running **make RunLocalTestAllSelection** will build the TestAllSelection test and run it in the pseudo cluster mode. |
| &lt;LibraryName&gt;           | This target builds a particular shared library named &lt;LibraryName&gt;. For example running **make SharedEmployee** will build the SharedEmployee library. |

### Building and running Unit Tests
To run the unit tests, issue the following commands:
```bash 
$ make unit-tests
$ make test
```
## Run PlinyCompute on a local machine (aka Pseudo cluster)
The following script launches an instance of PlinyCompute:
```bash 
$ startPseudoCluster.py
```
To verify that the pseudo cluster is up and running, issue the following command:
```bash 
$ ps aux | grep pdb
```
The output should show the following processes running (the output whole output is ommited for clarity purposes):
```bash 
bin/pdb-cluster localhost 8108 Y
bin/pdb-server 1 2048 localhost:8108 localhost:8109
bin/pdb-server 1 2048 localhost:8108 localhost:8109
```
In the above output, `pdb-cluster` is the master process running on localhost and listening on port 8108. The two `pdb-server` processes correspond to a worker node (each worker node runs a front-end and back-end process), listening on port 8109 and conected to the master process on port 8108.
## Cleanup PlinyCompute Data and Catalog metadata on the pseudo cluster
If you want to clean all data in your PlinyCompute instance, execute the following script. **Warning:** this script remove all data and catalog metadata from your instance.
```
$ scripts/cleanupNode.sh
```
## Installing and deploying PlinyCompute on a real Cluster
Although PlinyCompute running in one machine (e.g. a laptop) is ideal for getting acquainted with the system and testing some functionality, PlinyCompute high-performance properties are best suited for processing large data loads in a real distributed cluster such as Amazon AWS or other cloud provider. Follow these steps:

1. Log into a remote machine that will serve as the **master node** from a cloud provider (e.g. Amazon AWS).

2. Once logged in, clone PlinyCompute from GitHub, issuing the following command:
```bash 
$ git clone https://github.com/riceplinygroup/plinycompute.git
```
This command will download PlinyCompute in a folder named plinycompute. Make sure you are in that directory. In a linux machine the prompt should look something similar to:
```bash 
ubuntu@master:~/plinycompute$
```
3. Set the following environment variables:
a) `PDB_HOME`, this is the folder where PlinyCompute was cloned, in this example `/home/ubuntu/plinycompute`, and
b) `PDB_INSTALL`, this is a folder in the worker nodes (remote machines) where PlinyCompute executables will be installed. 
**Note:** the value of these variables can be different, but make sure that you have proper permissions on the remote machines to create folders and files.
```bash 
export PDB_HOME=/home/ubuntu/plinycompute
export PDB_INSTALL=/tmp/pdb_install
```
4. Edit the conf/serverlist file with the IP addresses of the worker nodes (machines) in the cluster, one IP address per line. The content of the file should look like (replace the IP's with your own):
```bash 
192.168.1.1
192.168.1.2
192.168.1.3
```
In the above example, the cluster will include one master node (where PlinyCompute) was clonned, and three worker nodes, whose IP'addresses are located in the conf/serverlist file.
5. Invoke cmake with the following command:
```bash 
$ cmake -DUSE_DEBUG:BOOL=OFF .
```
6. Build the following executables replacing the value of the -j argument with an integer number (this allows to execute multiple recipes in parallel):
```bash 
$ make -j 4 pdb-cluster
$ make -j 4 pdb-server
```
This will generate the following executables in the folder `bin`:
```bash 
pdb-cluster
pdb-master
```
7. Run the following script. This script will connect to each of the worker nodes and install PlinyCompute. 
```bash 
$ ./scripts/install.sh
```
8. Launch the master node
```bash  
$ ./scripts/startMaster.sh
```
You will see the message "master is started!"

10. Launch the worker nodes, issuing the following script from the master node (you do not have to run it on each worker node):
```bash  
$ ./scripts/startWorkers.sh $pem_file/private_key <MasterIPAddress>
```
Where the <MasterIPAdress> should be replaced with the master node IP. You will see the message "servers are started!." At this point you have a running distributed version of PlinyCompute!

11. Run a test case:
```bash  
$ ./scripts/startWorkers.sh $pem_file/private_key <MasterIPAddress>
```
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
https://svn.rice.edu/r/software_0/ObjectQueryModel/src/sharedLibraries/headers/AllSelection.h
https://svn.rice.edu/r/software_0/ObjectQueryModel/src/sharedLibraries/source/AllSelection.cc

Note, it MUST be a pdb :: Object instance, and follow all rules of pdb :: Object (Please search Object Model FAQ in the PDB google group). For example, you must include the ENABLE_DEEP_COPY macro in the public statements. You must include the header file "GetVTable.h", and have the GET_V_TABLE macro in the source file.


(2) Build your shared library.
Now you can build it by adding following to SConstruct(https://svn.rice.edu/r/software_0/ObjectQueryModel/SConstruct):

common_env.SharedLibrary('libraries/libChrisSelection.so', ['build/libraries/ChrisSelection.cc'] + all)

Then add 'libraries/libChrisSelection.so' to "main=common_env.Alias(...)"

In future, shared library should be able to be compiled at client side via a PDB client library.
