<p align="center">
  <img width="344" height="316" src="https://user-images.githubusercontent.com/16105671/36680884-a1f5614e-1adc-11e8-80cd-f84aafbf3422.png">
</p>
<hr/>

**PlinyCompute** is a platform for high-performance distributed tool and library development written in C++.

# Requirements and dependencies
PlinyCompute has been compiled, built, and tested in Ubuntu 16.04.4 LTS. For the system to work properly, make sure the following requirements are satisfied.

### Environment Variables:
The following environment variables have to be set:

1. **PDB_HOME**: this is the root path directory where PlinyCompute source code is installed when the project is cloned from github; by default is set to `~/plinycompute`

2. **PDB_INSTALL**: this is the root directory on the worker nodes where executables and scripts will be installed. Required only when running in **distributed** mode; by default is set to `/tmp/pdb_install`.

**Note:** the value of these variables is arbitrary (and they do not have to match), but make sure that you have proper permissions on the remote machines to create folders and write to files. In this example, PlinyCompute is installed on `/home/ubuntu/plinycompute` on the manager node, and on `/tmp/pdb_install` in the worker nodes.
```bash 
$ export PDB_HOME=/home/ubuntu/plinycompute
$ export PDB_INSTALL=/tmp/pdb_install
```

### <a name="req"></a>Third-party libraries and packages required:
The table below lists the libraries and packages required by PlinyCompute. If any of them is missing in your system, run the following script to install them `$PDB_HOME/scripts/internal/setupDependencies.py`.

| Library          | Packages             |
| ------------- | ---------------------------:|
| [Cmake](https://cmake.org/download/)        | cmake 3.5.1 |
| [Clang](https://clang.llvm.org/)        | clang version 3.8.0-2ubuntu4 |
| [Snappy](https://github.com/google/snappy)        | libsnappy1v5, libsnappy-dev |
| [GSL ](https://www.gnu.org/software/gsl/)         | libgsl-dev                  |
| [Boost](http://www.boost.org/)                    | libboost-dev, libboost-program-options-dev, libboost-filesystem-dev, libboost-system-dev |
| [Bison](https://www.gnu.org/software/bison/)      | bison                   |
| [Flex](https://github.com/westes/flex)            | flex                    |

# <a name="compiling"></a>Compiling PlinyCompute and building targets:

1. Clone PlinyCompute from GitHub, issuing the following command:
```bash 
$ git clone https://github.com/riceplinygroup/plinycompute.git
```
This command will download PlinyCompute in a folder named plinycompute. Make sure you are in that directory by typing:
```
$ cd plinycompute
``` 
In a Linux machine the prompt should look something similar to:
```bash 
ubuntu@master:~/plinycompute$
```
Invoke cmake, by default PlinyCompute is built without debug messages with the following command:
```bash 
$ cmake .
```
However, if you are interested in debugging PlinyCompute, issue the following command:
```bash 
$ cmake -DUSE_DEBUG:BOOL=ON .
```
Conversely, to turn debugging messages off, issue the following command:
```bash 
$ cmake -DUSE_DEBUG:BOOL=OFF .
```

<a name="targets"></a>This table lists the different make targets that can be built along its description:

| Target                  | Description                                                      |
| ----------------------- | ---------------------------------------------------------------- |
| pdb-manager             | Builds the executable that runs on the manager node.           |
| pdb-worker              | Builds the executable that runs on the worker nodes.          |
| shared-libraries        | Builds all the shared libraries.                                 |
| build-ml-tests          | Builds the machine learning executables and their dependencies.  |
| build-la-tests          | Builds the linear algebra executables and their dependencies.    |
| build-integration-tests | Builds the integration executables and their dependencies.       |
| build-tpch-tests        | Builds the tpch executables and their dependencies.              |
| build-tests             | Builds the unit tests executables and their dependencies.        |

Depending on what target you want to build, issue the following command, replacing <number-of-jobs> with a number (this allows to execute multiple recipes in parallel); and replacing <target> with one target from the table above.
```bash 
$ make -j <number-of-jobs> <target>
```
  
For example, the following command compiles and builds the executable pdb-manager (by default created in the folder `bin`).
```bash 
$ make -j 4 pdb-manager
```

### <a name="building"></a>Compiling, building targets, and running tests:

This table lists the different make targets for running different test suites:

| Target                  | Description                                                        |
| ----------------------- | ------------------------------------------------------------------ |
| unit-tests              | Builds all unit tests and their possible dependencies.           |
| run-integration-tests   | Builds all integration tests and their dependencies, then proceeds on running them one by one.  |
| run-la-tests   | Builds the **linear algebra** tests and their dependencies, then proceeds on running them one by one.  |
| run-ml-tests   | Builds the **machine learning**  tests and their dependencies, then proceeds on running them one by one.  |
| run-tpch-tests   | Builds the **tpch** tests and their dependencies, then proceeds on running them one by one.  |
| clean-integration-tests | If there happens to be a situation where an integration test would fail, this target will remove them. |
| &lt;TestName&gt;              | This target builds the test named &lt;TestName&gt; and all of the dependencies. For example running **make TestAllSelection** will build the TestAllSelection test. |
| RunLocal&lt;TestName&gt;              | This target builds the test named &lt;TestName&gt; and all of the dependencies, then proceeds on running it. For example running **make RunLocalTestAllSelection** will build the TestAllSelection test and run it in the pseudo cluster mode. |
| &lt;LibraryName&gt;           | This target builds a particular shared library named &lt;LibraryName&gt;. For example running **make SharedEmployee** will build the SharedEmployee library. |

### <a name="tests"></a>Example of building and running Unit Tests
To run the unit tests, issue the following commands:
```bash 
$ make unit-tests
$ make test
```

# Deploying and Launching PlinyCompute
PlinyCompute can be launched in two modes:
1. **standalone**: this mode is ideal for testing the functionality of PlinyCompute in a single machine (e.g. a personal computer or a laptop). The cluster is simulated by launching the manager node and one or more worker nodes as separate processes listening on different ports in one physical machine.
2. **distributed**: best suited for processing large datasets. In this mode, the manager node and one ore more worker nodes are launched in different machines.

### <a name="pseudo"></a>Running PlinyCompute on a local machine (standalone mode)
The following script launches an instance of PlinyCompute:
```bash 
$ $PDB_HOME/scripts/startCluster.sh standalone localhost
```
To verify that the pseudo cluster is up and running, issue the following command:
```bash 
$ ps aux | grep pdb
```
The output should show the following processes running (partial output is displayed for clarity purposes):
```bash 
bin/pdb-manager localhost 8108 Y
bin/pdb-worker 1 2048 localhost:8108 localhost:8109
bin/pdb-worker 1 2048 localhost:8108 localhost:8109
```
In the above output, `pdb-manager` is the manager process running on localhost and listening on port 8108. The two `pdb-worker` processes correspond to one worker node (each worker node runs a front-end and back-end process), which listen on port 8109 and connected to the manager process on port 8108.

### <a name="cluster"></a>Deploying PlinyCompute on a real distributed cluster
Although running PlinyCompute in one machine (e.g. a laptop) is ideal for becoming familiar with the system and testing some of its functionality, PlinyCompute's high-performance properties are best suited for processing large data loads in a real distributed cluster such as Amazon AWS, on-premise, or other cloud provider. To accomplish this, follow these steps:

1. Log into a remote machine that will serve as the **manager node** from a cloud provider (e.g. Amazon AWS).

2. Once logged in, clone PlinyCompute from GitHub, issuing the following command:
```bash 
$ git clone https://github.com/riceplinygroup/plinycompute.git
```
This command downloads PlinyCompute in a folder named plinycompute. Make sure you are in that directory. In a Linux machine the prompt should look something similar to:
```bash 
ubuntu@master:~/plinycompute$
```

3. Edit the $PDB_HOME/conf/serverlist file with the IP addresses of the worker nodes (machines) in the cluster; one IP address per line. This is a partial listing of such file (replace the IP's with your own, the ones shown here are ficticious):
```bash
.
.
.
192.168.1.1
192.168.1.2
192.168.1.3
```

In the above example, the cluster will include one manager node (where PlinyCompute was cloned), and three worker nodes, whose IP'addresses can be found in the conf/serverlist file.

4. Invoke cmake with the following command:
```bash 
$ cmake -DUSE_DEBUG:BOOL=OFF .
```

5. Build the following executables replacing the value of the -j argument with an integer to execute multiple recipes in parallel:
```bash 
$ make -j 4 pdb-manager
$ make -j 4 pdb-worker
```
This will generate two executables in the folder `$PDB_HOME/bin`:
```bash 
pdb-manager
pdb-worker
```

6. Run the following script. This script will connect to each of the worker nodes and install the required executables and scripts on the folder given by the $PDB_INSTALL variable. Note: the script's argument is the pem file required to connect to the machines in the cluster. 
```bash 
$ $PDB_HOME/scripts/install.sh conf/pdb-key.pem
```
This generates an output similar to this, for all nodes in the cluster (partial display shown here for clarity purposes):
```bash 
+++++++++++ install server: 192.168.1.1
pdb-worker                100%   55MB  55.1MB/s   00:01
cleanupNode.sh            100% 2072     2.0KB/s   00:00
startWorker.sh            100% 1247     1.2KB/s   00:00
stopWorker.sh             100%  766     0.8KB/s   00:00
checkProcess.sh           100% 1007     1.0KB/s   00:00

+++++++++++ install server: 192.168.1.2
pdb-worker                100%   55MB  27.5MB/s   00:02
.
.
.

At this point, executables and scripts are installed on all machines in the clusert.

```
7. Launch the cluster
```bash  
$ $PDB_HOME/scripts/startCluster.sh <cluster_type> <manager_node_ip> <pem_file> [num_threads] [shared_memory] &
```

Where, `<cluster_type>` is the type of cluster, either 'distributed' or 'standalone', `<manager_node_ip>` should have the manager node IP address; `<pem_file>` is the pem file with the private key to connect to the worker nodes; `[num_threads` the number of threads (this is optional, the default is set to 1); and `<shared_memory>`, specifies the amount of memory in Megabytes (the default is 2048).

For example, the following command launches a cluster using a private key file named `private_key.pem` located in the `conf` folder, whose manager IP address is `192.168.1.1`, using `4 cores` and `4Gb` of memory.
```bash  
$ $PDB_HOME/scripts/startCluster.sh distributed 192.168.1.1 conf/private_key.pem 2 2048 &
```
Once, the worker nodes are launched, the message `"servers are started!"` will be displayed. At this point there is a running distributed version of PlinyCompute!

# Stop Cluster
To stop a running instance of PlinyCompute, issue the following command:
```bash  
$ ./scripts/stopCluster.sh distributed conf/private_key.pem
```

## <a name="cleanup"></a>Cleanup PlinyCompute data and catalog metadata on the pseudo cluster
To clean all data in a PlinyCompute instance, execute the following script. **Warning:** this script removes all data and catalog metadata from your instance.
```
$ $PDB_HOME/scripts/cleanupNode.sh
```
## Upgrade Cluster (for developers and testers upgrade binaries and restart cluster with all data kept)
```bash
scripts/stopWorkers.sh $pem_file/private_key
scripts/upgrade.sh $pem_file/private_key
scripts/startManager.sh $pem_file/private_key
scripts/startWorkers.sh $pem_file/private_key $ManagerIPAddress $ThreadNum $SharedMemoryPoolSize
```

## Cleanup Catalog and Storage data
You can cleanup all catalog and storage data by running the following command in the manager node:

```bash  
$ ./scripts/cleanup.sh conf/private_key.pem
```
# Learn more about PlinyCompute

* [Creating user-defined data types](http://plinycompute.blogs.rice.edu/tutorials/user-defined-types/)
* [Creating user-defined computations](http://plinycompute.blogs.rice.edu/creating-computations/)
* [Storing data](http://plinycompute.blogs.rice.edu/tutorials/how-to-store-data/)
* [Writting and running Machine Learning code](http://plinycompute.blogs.rice.edu/tutorials/machine-learning/)
* [SQL-like queries](http://plinycompute.blogs.rice.edu/tutorials/sql-like-queries/)
* [FAQ's](http://plinycompute.blogs.rice.edu/faq/)
