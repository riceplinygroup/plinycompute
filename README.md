<p align="center">
  <img width="344" height="316" src="https://user-images.githubusercontent.com/16105671/36680884-a1f5614e-1adc-11e8-80cd-f84aafbf3422.png">
</p>
<hr/>

**PlinyCompute** is a platform for high-performance distributed tool and library development written in C++. It can be deployed in two different cluster modes: **standalone** or **distributed**. Visit the official web site for more details [http://plinycompute.rice.edu](http://plinycompute.rice.edu).

# Learn more about PlinyCompute

* [Creating user-defined data types](http://plinycompute.rice.edu/tutorials/user-defined-types/)
* [Creating user-defined computations](http://plinycompute.rice.edu/creating-computations/)
* [Storing data](http://plinycompute.rice.edu/tutorials/how-to-store-data/)
* [Writting and running Machine Learning code](http://plinycompute.rice.edu/tutorials/machine-learning/)
* [SQL-like queries](http://plinycompute.rice.edu/tutorials/sql-like-queries/)
* [System configuration](http://plinycompute.rice.edu/faq/system-configuration/)
* [FAQ's](http://plinycompute.rice.edu/faq/)

## Acknowledgement
This research was developed with funding from the Defense Advanced Research Projects Agency (DARPA) MUSE award #FA8750-14-2-0270.

**Disclaimer:** The views, opinions, and/or findings contained in this research are those of the authors and should not be interpreted as representing the official views or policies of the Department of Defense or the U.S. Government.

# Requirements and dependencies
PlinyCompute has been compiled, built, and tested in Ubuntu 16.04.4 LTS. For the system to work properly, make sure the following requirements are satisfied.

### Environment Variables:
These environment variables have to be set:

1. **PDB_HOME**: this is the root path directory where PlinyCompute source code is installed when the project is cloned from GitHub; by default is set to `~/plinycompute`

2. **PDB_INSTALL**: this is the root directory on the worker nodes where executables and scripts will be installed. Required only when running in **distributed** mode; by default is set to `/tmp/pdb_install`.

**Note:** the value of these variables is arbitrary (and they do not have to match), but make sure that you have proper permissions on the remote machines to create folders and write to files. In this example, PlinyCompute is installed in `/home/ubuntu/plinycompute` on the manager node, and in `/tmp/pdb_install` in the worker nodes.
```bash 
$ export PDB_HOME=/home/ubuntu/plinycompute
$ export PDB_INSTALL=/tmp/pdb_install
```

### Key-based authentication (PEM file)
PlinyCompute running in a distributed cluster uses key-based authentication. Thus, a private key has to be generated and placed on `$PDB_HOME/conf`. The file name of the key is arbitrary, by default it is `pdb-key.pem`, make sure it has the following permissions `$ chmod 400 $PDB_HOME/conf/pdb-key.pem`.

### <a name="req"></a>Third-party libraries and packages required:
In addition to Python (version 2.7.12 or greater), the table below lists the libraries and packages required by PlinyCompute. If any of them is missing in your system, run the following script to install them `$PDB_HOME/scripts/internal/setupDependencies.py`.

| Library/Software          | Packages             |
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
This command will download PlinyCompute in a directory named `plinycompute`. Make sure you are in that directory by typing:
```
$ cd plinycompute
``` 
In a Linux machine the prompt should look something similar to:
```bash 
ubuntu@manager:~/plinycompute$
```

2. Invoke cmake, by default PlinyCompute is built without debug messages with the following command:
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

3. Compile and build. This make target, builds two executables `pdb-manager` and `pdb-worker` in the `bin` folder; which are invoked by different scripts as described in the sections below.
```bash 
$ make pdb-main
```

4. As an example, compile and build a machine learning application [KMeans](https://github.com/riceplinygroup/plinycompute/tree/master/applications/TestKMeans), which creates this executable `./bin/TestKMeans`. This example will be used in step 3 in the next section, once the cluster is launched.
```bash 
$ make TestKMeans
```

5. If you want to explore and test different applications and libraries that use PlinyCompute as compute engine, build one target from the table below.

**Notes:**
  - the target `shared-libraries`, builds common shared libraries that are used by the different applications (build this first)
  - these targets are independent from each other and are not required for the PlinyCompute engine to work
  
<a name="targets"></a>Make targets for different applications:

| Target                  | Description                                                      | Source Code |
| ----------------------- | ---------------------------------------------------------------- | ----- |
| shared-libraries        | Common shared libraries used by these applications      | [Shared Libraries](https://github.com/riceplinygroup/plinycompute/tree/master/pdb/src/sharedLibraries) |
| build-ml-tests          | Machine learning executables and their dependencies  | [Gaussian Mixture Model](https://github.com/riceplinygroup/plinycompute/tree/master/applications/TestGMM), [Latent Dirichlet Allocation](https://github.com/riceplinygroup/plinycompute/tree/master/applications/TestLDA), [K-Means](https://github.com/riceplinygroup/plinycompute/tree/master/applications/TestKMeans) |
| build-la-tests          | Linear algebra executables and their dependencies    | [Linear Algebra](https://github.com/riceplinygroup/plinycompute/tree/master/applications/TestLA) |
| build-tpch-tests        | TPCH executables and their dependencies              | [TPCH Benchmark](https://github.com/riceplinygroup/plinycompute/tree/master/applications/TPCHBench) |
| build-tests             | Unit tests executables        | [Unit Tests](https://github.com/riceplinygroup/plinycompute/tree/master/tests/unit) |
| build-integration-tests | Integration tests executables       | [Integration Tests](https://github.com/riceplinygroup/plinycompute/tree/master/tests/integration) |

Depending on the target you want to build, issue the following command:
```bash 
$ make -j <number-of-jobs> <target>
```
replacing:
  - `<number-of-jobs>` with a number (this allows to execute multiple recipes in parallel)
  - `<target>` with one target from the table above
   
For example, the following command compiles and builds the executables and shared libraries for the machine learning application in the `bin` folder.
```bash 
$ make -j 4 build-ml-tests
```

### <a name="tests"></a>Compiling, building, and running Unit and Integration Tests
For developers who want to add new functionality to PlinyCompute, here are examples on how to build and run unit and integration tests.
  - unit tests
```bash 
$ make unit-tests && make test
```
  - integration tests
```bash 
$ make run-integration-tests
```

# Deploying and Launching PlinyCompute
PlinyCompute can be launched in two modes:
  - **standalone**: this mode is ideal for testing the functionality of PlinyCompute in a single machine (e.g. a personal computer or a laptop). The cluster is simulated by launching the manager node and one or more worker nodes as separate processes listening on different ports in one physical machine.

  - **distributed**: best suited for processing large datasets. In this mode, the manager node and one ore more worker nodes are launched in different machines.

### <a name="pseudo"></a>Running PlinyCompute on a local machine (standalone mode)
1. Launch PlinyCompute in standalone mode:
```bash 
$ $PDB_HOME/scripts/startCluster.sh standalone localhost
```

2. Verify that the pseudo cluster is up and running, issue the following command:
```bash 
$ ps aux | grep pdb
```
The output should show the following processes running (partial output is displayed for clarity purposes):
```bash 
bin/pdb-manager localhost 8108 Y
bin/pdb-worker 1 2048 localhost:8108 localhost:8109
bin/pdb-worker 1 2048 localhost:8108 localhost:8109
```
In the above output, `pdb-manager` is the manager process running on localhost and listening on port 8108. The two `pdb-worker` processes correspond to one worker node (each worker node runs a front-end and back-end process), listening on port 8109 and connected to the manager process on port 8108.

3. Run the `TestKMeans` example built in step 4 in the previous section (see [KMeans](https://github.com/riceplinygroup/plinycompute/tree/master/applications/TestKMeans) for more details).  
```bash 
$ ./bin/TestKMeans Y Y localhost Y 3 3 0.00001 applications/TestKMeans/kmeans_data
```
As the program is executed, the output will be displayed on the screen. Congratulations you have successfuly installed, deployed, and tested PlinyCompute.

### <a name="cluster"></a>Deploying PlinyCompute on a real distributed cluster
Although running PlinyCompute in one machine (e.g. a laptop) is ideal for becoming familiar with the system and testing some of its functionality, PlinyCompute's high-performance properties are best suited for processing large data loads in a real distributed cluster such as Amazon AWS, on-premise, or other cloud provider. To accomplish this, follow these steps:

1. Log into a remote machine that will serve as the **manager node** from a cloud provider (e.g. Amazon AWS).

2. Once logged in, clone PlinyCompute from GitHub, issuing the following command:
```bash 
$ git clone https://github.com/riceplinygroup/plinycompute.git
```
This command downloads PlinyCompute in a folder named `plinycompute.` Make sure you are in that directory. In a Linux machine the prompt should look something similar to:
```bash 
ubuntu@manager:~/plinycompute$
```

3. Edit the `$PDB_HOME/conf/serverlist` file, and add the public IP addresses of the worker nodes (machines) in the cluster; one IP address per line. Below is a partial listing of such file (replace the IP's with your own, the ones shown here are ficticious):
```bash
.
.
.
192.168.1.1
192.168.1.2
192.168.1.3
```

In the above example, the cluster will include one manager node (where PlinyCompute was cloned), and three worker nodes.

4. Invoke cmake with the following command:
```bash 
$ cmake -DUSE_DEBUG:BOOL=OFF .
```

5. Build the following target, replacing the value of the -j argument with an integer to execute multiple recipes in parallel (if you already built this target, skip this step):
```bash 
$ make -j 4 pdb-main
```
This will generate two executables in the directory `$PDB_HOME/bin`:
```bash 
pdb-manager
pdb-worker
```

6. Install the required executables and scripts on the worker nodes by running the following script. They will be installed on the path given by the `$PDB_INSTALL` envrionment variable. **Note:** the script's argument is the pem file required to connect to the machines in the cluster. 
```bash 
$ $PDB_HOME/scripts/install.sh conf/pdb-key.pem
```

After completion, the output should look similar to the one below, (partial display shown here for clarity purposes):
```bash 
.
.
.
---------------------------------
Results of script install.sh:
*** Failed results (0/3) ***

*** Successful results (3/3) ***
Worker node with IP: 192.168.1.1 successfully installed.
Worker node with IP: 192.168.1.2 successfully installed.
Worker node with IP: 192.168.1.3 successfully installed.
---------------------------------
```

At this point all executable programs and scripts are properly installed on the worker nodes!

7. Launching the cluster.
To start a cluster of PlinyCompute run the script: 
```bash
$ $PDB_HOME/scripts/startCluster.sh <cluster_type> <manager_node_ip> <pem_file> [num_threads] [shared_memory]
```

Where the following arguments are required:
  - `<cluster_type>` should be **distributed**
  - `<manager_node_ip>` the public IP address of the manager node
  - `<pem_file>` the pem file that allows to connect to the machines in the cluster
  
 The last two arguments are optional:
  - `<num_threads>` number of CPU cores on each worker node that PlinyCompute will use
  - `<shared_memory>` amount of RAM on each worker node (in Megabytes) that PlinyCompute will use

In the following example, the public IP address of the manager node is `192.168.1.0`; the pem file is `conf/pdb-key.pem`; by default the cluster is launched with `1` thread and `2Gb` of memory.

```bash
$ $PDB_HOME/scripts/startCluster.sh distributed 192.168.1.0 conf/pdb-key.pem
```

If you want to launch the cluster with more threads and memory, provide the fourth and fifth arguments. In the following example, a cluster is launched with 4 threads and 4GB of memory. For more information about tunning PlinyCompute visit the [system configuration page](http://plinycompute.rice.edu/faq/system-configuration/).
```bash
$ $PDB_HOME/scripts/startCluster.sh distributed 192.168.1.0 conf/pdb-key.pem 4 4096
```

After completion, the output should look similar to the one below, (partial display shown here for clarity purposes):
```bash 
.
.
.
---------------------------------
Results of script startCluster.sh:
*** Failed results (0/3) ***

*** Successful results (3/3) ***
Worker node with IP: 192.168.1.1 successfully started.
Worker node with IP: 192.168.1.2 successfully started.
Worker node with IP: 192.168.1.3 successfully started.

---------------------------------
```

8. To run the `TestKMeans` example in this cluster, issue the following command. Note that although the executable and arguments are the same as the ones in the previous section, because it is sent to a distributed PlinyCompute deployment, the computations are distributed to the three worker nodes for execution.
```bash 
$ ./bin/TestKMeans Y Y localhost Y 3 3 0.00001 applications/TestKMeans/kmeans_data
```

## Stopping a Cluster
To stop a running cluster of PlinyCompute, issue the following command if you are running in a distributed cluster:
```bash  
$ $PDB_HOME/scripts/stopCluster.sh distributed conf/private_key.pem
```

Conversely, use this one if you are running a standalone cluster:
```bash  
$ $PDB_HOME/scripts/stopCluster.sh standalone
```

## <a name="cleanup"></a>Cleanup PlinyCompute storage data and catalog metadata
To remove data in a PlinyCompute cluster, execute the following script. Note that the value of the first argument is `distributed`, meaning this will clean data in a real cluster. **Warning:** this will remove all PlinyCompute stored data and catalog metadata from the entire cluster, use it carefully.
```
$ $PDB_HOME/scripts/cleanup.sh distributed conf/pdb-key.pem
```

If you are running in a standalone cluster, run the following script:
```
$ $PDB_HOME/scripts/cleanup.sh standalone
```

## Upgrade Cluster (for developers and testers who want to upgrade binaries and restart cluster with all data kept)
```bash
$ $PDB_HOME/scripts/stopCluster.sh distributed conf/pdb-key.pem
$ $PDB_HOME/scripts/internal/upgrade.sh conf/pdb-key.pem
$ $PDB_HOME/scripts/startCluster.sh distributed 192.168.1.0 conf/pdb-key.pem
```

