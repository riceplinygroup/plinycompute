#!/usr/bin/env python
#  Copyright 2018 Rice University
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  ========================================================================    
#JiaNote: to run integrated tests on standalone node for storing and querying data


import subprocess
import sys
import time
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def startPseudoCluster():
    #run bin/pdb-cluster
    print bcolors.OKBLUE + "start a pdbServer as the coordinator" + bcolors.ENDC
    serverProcess = subprocess.Popen(['bin/pdb-cluster', 'localhost', '8108', 'Y', '0.25'])
    print bcolors.OKBLUE + "to check whether server is started..." + bcolors.ENDC
    subprocess.call(['bash', './scripts/checkProcess.sh', 'pdb-cluster'])
    print bcolors.OKBLUE + "to sleep to wait for server to be fully started" + bcolors.ENDC
    time.sleep(9)

    #run bin/pdb-server for instance 1
    print bcolors.OKBLUE + "start a pdbServer as the 1st worker" + bcolors.ENDC
    serverProcess = subprocess.Popen(['bin/pdb-server', '1', '512', 'localhost:8108', 'localhost:8109'])
    print bcolors.OKBLUE + "waiting for 9 seconds for server to be fully started..." + bcolors.ENDC
    time.sleep(9)
    subprocess.check_call(['bin/CatalogTests',  '--port', '8108', '--serverAddress', 'localhost', '--command', 'register-node', '--node-ip', 'localhost', '--node-port',  '8109', '--node-name', 'worker', '--node-type', 'worker'])

    #run bin/pdb-server for instance 2
    print bcolors.OKBLUE + "start a pdbServer as the 2nd worker" + bcolors.ENDC
    serverProcess = subprocess.Popen(['bin/pdb-server', '1', '512', 'localhost:8108', 'localhost:8110'])
    print bcolors.OKBLUE + "waiting for 9 seconds for server to be fully started..." + bcolors.ENDC
    time.sleep(9)
    subprocess.check_call(['bin/CatalogTests',  '--port', '8108', '--serverAddress', 'localhost', '--command', 'register-node', '--node-ip', 'localhost', '--node-port',  '8110', '--node-name', 'worker', '--node-type', 'worker'])

    #run bin/pdb-server for instance 3
    print bcolors.OKBLUE + "start a pdbServer as the 3rd worker" + bcolors.ENDC
    serverProcess = subprocess.Popen(['bin/pdb-server', '1', '512', 'localhost:8108', 'localhost:8111'])
    print bcolors.OKBLUE + "waiting for 9 seconds for server to be fully started..." + bcolors.ENDC
    time.sleep(9)
    subprocess.check_call(['bin/CatalogTests',  '--port', '8108', '--serverAddress', 'localhost', '--command', 'register-node', '--node-ip', 'localhost', '--node-port',  '8111', '--node-name', 'worker', '--node-type', 'worker'])

    #run bin/pdb-server for instance 4
    print bcolors.OKBLUE + "start a pdbServer as the 4th worker" + bcolors.ENDC
    serverProcess = subprocess.Popen(['bin/pdb-server', '1', '512', 'localhost:8108', 'localhost:8112'])
    print bcolors.OKBLUE + "waiting for 9 seconds for server to be fully started..." + bcolors.ENDC
    time.sleep(9)
    subprocess.check_call(['bin/CatalogTests',  '--port', '8108', '--serverAddress', 'localhost', '--command', 'register-node', '--node-ip', 'localhost', '--node-port',  '8112', '--node-name', 'worker', '--node-type', 'worker'])


print("#################################")
print("CLEAN THE TESTING ENVIRONMENT")
print("#################################")
subprocess.call(['bash', './scripts/cleanupNode.sh'])
print bcolors.OKBLUE + "waiting for 5 seconds for server to be fully cleaned up..."
time.sleep(5)

print("#################################")
print 'RUN DISTRIBUTED TEST:', sys.argv[1]
print("#################################")

try:
    #start pseudo cluster
    startPseudoCluster()

    #run test
    print bcolors.OKBLUE + "start a query client to store and query data from pdb cluster" + bcolors.ENDC
    subprocess.check_call([sys.argv[1], 'Y', 'Y', '1024', 'localhost'])

except subprocess.CalledProcessError as e:
    print bcolors.FAIL + "[ERROR] in running distributed test " + sys.argv[1] + bcolors.ENDC
    print e.returncode

else:
    print bcolors.OKBLUE + "[PASSED] distributed test " + sys.argv[1] + bcolors.ENDC
    subprocess.call(['bash', './scripts/cleanupNode.sh'])



                                              