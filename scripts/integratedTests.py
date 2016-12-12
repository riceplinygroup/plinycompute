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
#!python
#JiaNote: to run integrated tests on standalone node for storing and querying data


import subprocess
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

print("#################################")
print("CLEAN THE TESTING ENVIRONMENT")
print("#################################")
subprocess.call(['bash', './scripts/cleanupNode.sh'])
numTotal = 1
numErrors = 0
numPassed = 0


print("#################################")
print("RUN STANDALONE INTEGRATION TESTS")
print("#################################")

try:
    #run bin/test603
    print bcolors.OKBLUE + "start a standalone pdbServer" + bcolors.ENDC
    serverProcess = subprocess.Popen(['bin/test603', '1', '512'])
    print bcolors.OKBLUE + "waiting for 9 seconds for server to be fully started..." + bcolors.ENDC
    time.sleep(9)
    #run bin/test46 1024
    print bcolors.OKBLUE + "start a data client to store data to pdbServer" + bcolors.ENDC
    subprocess.check_call(['bin/test46', '640'])
    #run bin/test44 in a loop for 10 times
    print bcolors.OKBLUE + "start a query client to query data from pdbServer" + bcolors.ENDC
    subprocess.check_call(['bin/test44', 'n'])

except subprocess.CalledProcessError as e:
    print bcolors.FAIL + "[ERROR] in running standalone integration tests" + bcolors.ENDC
    print e.returncode
    numErrors = numErrors + 1

else:
    print bcolors.OKBLUE + "[PASSED] standalone integration tests" + bcolors.ENDC
    numPassed = numPassed + 1

subprocess.call(['bash', './scripts/cleanupNode.sh'])

print("#################################")
print("SUMMRY")
print("#################################")
print bcolors.OKGREEN + "TOTAL TESTS: " + str(numTotal) + bcolors.ENDC
print bcolors.OKGREEN + "PASSED TESTS: " + str(numPassed) + bcolors.ENDC
print bcolors.FAIL + "FAILED TESTS: " + str(numErrors) + bcolors.ENDC
                                              
