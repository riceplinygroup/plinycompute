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
import subprocess
import time


class BColor:
    HEADER = '\033[95m'
    OK_BLUE = '\033[94m'
    OK_GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END_C = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


thread_num = "1"
shared_memory_size = "2048"

num_total = 5
num_errors = 0
num_passed = 0

def run_tests(test_list):
    print("###############################################")
    print("REQUIRE 8192 MB MEMORY TO RUN INTEGRATION TESTS")
    print("###############################################")

    # iterate through all the tests and run them
    for name, command in test_list.items():
        run_test(name, command)

    print("#################################")
    print("SUMMARY")
    print("#################################")
    print(BColor.OK_GREEN + "TOTAL TESTS: " + str(num_total) + BColor.END_C)
    print(BColor.OK_GREEN + "PASSED TESTS: " + str(num_passed) + BColor.END_C)
    print(BColor.FAIL + "FAILED TESTS: " + str(num_errors) + BColor.END_C)


def run_test(test_name, test_command):
    # we want to use the global variables
    global num_errors
    global num_passed

    print("#################################")
    print("RUN %s" % test_name)
    print("#################################")

    try:
        print(BColor.OK_BLUE + "start a query client to store and query data from pdb cluster" + BColor.END_C)
        subprocess.check_call(test_command)

    except subprocess.CalledProcessError as e:
        print(BColor.FAIL + "[ERROR] in running distributed aggregation and selection mixed test" + BColor.END_C)
        print(e.returncode)
        num_errors = num_errors + 1

    else:
        print(BColor.OK_BLUE + "[PASSED] G2-distributed aggregation and selection mixed test" + BColor.END_C)
        num_passed = num_passed + 1

    # do the cleanup
    subprocess.call(['bash', './scripts/cleanupNode.sh'])
    print (BColor.OK_BLUE + "waiting for 5 seconds for server to be fully cleaned up...")
    time.sleep(5)


tests = {"AGGREGATION AND SELECTION MIXED TEST ON G-2 PIPELINE": ['bin/Test74', 'Y', 'Y', '1024', 'master', 'Y'],
         "JOIN TEST ON G-2 PIPELINE": ['bin/Test79', 'Y', 'Y', '1024', 'master', 'Y'],
         "SELECTION AND JOIN MIXED TEST ON G-2 PIPELINE": ['bin/Test78', 'Y', 'Y', '1024', 'master', 'Y'],
         "COMPLEX AGGREGATION TEST ON G-2 PIPELINE": ['bin/Test90', 'Y', 'Y', '1024', 'master', 'Y'],
         "LDA TEST ON G-2 PIPELINE": ['bin/TestLDA', 'master', '3', '100', '10', 'Y', 'N', '100']}

run_tests(tests)
