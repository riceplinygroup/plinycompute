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
import subprocess
import time
import sys


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

num_total = 0
num_errors = 0
num_passed = 0


def start_pseudo_cluster():
    try:
        print(BColor.OK_BLUE + "start a pdbServer as the coordinator" + BColor.END_C)
        serverProcess = subprocess.Popen(['bin/pdb-cluster', 'localhost', '8108', 'Y'])
        print(BColor.OK_BLUE + "waiting for 9 seconds for server to be fully started..." + BColor.END_C)
        time.sleep(9)
        num = 0
        with open('conf/serverlist.test') as f:
            for each_line in f:
                print(BColor.OK_BLUE + "start a pdbServer at " + each_line + "as " + str(
                    num) + "-th worker" + BColor.END_C)
                num = num + 1
                serverProcess = subprocess.Popen(
                    ['bin/pdb-server', thread_num, shared_memory_size, 'localhost:8108', each_line])
                print(BColor.OK_BLUE + "waiting for 9 seconds for server to be fully started..." + BColor.END_C)
                time.sleep(9)
                each_line = each_line.split(':')
                port = int(each_line[1])

    except subprocess.CalledProcessError as e:
        print(BColor.FAIL + "[ERROR] in starting pseudo cluster" + BColor.END_C)
        print(e.returncode)


def run_tests(test_list):
    print("###############################################")
    print("REQUIRE 8192 MB MEMORY TO RUN INTEGRATION TESTS")
    print("###############################################")

    print("#################################")
    print("CLEAN UP THE TESTING ENVIRONMENT")
    print("#################################")
    subprocess.call(['bash', './scripts/cleanupNode.sh'])

    subprocess.call(['bash', './scripts/cleanupNode.sh'])
    print(BColor.OK_BLUE + "waiting for 5 seconds for server to be fully cleaned up...")
    time.sleep(5)

    # set the total number of tests
    num_total = len(test_list.items())

    # iterate through all the tests and run them
    for id, test in test_list.items():
        run_test(test[0], test[1])

    print("#################################")
    print("SUMMARY")
    print("#################################")
    print(BColor.OK_GREEN + "TOTAL TESTS: " + str(num_total) + BColor.END_C)
    print(BColor.OK_GREEN + "PASSED TESTS: " + str(num_passed) + BColor.END_C)
    print(BColor.FAIL + "FAILED TESTS: " + str(num_errors) + BColor.END_C)


def run_specified_test(test_list, test):

    if test in test_list:
        run_tests({test : test_list[test]})
    else:
        print "Test with the name %s does not exist." % test


def run_test(test_name, test_command):
    # we want to use the global variables
    global num_errors
    global num_passed

    print("#################################")
    print("RUN %s" % test_name)
    print("#################################")

    try:
        start_pseudo_cluster()

        print(BColor.OK_BLUE + "start a query client to store and query data from pdb cluster" + BColor.END_C)
        subprocess.check_call(test_command)

    except subprocess.CalledProcessError as e:
        print(BColor.FAIL + "[ERROR] in running %s" % test_name + BColor.END_C)
        print(e.returncode)
        num_errors = num_errors + 1

    else:
        print(BColor.OK_BLUE + "[PASSED] %s" % test_name + BColor.END_C)
        num_passed = num_passed + 1

    # do the cleanup
    subprocess.call(['bash', './scripts/cleanupNode.sh'])
    print (BColor.OK_BLUE + "waiting for 10 seconds for server to be fully cleaned up...")
    time.sleep(10)


tests = {

    "TestAggregationAfterThreeWayJoin": ("SELECTION AND JOIN MIXED TEST ON G-2 PIPELINE", ['bin/TestAggregationAfterThreeWayJoin', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestAggregationOnDoubleVector": ("AGGREGATION ON A DOUBLE VECTOR TEST G-2 PIPELINE", ['bin/TestAggregationOnDoubleVector', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestAggregationWithoutCombiner": ("AGGREGATION WITHOUT COMBINER TEST G-2 PIPELINE", ['bin/TestAggregationWithoutCombiner', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestAllSelection": ("ALL SELECTION TEST G-2 PIPELINE", ['bin/TestAllSelection', 'Y', 'Y', '256', 'localhost', 'Y']),
    # "TestAllSelectionBuiltIn": ("ALL SELECTION BUILTIN TEST G-2 PIPELINE", ['bin/TestAllSelectionBuiltIn', 'Y', 'Y', '256', 'localhost', 'Y']),
    # "TestAllSelectionShared": ("ALL SELECTION SHARED TEST G-2 PIPELINE", ['bin/TestAllSelectionShared', 'Y', 'Y', '256', 'localhost', 'Y']),
    # "TestConsecutiveJoinWithTwoSinks": ("CONSECUTIVE JOIN WITH TWO SINKS TEST G-2 PIPELINE", ['bin/TestConsecutiveJoinWithTwoSinks', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestConsecutiveOptimizedJoin": ("CONSECUTIVE OPTIMIZED JOIN TEST G-2 PIPELINE", ['bin/TestConsecutiveOptimizedJoin', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestGroupByAggregation": ("GROUP BY AGGREGATION TEST G-2 PIPELINE", ['bin/TestGroupByAggregation', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestIterativeAggregation": ("ITERATIVE AGGREGATION TEST G-2 PIPELINE", ['bin/TestIterativeAggregation', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestMultiSelection": ("MULTI SELECTION TEST G-2 PIPELINE", ['bin/TestMultiSelection', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestMultiSelectionWithJoin": ("MULTI SELECTION WITH JOIN TEST G-2 PIPELINE", ['bin/TestMultiSelectionWithJoin', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestOptimizedGroupByAggregation": ("OPTIMIZED GROUP BY AGGREGATION TEST G-2 PIPELINE", ['bin/TestOptimizedGroupByAggregation', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestOptimizedMethodJoin": ("OPTIMIZED METHOD JOIN TEST G-2 PIPELINE", ['bin/TestOptimizedMethodJoin', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "SelfJoin": ("SELF JOIN TEST G-2 PIPELINE", ['bin/SelfJoin', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestSimpleAggregation": ("SIMPLE AGGREGATION TEST G-2 PIPELINE", ['bin/TestSimpleAggregation', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestSimpleSelection": ("SIMPLE SELECTION TEST G-2 PIPELINE", ['bin/TestSimpleSelection', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestSimpleSelectionWithRatio": ("SIMPLE SELECTION WITH RATIO TEST G-2 PIPELINE", ['bin/TestSimpleSelectionWithRatio', 'Y', 'Y', '1024', 'localhost', 'Y', '0.01']),
    # "TestTwoSelectionOneAggregation": ("AGGREGATION AND SELECTION MIXED TEST ON G-2 PIPELINE", ['bin/TestTwoSelectionOneAggregation', 'Y', 'Y', '1024', 'localhost', 'Y']),
    # "TestLDA": ("LDA TEST ON G-2 PIPELINE", ['bin/TestLDA', 'localhost', '3', '100', '10', 'Y', 'N', '100']),
    # "TestTopK": ("TOP K TEST ON G-2 PIPELINE", ['bin/TestTopK', '1024', 'localhost', 'Y', 'Y'])
}

if len(sys.argv) == 2:

    # runs the test specified in the argument
    run_specified_test(tests, sys.argv[1])
else:

    # run all the tests
    run_tests(tests)
