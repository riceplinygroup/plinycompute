/*****************************************************************************
 *                                                                           *
 *  Copyright 2018 Rice University                                           *
 *                                                                           *
 *  Licensed under the Apache License, Version 2.0 (the "License");          *
 *  you may not use this file except in compliance with the License.         *
 *  You may obtain a copy of the License at                                  *
 *                                                                           *
 *      http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                           *
 *  Unless required by applicable law or agreed to in writing, software      *
 *  distributed under the License is distributed on an "AS IS" BASIS,        *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *  See the License for the specific language governing permissions and      *
 *  limitations under the License.                                           *
 *                                                                           *
 *****************************************************************************/
#ifndef GETLISTOFNODESTEST_CC
#define GETLISTOFNODESTEST_CC

#include <iostream>
#include <vector>
#include <string>

#include "PDBServer.h"
#include "DistributionManagerServer.h"
#include "DistributionManagerClient.h"

#include "InterfaceFunctions.h"
#include "ListOfNodes.h"
#include "PDBVector.h"


typedef vector<string> CommandLineStringArgs;

using namespace pdb;
using namespace std;

// Cluster Heart Beat Test.

int main(int argc, char* argv[]) {

    std::cout << "start the cluster then \n";
    std::cout << "start:  ./bin/getListOfNodesTest port MasterNodeHostName frontEndDM.log    \n";

    CommandLineStringArgs cmdlineStringArgs(&argv[0], &argv[0 + argc]);

    if (argc == 4) {
        int portNumber = atoi(argv[1]);
        std::string logfile = cmdlineStringArgs[3];

        pdb::PDBLoggerPtr myLogger = make_shared<pdb::PDBLogger>(logfile);

        bool wasError;
        std::string errMsg;
        std::string hostname(cmdlineStringArgs[2]);

        DistributionManagerClientPtr myDMCLient = make_shared<DistributionManagerClient>(myLogger);

        Handle<ListOfNodes> listOfNodes =
            myDMCLient->getCurrentNodes(hostname, portNumber, wasError, errMsg);

        Handle<Vector<String>> myVectorOfNodes = listOfNodes->getHostNames();

        cout << "List of Nodes are:\n";
        // Now Iterate over the vector and get the current list of nodes
        for (int var = 0; var < myVectorOfNodes->size(); ++var) {
            cout << string(myVectorOfNodes->c_ptr()[var].c_str()) << endl;
        }

        std::cout << errMsg << std::endl;
    } else {
        cout << "Provide: portNumber, masterNodeHostName, masterNodePort"
             << "\n";
    }
}

#endif
