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
#ifndef RESOURCE_MANAGER_SERVER_CC
#define RESOURCE_MANAGER_SERVER_CC

#include "ResourceManagerServer.h"
#include "InterfaceFunctions.h"
#include "SimpleRequestHandler.h"
#include "RequestResources.h"
#include "AllocatedResources.h"
#include "DataTypes.h"
#include <stdlib.h>
#include <regex>
#include <iostream>
#include <fstream>
#include <sstream>

namespace pdb {


ResourceManagerServer :: ~ResourceManagerServer () { this->resources = nullptr; }


ResourceManagerServer :: ResourceManagerServer (std :: string catalogIp) {   

//TODO  

}


ResourceManagerServer :: ResourceManagerServer (std :: string pathToServerList, int port) {
    this->port = port;
    this->initialize (pathToServerList);
}


Handle<Vector<Handle<ResourceInfo>>> ResourceManagerServer :: getAllResources () {
    return resources;

}

Handle<Vector<Handle<NodeDispatcherData>>> ResourceManagerServer :: getAllNodes() {
   return nodes;
}


void ResourceManagerServer :: initialize (std :: string pathToServerList) {

    //Allocate a Vector
    int maxNodeNum = 1000;
    makeObjectAllocatorBlock (1024*1024, true);
    this->resources = makeObject<Vector<Handle<ResourceInfo>>>(maxNodeNum);
    makeObjectAllocatorBlock (1024*1024, true);
    this->nodes = makeObject<Vector<Handle<NodeDispatcherData>>>(maxNodeNum);
    analyzeNodes(pathToServerList);
    //to run a script to obtain all system resources
    system ("scripts/collect_proc.sh");    
    analyzeResources ("conf/cluster/cluster_info.txt");
}

void ResourceManagerServer :: analyzeNodes(std :: string serverlist) {
    std :: cout << serverlist << std :: endl;
    std :: string address;
    NodeID nodeId = 0;
    std :: ifstream nodeFile (serverlist);
    if (nodeFile.is_open()) {
       while (! nodeFile.eof()) {
           std :: getline (nodeFile, address);
           if(address.find(".") != string::npos) {
               //std :: cout << address << std :: endl;
               const UseTemporaryAllocationBlock block (1024);
               Handle<NodeDispatcherData> node = makeObject<NodeDispatcherData>(nodeId, port, address);
               this->nodes->push_back(node);
               std :: cout << "nodeId=" << nodeId << ", address=" << address << ", port=" << port << std :: endl;
               nodeId ++;
           }
       } 
       nodeFile.close();
    }
    else {
          std :: cout << "file can't be open" << std :: endl;
    } 
}


void ResourceManagerServer :: analyzeResources (std :: string resourceFileName) {

    //to analyze and obtain resources
    std :: string line;
    int numCores;
    int memSize; //in MB
    int numServers = 0;
    std :: ifstream resourceFile (resourceFileName);
    if (resourceFile.is_open()) {
        while ( ! resourceFile.eof() ) {

            std :: getline (resourceFile,line);
            if (line.find("CPUNumber") != string::npos) {
                //get the number of cores
                std :: cout << line << std :: endl;
                const std :: regex r("[0-9]+");
                numCores = 0;
                std :: sregex_iterator N(line.begin(), line.end(), r); 
                std :: stringstream SS(*N->begin());
                numCores = 0;
                SS >> numCores;
                std :: cout << "numCores =" << numCores << std :: endl;
            }

            if (line.find("MemTotal") != string::npos) {
                std :: cout << line << std :: endl;
                //get the size of memory in MB
                const std :: regex r("[0-9]+");
                memSize = 0;
                std :: sregex_iterator N(line.begin(), line.end(), r);
                std :: stringstream SS(*N->begin());
                SS >> memSize;
                std :: cout << "memSize =" << memSize << std :: endl;
                Handle<ResourceInfo> resource = makeObject<ResourceInfo>(numCores, memSize, (*this->nodes)[numServers]->getAddress(), port, numServers);
                std :: cout << numServers << ": address=" << resource->getAddress() << ", numCores=" << resource->getNumCores() << ", memSize=" << resource->getMemSize() << std :: endl;
                this->resources->push_back(resource);
                numServers ++;
            }


        }
       
        resourceFile.close();

    } else {

        std :: cout << resourceFileName << "can't be open." << std :: endl;

    }
}


void ResourceManagerServer :: registerHandlers (PDBServer &forMe) {
//Now we use ResourceManager through getFunctionality() at Scheduler and Dispatcher
}


}

#endif
