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

#include "PDBDebug.h"
#include "ResourceManagerServer.h"
#include "InterfaceFunctions.h"
#include "SimpleRequestHandler.h"
#include "RequestResources.h"
#include "DataTypes.h"
#include <stdlib.h>
#include <regex>
#include <iostream>
#include <fstream>
#include <sstream>
#include <arpa/inet.h>


namespace pdb {


ResourceManagerServer::~ResourceManagerServer() {}


void ResourceManagerServer::cleanup() {

    this->resources = nullptr;
    this->nodes = nullptr;
}


ResourceManagerServer::ResourceManagerServer(std::string pathToServerList,
                                             int port,
                                             bool pseudoClusterMode,
                                             std::string pemFile) {
    this->port = port;
    this->pseudoClusterMode = pseudoClusterMode;
    this->pemFile = pemFile;
    if (this->pseudoClusterMode == true) {

        pathToServerList = "conf/serverlist.test";
    }
    this->initialize(pathToServerList);
}


Handle<Vector<Handle<ResourceInfo>>> ResourceManagerServer::getAllResources() {
    return resources;
}

Handle<Vector<Handle<NodeDispatcherData>>> ResourceManagerServer::getAllNodes() {
    return nodes;
}


void ResourceManagerServer::initialize(std::string pathToServerList) {

    // Allocate a Vector
    int maxNodeNum = 1000;
    makeObjectAllocatorBlock(2 * 1024 * 1024, true);
    this->resources = makeObject<Vector<Handle<ResourceInfo>>>(maxNodeNum);
    this->nodes = makeObject<Vector<Handle<NodeDispatcherData>>>(maxNodeNum);
    analyzeNodes(pathToServerList);
    if (pseudoClusterMode == false) {
        // to run a script to obtain all system resources
        std::string command = std::string("scripts/collect_proc.sh ") + this->pemFile;
        PDB_COUT << command << std::endl;
        int ret = system(command.c_str());
        if (ret < 0) {
            std::cout << "Resource manager: failed to collect cluster information, try to use the "
                         "default one"
                      << std::endl;
        }
        analyzeResources("conf/cluster/cluster_info.txt");
    }
}

void ResourceManagerServer::analyzeNodes(std::string serverlist) {
    PDB_COUT << serverlist << std::endl;
    std::string inputLine;
    std::string address;
    int port;
    NodeID nodeId = 0;
    std::ifstream nodeFile(serverlist);
    if (nodeFile.is_open()) {
        while (!nodeFile.eof()) {
            std::getline(nodeFile, inputLine);
            size_t pos = inputLine.find("#");
            // processes only valid entries, skips commented and empty lines
            if (inputLine.length() != 0 && pos == string::npos) {
                pos = inputLine.find(":");
                if (pos != string::npos) {
                    port = stoi(inputLine.substr(pos + 1, inputLine.size()));
                    address = inputLine.substr(0, pos);
                    if (address.find('.') != string::npos) {
                        struct sockaddr_in sa;
                        int result = inet_pton(AF_INET, address.c_str(), &(sa.sin_addr));
                        if (result == 0) {
                            std::cout << "ERROR: can't connect to IP:" << address << std::endl;
                            continue;
                        }
                    } else {
                        hostent* record = gethostbyname(address.c_str());
                        if (record == nullptr) {
                            std::cout << "ERROR: can't connect to host:" << address << std::endl;
                            continue;
                        }
                    }
                } else {
                    // TODO: we should not hardcode 8108
                    port = 8108;
                    address = inputLine;
                }
                const UseTemporaryAllocationBlock block(1024);
                Handle<NodeDispatcherData> node = makeObject<NodeDispatcherData>(nodeId, port, address);
                this->nodes->push_back(node);
                PDB_COUT << "nodeId=" << nodeId << ", address=" << address << ", port=" << port
                         << std::endl;
                nodeId++;
            }
        }
        if (nodes->size() == 0) {
            PDB_COUT << "No workers in the cluster, stopping" << std::endl;
            exit(-1);
        }
        nodeFile.close();
    } else {
        std::cout << "file can't be open" << std::endl;
    }
}


void ResourceManagerServer::analyzeResources(std::string resourceFileName) {

    // to analyze and obtain resources
    std::string line;
    int numCores;
    int memSize;  // in MB
    int numServers = 0;
    std::ifstream resourceFile(resourceFileName);
    if (resourceFile.is_open()) {
        while (!resourceFile.eof()) {

            std::getline(resourceFile, line);
            if (line.find("CPUNumber") != string::npos) {
                // get the number of cores
                PDB_COUT << line << std::endl;
                const std::regex r("[0-9]+");
                numCores = 0;
                std::sregex_iterator N(line.begin(), line.end(), r);
                std::stringstream SS(*N->begin());
                numCores = 0;
                SS >> numCores;
                PDB_COUT << "numCores =" << numCores << std::endl;
            }

            if (line.find("MemTotal") != string::npos) {
                PDB_COUT << line << std::endl;
                // get the size of memory in MB
                const std::regex r("[0-9]+");
                memSize = 0;
                std::sregex_iterator N(line.begin(), line.end(), r);
                std::stringstream SS(*N->begin());
                SS >> memSize;
                PDB_COUT << "memSize =" << memSize << std::endl;
                Handle<ResourceInfo> resource = makeObject<ResourceInfo>(
                    numCores, memSize, (*this->nodes)[numServers]->getAddress(), port, numServers);
                PDB_COUT << numServers << ": address=" << resource->getAddress()
                         << ", numCores=" << resource->getNumCores()
                         << ", memSize=" << resource->getMemSize() << std::endl;
                this->resources->push_back(resource);
                numServers++;
            }
        }

        resourceFile.close();

    } else {

        std::cout << resourceFileName << "can't be open." << std::endl;
    }
}


void ResourceManagerServer::registerHandlers(PDBServer& forMe) {
    // Now we use ResourceManager through getFunctionality() at Scheduler and Dispatcher
}
}

#endif
