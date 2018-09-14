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

#ifndef WORKER_MAIN_CC
#define WORKER_MAIN_CC

#include "PDBServer.h"
#include "CatalogServer.h"
#include "CatalogClient.h"
#include "StorageClient.h"
#include "PangeaStorageServer.h"
#include "FrontendQueryTestServer.h"
#include "HermesExecutionServer.h"
#include "GenericWork.h"

int main(int argc, char* argv[]) {

    std::cout << "Starting up a PDB server!!\n";
    std::cout << "[Usage] #numThreads(optional) #sharedMemSize(optional, unit: MB) "
                 "#managerIp(optional) #localIp(optional)"
              << std::endl;

    ConfigurationPtr conf = make_shared<Configuration>();

    int numThreads = 1;
    size_t sharedMemSize = (size_t)12 * (size_t)1024 * (size_t)1024 * (size_t)1024;
    bool standalone = true;
    std::string managerIp;
    std::string localIp = conf->getServerAddress();
    int managerPort = conf->getPort();
    int localPort = conf->getPort();
    if (argc == 2) {
        numThreads = atoi(argv[1]);
    }

    if (argc == 3) {
        numThreads = atoi(argv[1]);
        sharedMemSize = (size_t)(atoi(argv[2])) * (size_t)1024 * (size_t)1024;
    }

    if (argc == 4) {
        std::cout << "You must provide both managerIp and localIp" << std::endl;
        exit(-1);
    }

    if (argc == 5) {
        numThreads = atoi(argv[1]);
        sharedMemSize = (size_t)(atoi(argv[2])) * (size_t)1024 * (size_t)1024;
        standalone = false;
        string managerAccess(argv[3]);
        size_t pos = managerAccess.find(':');
        if (pos != string::npos) {
            managerPort = stoi(managerAccess.substr(pos + 1, managerAccess.size()));

            managerIp = managerAccess.substr(0, pos);
        } else {
            managerPort = 8108;
            managerIp = managerAccess;
        }
        string workerAccess(argv[4]);
        pos = workerAccess.find(':');
        if (pos != string::npos) {
            localPort = stoi(workerAccess.substr(pos + 1, workerAccess.size()));
            localIp = workerAccess.substr(0, pos);
            conf->setPort(localPort);
        } else {
            localPort = 8108;
            localIp = workerAccess;
        }
    }
    conf->initDirs();

    std::cout << "Thread number =" << numThreads << std::endl;
    std::cout << "Shared memory size =" << sharedMemSize << std::endl;

    if (standalone) {
        std::cout << "We are now running in standalone mode" << std::endl;
    } else {
        std::cout << "We are now running in distribution mode" << std::endl;
        std::cout << "Manager IP:" << managerIp << std::endl;
        std::cout << "Manager Port:" << managerPort << std::endl;
        conf->setIsManager(false);
        conf->setManagerNodeHostName(managerIp);
        conf->setManagerNodePort(managerPort);
        std::cout << "Local IP:" << localIp << std::endl;
        std::cout << "Local Port:" << localPort << std::endl;
    }
    std::string frontendLoggerFile = std::string("frontend_") + localIp + std::string("_") +
        std::to_string(localPort) + std::string(".log");
    pdb::PDBLoggerPtr logger = make_shared<pdb::PDBLogger>(frontendLoggerFile);
    conf->setNumThreads(numThreads);
    conf->setShmSize(sharedMemSize);
    SharedMemPtr shm = make_shared<SharedMem>(conf->getShmSize(), logger);

    std::string ipcFile =
        std::string("/tmp/") + localIp + std::string("_") + std::to_string(localPort);
    std::cout << "ipcFile=" << ipcFile << std::endl;
    conf->setBackEndIpcFile(ipcFile);

    string errMsg;
    if (shm != nullptr) {
        pid_t child_pid = fork();
        if (child_pid == 0) {
            // I'm the backend server
            std::string backendLoggerFile = std::string("backend_") + localIp + std::string("_") +
                std::to_string(localPort) + std::string(".log");
            pdb::PDBLoggerPtr logger = make_shared<pdb::PDBLogger>(backendLoggerFile);
            pdb::PDBServer backEnd(conf->getBackEndIpcFile(), 100, logger);
            backEnd.addFunctionality<pdb::HermesExecutionServer>(
                shm, backEnd.getWorkerQueue(), logger, conf);
            bool usePangea = true;
            std::string clientLoggerFile = std::string("client_") + localIp + std::string("_") +
                std::to_string(localPort) + std::string(".log");
            backEnd.addFunctionality<pdb::StorageClient>(
                localPort, "localhost", make_shared<pdb::PDBLogger>(clientLoggerFile), usePangea);
            backEnd.startServer(nullptr);

        } else if (child_pid == -1) {
            std::cout << "Fatal Error: fork failed." << std::endl;
        } else {
            // I'm the frontend server
            pdb::PDBServer frontEnd(localPort, 100, logger);
            // frontEnd.addFunctionality<pdb :: PipelineDummyTestServer>();
            frontEnd.addFunctionality<pdb::PangeaStorageServer>(
                shm, frontEnd.getWorkerQueue(), logger, conf, standalone);
            frontEnd.getFunctionality<pdb::PangeaStorageServer>().startFlushConsumerThreads();
            bool createSet = true;
            if (!standalone) {
                createSet = false;
            }

            frontEnd.addFunctionality<pdb::FrontendQueryTestServer>(standalone, createSet);

            if (standalone) {

                string nodeType = "manager";

                frontEnd.addFunctionality<pdb::CatalogServer>("CatalogDir", true, "localhost", localPort);
                frontEnd.addFunctionality<pdb::CatalogClient>(localPort, "localhost", logger);
                std::cout << "to register node metadata in catalog..." << std::endl;

                if(!frontEnd.getFunctionality<pdb::CatalogServer>().registerNode("localhost", localPort, nodeType, errMsg)) {
                    std::cout << "Not able to register node metadata: " + errMsg << std::endl;
                    std::cout << "Please change the parameters: nodeIP, port, nodeName, nodeType, status." << std::endl;
                } else {
                    std::cout << "Node metadata successfully added.\n";
                }

            } else {

                std::string catalogFile = std::string("CatalogDir_") + localIp + std::string("_") + std::to_string(localPort);
                frontEnd.addFunctionality<pdb::CatalogServer>(catalogFile, false, managerIp, managerPort);
                frontEnd.addFunctionality<pdb::CatalogClient>(localPort, "localhost", logger);
            }

            frontEnd.startServer(nullptr);
        }
    }
}

#endif
