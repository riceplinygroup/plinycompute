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

#ifndef TEST_603_CC
#define TEST_603_CC

#include "PDBServer.h"
#include "CatalogServer.h"
#include "CatalogClient.h"
#include "StorageClient.h"
#include "PangeaStorageServer.h"
#include "FrontendQueryTestServer.h"
#include "HermesExecutionServer.h"
#include "PipelineDummyTestServer.h"

int main (int argc, char * argv[] ) {

       std :: cout << "Starting up a PDB server!!\n";
       std :: cout << "First run this, then run bin/test46 in another window, then run this again, then run bin/test44 in another window" << std :: endl;
       std :: cout << "[Usage] #numThreads(optional) #sharedMemSize(optional, unit: MB) #masterIp(optional) #localIp(optional)" << std :: endl;
       
       ConfigurationPtr conf = make_shared < Configuration > ();
       
       int numThreads = 1;
       size_t sharedMemSize = (size_t)12*(size_t)1024*(size_t)1024*(size_t)1024;
       bool standalone = true;
       std :: string masterIp;
       std :: string localIp="localhost";
       int masterPort = conf->getPort();
       int localPort = conf->getPort();
       if (argc == 2) {
            numThreads = atoi(argv[1]);
       }

       if (argc == 3) {
            numThreads = atoi(argv[1]);
            sharedMemSize = (size_t)(atoi(argv[2]))*(size_t)1024*(size_t)1024;
       } 

       if (argc == 4) {
            std :: cout << "You must provide both masterIp and localIp" << std :: endl;
            exit(-1); 
       } 

       if (argc == 5) {
            numThreads = atoi(argv[1]);
            sharedMemSize = (size_t)(atoi(argv[2]))*(size_t)1024*(size_t)1024;
            standalone = false;
            string masterAccess ( argv[3]);
            size_t pos = masterAccess.find(":");
            if (pos != string::npos) {
                masterPort = stoi(masterAccess.substr(pos+1, masterAccess.size()));
                 
                masterIp = masterAccess.substr(0, pos);
            } else {
                masterPort = 8108;
                masterIp = masterAccess;
            }
            string workerAccess ( argv[4]);
            pos = workerAccess.find(":");
            if (pos != string::npos) {
                localPort = stoi(workerAccess.substr(pos+1, workerAccess.size()));
                localIp = workerAccess.substr(0, pos);
                conf->setPort(localPort);
            } else {
                localPort = 8108;
                localIp = workerAccess;
            }
       }
       conf->initDirs();

       std :: cout << "Thread number =" << numThreads << std :: endl;
       std :: cout << "Shared memory size =" << sharedMemSize << std :: endl;

       if (standalone == true) {
            std :: cout << "We are now running in standalone mode" << std :: endl;
       } else {
            std :: cout << "We are now running in distribution mode" << std :: endl;
            std :: cout << "Master IP:" << masterIp << std :: endl;
            std :: cout << "Master Port:" << masterPort << std :: endl;
            std :: cout << "Local IP:" << localIp << std :: endl;
            std :: cout << "Local Port:" << localPort << std :: endl;
       }
       std :: string frontendLoggerFile=std::string("frontend_")+localIp+std::string("_")+std::to_string(localPort)+std::string(".log");  
       pdb :: PDBLoggerPtr logger = make_shared <pdb :: PDBLogger> (frontendLoggerFile);
       conf->setNumThreads(numThreads);
       conf->setShmSize(sharedMemSize);
       SharedMemPtr shm = make_shared< SharedMem > (conf->getShmSize(), logger);

       std :: string ipcFile=std::string("/tmp/")+localIp+std::string("_")+std::to_string(localPort);
       std :: cout << "ipcFile=" << ipcFile << std :: endl;
       conf->setBackEndIpcFile(ipcFile);

       string errMsg;
       if(shm != nullptr) {
               pid_t child_pid = fork();
               if(child_pid == 0) {
                   //I'm the backend server
                   std :: string backendLoggerFile=std::string("backend_")+localIp+std::string("_")+std::to_string(localPort)+std::string(".log");
                   pdb :: PDBLoggerPtr logger = make_shared <pdb :: PDBLogger> (backendLoggerFile);
                   pdb :: PDBServer backEnd (conf->getBackEndIpcFile(), 100, logger);
                   backEnd.addFunctionality<pdb :: HermesExecutionServer>(shm, backEnd.getWorkerQueue(), logger, conf);
                   bool usePangea = true;
                   std :: string clientLoggerFile=std::string("client_")+localIp+std::string("_")+std::to_string(localPort)+std::string(".log");
                   backEnd.addFunctionality<pdb :: StorageClient> (localPort, "localhost", make_shared <pdb :: PDBLogger> (clientLoggerFile), usePangea);
                   backEnd.startServer(nullptr);

               } else if (child_pid == -1) {
                   std :: cout << "Fatal Error: fork failed." << std :: endl;
               } else {
                   //I'm the frontend server
                   pdb :: PDBServer frontEnd (localPort, 100, logger);
                   //frontEnd.addFunctionality<pdb :: PipelineDummyTestServer>();
                   frontEnd.addFunctionality<pdb :: PangeaStorageServer> (shm, frontEnd.getWorkerQueue(), logger, conf, standalone);
                   frontEnd.getFunctionality<pdb :: PangeaStorageServer>().startFlushConsumerThreads();
                   bool createSet = true;
                   if (standalone == false) {
                       createSet = false;
                   }
                   frontEnd.addFunctionality<pdb :: FrontendQueryTestServer>(standalone, createSet);
                   if (standalone == true) {
                       string nodeName = "standalone";
                       string nodeType = "master";

                       pdb :: UseTemporaryAllocationBlock tempBlock {1024*1024};
                       pdb :: Handle<pdb :: CatalogNodeMetadata> nodeData = pdb :: makeObject<pdb :: CatalogNodeMetadata>(String("localhost:" + std::to_string(localPort)), String("localhost"), localPort, String(nodeName), String(nodeType), 1);                       
                       frontEnd.addFunctionality <pdb :: CatalogServer> ("/tmp/CatalogDir", true , "localhost", localPort);
                       frontEnd.addFunctionality <pdb :: CatalogClient> (localPort, "localhost", logger);
                       std :: cout << "to register node metadata in catalog..." << std :: endl;
                       if (!frontEnd.getFunctionality<pdb::CatalogServer>().addNodeMetadata(nodeData, errMsg)) {
                            std :: cout << "Not able to register node metadata: " + errMsg << std::endl;
                            std :: cout << "Please change the parameters: nodeIP, port, nodeName, nodeType, status."<<std::endl;
                       } else {
                            std :: cout << "Node metadata successfully added.\n";
                       }

                   } else {

                       std :: string catalogFile=std::string("/tmp/CatalogDir_")+localIp+std::string("_")+std::to_string(localPort);
                       frontEnd.addFunctionality <pdb :: CatalogServer> (catalogFile, false, masterIp, masterPort);
                       frontEnd.addFunctionality <pdb :: CatalogClient> (localPort, "localhost", logger);

                   }
                                      
                   frontEnd.startServer (nullptr);
               }
               
       }
}

#endif

