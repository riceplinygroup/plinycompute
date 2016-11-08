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

int main (int argc, char * argv[] ) {

       std :: cout << "Starting up a PDB server!!\n";
       std :: cout << "First run this, then run bin/test46 in another window, then run this again, then run bin/test44 in another window" << std :: endl;
       std :: cout << "[Usage] #numThreads(optional) #masterIp(optional) #localIp(optional)" << std :: endl;
       
       int numThreads = 1;
       bool standalone = true;
       std :: string masterIp;
       std :: string localIp;
       if (argc == 2) {
            numThreads = atoi(argv[1]);
       } 
       if (argc == 3) {
            std :: cout << "You must provide both masterIp and localIp" << std :: endl;
            exit(-1); 
       } 
       if (argc == 4) {
            numThreads = atoi(argv[1]);
            standalone = false;
            masterIp = argv[2];
            localIp = argv[3];
       }
       std :: cout << "Thread number =" << numThreads << std :: endl;
       if (standalone == true) {
            std :: cout << "We are now running in standalone mode" << std :: endl;
       } else {
            std :: cout << "We are now running in distribution mode" << std :: endl;
            std :: cout << "Master IP:" << masterIp << std :: endl;
            std :: cout << "Local IP:" << localIp << std :: endl;
       }
  
       pdb :: PDBLoggerPtr logger = make_shared <pdb :: PDBLogger> ("frontendLogFile.log");
       ConfigurationPtr conf = make_shared < Configuration > ();
       conf->setNumThreads(numThreads);
       SharedMemPtr shm = make_shared< SharedMem > (conf->getShmSize(), logger);
       conf->printOut();

       string errMsg;
       if(shm != nullptr) {
               pid_t child_pid = fork();
               if(child_pid == 0) {
                   //I'm the backend server
                   pdb :: PDBLoggerPtr logger = make_shared <pdb :: PDBLogger> ("backendLogFile.log");
                   pdb :: PDBServer backEnd (conf->getBackEndIpcFile(), 100, logger);
                   backEnd.addFunctionality<pdb :: HermesExecutionServer>(shm, backEnd.getWorkerQueue(), logger, conf);
                   bool usePangea = true;
                   backEnd.addFunctionality<pdb :: StorageClient> (8108, "localhost", make_shared <pdb :: PDBLogger> ("clientLog"), usePangea);
                   backEnd.startServer(nullptr);

               } else if (child_pid == -1) {
                   std :: cout << "Fatal Error: fork failed." << std :: endl;
               } else {
                   //I'm the frontend server
                   pdb :: PDBServer frontEnd (8108, 100, logger);
                   frontEnd.addFunctionality<pdb :: PangeaStorageServer> (shm, frontEnd.getWorkerQueue(), logger, conf);
                   frontEnd.getFunctionality<pdb :: PangeaStorageServer>().startFlushConsumerThreads();
                   frontEnd.addFunctionality<pdb :: FrontendQueryTestServer>();
                   if (standalone == true) {
                       string nodeName = "standalone";
                       string nodeType = "master";
                       pdb :: Handle<pdb :: CatalogNodeMetadata> nodeData = pdb :: makeObject<pdb :: CatalogNodeMetadata>(String("localhost:" + std::to_string(conf->getPort())), String("localhost"), conf->getPort(), String(nodeName), String(nodeType), 1);                       
                       frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir", false);
                       frontEnd.addFunctionality <pdb :: CatalogClient> (conf->getPort(), "localhost", logger);
                       /*std :: cout << "to register node metadata in catalog..." << std :: endl;
                       if (frontEnd.getFunctionality<pdb::CatalogServer>().addNodeMetadata(nodeData, errMsg)) {
                            std :: cout << "Not able to register node metadata: " + errMsg << std::endl;
                            std :: cout << "Please change the parameters: nodeIP, port, nodeName, nodeType, status."<<std::endl;
                       } else {
                            std :: cout << "Node metadata successfully added.\n";
                       }*/

                   } else {
                       string nodeName = localIp;
                       string nodeType = "worker";
                       pdb :: Handle<pdb :: CatalogNodeMetadata> nodeData = pdb :: makeObject<pdb :: CatalogNodeMetadata>(String(localIp + ":" + std::to_string(conf->getPort())), String(localIp), conf->getPort(), String(nodeName), String(nodeType), 1);
                       frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir", false);
                       frontEnd.addFunctionality <pdb :: CatalogClient> (conf->getPort(), "localhost", logger);
                       pdb :: CatalogClient catClient (conf->getPort(), masterIp, make_shared <pdb :: PDBLogger> ("clientCatalogLog"));
                       if (!catClient.registerNodeMetadata (nodeData, errMsg)) {
                           std :: cout << "Not able to register node metadata: " + errMsg << std::endl;
                           std :: cout << "Please change the parameters: nodeIP, port, nodeName, nodeType, status."<<std::endl;
                       } else {
                           std :: cout << "Node metadata successfully added. \n";
                       }
                   }
                                      
                   frontEnd.startServer (nullptr);
               }
               
       }
}

#endif

