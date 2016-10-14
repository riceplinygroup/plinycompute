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

int main () {

       std :: cout << "Starting up a frontend server!!\n";
       std :: cout << "First run this, then run bin/test602 in another window, then run this again, then run bin/test18 in another window" << std :: endl;
       pdb :: PDBLoggerPtr logger = make_shared <pdb :: PDBLogger> ("frontendLogFile.log");
       ConfigurationPtr conf = make_shared < Configuration > ();
       SharedMemPtr shm = make_shared< SharedMem > (conf->getShmSize(), logger);
       conf->printOut();

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
                   frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir", true, false);
                   frontEnd.addFunctionality <pdb :: CatalogClient> (8108, "localhost", logger);
                   frontEnd.addFunctionality<pdb :: PangeaStorageServer> (shm, frontEnd.getWorkerQueue(), logger, conf);
                   frontEnd.getFunctionality<pdb :: PangeaStorageServer>().startFlushConsumerThreads();
                   frontEnd.addFunctionality<pdb :: FrontendQueryTestServer>();
                   frontEnd.startServer (nullptr);
               }
               
       }
}

#endif

