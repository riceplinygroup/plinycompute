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

#ifndef TEST_19_CC
#define TEST_19_CC

#include "PangeaStorageServer.h"
#include "PDBVector.h"
#include "Employee.h"
#include "InterfaceFunctions.h"
#include "UseTemporaryAllocationBlock.h"

#include <unistd.h>
#include <stdio.h>




int main (int argc, char * argv[]) {

        int numPagesToWrite;
        if (argc == 1) {
            numPagesToWrite = 6;
            std :: cout << "to generate 6 pages by default..." << std :: endl;
        } else {
            numPagesToWrite = atoi(argv[1]);
            std :: cout << "to generate "<< numPagesToWrite << " by default..." << std :: endl;
        }

        ConfigurationPtr conf = make_shared < Configuration > ();
        pdb :: PDBLoggerPtr logger = make_shared < pdb :: PDBLogger> (conf->getLogFile());
        SharedMemPtr shm = make_shared< SharedMem > (conf->getShmSize(), logger);
        pdb :: PDBWorkerQueuePtr workers = make_shared < pdb :: PDBWorkerQueue > (logger, conf->getMaxConnections()); 
        pdb :: PangeaStorageServerPtr storage = make_shared<pdb :: PangeaStorageServer> (shm, workers, logger, conf);	
        storage->startFlushConsumerThreads();
        
        //add database
        storage->addDatabase ("testDatabase");
        storage->addType ("Employee", 4001);
        storage->addSet("testDatabase", "Employee", "testSet");
        SetPtr set = storage->getSet(std :: pair <std :: string, std :: string> ("testDatabase", "testSet"));
        storage->getCache()->pin(set, MRU, Write);

        //writing data to the set        
        int pagesWritten = 0;

        while (pagesWritten < numPagesToWrite) {
            PDBPagePtr page = storage->getNewPage(std :: pair <std :: string, std :: string>("testDatabase", "testSet"));
            if (page == nullptr) {
                std :: cout << "can't get page, exit..." << std :: endl;
                exit (EXIT_FAILURE);
            }
            
            const pdb :: UseTemporaryAllocationBlock block{page->getBytes(), page->getSize()};
            pdb :: Handle <pdb :: Vector <pdb :: Handle <pdb :: Employee>>> storeMe = pdb :: makeObject <pdb :: Vector <pdb :: Handle<pdb :: Employee>>> ();
            
            try {
                 for (int i = 0; true; i++) {
                     pdb :: Handle <pdb :: Employee> myData = pdb :: makeObject <pdb :: Employee> ("Joe Johnson" + to_string (i), i + 45);
                     storeMe->push_back (myData);
                 }

            } catch ( pdb :: NotEnoughSpace &n ) {
                 //now we can unpin this page
                 cout << "we have finished one page!" << std :: endl;
                 getRecord (storeMe);                 
                 page->unpin();
                 pagesWritten ++;
            }            
        }

        //let's flush
        int num = storage->getCache()->unpinAndEvictAllDirtyPages();
        std :: cout << num << " pages are added to flush buffer!" << std :: endl;
        std :: cout << "sleep 5 seconds to wait for flushing threads to be scheduled..." << std :: endl;
        sleep (5);
        std :: cout << "finish!" << std :: endl;
        storage->stopFlushConsumerThreads();

}

#endif

