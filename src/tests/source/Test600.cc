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

// This tests query processing using Pangea storage.
#include "CheckEmployees.h"

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
        storage->addSet("testDatabase", "testSetInput");
        storage->addSet("testDatabase", "testSetOutput");
        SetPtr inputSet = storage->getSet(std :: pair <std :: string, std :: string> ("testDatabase", "testSetInput"));
        storage->getCache()->pin(inputSet, MRU, Write);

        //writing data to the set        
        int pagesWritten = 0;

        while (pagesWritten < numPagesToWrite) {
            PDBPagePtr page = storage->getNewPage(std :: pair <std :: string, std :: string>("testDatabase", "testSetInput"));
            if (page == nullptr) {
                std :: cout << "can't get page, exit..." << std :: endl;
                exit (EXIT_FAILURE);
            }
            
            const pdb :: UseTemporaryAllocationBlock block{page->getBytes(), page->getSize()};
            pdb :: Handle <pdb :: Vector <pdb :: Handle <pdb :: Supervisor>>> supers = pdb :: makeObject <pdb :: Vector <pdb :: Handle<pdb :: Supervisor>>> ();

            try {

                for (int i = 0; true; i++) {
                	pdb :: Handle <pdb :: Supervisor> super = pdb :: makeObject <pdb :: Supervisor> ("Joe Johnson", 20 + (i % 29));
                	supers->push_back(super);

                	pdb :: Handle <pdb :: Employee> myData;
                	for (int j = 0; j < 10; ++j) {
                		if (j & 1)
                			myData = pdb :: makeObject <pdb :: Employee> ("Steve Stevens", (i % 29) + 20);
                		else
                			myData = pdb :: makeObject <pdb :: Employee> ("Albert Albertson", (i % 29) + 20);
                		
                		(*supers)[i]->addEmp (myData);
                	}
                    
                }

            } catch ( pdb :: NotEnoughSpace &n ) {
                //now we can unpin this page
                cout << "we have finished one page!" << std :: endl;
                getRecord (supers);                 
                page->unpin();
                pagesWritten ++;
            }            
        }


        //let's flush
        {
	        int num = storage->getCache()->unpinAndEvictAllDirtyPages();
	        std :: cout << num << " pages are added to flush buffer!" << std :: endl;
	        std :: cout << "sleep 5 seconds to wait for flushing threads to be scheduled..." << std :: endl;
	        sleep (5);
	        std :: cout << "done flushing!" << std :: endl;
		}

        //now we run a query
        pdb :: makeObjectAllocatorBlock (1024 * 1024, true);
        pdb :: Handle <pdb :: CheckEmployee> myQuery = pdb :: makeObject <pdb :: CheckEmployee> (std :: string ("Steve Stevens"));	

		// get a query processor and intitialize it
		auto queryProc = myQuery->getProcessor ();
		queryProc->initialize ();
		SetPtr outputSet = storage->getSet(std :: pair <std :: string, std :: string> ("testDatabase", "testSetOutput"));
        storage->getCache()->pin(inputSet, MRU, Read);
        storage->getCache()->pin(outputSet, MRU, Write);

        PDBPagePtr prevOutput;
        {
        	//Load the first output page
        	PDBPagePtr outputPage = storage->getNewPage(std :: pair <std :: string, std :: string>("testDatabase", "testSetOutput"));
        	queryProc->loadOutputPage (outputPage->getBytes (), outputPage->getSize());
        	prevOutput = outputPage;
        }

        {
	        vector<PageIteratorPtr> * pageIters = inputSet->getIterators();
	        //Now we loop through all input pages
	        int numIterators = pageIters->size();
	        for (int i = 0; i < numIterators; i++) {
	        	PageIteratorPtr iter = pageIters->at(i);
	            while (iter->hasNext()){
	                PDBPagePtr inputPage = iter->next();
	                if (inputPage != nullptr) {
	                    std :: cout << "processing page with pageId=" << inputPage->getPageID()<<std :: endl;
	        			queryProc->loadInputPage (inputPage->getBytes ());

	        			while (queryProc->fillNextOutputPage ()) {
	        				//Load new output page as we fill the current output page.
	        				PDBPagePtr outputPage = storage->getNewPage(std :: pair <std :: string, std :: string>("testDatabase", "testSetOutput"));
	        				queryProc->loadOutputPage (outputPage->getBytes (), outputPage->getSize());
	        				prevOutput->unpin();
	        				prevOutput = outputPage;
	        			}

	                }
	                inputPage->unpin();
	            }
	        }

	        queryProc->finalize();
	        queryProc->fillNextOutputPage ();
	        
		}

		std :: cout << "Done processing the query! Flushing all pages" << std :: endl;
		{
			int num = storage->getCache()->unpinAndEvictAllDirtyPages();
	        std :: cout << num << " pages are added to flush buffer!" << std :: endl;
	        std :: cout << "sleep 5 seconds to wait for flushing threads to be scheduled..." << std :: endl;
	        sleep (5);
	        std :: cout << "done flushing! Printing query results!" << std :: endl;
    	}

		using namespace pdb;
        storage->getCache()->pin(outputSet, MRU, Read);
        vector<PageIteratorPtr> * pageIters = outputSet->getIterators();
        //Now we loop through all output pages
	    int numIterators = pageIters->size();
	    for (int i = 0; i < numIterators; i++) {
	        PageIteratorPtr iter = pageIters->at(i);
	        while (iter->hasNext()){
	            PDBPagePtr outputPage = iter->next();
	            
	            //print out the results
	            if (outputPage != nullptr) {

	            	auto *temp = (Record <Vector <Handle <Vector <Handle <Employee>>>>> *) outputPage->getBytes ();
					auto myGuys = temp->getRootObject ();
					for (int i = 0; i < myGuys->size (); i++) {
						for (int j = 0; j < (*myGuys)[i]->size (); j++) {
							(*((*myGuys)[i]))[j]->print ();
							std :: cout << "\n";
						}
					}

	            }

	            outputPage->unpin();
	        }
		}

        std :: cout << "finish!" << std :: endl;
        storage->stopFlushConsumerThreads();

}


