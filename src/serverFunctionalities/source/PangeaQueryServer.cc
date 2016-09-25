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

#include <cstddef>
#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

#include "PangeaQueryServer.h"
#include "SimpleRequestHandler.h"
#include "BuiltInObjectTypeIDs.h"
#include "SimpleRequestResult.h"
#include "QueryBase.h"
#include "ExecuteQuery.h"
#include "InterfaceFunctions.h"
#include "GenericWork.h"
#include "DeleteSet.h"
#include "CatalogServer.h"
#include "SetScan.h"
#include "Selection.h"
#include "KeepGoing.h"
#include "DoneWithResult.h"
#include "PangeaStorageServer.h"

namespace pdb {

PangeaQueryServer :: PangeaQueryServer (int numThreadsToUseIn) {
	numThreadsToUse = numThreadsToUseIn;
	tempSetName = 0;
}

PangeaQueryServer :: ~PangeaQueryServer () {}

void PangeaQueryServer :: registerHandlers (PDBServer &forMe) {
	
	// handle a request to execute a query
	forMe.registerHandler (ExecuteQuery_TYPEID, make_shared <SimpleRequestHandler <ExecuteQuery>> (
		[&] (Handle <ExecuteQuery> request, PDBCommunicatorPtr sendUsingMe) {

			// this will allow us to have some extra RAM for local allocations; in particular,
			// we will want to store all of the names of the output sets
			const UseTemporaryAllocationBlock tempBlock {1024 * 128};
			{
	
				// this lists all of the temporary sets created
				std :: vector <std :: string> setsCreated;

				// get the list of queries to execute
				std :: string errMsg;
				bool success;
				Handle <Vector <Handle <QueryBase>>> runUs = sendUsingMe->getNextObject <Vector <Handle <QueryBase>>> (success, errMsg);
				if (!success) {
					return std :: make_pair (false, errMsg);
				}

				// this is the name of the set that we are going to write temporary data to
				std :: string tempSetPrefix = "tempSet" + std :: to_string (tempSetName);
				tempSetName++;
	
				// this keeps track of which node in the query plan we computed
				int whichNode = 0;
	
				// first, loop through all of the outputs and compute them
				for (int i = 0; i < runUs->size (); i++) {
					computeQuery ("", tempSetPrefix, whichNode, (*runUs)[i], setsCreated);
				}	

				// delete all of the temporary sets created
				if (runUs->size () > 0) {
					std :: string whichDatabase = (*runUs)[0]->getDBName ();
					for (auto &s : setsCreated) {
						std :: string errMsg;
						if (!getFunctionality <CatalogServer> ().deleteSet (whichDatabase, s, errMsg)) {
							std :: cout << "Error deleting set " << s << ": " << errMsg << "\n";	
						} else {
							std :: cout << "Successfully deleted set " << s << "\n";
						}
					}
				}

				// now, we send back the result
				const UseTemporaryAllocationBlock tempBlock {1024};
				Handle <Vector <String>> result = makeObject <Vector <String>> ();
				for (int i = 0; i < runUs->size (); i++) {
					if ((*runUs)[i]->getQueryType () == "localoutput") {
						result->push_back ((*runUs)[i]->getSetName ());
					} else {
						std :: cout << "We only support set: outputs for queries.\n";
					}
				}

				// return the results
				if (!sendUsingMe->sendObject (result, errMsg)) {
					return std :: make_pair (false, errMsg);
				}
			}

			return std :: make_pair (true, std :: string("execution complete"));
		}));
	
	// handle a request to delete a file
	forMe.registerHandler (DeleteSet_TYPEID, make_shared <SimpleRequestHandler <DeleteSet>> (
		[&] (Handle <DeleteSet> request, PDBCommunicatorPtr sendUsingMe) {

			const UseTemporaryAllocationBlock tempBlock {1024};
			{
				std :: string errMsg;
				if (!getFunctionality <CatalogServer> ().deleteSet (request->whichDatabase (), request->whichSet (), errMsg)) {
					Handle <SimpleRequestResult> result = makeObject <SimpleRequestResult> 
						(false, std :: string ("error attempting to delete set: " + errMsg));
					if (!sendUsingMe->sendObject (result, errMsg)) {
						return std :: make_pair (false, errMsg);
					}
				} else {
					Handle <SimpleRequestResult> result = makeObject <SimpleRequestResult> (true, std :: string ("successfully deleted set"));
					if (!sendUsingMe->sendObject (result, errMsg)) {
						return std :: make_pair (false, errMsg);
					}
				}
				return std :: make_pair (true, std :: string ("delete complete"));
			}
		}));

	// handle a request to iterate through a file	
	forMe.registerHandler (SetScan_TYPEID, make_shared <SimpleRequestHandler <SetScan>> (
		[&] (Handle <SetScan> request, PDBCommunicatorPtr sendUsingMe) {
                   
			// for error handling
			std :: string errMsg;

			// this is the number of pages
			std :: string whichDatabase = request->getDatabase ();
			std :: string whichSet = request->getSetName ();
                        //std :: cout << "we are now iterating set:" << whichSet << std :: endl;
			// and keep looping while someone wants to get the output
			SetPtr loopingSet = getFunctionality <PangeaStorageServer> ().getSet (std :: make_pair (whichDatabase, whichSet));
			loopingSet->setPinned(true);
			vector<PageIteratorPtr> * pageIters = loopingSet->getIterators();

			// loop through all pages
			int numIterators = pageIters->size();
                        //std :: cout << "number of iterators:" << numIterators << std :: endl;
		    for (int i = 0; i < numIterators; i++) {
                        //std :: cout << "to use iterator " << i << std :: endl;
		        PageIteratorPtr iter = pageIters->at(i);
		        while (iter->hasNext()){
                            //std :: cout << "has next!" << std :: endl;
		            PDBPagePtr nextPage = iter->next();
		            
		            // send the relevant page.
		            if (nextPage != nullptr) {
                                                //std :: cout << "We've got a page to sent..." << std :: endl;
						const UseTemporaryAllocationBlock tempBlock {1024};
						if (!sendUsingMe->sendBytes (nextPage->getBytes (), nextPage->getSize (), errMsg)) {
							return std :: make_pair (false, errMsg);	
						}

						// see whether or not the client wants to see more results
						bool success;
						if (sendUsingMe->getObjectTypeID () != DoneWithResult_TYPEID) {
							Handle <KeepGoing> temp = sendUsingMe->getNextObject <KeepGoing> (success, errMsg);
							if (!success)
								return std :: make_pair (false, errMsg);
						} else {
							Handle <DoneWithResult> temp = sendUsingMe->getNextObject <DoneWithResult> (success, errMsg);
							if (!success)
								return std :: make_pair (false, errMsg);
							else
								return std :: make_pair (true, std :: string ("everything OK!"));
						}

		            } else {
                                                //std :: cout << "We've got a null page!!!" << std :: endl;
                            }
                            nextPage->unpin();

		        }
			}
                        loopingSet->setPinned(false);
			// tell the caller we are done
			const UseTemporaryAllocationBlock tempBlock {1024};
			Handle <DoneWithResult> temp = makeObject <DoneWithResult> ();
			if (!sendUsingMe->sendObject (temp, errMsg)) {
				return std :: make_pair (false, "could not send done message: " + errMsg);
			}
			// we got to here means success!!  We processed the query, and got all of the results
			return std :: make_pair (true, std :: string ("query completed!!"));
		}));
}

// this recursively traverses a simple query graph, where each node can only have one input,
// makes sure that each node has been computed... the return value is the (DB, set) pair holding
// the result of the query
void PangeaQueryServer :: computeQuery (std :: string setNameToUse, std :: string setPrefix, int &whichNode, 
	Handle <QueryBase> &computeMe, std :: vector <std :: string> &setsCreated) {
	
	// base case: this node has been computed, so we are done
	if (computeMe->getSetName () != "" && computeMe->getQueryType () != "localoutput") {
                //std :: cout << "the node is saying I can return" << std :: endl;
		return;
	}	

	// recursive case: compute the parent of this node... we assume only one input in this simple case
	whichNode++;

	// now, execute this node
	if (computeMe->getQueryType () == "selection") {
		
		// run the rest of the query plan
		computeQuery ("", setPrefix, whichNode, computeMe->getIthInput (0), setsCreated);

		// now run this guy
		if (setNameToUse == "") {
			std :: string tempFileName = setPrefix + "." + std :: to_string (++whichNode);
			setsCreated.push_back (tempFileName);
			doSelection (tempFileName, computeMe);	
		} else {
			doSelection (setNameToUse, computeMe);	
		}

	} else if (computeMe->getQueryType () == "localoutput") {
		
		// run the rest of the query plan
		computeQuery (computeMe->getSetName (), setPrefix, whichNode, computeMe->getIthInput (0), setsCreated);

	} else {

		// other node types go here!
		std :: cout << "I didn't recognize the query node type!!\n";
	}
}

void PangeaQueryServer :: doSelection (std :: string setNameToUse, Handle <QueryBase> &computeMe) {

	// the query we are executing
	Handle <Selection <Object, Object>> myQuery = unsafeCast <Selection <Object, Object>> (computeMe);

	// get the input information from the query node
	std :: string inputSet = computeMe->getIthInput (0)->getSetName ();
	std :: string inputDatabase = computeMe->getIthInput (0)->getDBName ();
	std :: pair <std :: string, std :: string> databaseAndSet = std :: make_pair (inputDatabase, inputSet);
	SetPtr inputSet_sp = getFunctionality <PangeaStorageServer> ().getSet (databaseAndSet);
        inputSet_sp->setPinned(true);
	vector<PageIteratorPtr> * inPageIters = inputSet_sp->getIterators();
	int numIterators = inPageIters->size();

	// add the output set
	std :: pair <std :: string, std :: string> outDatabaseAndSet = std :: make_pair (inputDatabase, setNameToUse);
        //std :: cout << "now we add a set with name=" << setNameToUse << std :: endl;
	getFunctionality <PangeaStorageServer> ().addSet(inputDatabase, setNameToUse);
        SetPtr set = getFunctionality <PangeaStorageServer> ().getSet(outDatabaseAndSet);
	set->setPinned(true);
	// create the output set in the storage manager and in the catalog
	std :: string errMsg;
	int16_t outType = getFunctionality <CatalogServer> ().searchForObjectTypeName (myQuery->getOutputType ());
	if (!getFunctionality <CatalogServer> ().addSet (outType, outDatabaseAndSet.first, outDatabaseAndSet.second, errMsg)) {
		//std :: cout << "Could not create the query output set " << outDatabaseAndSet.second << ": " << errMsg << "\n";	
		exit (1);
	}

	// annotate this guy with his output name
	computeMe->setSetName (outDatabaseAndSet.second);

	// this tells us how much more work to do... protected by a mutex
	int pageIterNum = 0;
	PageIteratorPtr iter = inPageIters->at(pageIterNum);
	pthread_mutex_t workerMutex;
	pthread_mutex_init(&workerMutex, nullptr);

	// we'll wait on this
	PDBBuzzerPtr myBuzzer = make_shared <PDBBuzzer> (nullptr);
	int numOutputPages = 0;
	int numInputPage = 0;
	// get all of the workers
	for (int i = 0; i < numThreadsToUse; i++) {

		// get the worker
		PDBWorkerPtr myWorker = getWorker ();

		// create a piece of work for this guy to do
		PDBWorkPtr myWork = make_shared <GenericWork> (
			[&] (PDBBuzzerPtr callerBuzzer) {

				// get the page where we write output to
				PDBPagePtr myOutPage = getFunctionality <PangeaStorageServer> ().getNewPage (outDatabaseAndSet);
				PDBPagePtr prevOutpage = myOutPage;
				// get the query processor for this guy
				auto queryProc = myQuery->getProcessor ();
				queryProc->initialize ();
				++numOutputPages;
				queryProc->loadOutputPage (myOutPage->getBytes (), myOutPage->getSize());
				
				// loop while we still have pages to process
				bool allDone = false;
				while (!allDone) {
					
					PDBPagePtr inputPage = nullptr;
                                        while (inputPage == nullptr)
					{
						const LockGuard guard {workerMutex};
						if (pageIterNum + 1 == numIterators && !iter->hasNext()) {
							queryProc->finalize ();
							allDone = true;
                                                        break;
						} else if (iter->hasNext()){
							inputPage = iter->next();
							++numInputPage;
						} else {
							iter = inPageIters->at(++pageIterNum);
							if (iter->hasNext()) {
								inputPage = iter->next();
								++numInputPage;
							} else {
								queryProc->finalize ();
								allDone = true;
                                                                break;
							}
						}
					}
                                        //std :: cout << "to process one page..." << std :: endl;
					// load up the next page
					if (!allDone) {
						queryProc->loadInputPage (inputPage->getBytes ());
					}
					
					// get the results
					while (queryProc->fillNextOutputPage ()) {
                                                //std :: cout << "to write another page..." << std :: endl;
						++numOutputPages;
						//Load new output page as we fill the current output page.
        				PDBPagePtr outputPage = getFunctionality <PangeaStorageServer> ().getNewPage (outDatabaseAndSet);
        				queryProc->loadOutputPage (outputPage->getBytes (), outputPage->getSize());
        				prevOutpage->unpin();
        				prevOutpage = outputPage;

					}
                                        if (inputPage != nullptr) {
                                             inputPage->unpin();
                                        }
				}
				prevOutpage->unpin();
                                //std :: cout << "finished!" << std :: endl;
				// let the caller know that we are all done
				callerBuzzer->buzz (PDBAlarm :: WorkAllDone);
			}	
		);
                
		// ask our worker to do the work
		myWorker->execute (myWork, myBuzzer);
	}
		
	// now, wait until we are done
	myBuzzer->wait ();	
        set->setPinned(false);
        inputSet_sp->setPinned(false);
	// deallocate the mutex
	pthread_mutex_destroy(&workerMutex);
}

}
