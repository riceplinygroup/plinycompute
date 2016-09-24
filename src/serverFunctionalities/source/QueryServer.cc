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

#include "QueryServer.h"
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
#include "StorageServer.h"

namespace pdb {

QueryServer :: QueryServer (int numThreadsToUseIn) {
	numThreadsToUse = numThreadsToUseIn;
	tempSetName = 0;
}

QueryServer :: ~QueryServer () {}

void QueryServer :: registerHandlers (PDBServer &forMe) {

	// handle a request to execute a query
	forMe.registerHandler (ExecuteQuery_TYPEID, make_shared <SimpleRequestHandler <ExecuteQuery>> (
		[&] (Handle <ExecuteQuery> request, PDBCommunicatorPtr sendUsingMe) {

			// this will allow us to have some extra RAM for local allocations; in particular,
			// we will want to store all of the names of the output sets
			const UseTemporaryAllocationBlock tempBlock {1024 * 256};
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
						}	
					}
				}

				// now, we send back the result
				//const UseTemporaryAllocationBlock tempBlock {1024};
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
					Handle <SimpleRequestResult> result = makeObject <SimpleRequestResult> (false, std :: string ("successfully deleted set"));
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

			// this is the page we are on
			size_t whichPage = 0;

			// this is the number of pages
			std :: string whichDatabase = request->getDatabase ();
			std :: string whichSet = request->getSetName ();
			size_t numPages = getFunctionality <CatalogServer> ().getNumPages (whichDatabase, whichSet);

			// and keep looping while someone wants to get the output
			MyDB_BufferManagerPtr myBufferMgr = getFunctionality <StorageServer> ().getBufferManager ();
			while (whichPage != numPages) {

				const UseTemporaryAllocationBlock tempBlock {1024};
				// now, get the relevant page
				MyDB_PageHandle page = getFunctionality <StorageServer> ().getPage (std :: make_pair (whichDatabase, whichSet), whichPage);
				whichPage++;

				// and send it
				if (!sendUsingMe->sendBytes (page->getBytes (), myBufferMgr->getPageSize (), errMsg)) {
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
			}

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
void QueryServer :: computeQuery (std :: string setNameToUse, std :: string setPrefix, int &whichNode, 
	Handle <QueryBase> &computeMe, std :: vector <std :: string> &setsCreated) {

	// base case: this node has been computed, so we are done
	if (computeMe->getSetName () != "" && computeMe->getQueryType () != "localoutput") {
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

void QueryServer :: doSelection (std :: string setNameToUse, Handle <QueryBase> &computeMe) {

	// the query we are executing
	Handle <Selection <Object, Object>> myQuery = unsafeCast <Selection <Object, Object>> (computeMe);

	// get the input information from the query node
	std :: string inputSet = computeMe->getIthInput (0)->getSetName ();
	std :: string inputDatabase = computeMe->getIthInput (0)->getDBName ();
	std :: pair <std :: string, std :: string> databaseAndSet = std :: make_pair (inputDatabase, inputSet);
	MyDB_TablePtr tableToProcess = getFunctionality <StorageServer> ().getTable (databaseAndSet);
	int numPagesToProcess = getFunctionality <CatalogServer> ().getNumPages (inputDatabase, inputSet);

	// make sure we got a valid result
	if (numPagesToProcess < 0) {
		std :: cout << "Error: is " << inputDatabase << "." << inputSet << " a valid data set?\n";
	}

	// this is the output table
	std :: pair <std :: string, std :: string> outDatabaseAndSet = std :: make_pair (inputDatabase, setNameToUse);

	// create the output table in the storage manager and in the catalog
	std :: string errMsg;
	int16_t outType = getFunctionality <CatalogServer> ().searchForObjectTypeName (myQuery->getOutputType ());
	if (!getFunctionality <CatalogServer> ().addSet (outType, outDatabaseAndSet.first, outDatabaseAndSet.second, errMsg)) {
		std :: cout << "Could not create the query output set " << outDatabaseAndSet.second << ": " << errMsg << "\n";	
		exit (1);
	}
	MyDB_TablePtr resultTable = getFunctionality <StorageServer> ().getTable (outDatabaseAndSet);

	// annotate this guy with his output name
	computeMe->setSetName (outDatabaseAndSet.second);

	// now we get the buffer manager
	MyDB_BufferManagerPtr myBufferMgr = getFunctionality <StorageServer> ().getBufferManager ();

	// this tells us how much more work to do... protected by a mutex
	int lastPageProcessed = 0;
	pthread_mutex_t workerMutex;
	pthread_mutex_init(&workerMutex, nullptr);

	// we'll wait on this
	PDBBuzzerPtr myBuzzer = make_shared <PDBBuzzer> (nullptr);

	// get all of the workers
	for (int i = 0; i < numThreadsToUse; i++) {

		// get the worker
		PDBWorkerPtr myWorker = getWorker ();

		// create a piece of work for this guy to do
		PDBWorkPtr myWork = make_shared <GenericWork> (
			[&] (PDBBuzzerPtr callerBuzzer) {
					
				// get the page where we write output to
				MyDB_PageHandle myOutPage = getFunctionality <StorageServer> ().getNewPage (outDatabaseAndSet);

				// get the query processor for this guy
				auto queryProc = myQuery->getProcessor ();
				queryProc->initialize ();
				queryProc->loadOutputPage (myOutPage->getBytes (), myBufferMgr->getPageSize ());
				
				// loop while we still have pages to process
				bool allDone = false;
				while (!allDone) {
					
					// get the last page processed
					int myInPage;
					{
						const LockGuard guard {workerMutex};
						if (lastPageProcessed == numPagesToProcess) {
							queryProc->finalize ();
							allDone = true;
						} else {
							myInPage = lastPageProcessed;
							lastPageProcessed++;
						}
					}

					// load up the next page
					if (!allDone) {
						MyDB_PageHandle pageToRead = myBufferMgr->getPage (tableToProcess, myInPage);
						queryProc->loadInputPage (pageToRead->getBytes ());
					}

					// get the results
					while (queryProc->fillNextOutputPage ()) {

						// tell the buffer manager that we wrote the current output page
						myOutPage->wroteBytes ();
						myOutPage->unpin ();
						myOutPage->flush ();
						myOutPage = getFunctionality <StorageServer> ().getNewPage (outDatabaseAndSet);
							
						// and get the next output page
						queryProc->loadOutputPage (myOutPage->getBytes (), myBufferMgr->getPageSize ());
					}
				}

				// allow the page to be written back
				myOutPage->wroteBytes ();
				myOutPage->unpin ();
				myOutPage->flush ();

				// let the caller know that we are all done
				callerBuzzer->buzz (PDBAlarm :: WorkAllDone);
			}	
		);

		// ask our worker to do the work
		myWorker->execute (myWork, myBuzzer);
	}
		
	// now, wait until we are done
	myBuzzer->wait ();	

	// deallocate the mutex
	pthread_mutex_destroy(&workerMutex);
}

}
