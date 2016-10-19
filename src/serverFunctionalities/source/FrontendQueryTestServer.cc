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

#include "FrontendQueryTestServer.h"
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
#include "BackendExecuteSelection.h"
#include "KeepGoing.h"
#include "DoneWithResult.h"
#include "PangeaStorageServer.h"
#include "JobStage.h"


namespace pdb {

FrontendQueryTestServer :: ~FrontendQueryTestServer () {}

void FrontendQueryTestServer :: registerHandlers (PDBServer &forMe) {

       // handle a request to execute a job stage
       forMe.registerHandler (JobStage_TYPEID, make_shared <SimpleRequestHandler <JobStage>> (
               [&] (Handle <JobStage> request, PDBCommunicatorPtr sendUsingMe) {

                std :: string errMsg;
                bool success;
                std :: cout << "Frontend got a request for JobStage" << std :: endl;
                request->print();
                const UseTemporaryAllocationBlock tempBlock {128*1024*1024};
                PDBCommunicatorPtr communicatorToBackend = make_shared<PDBCommunicator>();
                if (communicatorToBackend->connectToLocalServer(getFunctionality<PangeaStorageServer>().getLogger(), getFunctionality<PangeaStorageServer>().getPathToBackEndServer(), errMsg)) {
                    std :: cout << errMsg << std :: endl;
                    exit(1);
                }
                std :: cout << "Frontend connected to backend" << std :: endl;
                Handle<JobStage> newRequest = makeObject<JobStage>(request->getStageId());
                
                //restructure the input information  
                std :: string inDatabaseName = request->getInput()->getDatabase();
                std :: string inSetName = request->getInput()->getSetName();
                Handle<SetIdentifier> input = makeObject<SetIdentifier>(inDatabaseName, inSetName);
                
                SetPtr inputSet = getFunctionality <PangeaStorageServer> ().getSet (std :: pair<std ::string, std::string>(inDatabaseName, inSetName));               
                input->setDatabaseId(inputSet->getDbID());
                input->setTypeId(inputSet->getTypeID());
                input->setSetId(inputSet->getSetID());
                newRequest->setInput(input);


                std :: string outDatabaseName = request->getOutput()->getDatabase();
                std :: string outSetName = request->getOutput()->getSetName();
       
                // add the output set
                //TODO: check whether output set exists
                std :: pair <std :: string, std :: string> outDatabaseAndSet = std :: make_pair (outDatabaseName, outSetName);
                getFunctionality <PangeaStorageServer> ().addSet(outDatabaseName, outSetName);
                SetPtr outputSet = getFunctionality <PangeaStorageServer> ().getSet(outDatabaseAndSet);

                // create the output set in the storage manager and in the catalog
                int16_t outType = getFunctionality <CatalogServer> ().searchForObjectTypeName (request->getOutputTypeName ());
                if (!getFunctionality <CatalogServer> ().addSet (outType, outDatabaseAndSet.first, outDatabaseAndSet.second, errMsg)) {
                        //std :: cout << "Could not create the query output set " << outDatabaseAndSet.second << ": " << errMsg << "\n";
                        exit (1);
                }

                //restructure the output information
                Handle<SetIdentifier> output = makeObject<SetIdentifier>(outDatabaseName, outSetName);
                output->setDatabaseId(outputSet->getDbID());
                output->setTypeId(outputSet->getTypeID());
                output->setSetId(outputSet->getSetID());
                newRequest->setOutput(output);
                newRequest->setOutputTypeName(request->getOutputTypeName());
                
                //copy operators
                Vector<Handle<ExecutionOperator>> operators = request->getOperators();
                for (int i=0; i < operators.size(); i++) {
                     newRequest->addOperator(operators[i]);
                }              
 
                newRequest->print();

                if (!communicatorToBackend->sendObject(newRequest, errMsg)) {
                    std :: cout << errMsg << std :: endl;
                    exit(1);
                }
                std :: cout << "Frontend sent request to backend" << std :: endl;
                // wait for backend to finish.
                communicatorToBackend->getNextObject<SimpleRequestResult>(success, errMsg);
                if (!success) {
                    std :: cout << "Error waiting for backend to finish this job stage. " << errMsg << std :: endl;
                    exit(1);
                }

                // now, we send back the result
                Handle <Vector <String>> result = makeObject <Vector <String>> ();
                result->push_back (request->getOutput()->getSetName());
                std :: cout << "Query is done. " << std :: endl;
                // return the results
                if (!sendUsingMe->sendObject (result, errMsg)) {
                     return std :: make_pair (false, errMsg);
                }

                return std :: make_pair (true, std :: string("execution complete")); 
 

             } ));




	
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
				std :: cout << "Query is done. " << std :: endl;
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
                        std :: cout << "we are now iterating set:" << whichSet << std :: endl;
			// and keep looping while someone wants to get the output
			SetPtr loopingSet = getFunctionality <PangeaStorageServer> ().getSet (std :: make_pair (whichDatabase, whichSet));
			loopingSet->setPinned(true);
			vector<PageIteratorPtr> * pageIters = loopingSet->getIterators();
			// loop through all pages
			int numIterators = pageIters->size();
			std::cout << "Number pages to send " << std::to_string(loopingSet->getNumPages()) << std::endl;
                        std::cout << "Number of iterators" << numIterators << std :: endl;
		    for (int i = 0; i < numIterators; i++) {
                        std :: cout << "the " << i << "-th iterator" << std :: endl;
		        PageIteratorPtr iter = pageIters->at(i);
		        while (iter->hasNext()){
		            PDBPagePtr nextPage = iter->next();
                            std :: cout << "Got a page!" << std :: endl;
		            // send the relevant page.
		            if (nextPage != nullptr) {
		            	std::cout << "Page is not null!! Sending out next page!" << std::endl;
                                std :: cout << "check the page at server side" << std :: endl;
                                Record <Vector <Handle<Object>>> * myRec = (Record <Vector<Handle<Object>>> *) (nextPage->getBytes());
                                Handle<Vector<Handle<Object>>> inputVec = myRec->getRootObject ();
                                int vecSize = inputVec->size();
                                std :: cout << "in the page to sent: vector size =" << vecSize << std :: endl;
                                if (vecSize != 0) {          
      						const UseTemporaryAllocationBlock tempBlock {1024};
						if (!sendUsingMe->sendBytes (nextPage->getBytes (), nextPage->getSize (), errMsg)) {
							return std :: make_pair (false, errMsg);	
						}
                                                std :: cout << "Page sent to client!" << std :: endl;
						// see whether or not the client wants to see more results
						bool success;
						if (sendUsingMe->getObjectTypeID () != DoneWithResult_TYPEID) {
							Handle <KeepGoing> temp = sendUsingMe->getNextObject <KeepGoing> (success, errMsg);
                                                        std :: cout << "Keep going" << std :: endl;
							if (!success)
								return std :: make_pair (false, errMsg);
						} else {
							Handle <DoneWithResult> temp = sendUsingMe->getNextObject <DoneWithResult> (success, errMsg);
                                                        std :: cout << "Done" << std :: endl;
							if (!success)
								return std :: make_pair (false, errMsg);
							else
								return std :: make_pair (true, std :: string ("everything OK!"));
						}
                               }

		            } else {
                              std :: cout << "We've got a null page!!!" << std :: endl;
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
void FrontendQueryTestServer :: computeQuery (std :: string setNameToUse, std :: string setPrefix, int &whichNode, 
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

void FrontendQueryTestServer :: doSelection (std :: string setNameToUse, Handle <QueryBase> &computeMe) {

	Handle <Selection <Object, Object>> myQuery = unsafeCast <Selection <Object, Object>> (computeMe);
	// forward execute query request to backend.
	const UseTemporaryAllocationBlock tempBlock {1024 * 128};
	{
		std :: string errMsg;
		bool success;
		// get the input information from the query node
		std :: string inputSet = computeMe->getIthInput (0)->getSetName ();
		std :: string inputDatabase = computeMe->getIthInput (0)->getDBName ();
		std :: pair <std :: string, std :: string> databaseAndSet = std :: make_pair (inputDatabase, inputSet);
		SetPtr inputSet_sp = getFunctionality <PangeaStorageServer> ().getSet (databaseAndSet);

		// add the output set
		std :: pair <std :: string, std :: string> outDatabaseAndSet = std :: make_pair (inputDatabase, setNameToUse);
		getFunctionality <PangeaStorageServer> ().addSet(inputDatabase, setNameToUse);
	    SetPtr set = getFunctionality <PangeaStorageServer> ().getSet(outDatabaseAndSet);

		// create the output set in the storage manager and in the catalog
		int16_t outType = getFunctionality <CatalogServer> ().searchForObjectTypeName (myQuery->getOutputType ());
		if (!getFunctionality <CatalogServer> ().addSet (outType, outDatabaseAndSet.first, outDatabaseAndSet.second, errMsg)) {
			//std :: cout << "Could not create the query output set " << outDatabaseAndSet.second << ": " << errMsg << "\n";	
			exit (1);
		}

		// annotate this guy with his output name
		computeMe->setSetName (outDatabaseAndSet.second);

		DatabaseID dbIdIn = inputSet_sp->getDbID();
	    UserTypeID typeIdIn = inputSet_sp->getTypeID();
	    SetID setIdIn = inputSet_sp->getSetID();
		DatabaseID dbIdOut = set->getDbID();
	    UserTypeID typeIdOut = set->getTypeID();
	    SetID setIdOut = set->getSetID();

		Handle <BackendExecuteSelection> executeQuery = makeObject <BackendExecuteSelection> (dbIdIn, typeIdIn, setIdIn, dbIdOut, typeIdOut, setIdOut);
		PDBCommunicatorPtr communicatorToBackend = make_shared<PDBCommunicator>();
	    if (communicatorToBackend->connectToLocalServer(getFunctionality<PangeaStorageServer>().getLogger(), getFunctionality<PangeaStorageServer>().getPathToBackEndServer(), errMsg)) {
	        std :: cout << errMsg << std :: endl;
	        exit(1);
	    }
	    if (!communicatorToBackend->sendObject(executeQuery, errMsg)) {
	    	std :: cout << errMsg << std :: endl;
	        exit(1);
	    }

	    Handle <Vector <Handle <QueryBase>>> runUs = makeObject <Vector <Handle <QueryBase>>> ();
	    runUs->push_back(myQuery);
	    if (!communicatorToBackend->sendObject(runUs, errMsg)) {
	    	std :: cout << errMsg << std :: endl;
	        exit(1);
	    }
	    // wait for backend to finish.
	    communicatorToBackend->getNextObject<SimpleRequestResult>(success, errMsg);
	    if (!success) {
	    	std :: cout << "Error waiting for backend to finish selection query execution. " << errMsg << std :: endl;
	    	exit(1);
	    }

	}

}


}
