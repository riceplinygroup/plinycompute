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

#ifndef QUERY_CLIENT
#define QUERY_CLIENT

#include "Set.h"
#include "SetIterator.h"
#include "Handle.h"
#include "PDBLogger.h"
#include "PDBVector.h"
#include "CatalogClient.h"
#include "DeleteSet.h"
#include "ExecuteQuery.h"
#include "TupleSetExecuteQuery.h"
#include "Computation.h"
namespace pdb {

class QueryClient {

public:

        QueryClient() {}

	// connect to the database
	QueryClient (int portIn, std :: string addressIn, PDBLoggerPtr myLoggerIn, int useScheduler=false) : myHelper (portIn, addressIn, myLoggerIn) {
		port = portIn;
		address = addressIn;
		myLogger = myLoggerIn;
		runUs = makeObject <Vector <Handle <QueryBase>>> ();
                this->useScheduler = useScheduler;
	}

        //added by Jia
        ~QueryClient() {
                runUs = nullptr;
       }

	// access a set in the database
	template <class Type>
	Handle <Set <Type>> getSet (std :: string databaseName, std :: string setName) {
	
		// verify that the database and set work 
#ifdef DEBUG_SET_TYPE
		std :: string errMsg;

		std :: string typeName = myHelper.getObjectType (databaseName, setName, errMsg);
		
		if (typeName == "") {
			std :: cout << "I was not able to obtain the type for database set " << setName << "\n";
			myLogger->error ("query client: not able to verify type: " + errMsg);
			Handle <Set <Type>> returnVal = makeObject <Set <Type>> (false);
			return returnVal;
		}

		if (typeName != getTypeName <Type> ()) {
			std :: cout << "Wrong type for database set " << setName << "\n";
			Handle <Set <Type>> returnVal = makeObject <Set <Type>> (false);
			return returnVal;
		}
#endif
		Handle <Set <Type>> returnVal = makeObject <Set <Type>> (databaseName, setName);
		return returnVal;
	}

	// get an iterator for a set in the database
	template <class Type>
	SetIterator <Type> getSetIterator (std :: string databaseName, std :: string setName) {

		// verify that the database and set work 
#ifdef DEBUG_SET_TYPE
		std :: string errMsg;
		std :: string typeName = myHelper.getObjectType (databaseName, setName, errMsg);
		
		if (typeName == "") {
			myLogger->error ("query client: not able to verify type: " + errMsg);
			SetIterator <Type> returnVal;
			return returnVal;
		}
#endif
                //commented by Jia, below type check can not work with complex types such as Vector<Handle<Foo>>
                /*
		if (typeName != getTypeName <Type> ()) {
			std :: cout << "Wrong type for database set " << setName << "\n";
			SetIterator <Type> returnVal;
			return returnVal;
		}
                */
		SetIterator <Type> returnVal (myLogger, port, address, databaseName, setName);
		return returnVal;
	}

	bool deleteSet (std :: string databaseName, std :: string setName) {
                // this is for query testing stuff
		return simpleRequest <DeleteSet, SimpleRequestResult, bool, String, String> (myLogger, port, 
		address, false, 124 * 1024, 
                [&] (Handle <SimpleRequestResult> result) {
			std :: string errMsg;
                        if (result != nullptr) {

				// make sure we got the correct number of results
				if (!result->getRes ().first) {
                                        errMsg = "Could not remove set: " + result->getRes ().second;
                                        myLogger->error ("QueryErr: " + errMsg);
                                        return false;
                                }

                                return true;
                        }
                        errMsg = "Error getting type name: got nothing back from catalog";
                        return false;}, databaseName, setName);
	}


        bool executeQuery (std :: string &errMsg, Handle<Vector<Handle<Computation>>> computations, bool isAggregation = false) {

                        // this is the request
                const UseTemporaryAllocationBlock myBlock {1024};

                Handle<TupleSetExecuteQuery> executeQuery;

                if (isAggregation == false) {
                      executeQuery = makeObject <TupleSetExecuteQuery> ();
                } else {
                      executeQuery = makeObject <TupleSetExecuteQuery> (true);
                }

                // this call asks the database to execute the query, and then it inserts the result set name
                // within each of the results, as well as the database connection information

                // this is for query scheduling stuff
                return simpleDoubleRequest<TupleSetExecuteQuery, Vector<Handle<Computation>>, SimpleRequestResult, bool> (myLogger, port, address, false, 124 * 1024,
                [&] (Handle<SimpleRequestResult> result) {
                        if (result != nullptr) {
                                if (!result->getRes ().first) {
                                        errMsg = "Error in query: " + result->getRes ().second;
                                        myLogger->error ("Error querying data: " + result->getRes ().second);
                                        return false;
                                }
                                return true;
                        }
                        errMsg = "Error getting type name: got nothing back from server";
                        return false;


                }, executeQuery, computations);

        }


	template <class ...Types>
	bool execute (std :: string &errMsg, Handle <QueryBase> firstParam, Handle <Types>... args) {
		if (firstParam->wasError ()) {
			std :: cout << "There was an error constructing this query.  Can't run it.\n";
			exit (1);
		}
		if (firstParam->getQueryType () != "localoutput") {
			std :: cout << "Currently, we can only execute queries that write output sets to the server...\n";
			std :: cout << "You seem to have done something else.  Who knows what will happen???\n";
		}

		runUs->push_back (firstParam);
		return execute (errMsg, args...);	
	}

	bool execute (std :: string &errMsg) {

		// this is the request
		const UseTemporaryAllocationBlock myBlock {1024};
		Handle <ExecuteQuery> executeQuery = makeObject <ExecuteQuery> ();

		// this call asks the database to execute the query, and then it inserts the result set name
		// within each of the results, as well as the database connection information

                // this is for query scheduling stuff
                if (useScheduler == true) {
                     return simpleDoubleRequest<ExecuteQuery, Vector<Handle<QueryBase>>, SimpleRequestResult, bool> (myLogger, port, address, false, 124 * 1024,
                [&] (Handle<SimpleRequestResult> result) {
                        if (result != nullptr) {
                                if (!result->getRes ().first) {
                                        errMsg = "Error in query: " + result->getRes ().second;
                                        myLogger->error ("Error querying data: " + result->getRes ().second);
                                        return false;
                                }
                                return true;
                        }
                        errMsg = "Error getting type name: got nothing back from server";
                        return false;


                }, executeQuery, runUs);


                } else {
		     return simpleDoubleRequest <ExecuteQuery, Vector <Handle <QueryBase>>, Vector <String>, bool> (myLogger, port, 
		address, false, 124 * 1024, 
                [&] (Handle <Vector <String>> result) {
                        if (result != nullptr) {

				// make sure we got the correct number of results
				if (result->size () != runUs->size ()) {
                                        errMsg = "Got a strange result size from execute";
                                        myLogger->error ("QueryErr: " + errMsg);
                                        return false;
                                }

				// make sure the results are all sets
				for (int i = 0; i < result->size (); i++) {

					if ((*runUs)[i]->getQueryType () != "localoutput")
						std :: cout << "This is bad... there was an output that was not writing to a set.\n";

					myLogger->info (std :: string ("Query execute: wrote set ") + std :: string ((*result)[i]));
				}
                                return true;
                        }
                        errMsg = "Error getting query execution results";
                        return false;}, executeQuery, runUs);
                 }
         
	}

        void setUseScheduler(bool useScheduler) {
                this->useScheduler = useScheduler;
        }

private:

	// how we connect to the catalog
	CatalogClient myHelper;

	// this is the query graph we'll execute
	Handle <Vector <Handle <QueryBase>>> runUs;

	// connection info
	int port;
	std :: string address;

	// for logging
	PDBLoggerPtr myLogger;

        bool useScheduler;

};

}

#endif

