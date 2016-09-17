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

#ifndef LOC_QUERY_OUTPUT_H
#define LOC_QUERY_OUTPUT_H

#include "Query.h"
#include "OutputIterator.h"
#include "TypeName.h"
#include "SetScan.h"

// PRELOAD %LocalQueryOutput <Nothing>%

namespace pdb {

template <class OutType>
class LocalQueryOutput : public Query <OutType> {

public:

	LocalQueryOutput () {
		myOutType = getTypeName <OutType> ();
	}

	~LocalQueryOutput () {}

	ENABLE_DEEP_COPY

	// this basically sets up a connection to the server, and returns it
	OutputIterator <OutType> begin () {
		
		// establish a connection 
        	std :: string errMsg;
		PDBCommunicatorPtr temp = std :: make_shared <PDBCommunicator> ();
		if (temp->connectToInternetServer (*myLogger, port, serverName, errMsg)) {	
			(*myLogger)->error (errMsg);
			(*myLogger)->error ("output iterator: not able to connect to server.\n");
			return OutputIterator <OutType> ();
		}

		// build the request
		const UseTemporaryAllocationBlock tempBlock{1024};
		Handle <SetScan> request = makeObject <SetScan> (this->getDBName (), this->getSetName ());
		if (!temp->sendObject (request, errMsg)) {
			(*myLogger)->error (errMsg);
			(*myLogger)->error ("output iterator: not able to send request to server.\n");
			return OutputIterator <OutType> ();
		}

		return OutputIterator <OutType> (temp);
	}	

	OutputIterator <OutType> end () {
		return OutputIterator <OutType> ();
	}	

	virtual int getNumInputs () override {
		return 1;
	}

	virtual std :: string getIthInputType (int i) override {
		if (i != 0) {
			return "Bad index";
		}
		return myOutType;
	}

	virtual std :: string getQueryType () override {
		return "localoutput";
	}

	void setServer (int portIn, std :: string serverIn, PDBLoggerPtr &myLoggerIn) {
		port = portIn;
		serverName = serverIn;	
		myLogger = &myLoggerIn;
	}

	void execute (QueryAlgo&) override {}

private:

	// these are used so that the output knows how to connect to the server for iteration
	int port;
	String serverName;
	PDBLoggerPtr *myLogger;

	// records the output type
	String myOutType;
};

}

#endif
