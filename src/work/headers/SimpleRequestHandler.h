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

#ifndef SIMPLE_REQUEST_HANDLER_H
#define SIMPLE_REQUEST_HANDLER_H

#include "PDBCommunicator.h"
#include "PDBCommWork.h"
#include "UseTemporaryAllocationBlock.h"
#include "PDBBuzzer.h"
#include <memory>

// This template is used to make a simple piece of work that accepts an object of type RequestType from the client,
// processes the request, then sends the response back via a communicator.  The constructor for the class
// takes as an argument the lambda that is to be used to process the RequestType object
namespace pdb {

template <class RequestType> 
class SimpleRequestHandler : public PDBCommWork {

public:

	// this accepts the lambda that is used to process the RequestType object
	SimpleRequestHandler (std :: function <std :: pair <bool, std :: string> (Handle <RequestType>, PDBCommunicatorPtr)> useMe) {
		processRequest = useMe;
	}

	PDBCommWorkPtr clone () {
		return std :: make_shared <SimpleRequestHandler <RequestType>> (processRequest);
	}

	void execute (PDBBuzzerPtr callerBuzzer) {

		// first, get the request
		PDBCommunicatorPtr myCommunicator = getCommunicator ();
		PDBLoggerPtr myLogger = getLogger ();
		bool success;
		std :: string errMsg;
                size_t objectSize = myCommunicator->getSizeOfNextObject();
                myLogger->debug (std::string("SimpleRequestHandle: to receive object with size=") + std::to_string(objectSize));
                if (objectSize == 0) {
                      std::cout << "SimpleRequestHandler: object size=0" << std::endl;
                      myLogger->error("SimpleRequestHandler: object size=0");
                      callerBuzzer->buzz(PDBAlarm::GenericError);
                      return;
                }
		void *memory = malloc (myCommunicator->getSizeOfNextObject ());
		{
			Handle <RequestType> request = myCommunicator->getNextObject <RequestType> (memory, success, errMsg);
			
			if (!success) {
				myLogger->error ("SimpleRequestHandler: tried to get the next object and failed; " + errMsg);
                                ///JiaNote: we need free memory and return here
                                free(memory);
				callerBuzzer->buzz (PDBAlarm :: GenericError);
                                return;
			}
	
			std :: pair <bool, std :: string> res = processRequest (request, myCommunicator);
			if (!res.first) {
				myLogger->error ("SimpleRequestHandler: tried to process the request and failed; " + errMsg);
                                ///JiaNote: we need free memory here
                                free(memory);
				callerBuzzer->buzz (PDBAlarm :: GenericError);
				return;
			}
	
			myLogger->info ("SimpleRequestHandler: finished processing requet.");
		        free (memory);
			callerBuzzer->buzz (PDBAlarm :: WorkAllDone);
		}
		return;
	}

private:

	function <pair <bool, std :: string> (Handle <RequestType>, PDBCommunicatorPtr)> processRequest;
};

}

#endif

