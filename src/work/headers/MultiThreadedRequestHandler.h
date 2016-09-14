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
/**
 * Author: Jia
 * Sept 12, 2016
 */


#ifndef MULTI_THREADED_REQUEST_HANDLER_H
#define MULTI_THREADED_REQUEST_HANDLER_H

#include "PDBCommunicator.h"
#include "PDBBuzzer.h"
#include "PDBCommWork.h"
#include "UseTemporaryAllocationBlock.h"
#include <memory>

// This template is used to make a simple piece of work that accepts an object of type RequestType from the client,
// processes the request using multiple threads, waits for all threads to return, and then sends the response back via a communicator.  The constructor for the class
// takes as an argument the lambda that is to be used to process the RequestType object

namespace pdb {

template <class RequestType> 
class MultiThreadedRequestHandler : public PDBCommWork {

public:

	// this accepts the lambda that is used to process the RequestType object
	MultiThreadedRequestHandler (std :: function <std :: pair <bool, std :: string> (Handle <RequestType>, PDBCommunicatorPtr, MultiThreadedRequestHandler<RequestType> &)> useMe) {
		processRequest = useMe;
	}

	PDBCommWorkPtr clone () {
		return std :: make_shared <MultiThreadedRequestHandler <RequestType>> (processRequest);
	}



        virtual PDBBuzzerPtr getLinkedBuzzer() override {
                //std :: cout << "*****************************" << std :: endl;
                //std :: cout << "to create buzzer with intFunc!" << std :: endl;
                return make_shared<PDBBuzzer>(
                           [&] (PDBAlarm myAlarm, int & counter) {
                                    counter ++;
                                    std :: cout << "counter = " << counter << std :: endl;
                           });
        }

	void execute (PDBBuzzerPtr callerBuzzer) {

		// first, get the request
		PDBCommunicatorPtr myCommunicator = getCommunicator ();
		bool success;
		std :: string errMsg;
                size_t nextObjectSize = myCommunicator->getSizeOfNextObject ();
                //std :: cout << "nextObjectSize = " << nextObjectSize << std :: endl;
		void *memory = malloc (nextObjectSize);
		{
			UseTemporaryAllocationBlock tempBlock {memory, nextObjectSize};
			Handle <RequestType> request = myCommunicator->getNextObject <RequestType> (success, errMsg);
			
			PDBLoggerPtr myLogger = getLogger ();
			if (!success) {
				myLogger->error ("MultiThreadedRequestHandler: tried to get the next object and failed; " + errMsg);
				callerBuzzer->buzz (PDBAlarm :: GenericError);
			}
	
			std :: pair <bool, std :: string> res = processRequest (request, myCommunicator, *this);
			if (!res.first) {
				myLogger->error ("MultiThreadedRequestHandler: tried to process the request and failed; " + errMsg);
				callerBuzzer->buzz (PDBAlarm :: GenericError);
				return;
			}
	
			myLogger->info ("MultiThreadedRequestHandler: finished processing requet.");
			callerBuzzer->buzz (PDBAlarm :: WorkAllDone);
		}
		free (memory);
		return;
	}

private:

	function <pair <bool, std :: string> (Handle <RequestType>, PDBCommunicatorPtr, MultiThreadedRequestHandler<RequestType> &)> processRequest;
};

}

#endif

