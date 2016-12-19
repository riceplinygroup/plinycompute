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

#ifndef SIMPLE_REQUEST_CC
#define SIMPLE_REQUEST_CC

#include <functional>
#include <string>

#include "InterfaceFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "PDBCommunicator.h"

using std::function;
using std::string;

#ifndef MAX_RETRIES
   #define MAX_RETRIES 5
#endif
#define BLOCK_HEADER_SIZE 20
namespace pdb {

template <class RequestType, class ResponseType, class ReturnType, class ...RequestTypeParams>
ReturnType simpleRequest (PDBLoggerPtr myLogger, int port, std :: string address, ReturnType onErr, size_t bytesForRequest, 
	function <ReturnType (Handle <ResponseType>)> processResponse, RequestTypeParams&&... args) {

        int numRetries = 0;  

        while (numRetries <= MAX_RETRIES) {
	    PDBCommunicator temp;
	    string errMsg;
	    bool success;

	    if (temp.connectToInternetServer (myLogger, port, address, errMsg)) {
		myLogger->error (errMsg);
		myLogger->error ("simpleRequest: not able to connect to server.\n");
		//return onErr;
                std :: cout << "FATAL ERROR: can not connect to remote server with port=" << port << " and address=" << address << std :: endl;
                exit (-1);
	    }
            myLogger->info(std::string( "Successfully connected to remote server with port=") + std::to_string(port) + std::string(" and address=") + address);
            std :: cout << "Successfully connected to remote server with port=" << port << " and address=" << address << std :: endl;
	    // build the request
            std :: cout << "bytesForRequest=" << bytesForRequest << std:: endl;
            if (bytesForRequest <= BLOCK_HEADER_SIZE) {
                std :: cout << "FATAL ERROR: too small buffer size for processing simple request" << std :: endl;
                exit (-1);
            }
	    const UseTemporaryAllocationBlock tempBlock{bytesForRequest};
            std :: cout << "to make object" << std :: endl;
	    Handle <RequestType> request = makeObject <RequestType> (args...);;
            std :: cout << "to send object" << std :: endl;
	    if (!temp.sendObject (request, errMsg)) {
		myLogger->error (errMsg);
		myLogger->error ("simpleRequest: not able to send request to server.\n");
                if (numRetries < MAX_RETRIES) {
                    numRetries ++;
                    continue;
                } else {
		    return onErr;
                }
	    }
            std :: cout << "sent object..." << std :: endl;
	    // get the response and process it
	    ReturnType finalResult;
            size_t objectSize = temp.getSizeOfNextObject();
            if (objectSize == 0) {
                if (numRetries < MAX_RETRIES) {
                    numRetries ++;
                    continue;
                } else {
                    return onErr;
                }
            }
	    void *memory = malloc (objectSize);
            if (memory == nullptr) {
                errMsg = "FATAL ERROR in simpleRequest: Can't allocate memory";
                myLogger->error(errMsg);
                std :: cout << errMsg << std :: endl;
                exit (-1);
            }
	    {
		Handle <ResponseType> result = temp.getNextObject <ResponseType> (memory, success, errMsg);
		if (!success) {
			myLogger->error (errMsg);
			myLogger->error ("simpleRequest: not able to get next object over the wire.\n");
                        ///JiaNote: we need free memory here !!!

                        free(memory);
                        if (numRetries < MAX_RETRIES) {
                             numRetries ++;
                             continue;
                        } else {
			     return onErr;
                        }
		}

		finalResult = processResponse (result);
	    }
	    free (memory);
	    return finalResult;
        }
        return onErr;
}

template <class RequestType, class SecondRequestType, class ResponseType, class ReturnType>
ReturnType simpleDoubleRequest (PDBLoggerPtr myLogger, int port, std :: string address, ReturnType onErr, size_t bytesForRequest,
        function <ReturnType (Handle <ResponseType>)> processResponse, Handle <RequestType> &firstRequest,
        Handle <SecondRequestType> &secondRequest) {

	PDBCommunicator temp;
	string errMsg;
	bool success;

	if (temp.connectToInternetServer (myLogger, port, address, errMsg)) {
		myLogger->error (errMsg);
		myLogger->error ("simpleRequest: not able to connect to server.\n");
		//return onErr;
                std :: cout << "FATAL ERROR: can not connect to remote server with port=" << port << " and address=" << address << std :: endl;
                exit (-1);
	}
        std :: cout << "Successfully connected to remote server with port=" << port << " and address=" << address << std :: endl;
	// build the request
	if (!temp.sendObject (firstRequest, errMsg)) {
		myLogger->error (errMsg);
		myLogger->error ("simpleDoubleRequest: not able to send first request to server.\n");
		return onErr;
	}
       
	if (!temp.sendObject (secondRequest, errMsg)) {
		myLogger->error (errMsg);
		myLogger->error ("simpleDoubleRequest: not able to send second request to server.\n");
		return onErr;
	}

	// get the response and process it
	ReturnType finalResult;
	void *memory = malloc (temp.getSizeOfNextObject ());
	{
		Handle <ResponseType> result = temp.getNextObject <ResponseType> (memory, success, errMsg);
		if (!success) {
			myLogger->error (errMsg);
			myLogger->error ("simpleRequest: not able to get next object over the wire.\n");
			return onErr;
		}

		finalResult = processResponse (result);
	}
	free (memory);
	return finalResult;
}

}
#endif
