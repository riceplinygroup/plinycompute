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

#ifndef SIMPLE_SEND_OBJECT_REQUEST_H
#define SIMPLE_SEND_OBJECT_REQUEST_H

#include "PDBLogger.h"

// This templated function makes it easy to write a simple network client that asks a request,
// as a PDB object, then sends some data (as a Handle <pdb :: Vector <DataType>>) to a server and
// then gets a result.
//
// The type args are:
// 	RequestType: the type of object to create to send over the wire
//      DataType: the type of data to send in an object
//	ResponseType: the type of object we expect to receive over the wire
//	ReturnType: the type we will return to the caller
//	RequestTypeParams: type of the params to use for the contructor to the object we send over the
//wre
//
// The params are:
//	myLogger: The logger we write error messages to
//	port: the port to send the request to
//	address: the address to send the request to
//	onErr: the value to return if there is an error sending/receiving data
//	bytesForRequest: the number of bytes to give to the allocator used to build the request
//	processResponse: the function used to process the response to the request
//	args: the arguments to give to the constructor of the request
//

namespace pdb {

template <class RequestType,
          class DataType,
          class ResponseType,
          class ReturnType,
          class... RequestTypeParams>
ReturnType simpleSendObjectRequest(PDBLoggerPtr myLogger,
                                   int port,
                                   std::string address,
                                   ReturnType onErr,
                                   size_t bytesForRequest,
                                   function<ReturnType(Handle<ResponseType>)> processResponse,
                                   DataType dataToSend,
                                   RequestTypeParams&&... args);
}

#endif

#include "SimpleSendObjectRequest.cc"
