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
#ifndef OBJECTQUERYMODEL_DISPATCHERCLIENTTEMPLATE_CC
#define OBJECTQUERYMODEL_DISPATCHERCLIENTTEMPLATE_CC

#include "DispatcherClient.h"
#include "DispatcherAddData.h"
#include "SimpleSendDataRequest.h"
#include "SimpleSendBytesRequest.h"
#include "SimpleRequestResult.h"
#include <snappy.h>
namespace pdb {

template <class DataType>
bool DispatcherClient::sendData(std::pair<std::string, std::string> setAndDatabase, Handle<Vector<Handle<DataType>>> dataToSend, std::string& errMsg) {
    return simpleSendDataRequest <DispatcherAddData, Handle<DataType>, SimpleRequestResult, bool> (logger, port, address, false, 1024,
        [&](Handle <SimpleRequestResult> result) {
            if (result != nullptr){
                if (!result->getRes ().first) {
                    errMsg = "Error dispatching data: " + result->getRes().second;
                    logger->error (errMsg);
                }
            }
            return true;}, dataToSend, setAndDatabase.second, setAndDatabase.first, getTypeName <DataType> ());
}


template <class DataType>    
bool DispatcherClient::sendBytes(std::pair<std::string, std::string> setAndDatabase, char * bytes, size_t numBytes, std::string& errMsg) {
#ifdef ENABLE_COMPRESSION
     char * compressedBytes = new char[snappy::MaxCompressedLength(numBytes)];
     size_t compressedSize;
     snappy::RawCompress((char *)bytes, numBytes, compressedBytes, &compressedSize);
     std :: cout << "size before compression is " << numBytes << " and size after compression is " << compressedSize << std :: endl;
     return simpleSendBytesRequest <DispatcherAddData, SimpleRequestResult, bool> (logger, port, address, false, 1024,
                 [&] (Handle <SimpleRequestResult> result) {
                     if (result != nullptr)
                         if (!result->getRes ().first) {
                             logger->error ("Error sending data: " + result->getRes ().second);
                             errMsg = "Error sending data: " + result->getRes ().second;
                         }
                         return true;}, compressedBytes, compressedSize, setAndDatabase.second, setAndDatabase.first, getTypeName<DataType>(), true);
#else
     return simpleSendBytesRequest <DispatcherAddData, SimpleRequestResult, bool> (logger, port, address, false, 1024,
                 [&] (Handle <SimpleRequestResult> result) {
                     if (result != nullptr)
                         if (!result->getRes ().first) {
                             logger->error ("Error sending data: " + result->getRes ().second);
                             errMsg = "Error sending data: " + result->getRes ().second;
                         }
                         return true;}, bytes, numBytes, setAndDatabase.second, setAndDatabase.first, getTypeName<DataType>());


#endif



}




}

#endif
