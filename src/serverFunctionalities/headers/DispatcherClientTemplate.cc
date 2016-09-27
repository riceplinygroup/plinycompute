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
#include "SimpleRequestResult.h"

namespace pdb {

template <class DataType>
bool DispatcherClient::sendData(std::pair<std::string, std::string> setAndDatabase, Handle<Vector<Handle<DataType>>> dataToSend, std::string& errMsg) {

    std::cout << "Sending data to " << port << " : " << address << std::endl;

    bool res = simpleSendDataRequest <DispatcherAddData, Handle<DataType>, SimpleRequestResult, bool> (logger, port, address, false, 1024,
        [&](Handle <SimpleRequestResult> result) {
            if (result != nullptr){
                if (!result->getRes ().first) {
                    errMsg = "Error dispatching data: " + result->getRes().second;
                    logger->error (errMsg);
                }
            }
            return true;}, dataToSend, setAndDatabase.second, setAndDatabase.first, getTypeName <DataType> ()); // TODO: Set the type id in the future
    if (!res) {
        errMsg = "Failed to receive response from Dispatcher.sendData()";
        std::cout << "Failed to send data" << std::endl;
    } else {
        std::cout << "Successfully sent data" << std::endl;
    }
    return res;
}

}

#endif