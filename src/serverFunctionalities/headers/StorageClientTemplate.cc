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

#ifndef STORAGE_CLIENT_TEMPLATE_CC
#define STORAGE_CLIENT_TEMPLATE_CC

#include "StorageClient.h"
#include "StorageAddData.h"
#include "SimpleRequestResult.h"
#include "SimpleSendDataRequest.h"

namespace pdb {

template <class DataType>
bool StorageClient :: storeData (Handle <Vector <Handle <DataType>>> data, std :: string databaseName, std :: string setName, std :: string &errMsg) {
	
	return simpleSendDataRequest <StorageAddData, Handle <DataType>, SimpleRequestResult, bool> (myLogger, port, address, false, 1024,
		[&] (Handle <SimpleRequestResult> result) {
			if (result != nullptr) 
				if (!result->getRes ().first) {
					myLogger->error ("Error sending data: " + result->getRes ().second);
					errMsg = "Error sending data: " + result->getRes ().second;
				} 
			return true;},
		data, databaseName, setName, getTypeName <DataType> ());
}

template <class DataType>
bool StorageClient :: createSet (std :: string databaseName, std :: string setName, std :: string &errMsg) {
        return myHelper.createSet <DataType> (databaseName, setName, errMsg);
}

}
#endif
