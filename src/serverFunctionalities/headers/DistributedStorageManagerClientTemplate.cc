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

#ifndef DISTRIBUTED_STORAGE_MANAGER_CLIENT_TEMPLATE_CC
#define DISTRIBUTED_STORAGE_MANAGER_CLIENT_TEMPLATE_CC

#include "DistributedStorageManagerClient.h"
#include "StorageAddSet.h"
#include "SimpleRequest.h"
#include "DistributedStorageAddSet.h"
#include "SimpleRequestResult.h"
#include "DataTypes.h"
#include <cstddef>
#include <fcntl.h>
#include <fstream>
#include <iostream>

namespace pdb {


template <class DataType>
bool DistributedStorageManagerClient :: createSet (std :: string databaseName, std :: string setName, std :: string &errMsg) {
        std :: string typeName = getTypeName <DataType>();
        int16_t typeId = getTypeID <DataType>();
        std :: cout << "typeName for set to create ="<<typeName << ", typeId="<< typeId << std :: endl;
        return simpleRequest <DistributedStorageAddSet, SimpleRequestResult, bool> (logger, port, address, false, 1024,
    generateResponseHandler("Could not add set to distributed storage manager:", errMsg), databaseName, setName, typeName);
}



}
#endif
