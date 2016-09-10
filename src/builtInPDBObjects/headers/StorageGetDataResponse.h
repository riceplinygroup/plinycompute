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

#ifndef STORAGE_GET_DATA_RESPONSE_H
#define STORAGE_GET_DATA_RESPONSE_H

#include "Object.h"
#include "Handle.h"
#include <utility>

// PRELOAD %StorageGetDataResponse%

namespace pdb {

// encapsulates a response for request to obtain data from a storage set
class StorageGetDataResponse : public Object {

public:

	StorageGetDataResponse () {};
	~StorageGetDataResponse () {};
	StorageGetDataResponse (int numPages, std :: string databaseName, std :: string setName, bool success, std :: string errMsg) : 
		numPages (numPages), databaseName (databaseName), setName (setName), errMsg (errMsg), success (success) {}

	int getNumPages () {
		return numPages;
	}

        std :: string getDatabaseName() {
                return databaseName;
        }

        std :: string getSetName() {
                return setName;
        }

	std :: pair <bool, std :: string> wasSuccessful () {
		return std :: make_pair (success, errMsg);
	}

	ENABLE_DEEP_COPY

private:

	int numPages;
        std :: string databaseName;
        std :: string setName;
	String errMsg;
	bool success;
};

}

#endif
