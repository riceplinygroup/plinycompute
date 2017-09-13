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

#ifndef STORAGE_ADD_DATA_H
#define STORAGE_ADD_DATA_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

// PRELOAD %StorageAddData%

namespace pdb {

// encapsulates a request to add data to a set in storage
class StorageAddData  : public Object {

public:

	StorageAddData () {}
	~StorageAddData () {}

	StorageAddData (std :: string dataBase, std :: string setName, std :: string typeName, bool typeCheck = true, bool flushOrNot = true, bool compressedOrNot = false) : dataBase (dataBase), setName (setName),
		typeName (typeName) {
                this->typeCheck = typeCheck;
                this->typeID = -1;
                this->flushOrNot = flushOrNot;
                this->compressedOrNot = compressedOrNot;
        }


        StorageAddData (std :: string dataBase, std :: string setName, std :: string typeName, int typeID, bool typeCheck = true, bool flushOrNot = true, bool compressedOrNot = false) : dataBase (dataBase), setName (setName),
                typeName (typeName) {
                this->typeCheck = typeCheck;
                this->typeID = typeID;
                this->flushOrNot = flushOrNot;
                this->compressedOrNot = compressedOrNot;
        }

	std :: string getDatabase () {
		return dataBase;
	}

	std :: string getSetName () {
		return setName;
	}

	std :: string getType () {
		return typeName;
	}

        int getTypeID() {
                return typeID;
        }

        bool isTypeCheck() {
                return typeCheck;
        }

        bool isFlushing() {
                return flushOrNot;
        }

        bool isCompressed() {
                return compressedOrNot;
        }

       

	ENABLE_DEEP_COPY

private:

	String dataBase;
	String setName;
	String typeName;
        int typeID;
        bool typeCheck;
        bool flushOrNot;
        bool compressedOrNot;
};

}

#endif
