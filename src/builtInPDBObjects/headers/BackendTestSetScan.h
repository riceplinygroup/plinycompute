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

#ifndef BACKEND_TEST_SET_SCAN_H
#define BACKEND_TEST_SET_SCAN_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"
#include "DataTypes.h"

// PRELOAD %BackendTestSetScan%

namespace pdb {

// encapsulates a request to scan a set stored in the database 
class BackendTestSetScan : public Object {

public:

	BackendTestSetScan (DatabaseID dbIdIn, UserTypeID typeIdIn, SetID setIdIn) {
		dbId = dbIdIn;
		typeId = typeIdIn;
                setId = setIdIn;	
	}

	BackendTestSetScan () {}
	~BackendTestSetScan () {}

	DatabaseID getDatabaseID () {
		return dbId;
	}

        UserTypeID getUserTypeID () {
                return typeId;
        }

	SetID getSetID () {
		return setId;
	}
	
	ENABLE_DEEP_COPY

private:

	// this is the database that we are computing over
	DatabaseID dbId;

        // this is the type that we are computing over
        UserTypeID typeId;
	
	// and the set
	SetID setId;
};

}

#endif
