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

#ifndef CAT_ADD_NODE_TO_DB_H
#define CAT_ADD_NODE_TO_DB_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

// PRELOAD %CatAddNodeToDatabaseRequest%

namespace pdb {

// encapsulates a request to add a node to an existing database
class CatAddNodeToDatabaseRequest : public Object {

public:

	~CatAddNodeToDatabaseRequest () {}
	CatAddNodeToDatabaseRequest () {}
	CatAddNodeToDatabaseRequest (std :: string dbName, std :: string nodeIP) :
	    dbName (dbName), nodeIP (nodeIP) {}

	std :: string nodeToAdd () {
		return nodeIP;
	}

    std :: string whichDB () {
        return dbName;
    }


	ENABLE_DEEP_COPY

private:

	String dbName;
    String nodeIP;

};

}

#endif
