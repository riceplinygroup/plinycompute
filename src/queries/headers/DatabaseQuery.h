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

#ifndef DATABASE_QUERY_H
#define DATABASE_QUERY_H

#include <string>
#include "Handle.h"
#include "SetScan.h"

namespace pdb {

// this is the class that allows us to ask queries...
// a QueryClient is a factory for these objects
class DatabaseQuery {

public:

	// run the query!  Returns 1 on success
	bool execute (std :: string errMSg);

	// produces a scan of an input set, in the database/set combo...
	// the type of object stored in that database/set should match
	// the type DataType
	template <class DataType>
	Handle <SetScan <DataType>> scan (std :: string setName);
};

}

#endif
