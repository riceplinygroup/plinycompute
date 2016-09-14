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
#ifndef EXEC_QUERY_ON_CLUSTER_H
#define EXEC_QUERY_ON_CLUSTER_H

#include "Object.h"
#include "Handle.h"
#include "QueryBase.h"

#include "PDBString.h"

// PRELOAD %ExecuteQueryOnSingleHost%

namespace pdb {

// encapsulates a request to execute a single query on a single remote node.

class ExecuteQueryOnSingleHost: public Object {

public:

	ExecuteQueryOnSingleHost() {
	}

	~ExecuteQueryOnSingleHost() {
	}

	Handle<Vector<Handle<QueryBase>>> getQueries() {
		return queries;
	}


	Handle<String> getHostname()
	{
		return hostname;
	}

	void setHostname(Handle<String> hostname) {
		this->hostname = hostname;
	}




	 Handle<int> getPort()
	{
		return port;
	}

	void setPort(const Handle<int>& port)
	{
		this->port = port;
	}

	ENABLE_DEEP_COPY


private:

	// all of the queries that have to be executed.
	Handle<Vector<Handle<QueryBase>>> queries;

	Handle<String> hostname;
	Handle<int> port;

};

}

#endif
