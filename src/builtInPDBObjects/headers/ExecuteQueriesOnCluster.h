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
#ifndef EXEC_QUERIES_ON_CLUSTER_H
#define EXEC_QUERIES_ON_CLUSTER_H

#include "Object.h"
#include "Handle.h"
#include "QueryBase.h"

#include "PDBString.h"

// PRELOAD %ExecuteQueriesOnCluster%

namespace pdb {

// encapsulates a request to execute a set of queries on the PDB cluster nodes.

class ExecuteQueriesOnCluster: public Object {

public:

	ExecuteQueriesOnCluster() {
	}
	~ExecuteQueriesOnCluster() {

	}

	ExecuteQueriesOnCluster(Handle<Vector<Handle<QueryBase>>> allOutputs) : allOutputs (allOutputs) {}

	Handle <Vector <Handle <QueryBase>>> getOutputs () {
		return allOutputs;
	}

	ENABLE_DEEP_COPY

private:

			// all of the queries that have to be executed.
			Handle <Vector <Handle <QueryBase>>> allOutputs;

			// hostnames of the nodes
			Handle <Vector <Handle <String>>> hostNames;

			// ports of the nodes
			Handle <Vector <Handle <int>>> hostPorts;

		};

	}

#endif
