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

#ifndef QUERIES_AND_PLAN_H
#define QUERIES_AND_PLAN_H

#include "Object.h"
#include "PDBString.h"
#include "PDBVector.h"
#include "BaseQuery.h"

// PRELOAD %QueriesAndPlan%

namespace pdb {

class QueriesAndPlan : public Object {

public:

	QueriesAndPlan () {}

	void setPlan(String myPlan) {
		plan = myPlan;
	}

	String getPlan() {return plan;}

	Handle<Vector <Handle <BaseQuery>>> getQueries() { return queries; }

	void addQuery(Handle <BaseQuery> query) {
		if (queries == nullptr) {
			queries = makeObject <Vector <Handle <BaseQuery>>> (1);
		}
		queries->push_back(query);
	}
	
	ENABLE_DEEP_COPY

private:

	String plan;

	// The queries to run the plan
	Handle <Vector <Handle <BaseQuery>>> queries;
};

}

#endif
