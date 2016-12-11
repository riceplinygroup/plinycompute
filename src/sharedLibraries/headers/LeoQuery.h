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
#ifndef LEO_QUERY_H
#define LEO_QUERY_H

#include "BaseQuery.h"
#include "Supervisor.h"
#include "Employee.h"
#include "LambdaCreationFunctions.h"

using namespace pdb;

class LeoQuery : public BaseQuery {

public:

	ENABLE_DEEP_COPY

	LeoQuery() {}

	Lambda <bool> getSelection (Handle <Supervisor> &checkMe) {
		return makeLambdaFromMethod (checkMe, getSteve) == makeLambdaFromMember (checkMe, me);
	}

	Lambda <Handle <Employee>> getProjection (Handle <Supervisor> &checkMe) {
		return makeLambdaFromMethod (checkMe, getMe);
	}

	virtual void toMap(std :: map <std :: string, GenericLambdaObjectPtr> &fillMe, int &identifier) override {
		Handle <Supervisor> temp = nullptr;
    	getSelection(temp).toMap (fillMe, identifier);
    	getProjection(temp).toMap (fillMe, identifier);
    }
};

#endif