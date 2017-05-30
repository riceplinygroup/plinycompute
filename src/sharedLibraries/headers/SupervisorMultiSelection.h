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

#ifndef SUPERVISOR_MULTI_SELECT_H
#define SUPERVISOR_MULTI_SELECT_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"
#include "Employee.h"
#include "Supervisor.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Supervisor.h"

using namespace pdb;
class SupervisorMultiSelection : public MultiSelectionComp <Employee, Supervisor> {

public:

	ENABLE_DEEP_COPY

	SupervisorMultiSelection () {}

	Lambda <bool> getSelection (Handle <Supervisor> checkMe) override {
		return makeLambdaFromMethod (checkMe, getSteve) == makeLambdaFromMember (checkMe, me);
	}

	Lambda <Vector<Handle <Employee>>> getProjection (Handle <Supervisor> checkMe) override {
                return makeLambdaFromMember (checkMe, myGuys);
	}
};


#endif
