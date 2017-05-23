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
#ifndef EMPLOYEE_IDENTITY_SELECT_H
#define EMPLOYEE_IDENTITY_SELECT_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "SelectionComp.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "SharedEmployee.h"

//By Binhang, May 2017

using namespace pdb;
class EmployeeIdentitySelection : public SelectionComp <SharedEmployee, SharedEmployee> {

public:

	ENABLE_DEEP_COPY

	EmployeeIdentitySelection () {}

	Lambda <bool> getSelection (Handle <SharedEmployee> checkMe) override {
                return makeLambda (checkMe, [] (Handle<SharedEmployee>& checkMe) {return true;});
	}

	Lambda <Handle <SharedEmployee>> getProjection (Handle <SharedEmployee> checkMe) override {
		return makeLambda (checkMe, [] (Handle<SharedEmployee>& checkMe) {
            checkMe->print();
            
            Handle <SharedEmployee> newEmployee = 
            makeObject <SharedEmployee>(*(checkMe->getName()),100,checkMe->getSalary()); // cannot get age!
            newEmployee->print();
            return newEmployee;
            
            //return checkMe;
        });
	}
};


#endif