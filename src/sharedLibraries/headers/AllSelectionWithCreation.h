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

#ifndef ALL_SELECT_WITH_CREATION_H
#define ALL_SELECT_WITH_CREATION_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "SelectionComp.h"
#include "Employee.h"
#include "Supervisor.h"
#include "PDBVector.h"
#include "PDBString.h"

using namespace pdb;
class AllSelectionWithCreation : public SelectionComp <Employee, Employee> {

public:

	ENABLE_DEEP_COPY

	AllSelectionWithCreation () {}

	Lambda <bool> getSelection (Handle <Employee> checkMe) override {
                return makeLambda (checkMe, [] (Handle<Employee> & checkMe) {return true;});
	}

	Lambda <Handle <Employee>> getProjection (Handle <Employee> checkMe) override {
               return makeLambda (checkMe, [] (Handle<Employee>& checkMe) {
                   //checkMe->print();
                   Handle <Employee> newEmployee = 
                      makeObject <Employee>(*(checkMe->getName()),100, checkMe->department, checkMe->salary); // cannot get age!
                   //newEmployee->print();
                   return newEmployee;
               });
	}
};


#endif
