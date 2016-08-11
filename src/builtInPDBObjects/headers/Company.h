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

#ifndef COMPANY_H
#define COMPANY_H

#include "Object.h"
#include "PDBVector.h"
#include "Supervisor.h"
#include "Handle.h"

//  PRELOAD %Company%

namespace pdb {

class Company : public Object {

private:

        Vector <int> labels;
        Vector <Handle <Supervisor>> myGuys;

public:

	ENABLE_DEEP_COPY

        ~Company () {}
        Company () {}

	void addSupervisor (Handle <Supervisor> supToAdd, int labelToAdd) {
		labels.push_back (labelToAdd);
		myGuys.push_back (supToAdd);
	}

	Handle <Supervisor> &getSupervisor (int which) {
		return myGuys[which];
	}

        void print () {
		for (int i = 0; i < labels.size (); i++) {
			std :: cout << labels[i] << " ";
			myGuys[i]->print ();
		}
        }

};

}

#endif
