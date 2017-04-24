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

#ifndef DEPARTMENTAL_TOTAL_H
#define DEPARTMENTAL_TOTAL_H

#include "Object.h"
#include "PDBString.h"

//  PRELOAD %DepartmentTotal%

namespace pdb {

class DepartmentTotal : public Object {

public:

	double totSales;
	String departmentName;
	
	ENABLE_DEEP_COPY

	bool checkSales () {
		if ((((int) (totSales * 10)) + 5) / 10 == ((int) (totSales * 10)) / 10) {
			return true;
		}
		return false;
	}

	Handle <double> getTotSales () {
		Handle <double> returnVal = makeObject <double> (totSales);
		return returnVal;
	}

	String &getKey () {
		return departmentName;
	}
	
	double &getValue () {
		return totSales;
	}

        void print() {
               std :: cout << departmentName << ":" << totSales << std :: endl;
        }
};

}

#endif
