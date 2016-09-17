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

#ifndef STRING_SELECT_CC
#define STRING_SELECT_CC

#include "Selection.h"
#include "PDBVector.h"
#include "GetVTable.h"
#include "PDBString.h"

using namespace pdb;

class StringSelection : public Selection <String, String> {

public:

	ENABLE_DEEP_COPY

	Lambda <bool> getSelection (Handle <String> &checkMe) override {
		return makeLambda (checkMe, [&] () {
			return (*(checkMe) == "Joe Johnson488") ||  (*(checkMe) == "Joe Johnson489");
		});
	}

	Lambda <Handle <String>> getProjection (Handle <String> &checkMe) override {
		return makeLambda (checkMe, [&] () {
			return checkMe;
		});
	}
};

GET_V_TABLE (StringSelection)

#endif
