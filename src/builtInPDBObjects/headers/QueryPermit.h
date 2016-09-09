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
/*
 * QueryPermit.h
 *
 *  Created on: Mar 7, 2016
 *      Author: Kia
 */

#ifndef QUERY_PERMIT_H
#define QUERY_PERMIT_H

#include "Object.h"
#include "DataTypes.h"
#include "PDBString.h"


//  PRELOAD %QueryPermit%


/**
 * This class encapsulates the response to running a query request.
 *
 */

namespace pdb {

class QueryPermit : public pdb :: Object {

public:

	QueryPermit(){}
	~QueryPermit(){}

	ENABLE_DEEP_COPY

private:

};

}
#endif
