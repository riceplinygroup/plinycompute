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

#ifndef PLACE_OF_QUERY_PLANNER_H
#define PLACE_OF_QUERY_PLANNER_H

#include "Object.h"
#include "DataTypes.h"

//  PRELOAD %PlaceOfQueryPlanner%


/**
 * This class encapsulates the place of the query planner.
 *
 */
namespace pdb {

class PlaceOfQueryPlanner : public pdb :: Object {

public:

	PlaceOfQueryPlanner(){}
	~PlaceOfQueryPlanner(){}

	ENABLE_DEEP_COPY
private:

};

}

#endif
