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

#ifndef TUPLE_SET_REPARTITION_H
#define TUPLE_SET_REPARTITION_H

#include "Object.h"
#include "PDBString.h"
#include "DataTypes.h"

// PRELOAD %TupleSetRepartition%

namespace pdb {

// encapsulates a request to run a query
class TupleSetRepartition : public Object {

public:

	ENABLE_DEEP_COPY

	TupleSetRepartition () {}
        TupleSetRepartition (std :: string dbName, std :: string setNamePrefix) {
            this->databaseName = dbName;
            this->setNamePrefix = setNamePrefix;
}
	~TupleSetRepartition () {}

private:

        String databaseName;
        String setNamePrefix;

};

}

#endif
