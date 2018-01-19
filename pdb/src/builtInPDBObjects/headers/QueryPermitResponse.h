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
 * QueryPermitResponse.h
 *
 *  Created on: Apr 12, 2016
 *      Author: Kia
 */

#ifndef QUERY_PERMIT_RESPONSE_H
#define QUERY_PERMIT_RESPONSE_H

#include "Object.h"
#include "DataTypes.h"
#include "PDBString.h"


//  PRELOAD %QueryPermitResponse%


/**
 * This class encapsulates the permission request for running a query on distributed PDB cluster.
 *
 */
namespace pdb {

class QueryPermitResponse : public pdb::Object {
public:
    QueryPermitResponse() {}

    ~QueryPermitResponse() {}

    pdb::String& getQueryId() {
        return queryID;
    }

    void setQueryId(pdb::String& queryId) {
        this->queryID = queryId;
    }


private:
    pdb::String queryID;
};
}
#endif
