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

#ifndef QUERY_H
#define QUERY_H

#include "Handle.h"
#include "Object.h"
#include "QueryBase.h"
#include "PDBVector.h"
#include "TypeName.h"

namespace pdb {

// this is the basic query type... all queries returning OutType derive from this class
template <typename OutType>
class Query : public QueryBase {

public:
    Query() {
        myOutType = getTypeName<OutType>();
    }

    // gets the name of this output type
    std::string getOutputType() override {
        return myOutType;
    }

    // from QueryBase
    // virtual int getNumInputs () = 0;
    // virtual std :: string getIthInputType (int i) = 0;
    // virtual std :: string getQueryType () = 0;

    String myOutType;
};
}

#endif
