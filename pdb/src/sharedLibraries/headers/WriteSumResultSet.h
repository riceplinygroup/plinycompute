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
#ifndef WRITE_SUM_RESULT_SET_H
#define WRITE_SUM_RESULT_SET_H

#include "WriteUserSet.h"
#include "SumResult.h"

using namespace pdb;
class WriteSumResultSet : public WriteUserSet<SumResult> {

public:
    ENABLE_DEEP_COPY

    WriteSumResultSet() {}

    WriteSumResultSet(std::string dbName, std::string setName) {
        this->setOutput(dbName, setName);
    }
};


#endif
