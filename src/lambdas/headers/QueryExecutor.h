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

#ifndef QUERY_EXEC_H
#define QUERY_EXEC_H

#include "TupleSet.h"

namespace pdb {

// a nice little typedef to shared_ptrs to QueryExecutor objects
class QueryExecutor;
typedef std :: shared_ptr <QueryExecutor> QueryExecutorPtr;

// this is a QueryExecutor.  By definition, it has one method that takes an input a TupleSet, and
// then somehow transofrms it to create a new TupleSet (a TupleSet is a column-oriented list of
// tuples)
class QueryExecutor {

public:

	virtual TupleSetPtr process (TupleSetPtr input) = 0;

};

}

#endif
