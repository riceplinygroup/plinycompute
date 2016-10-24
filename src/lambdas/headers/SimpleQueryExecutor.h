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

#ifndef SIMPLE_QUERY_EXEC_H
#define SIMPLE_QUERY_EXEC_H

#include "TupleSet.h"
#include "QueryExecutor.h"
#include <memory>

namespace pdb {

class SimpleQueryExecutor;
typedef std :: shared_ptr <SimpleQueryExecutor> SimpleQueryExecutorPtr;

// this is a simple generic implementation of a QueryExecutor
class SimpleQueryExecutor : public QueryExecutor {

private:

	// this is the output TupleSet that we return
	TupleSetPtr output;

	// this is a lambda that we'll call to process input
	std :: function <TupleSetPtr (TupleSetPtr)> processInput;

public:

	SimpleQueryExecutor (TupleSetPtr outputIn, std :: function <TupleSetPtr (TupleSetPtr)> processInputIn) {
		output = outputIn;
		processInput = processInputIn;
	}

	TupleSetPtr process (TupleSetPtr input) override {
		return processInput (input);
	}
};

}

#endif
