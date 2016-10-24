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

#ifndef FILTER_QUERY_EXEC_H
#define FILTER_QUERY_EXEC_H

#include "QueryExecutor.h"
#include "TupleSetMachine.h"
#include "TupleSet.h"
#include <vector>

namespace pdb {

// runs a filter operation
class FilterQueryExecutor : public QueryExecutor {

private:

	// this is the output TupleSet that we return
	TupleSetPtr output;

	// the attribute to operate on
	int whichAtt;

	// to setup the output tuple set
	TupleSetSetupMachine <bool> myMachine;

public:

	FilterQueryExecutor (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput) : 
		myMachine (inputSchema, attsToIncludeInOutput) {

		output = std :: make_shared <TupleSet> ();
	
		// this is the input attribute that we will process
		std :: vector <int> matches = myMachine.match (attsToOperateOn);
		whichAtt = matches[0];
	}

	TupleSetPtr process (TupleSetPtr input) override {

		// set up the output tuple set
		myMachine.setup (input, output);

		// get the input column to use as a filter
		std :: vector <bool> &inputColumn = input->getColumn <bool> (whichAtt);

		// loop over the columns and filter
		int numColumns = output->getNumColumns ();
		for (int i = 0; i < numColumns; i++) {
			output->filterColumn (i, inputColumn);
		}

		return output;
	}
};

}

#endif
