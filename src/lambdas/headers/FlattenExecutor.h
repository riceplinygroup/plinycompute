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

#ifndef FLATTEN_QUERY_EXEC_H
#define FLATTEN_QUERY_EXEC_H

#include "ComputeExecutor.h"
#include "TupleSetMachine.h"
#include "TupleSet.h"
#include <vector>

//by Jia, May 2017

namespace pdb {

// runs an appending 1 operation
class FlattenExecutor : public ComputeExecutor {

private:

	// this is the output TupleSet that we return
	TupleSetPtr output;

	// the attribute to operate on
	int whichAtt;

        // the output attribute
        int outAtt;

	// to setup the output tuple set
	TupleSetSetupMachine myMachine;

        // the list of counts for matches of each of the input tuples
        std :: vector <uint32_t> counts;

public:

	// currently, we just ignore the extra parameter to the filter if we get it
	FlattenExecutor (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput, ComputeInfoPtr) :
                myMachine (inputSchema, attsToIncludeInOutput) {
		
		// this is the input attribute that we will process
		output = std :: make_shared <TupleSet> ();
		std :: vector <int> matches = myMachine.match (attsToOperateOn);
		whichAtt = matches[0];
                outAtt = attsToIncludeInOutput.getAtts().size();
	}

	FlattenExecutor (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput) : 
		myMachine (inputSchema, attsToIncludeInOutput) {

		// this is the input attribute that we will process
		output = std :: make_shared <TupleSet> ();
		std :: vector <int> matches = myMachine.match (attsToOperateOn);
		whichAtt = matches[0];
                outAtt = attsToIncludeInOutput.getAtts().size();
	}

	TupleSetPtr process (TupleSetPtr input) override {

                //std :: cout << "FlattenExecutor: to process a tuple set" << std :: endl;

                std :: vector<Vector<Handle<Object>>> inputVecData = input->getColumn<Vector<Handle<Object>>> (whichAtt);

                //redo the vector of counts if it's not the correct size
                if (counts.size() != inputVecData.size()) {
                    counts.resize(inputVecData.size());
                }
     
                //get counts for replication
                int numFlattenedRows = 0;
                for (int i = 0; i < inputVecData.size(); i++) {
                    Vector<Handle<Object>> & myVec = inputVecData[i];
                    int mySize = myVec.size();
                    std :: cout << "mySize is" << mySize << std :: endl;
                    counts[i] = mySize;
                    numFlattenedRows += mySize;
                }
                
                //replicates other columns
                myMachine.replicate (input, output, counts, 0);

                //add new column
                if (!output->hasColumn(outAtt)) {
                    std :: vector <Handle<Object>> * outColumn = new std :: vector <Handle<Object>> ();
                    output->addColumn(outAtt, outColumn, true);
                }

                //get the output column
                std :: vector<Handle<Object>> &outColumn = output->getColumn<Handle<Object>>(outAtt);
                //loop over to set the rows
                outColumn.resize(numFlattenedRows);
                int overallCounter = 0;
                for (int i = 0; i < inputVecData.size(); i++) {
                    Vector<Handle<Object>> & myVec = inputVecData[i];
                    for (int j = 0; j < myVec.size(); j++) {
                        outColumn[overallCounter] = myVec[j];
                        std :: cout << "flattened " << j << "-th object in the " << i << "-th vector" << std :: endl;
                        overallCounter ++;
                    }
                }

		return output;
	}


        std :: string getType () override {
                return "FLATTEN";
        }

};

}

#endif
