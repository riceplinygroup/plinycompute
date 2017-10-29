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

#ifndef HASHONE_QUERY_EXEC_H
#define HASHONE_QUERY_EXEC_H

#include "ComputeExecutor.h"
#include "TupleSetMachine.h"
#include "TupleSet.h"
#include <vector>


namespace pdb {

// runs an appending 1 operation
class HashOneExecutor : public ComputeExecutor {

private:
    // this is the output TupleSet that we return
    TupleSetPtr output;

    // the attribute to operate on
    int whichAtt;

    // the output attribute
    int outAtt;

    // to setup the output tuple set
    TupleSetSetupMachine myMachine;

public:
    // currently, we just ignore the extra parameter to the filter if we get it
    HashOneExecutor(TupleSpec& inputSchema,
                    TupleSpec& attsToOperateOn,
                    TupleSpec& attsToIncludeInOutput,
                    ComputeInfoPtr)
        : myMachine(inputSchema, attsToIncludeInOutput) {

        // this is the input attribute that we will process
        output = std::make_shared<TupleSet>();
        std::vector<int> matches = myMachine.match(attsToOperateOn);
        whichAtt = matches[0];
        outAtt = attsToIncludeInOutput.getAtts().size();
    }

    HashOneExecutor(TupleSpec& inputSchema,
                    TupleSpec& attsToOperateOn,
                    TupleSpec& attsToIncludeInOutput)
        : myMachine(inputSchema, attsToIncludeInOutput) {

        // this is the input attribute that we will process
        output = std::make_shared<TupleSet>();
        std::vector<int> matches = myMachine.match(attsToOperateOn);
        whichAtt = matches[0];
        outAtt = attsToIncludeInOutput.getAtts().size();
    }

    TupleSetPtr process(TupleSetPtr input) override {


        // set up the output tuple set
        myMachine.setup(input, output);

        // create the output attribute, if needed
        if (!output->hasColumn(outAtt)) {
            std::vector<size_t>* outColumn = new std::vector<size_t>();
            output->addColumn(outAtt, outColumn, true);
        }


        // get the output column
        std::vector<size_t>& outColumn = output->getColumn<size_t>(outAtt);
        // loop over the columns and set the output to be 1
        int numRows = output->getNumRows(whichAtt);
        outColumn.resize(numRows);
        for (int i = 0; i < numRows; i++) {
            outColumn[i] = 1;
        }


        return output;
    }


    std::string getType() override {
        return "HASHONE";
    }
};
}

#endif
