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

#ifndef TRUE_LAM_H
#define TRUE_LAM_H

#include "Handle.h"
#include <string>
#include "Ptr.h"
#include "TupleSet.h"
#include <vector>
#include "SimpleComputeExecutor.h"
#include "TupleSetMachine.h"

namespace pdb {

template <class ClassType> 
class TrueLambda : public TypedLambdaObject <bool> {

public:

	std :: string inputTypeName;

public:

	// create an att access lambda; offset is the position in the input object where we are going to find the input att
	TrueLambda (Handle <ClassType> & input) {
		inputTypeName = getTypeName <ClassType> ();		
                std :: cout << "TrueLambda: input class is " << input.getExactTypeInfoValue() << std :: endl;
	}

	std :: string getTypeOfLambda () override {
		return std :: string ("true");
	}

	std :: string typeOfAtt () {
		return inputTypeName;
	}

	std :: string getInputType () {
		return inputTypeName;
	}

	int getNumChildren () override {
		return 0;
	}

	GenericLambdaObjectPtr getChild (int which) override {
		return nullptr;
	}

        ComputeExecutorPtr getExecutor (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput) override {

	        // create the output tuple set
	        TupleSetPtr output = std :: make_shared <TupleSet> ();

       		// create the machine that is going to setup the output tuple set, using the input tuple set
	        TupleSetSetupMachinePtr myMachine = std :: make_shared <TupleSetSetupMachine> (inputSchema, attsToIncludeInOutput);

	        // this is the input attribute that we will process
	        std :: vector <int> matches = myMachine->match (attsToOperateOn);
	        int whichAtt = matches[0];

	        // this is the output attribute
	        int outAtt = attsToIncludeInOutput.getAtts ().size ();

                return std :: make_shared <SimpleComputeExecutor> (
                        output,
                        [=] (TupleSetPtr input) {

                                // set up the output tuple set
                                myMachine->setup (input, output);

                                // get the columns to operate on
                                std :: vector <Handle<ClassType>> &inputColumn = input->getColumn <Handle<ClassType>> (whichAtt);

                                // setup the output column, if it is not already set up
                                if (!output->hasColumn (outAtt)) {
                                        std :: vector <bool> *outputCol = new std :: vector <bool>;
                                        output->addColumn (outAtt, outputCol, true);
                                }

                                // get the output column
                                std :: vector <bool> &outColumn = output->getColumn <bool> (outAtt);

                                // loop down the columns, setting the output
                                int numTuples = inputColumn.size ();
                                outColumn.resize (numTuples);
                                for (int i = 0; i < numTuples; i++) {
                                        outColumn [i] = true;
                                }

                                return output;
                        }
                );

        }

};

}

#endif
