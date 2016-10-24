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

#ifndef ATT_ACCESS_LAM_H
#define ATT_ACCESS_LAM_H

#include "Handle.h"
#include <string>
#include "Ptr.h"
#include "TupleSet.h"
#include <vector>
#include "SimpleQueryExecutor.h"
#include "TupleSetMachine.h"

namespace pdb {

template <class Out, class ClassType> 
class AttAccessLambda : public TypedLambdaObject <Ptr <Out>> {

public:

	size_t offsetOfAttToProcess;
	std :: string inputTypeName;
	std :: string attName;
	std :: string attTypeName;

public:

	// create an att access lambda; offset is the position in the input object where we are going to find the input att
	AttAccessLambda (std :: string inputTypeNameIn, std :: string attNameIn, 
		std :: string attType, Handle <ClassType> &input, size_t offset) :
		offsetOfAttToProcess (offset), inputTypeName (inputTypeNameIn), attName (attNameIn), attTypeName (attType) {
		this->getBoundInputs ().push_back ((Handle <Object> *) &input);
	}

	std :: string getTypeOfLambda () override {
		return std :: string ("attAccess");
	}

	std :: string typeOfAtt () {
		return attTypeName;
	}

	std :: string whichAttWeProcess () {
		return attName;
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

        QueryExecutorPtr getHasher (TupleSpec &, TupleSpec &, TupleSpec &) override {
                return nullptr;
        }

	QueryExecutorPtr getExecutor (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput) override {
	
		// create the output tuple set
		TupleSetPtr output = std :: make_shared <TupleSet> ();

		// create the machine that is going to setup the output tuple set, using the input tuple set
		TupleSetSetupMachinePtr <bool> myMachine = std :: make_shared <TupleSetSetupMachine<bool>> (inputSchema, attsToIncludeInOutput);

		// this is the input attribute that we will process    
		std :: vector <int> matches = myMachine->match (attsToOperateOn);
		int whichAtt = matches[0];

		// this is the output attribute
		int outAtt = attsToIncludeInOutput.getAtts ().size ();

		return std :: make_shared <SimpleQueryExecutor> (
			output, 
			[=] (TupleSetPtr input) {

				// set up the output tuple set
				myMachine->setup (input, output);	

				// get the columns to operate on
				std :: vector <Handle<ClassType>> &inputColumn = input->getColumn <Handle<ClassType>> (whichAtt);

				// setup the output column, if it is not already set up
				if (!output->hasColumn (outAtt)) {
					std :: vector <Ptr <Out>> *outputCol = new std :: vector <Ptr <Out>>;
					output->addColumn (outAtt, outputCol, true); 
				}

				// get the output column
				std :: vector <Ptr <Out>> &outColumn = output->getColumn <Ptr <Out>> (outAtt);

				// loop down the columns, setting the output
				int numTuples = inputColumn.size ();
				outColumn.resize (numTuples);
				for (int i = 0; i < numTuples; i++) {
					outColumn [i] = (Out *) ((char *) &(*(inputColumn[i])) + offsetOfAttToProcess);
				}
				
				return output;
			}
		);
		
	}
};

}

#endif
