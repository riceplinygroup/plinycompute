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
#include "SimpleComputeExecutor.h"
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
		std :: string attType, Handle <ClassType> & input, size_t offset) :
		offsetOfAttToProcess (offset), inputTypeName (inputTypeNameIn), attName (attNameIn), attTypeName (attType) {

            std :: cout << "AttAccessLambda: input type code = " << input.getExactTypeInfoValue() << std :: endl;


	}

/*        bool addColumnToTupleSet (std :: string &pleaseCreateThisType, TupleSetPtr input, int outAtt) override {  
                if (pleaseCreateThisType == getTypeName <Ptr <Out>> ()) {  
                        std :: vector <Ptr <Out>> *outColumn = new std :: vector <Ptr <Out>>; 
                        input->addColumn (outAtt, outColumn, true); 
                        return true;   
                } 
		return false;
	}
*/
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

        std :: string toTCAPString (std :: string inputTupleSetName, std :: vector<std :: string> inputColumnNames, std :: vector<std :: string> inputColumnsToApply, int lambdaLabel, std :: string computationName, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string> & outputColumns, std :: string& outputColumnName) override {
                std :: string tcapString = "";
                outputTupleSetName = "attAccess_"+ std :: to_string(lambdaLabel) +"OutFor"+computationName+std :: to_string(computationLabel);

                outputColumnName = "att_"+attName;
                outputColumns.clear();
                for (int i = 0; i < inputColumnNames.size(); i++) {
                    outputColumns.push_back(inputColumnNames[i]);
                }
                outputColumns.push_back(outputColumnName);
                tcapString += outputTupleSetName + "(" + outputColumns[0];
                for (int i = 1; i < outputColumns.size(); i++) {
                    tcapString += ",";
                    tcapString += outputColumns[i];
                }
                tcapString += ") <= APPLY (";
                tcapString += inputTupleSetName + "(" + inputColumnsToApply[0];
                for (int i = 1; i < inputColumnsToApply.size(); i++) {
                    tcapString += ",";
                    tcapString += inputColumnsToApply[i];
                }
                tcapString += "), " + inputTupleSetName + "(" + inputColumnNames[0];
                for (int i = 1; i < inputColumnNames.size(); i++) {
                    tcapString += ",";
                    tcapString += inputColumnNames[i];
                }
                tcapString += "), '" + computationName + "_" + std :: to_string(computationLabel) + "', '"+ getTypeOfLambda() + "_" + std :: to_string(lambdaLabel) +"')\n";

                return tcapString;
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
