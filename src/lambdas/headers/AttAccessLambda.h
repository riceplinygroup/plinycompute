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
            this->setInputIndex(0, -(input.getExactTypeInfoValue()+1));

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

        unsigned int getNumInputs() override {
            return 1;
        }


	std :: string getInputType () {
		return inputTypeName;
	}

        std :: string toTCAPString (std :: vector <std :: string> inputTupleSetNames, std :: vector<std :: string> inputColumnNames, std :: vector<std :: string> inputColumnsToApply, std :: vector <std :: string> childrenLambdaNames, int lambdaLabel, std :: string computationName, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string> & outputColumns, std :: string& outputColumnName, std :: string & myLambdaName, MultiInputsBase * multiInputsComp = nullptr, bool amIPartOfJoinPredicate = false, bool amILeftChildOfEqualLambda = false, bool amIRightChildOfEqualLambda = false, std :: string parentLambdaName = "") override {
                std :: string tcapString = "";
   
                std :: string inputTupleSetName = inputTupleSetNames[0];

                std :: string tupleSetMidTag = "";
                std :: string computationNameWithLabel = computationName + "_"  + std :: to_string(computationLabel);
                myLambdaName = getTypeOfLambda() + "_" + std :: to_string(lambdaLabel);
          
                int myIndex;
                if (multiInputsComp == nullptr) {
                    tupleSetMidTag = "OutFor";
                } else {
                    tupleSetMidTag = "ExtractedFor";
                    myIndex = this->getInputIndex(0);
                    inputTupleSetName = multiInputsComp->getTupleSetNameForIthInput(myIndex);
                    inputColumnNames = multiInputsComp->getInputColumnsForIthInput(myIndex);
                    inputColumnsToApply.clear();
                    inputColumnsToApply.push_back(multiInputsComp->getNameForIthInput(myIndex));
                    
                }


                outputTupleSetName = "attAccess_"+ std :: to_string(lambdaLabel) +tupleSetMidTag+computationName+std :: to_string(computationLabel);

                outputColumnName = "att_"+tupleSetMidTag+"_"+attName;
                outputColumns.clear();
                for (unsigned int i = 0; i < inputColumnNames.size(); i++) {
                    outputColumns.push_back(inputColumnNames[i]);
                }
                outputColumns.push_back(outputColumnName);

                tcapString += this->getTCAPString(inputTupleSetName, inputColumnNames, inputColumnsToApply, outputTupleSetName, outputColumns, outputColumnName, "APPLY", computationNameWithLabel, myLambdaName);

                if (multiInputsComp != nullptr) {
                    if (amILeftChildOfEqualLambda || amIRightChildOfEqualLambda) {
                        inputTupleSetName = outputTupleSetName;
                        inputColumnNames.clear();
                        for (unsigned int i = 0; i < outputColumns.size(); i++) {
                            //we want to remove the extracted value column from here
                            if (outputColumns[i] != outputColumnName) {
                                inputColumnNames.push_back(outputColumns[i]);
                            }
                        }
                        inputColumnsToApply.clear();
                        inputColumnsToApply.push_back(outputColumnName);

                        std :: string hashOperator = "";
                        if (amILeftChildOfEqualLambda == true) {
                            hashOperator = "HASHLEFT";
                        } else {
                            hashOperator = "HASHRIGHT";
                        }
                        outputTupleSetName =outputTupleSetName+"_hashed";
                        outputColumnName = outputColumnName+"_hash";
                        outputColumns.clear();

                        for (unsigned int i = 0; i < inputColumnNames.size(); i++) {
                            outputColumns.push_back(inputColumnNames[i]);
                        }
                        outputColumns.push_back(outputColumnName);

                        tcapString += this->getTCAPString(inputTupleSetName, inputColumnNames, inputColumnsToApply, outputTupleSetName, outputColumns, outputColumnName, hashOperator, computationNameWithLabel, parentLambdaName);
                    }
                    for (unsigned int index = 0; index < multiInputsComp->getNumInputs(); index++) {
                        std :: string curInput = multiInputsComp->getNameForIthInput(index);
                        auto iter = std :: find (outputColumns.begin(), outputColumns.end(), curInput);
                        if (iter != outputColumns.end()) {
                            multiInputsComp->setTupleSetNameForIthInput(index, outputTupleSetName);
                            multiInputsComp->setInputColumnsForIthInput(index, outputColumns);
                            multiInputsComp->setInputColumnsToApplyForIthInput(index, outputColumnName);
                        }
                    }
                }


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
