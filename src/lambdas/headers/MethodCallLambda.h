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

#ifndef METHOD_CALL_LAM_H
#define METHOD_CALL_LAM_H

#include <vector>
#include "Lambda.h"
#include "ComputeExecutor.h"

namespace pdb {

template <class Out, class ClassType> 
class MethodCallLambda : public TypedLambdaObject <Out> {

public:

	std::function <ComputeExecutorPtr (TupleSpec &, TupleSpec &, TupleSpec &)> getExecutorFunc;
	std::function <bool (std :: string &, TupleSetPtr, int)> columnBuilder;
	std :: string inputTypeName;
	std :: string methodName;
	std :: string returnTypeName;

public:

	// create an att access lambda; offset is the position in the input object where we are going to find the input att
	MethodCallLambda (std :: string inputTypeName, std :: string methodName, std :: string returnTypeName, Handle <ClassType> & input, 
		std::function <bool (std :: string &, TupleSetPtr, int)> columnBuilder,
		std::function <ComputeExecutorPtr (TupleSpec &, TupleSpec &, TupleSpec &)> getExecutorFunc) :
		getExecutorFunc (getExecutorFunc), columnBuilder (columnBuilder), inputTypeName (inputTypeName), 
		methodName (methodName), returnTypeName (returnTypeName) {

             PDB_COUT << "MethodCallLambda: input type code is " << input.getExactTypeInfoValue() << std :: endl;
             this->setInputIndex(0, -(input.getExactTypeInfoValue()+1));

	}

	/* bool addColumnToTupleSet (std :: string &typeToMatch, TupleSetPtr addToMe, int posToAddTo) override {
		return columnBuilder (typeToMatch, addToMe, posToAddTo);
	} */

	std :: string getTypeOfLambda () override {
		return std :: string ("methodCall");
	}

	std :: string whichMethodWeCall () {
		return methodName;
	}

        unsigned int getNumInputs() override {
            return 1;
        }

	std :: string getInputType () {
		return inputTypeName;
	}

	std :: string getOutputType () override {
		return returnTypeName;
	}

        std :: string toTCAPString (std :: vector <std :: string> & inputTupleSetNames, std :: vector<std :: string> & inputColumnNames, std :: vector<std :: string> & inputColumnsToApply, std :: vector <std :: string> & childrenLambdaNames, int lambdaLabel, std :: string computationName, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string> & outputColumns, std :: string& outputColumnName, std :: string &myLambdaName, MultiInputsBase * multiInputsComp = nullptr, bool amIPartOfJoinPredicate = false, bool amILeftChildOfEqualLambda = false, bool amIRightChildOfEqualLambda = false, std :: string parentLambdaName = "") override {
                std :: string tcapString = "";
                std :: string computationNameWithLabel = computationName + "_"  + std :: to_string(computationLabel);
                myLambdaName = getTypeOfLambda() + "_" + std :: to_string(lambdaLabel);
                std :: string inputTupleSetName = inputTupleSetNames[0];
                std :: string tupleSetMidTag = "";
                int myIndex;
                std :: string originalInputColumnToApply;

                if (multiInputsComp == nullptr) {
                    tupleSetMidTag = "OutFor_";
                } else {
                    tupleSetMidTag = "ExtractedFor_)";
                    myIndex = this->getInputIndex(0);
                    inputTupleSetName = multiInputsComp->getTupleSetNameForIthInput(myIndex);
                    inputColumnNames = multiInputsComp->getInputColumnsForIthInput(myIndex);
                    inputColumnsToApply.clear();
                    inputColumnsToApply.push_back(multiInputsComp->getNameForIthInput(myIndex));
                    originalInputColumnToApply = multiInputsComp->getNameForIthInput(myIndex);
                }


                outputTupleSetName = "methodCall_"+std :: to_string(lambdaLabel)+tupleSetMidTag+computationName+std :: to_string(computationLabel);

                outputColumnName = "methodCall_"+tupleSetMidTag+methodName;
                outputColumns.clear();
                for (int i = 0; i < inputColumnNames.size(); i++) {
                    outputColumns.push_back(inputColumnNames[i]);
                }
                outputColumns.push_back(outputColumnName);



                tcapString += this->getTCAPString(inputTupleSetName, inputColumnNames, inputColumnsToApply, outputTupleSetName, outputColumns, outputColumnName, "APPLY", computationNameWithLabel, myLambdaName);
                if (multiInputsComp != nullptr) {
                    if (amILeftChildOfEqualLambda || amIRightChildOfEqualLambda) {
                        inputTupleSetName = outputTupleSetName;
                        inputColumnNames.clear();
                        for (int i = 0; i < outputColumns.size(); i++) {
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

                        for (int i = 0; i < inputColumnNames.size(); i++) {
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
                        if (originalInputColumnToApply == curInput) {
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
		return getExecutorFunc (inputSchema, attsToOperateOn, attsToIncludeInOutput); 
	}

};

}

#endif
