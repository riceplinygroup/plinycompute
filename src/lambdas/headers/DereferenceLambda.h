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

#ifndef DEREF_LAM_H
#define DEREF_LAM_H

#include <vector>
#include "LambdaHelperClasses.h"
#include "ComputeExecutor.h"
#include "SimpleComputeExecutor.h"
#include "TupleSetMachine.h"
#include "TupleSet.h"
#include "Ptr.h"

namespace pdb {

template <class OutType>
class DereferenceLambda : public TypedLambdaObject<OutType> {

public:
    LambdaTree<Ptr<OutType>> input;

public:
    unsigned int getInputIndex(int i) override {

        return input.getInputIndex(i);
    }

    DereferenceLambda(LambdaTree<Ptr<OutType>>& input) : input(input) {}

    unsigned int getNumInputs() override {
        return 1;
    }

    std::string getTypeOfLambda() override {
        return std::string("deref");
    }

    std::string toTCAPString(std::vector<std::string>& inputTupleSetNames,
                             std::vector<std::string>& inputColumnNames,
                             std::vector<std::string>& inputColumnsToApply,
                             std::vector<std::string>& childrenLambdaNames,
                             int lambdaLabel,
                             std::string computationName,
                             int computationLabel,
                             std::string& outputTupleSetName,
                             std::vector<std::string>& outputColumns,
                             std::string& outputColumnName,
                             std::string& myLambdaName,
                             MultiInputsBase* multiInputsComp = nullptr,
                             bool amIPartOfJoinPredicate = false,
                             bool amILeftChildOfEqualLambda = false,
                             bool amIRightChildOfEqualLambda = false,
                             std::string parentLambdaName = "",
                             bool isSelfJoin = false) override {
        std::string tcapString = "";
        myLambdaName = getTypeOfLambda() + "_" + std::to_string(lambdaLabel);
        std::string inputTupleSetName = inputTupleSetNames[0];
        std::string tupleSetMidTag = "";


        int myIndex;
        if (multiInputsComp == nullptr) {
            tupleSetMidTag = "OutFor";
        } else {
            tupleSetMidTag = "ExtractedFor";
            myIndex = this->getInputIndex(0);
            inputTupleSetName = multiInputsComp->getTupleSetNameForIthInput(myIndex);
            inputColumnNames = multiInputsComp->getInputColumnsForIthInput(myIndex);
            inputColumnsToApply = multiInputsComp->getInputColumnsToApplyForIthInput(myIndex);
        }


        outputTupleSetName = "deref_" + std::to_string(lambdaLabel) + tupleSetMidTag +
            computationName + std::to_string(computationLabel);

        outputColumnName = inputColumnsToApply[0];
        PDB_COUT << "OuputColumnName: " << outputColumnName << std::endl;
        outputColumns.clear();
        for (int i = 0; i < inputColumnNames.size(); i++) {
            if (inputColumnNames[i] != outputColumnName) {
                outputColumns.push_back(inputColumnNames[i]);
            }
        }
        outputColumns.push_back(outputColumnName);
        if (outputColumns.size() > 0) {
            tcapString += outputTupleSetName + "(" + outputColumns[0];
            for (int i = 1; i < outputColumns.size(); i++) {
                tcapString += ",";
                tcapString += outputColumns[i];
            }
        } else {
            tcapString += outputTupleSetName + "(";
        }
        tcapString += ") <= APPLY (";
        tcapString += inputTupleSetName + "(" + inputColumnsToApply[0];
        for (int i = 1; i < inputColumnsToApply.size(); i++) {
            tcapString += ",";
            tcapString += inputColumnsToApply[i];
        }
        std::vector<std::string> inputColumnsToKeep;
        for (int i = 0; i < inputColumnNames.size(); i++) {
            int j = 0;
            for (j = 0; j < inputColumnsToApply.size(); j++) {
                if (inputColumnNames[i] == inputColumnsToApply[j]) {
                    break;
                }
            }
            if (j == inputColumnsToApply.size()) {
                inputColumnsToKeep.push_back(inputColumnNames[i]);
            }
        }
        if (inputColumnsToKeep.size() > 0) {
            tcapString += "), " + inputTupleSetName + "(" + inputColumnsToKeep[0];
            for (int i = 1; i < inputColumnsToKeep.size(); i++) {
                tcapString += ",";
                tcapString += inputColumnsToKeep[i];
            }
        } else {
            tcapString += "), " + inputTupleSetName + "(";
        }
        tcapString += "), '" + computationName + "_" + std::to_string(computationLabel) + "', '" +
            myLambdaName + "')\n";
        if (multiInputsComp != nullptr) {
            if (amILeftChildOfEqualLambda || amIRightChildOfEqualLambda) {
                inputTupleSetName = outputTupleSetName;
                inputColumnNames.clear();
                for (int i = 0; i < outputColumns.size(); i++) {
                    // we want to remove the extracted value column from here
                    if (outputColumns[i] != outputColumnName) {
                        inputColumnNames.push_back(outputColumns[i]);
                    }
                }
                inputColumnsToApply.clear();
                inputColumnsToApply.push_back(outputColumnName);

                std::string hashOperator = "";
                if (amILeftChildOfEqualLambda == true) {
                    hashOperator = "HASHLEFT";
                } else {
                    hashOperator = "HASHRIGHT";
                }
                outputTupleSetName = outputTupleSetName + "_hashed";
                outputColumnName = outputColumnName + "_hash";
                outputColumns.clear();

                for (int i = 0; i < inputColumnNames.size(); i++) {
                    outputColumns.push_back(inputColumnNames[i]);
                }
                outputColumns.push_back(outputColumnName);
                std::string computationNameWithLabel =
                    computationName + std::to_string(computationLabel);
                tcapString += this->getTCAPString(inputTupleSetName,
                                                  inputColumnNames,
                                                  inputColumnsToApply,
                                                  outputTupleSetName,
                                                  outputColumns,
                                                  outputColumnName,
                                                  hashOperator,
                                                  computationNameWithLabel,
                                                  parentLambdaName);
                ;
            }
            if (isSelfJoin == false) {
                for (unsigned int index = 0; index < multiInputsComp->getNumInputs(); index++) {
                    std::string curInput = multiInputsComp->getNameForIthInput(index);
                    auto iter = std::find(outputColumns.begin(), outputColumns.end(), curInput);
                    if (iter != outputColumns.end()) {
                        multiInputsComp->setTupleSetNameForIthInput(index, outputTupleSetName);
                        multiInputsComp->setInputColumnsForIthInput(index, outputColumns);
                        multiInputsComp->setInputColumnsToApplyForIthInput(index, outputColumnName);
                    }
                }
            } else {
                // only update myIndex
                multiInputsComp->setTupleSetNameForIthInput(myIndex, outputTupleSetName);
                multiInputsComp->setInputColumnsForIthInput(myIndex, outputColumns);
                multiInputsComp->setInputColumnsToApplyForIthInput(myIndex, outputColumnName);
            }
        }


        return tcapString;
    }

    int getNumChildren() override {
        return 1;
    }

    GenericLambdaObjectPtr getChild(int which) override {
        if (which == 0)
            return input.getPtr();
        return nullptr;
    }

    /* bool addColumnToTupleSet (std :: string &pleaseCreateThisType, TupleSetPtr input, int outAtt)
    override {
            if (pleaseCreateThisType == getTypeName <OutType> ()) {
                    std :: vector <OutType> *outColumn = new std :: vector <OutType>;
                    input->addColumn (outAtt, outColumn, true);
                    return true;
            }
            return false;
    }
    */

    ComputeExecutorPtr getExecutor(TupleSpec& inputSchema,
                                   TupleSpec& attsToOperateOn,
                                   TupleSpec& attsToIncludeInOutput) override {

        // create the output tuple set
        TupleSetPtr output = std::make_shared<TupleSet>();

        // create the machine that is going to setup the output tuple set, using the input tuple set
        TupleSetSetupMachinePtr myMachine =
            std::make_shared<TupleSetSetupMachine>(inputSchema, attsToIncludeInOutput);

        // these are the input attributes that we will process
        std::vector<int> inputAtts = myMachine->match(attsToOperateOn);
        int firstAtt = inputAtts[0];

        // this is the output attribute
        int outAtt = attsToIncludeInOutput.getAtts().size();

        return std::make_shared<SimpleComputeExecutor>(
            output,
            [=](TupleSetPtr input) {

                // set up the output tuple set
                myMachine->setup(input, output);

                // get the columns to operate on
                std::vector<Ptr<OutType>>& inColumn = input->getColumn<Ptr<OutType>>(firstAtt);

                // create the output attribute, if needed
                if (!output->hasColumn(outAtt)) {
                    std::vector<OutType>* outColumn = new std::vector<OutType>;
                    output->addColumn(outAtt, outColumn, true);
                }

                // get the output column
                std::vector<OutType>& outColumn = output->getColumn<OutType>(outAtt);

                // loop down the columns, setting the output
                int numTuples = inColumn.size();
                outColumn.resize(numTuples);
                for (int i = 0; i < numTuples; i++) {
                    outColumn[i] = *inColumn[i];
                }
                return output;
            },

            "dereferenceLambda");
    }
};
}

#endif
