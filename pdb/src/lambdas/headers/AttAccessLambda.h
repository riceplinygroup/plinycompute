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
#include "mustache.h"
#include "SimpleComputeExecutor.h"
#include "TupleSetMachine.h"

namespace pdb {

template<class Out, class ClassType>
class AttAccessLambda : public TypedLambdaObject<pdb::Ptr<Out>> {
 public:
  size_t offsetOfAttToProcess;
  std::string inputTypeName;
  std::string attName;
  std::string attTypeName;

  // create an att access lambda; offset is the position in the input object where we are going to
  // find the input att
  AttAccessLambda(std::string inputTypeNameIn,
                  std::string attNameIn,
                  std::string attType,
                  Handle<ClassType> &input,
                  size_t offset)
      : offsetOfAttToProcess(offset),
        inputTypeName(inputTypeNameIn),
        attName(attNameIn),
        attTypeName(attType) {

      this->setInputIndex(0, -(input.getExactTypeInfoValue() + 1));
  }

  std::string getTypeOfLambda() override {
      return std::string("attAccess");
  }

  std::string typeOfAtt() {
      return attTypeName;
  }

  std::string whichAttWeProcess() {
      return attName;
  }

  unsigned int getNumInputs() override {
      return 1;
  }

  std::string getInputType() {
      return inputTypeName;
  }

  std::string toTCAPString(std::vector<std::string> &inputTupleSetNames,
                           std::vector<std::string> &inputColumnNames,
                           std::vector<std::string> &inputColumnsToApply,
                           std::vector<std::string> &childrenLambdaNames,
                           int lambdaLabel,
                           std::string computationName,
                           int computationLabel,
                           std::string &outputTupleSetName,
                           std::vector<std::string> &outputColumns,
                           std::string &outputColumnName,
                           std::string &lambdaName,
                           MultiInputsBase *multiInputsComp = nullptr,
                           bool amIPartOfJoinPredicate = false,
                           bool amILeftChildOfEqualLambda = false,
                           bool amIRightChildOfEqualLambda = false,
                           std::string parentLambdaName = "",
                           bool isSelfJoin = false) override {

      // create the data for the lambda
      mustache::data lambdaData;
      lambdaData.set("computationName", computationName);
      lambdaData.set("computationLabel", std::to_string(computationLabel));
      lambdaData.set("typeOfLambda", getTypeOfLambda());
      lambdaData.set("lambdaLabel", std::to_string(lambdaLabel));

      // create the computation name with label
      mustache::mustache computationNameWithLabelTemplate{"{{computationName}}_{{computationLabel}}"};
      std::string computationNameWithLabel = computationNameWithLabelTemplate.render(lambdaData);

      // create the lambda name
      mustache::mustache lambdaNameTemplate{"{{typeOfLambda}}_{{lambdaLabel}}"};
      lambdaName = lambdaNameTemplate.render(lambdaData);

      // things we need to figure out in the next step
      int index;
      std::string inputTupleSetName;
      std::string tupleSetMidTag;
      std::string originalInputColumnToApply;

      if (multiInputsComp == nullptr) {
          tupleSetMidTag = "OutFor";
          inputTupleSetName = inputTupleSetNames[0];
      } else {
          tupleSetMidTag = "ExtractedFor";
          index = this->getInputIndex(0);
          PDB_COUT << lambdaName << ": myIndex=" << index << std::endl;
          inputTupleSetName = multiInputsComp->getTupleSetNameForIthInput(index);
          PDB_COUT << "inputTupleSetName=" << inputTupleSetName << std::endl;
          inputColumnNames = multiInputsComp->getInputColumnsForIthInput(index);
          inputColumnsToApply.clear();
          inputColumnsToApply.push_back(multiInputsComp->getNameForIthInput(index));
          originalInputColumnToApply = multiInputsComp->getNameForIthInput(index);
          PDB_COUT << "originalInputColumnToApply=" << originalInputColumnToApply << std::endl;
      }

      // set the lambda data
      lambdaData.set("tupleSetMidTag", tupleSetMidTag);
      lambdaData.set("attName", attName);

      // create the output tuple set name
      mustache::mustache outputTupleSetNameTemplate{"attAccess_{{lambdaLabel}}{{tupleSetMidTag}}{{computationName}}{{computationLabel}}"};
      outputTupleSetName = outputTupleSetNameTemplate.render(lambdaData);

      // create the output column name
      mustache::mustache outputColumnNameTemplate{"att_{{lambdaLabel}}{{tupleSetMidTag}}_{{attName}}"};
      outputColumnName = outputColumnNameTemplate.render(lambdaData);

      // initialize the output columns
      outputColumns.clear();
      for (const auto &inputColumnName : inputColumnNames) {
          outputColumns.push_back(inputColumnName);
      }
      outputColumns.push_back(outputColumnName);

      // generate the TCAP string for the lambda
      std::string tcapString;
      tcapString += this->getTCAPString(inputTupleSetName,
                                        inputColumnNames,
                                        inputColumnsToApply,
                                        outputTupleSetName,
                                        outputColumns,
                                        outputColumnName,
                                        "APPLY",
                                        computationNameWithLabel,
                                        lambdaName);

      if (multiInputsComp != nullptr) {
          if (amILeftChildOfEqualLambda || amIRightChildOfEqualLambda) {
              inputTupleSetName = outputTupleSetName;
              inputColumnNames.clear();
              for (const auto &outputColumn : outputColumns) {
                  // we want to remove the extracted value column from here
                  if (outputColumn != outputColumnName) {
                      inputColumnNames.push_back(outputColumn);
                  }
              }
              inputColumnsToApply.clear();
              inputColumnsToApply.push_back(outputColumnName);

              std::string hashOperator = amILeftChildOfEqualLambda ? "HASHLEFT" : "HASHRIGHT";
              outputTupleSetName = outputTupleSetName.append("_hashed");
              outputColumnName = outputColumnName.append("_hash");
              outputColumns.clear();

              std::copy(inputColumnNames.begin(), inputColumnNames.end(), std::back_inserter(outputColumns));
              outputColumns.push_back(outputColumnName);

              tcapString += this->getTCAPString(inputTupleSetName,
                                                inputColumnNames,
                                                inputColumnsToApply,
                                                outputTupleSetName,
                                                outputColumns,
                                                outputColumnName,
                                                hashOperator,
                                                computationNameWithLabel,
                                                parentLambdaName);
          }

          if (!isSelfJoin) {
              for (unsigned int i = 0; i < multiInputsComp->getNumInputs(); i++) {
                  std::string curInput = multiInputsComp->getNameForIthInput(i);
                  auto iter = std::find(outputColumns.begin(), outputColumns.end(), curInput);
                  if (iter != outputColumns.end()) {
                      PDB_COUT << "MultiInputBase for index=" << i << " is updated" << std::endl;
                      multiInputsComp->setTupleSetNameForIthInput(i, outputTupleSetName);
                      multiInputsComp->setInputColumnsForIthInput(i, outputColumns);
                      multiInputsComp->setInputColumnsToApplyForIthInput(i, outputColumnName);
                  }
                  if (originalInputColumnToApply == curInput) {
                      PDB_COUT << "MultiInputBase for index=" << i << " is updated" << std::endl;
                      multiInputsComp->setTupleSetNameForIthInput(i, outputTupleSetName);
                      multiInputsComp->setInputColumnsForIthInput(i, outputColumns);
                      multiInputsComp->setInputColumnsToApplyForIthInput(i, outputColumnName);
                  }
              }
          } else {
              // only update myIndex
              multiInputsComp->setTupleSetNameForIthInput(index, outputTupleSetName);
              multiInputsComp->setInputColumnsForIthInput(index, outputColumns);
              multiInputsComp->setInputColumnsToApplyForIthInput(index, outputColumnName);
          }
      }
      return tcapString;
  }

  int getNumChildren() override {
      return 0;
  }

  GenericLambdaObjectPtr getChild(int which) override {
      return nullptr;
  }

  ComputeExecutorPtr getExecutor(TupleSpec &inputSchema,
                                 TupleSpec &attsToOperateOn,
                                 TupleSpec &attsToIncludeInOutput) override {

      // create the output tuple set
      TupleSetPtr output = std::make_shared<TupleSet>();

      // create the machine that is going to setup the output tuple set, using the input tuple set
      TupleSetSetupMachinePtr myMachine =
          std::make_shared<TupleSetSetupMachine>(inputSchema, attsToIncludeInOutput);

      // this is the input attribute that we will process
      std::vector<int> matches = myMachine->match(attsToOperateOn);
      int whichAtt = matches[0];

      // this is the output attribute
      int outAtt = attsToIncludeInOutput.getAtts().size();

      return std::make_shared<SimpleComputeExecutor>(
          output,
          [=](TupleSetPtr input) {

            // set up the output tuple set
            myMachine->setup(input, output);

            // get the columns to operate on
            std::vector<Handle<ClassType>> &inputColumn =
                input->getColumn<Handle<ClassType>>(whichAtt);

            // setup the output column, if it is not already set up
            if (!output->hasColumn(outAtt)) {
                std::vector<Ptr<Out>> *outputCol = new std::vector<Ptr<Out>>;
                output->addColumn(outAtt, outputCol, true);
            }

            // get the output column
            std::vector<Ptr<Out>> &outColumn = output->getColumn<Ptr<Out>>(outAtt);

            // loop down the columns, setting the output
            int numTuples = inputColumn.size();
            outColumn.resize(numTuples);
            for (int i = 0; i < numTuples; i++) {
                outColumn[i] = (Out *) ((char *) &(*(inputColumn[i])) + offsetOfAttToProcess);
            }

            return output;
          },
          "attAccessLambda");
  }
};
}

#endif
