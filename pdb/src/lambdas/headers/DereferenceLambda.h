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
#include "mustache.h"
#include "LambdaHelperClasses.h"
#include "ComputeExecutor.h"
#include "SimpleComputeExecutor.h"
#include "TupleSetMachine.h"
#include "TupleSet.h"
#include "Ptr.h"

namespace pdb {

template<class OutType>
class DereferenceLambda : public TypedLambdaObject<OutType> {

 public:
  LambdaTree<Ptr<OutType>> input;

 public:
  unsigned int getInputIndex(int i) override {

    return input.getInputIndex(i);
  }

  DereferenceLambda(LambdaTree<Ptr<OutType>> &input) : input(input) {}

  unsigned int getNumInputs() override {
    return 1;
  }

  std::string getTypeOfLambda() override {
    return std::string("deref");
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
    lambdaData.set("typeOfLambda", getTypeOfLambda());
    lambdaData.set("lambdaLabel", std::to_string(lambdaLabel));
    lambdaData.set("computationName", computationName);
    lambdaData.set("computationLabel", std::to_string(computationLabel));

    // create the lambda name
    mustache::mustache lambdaNameTemplate{"{{typeOfLambda}}_{{lambdaLabel}}"};
    lambdaName = lambdaNameTemplate.render(lambdaData);

    std::string inputTupleSetName;
    std::string tupleSetMidTag;
    int index;

    if (multiInputsComp == nullptr) {
      tupleSetMidTag = "OutFor";
      inputTupleSetName = inputTupleSetNames[0];
    } else {
      tupleSetMidTag = "ExtractedFor";
      index = this->getInputIndex(0);
      inputTupleSetName = multiInputsComp->getTupleSetNameForIthInput(index);
      inputColumnNames = multiInputsComp->getInputColumnsForIthInput(index);
      inputColumnsToApply = multiInputsComp->getInputColumnsToApplyForIthInput(index);
    }

    // create the data for the lambda
    lambdaData.set("tupleSetMidTag", tupleSetMidTag);

    // create the output tuple set name
    mustache::mustache outputTupleSetNameTemplate{"deref_{{lambdaLabel}}{{tupleSetMidTag}}{{computationName}}{{computationLabel}}"};
    outputTupleSetName = outputTupleSetNameTemplate.render(lambdaData);

    // set the output column name
    outputColumnName = inputColumnsToApply[0];
    PDB_COUT << "OuputColumnName: " << outputColumnName << "\n";

    // fill up the output columns and setup the data
    mustache::data outputColumnsData = mustache::data::type::list;
    outputColumns.clear();
    for (const auto &inputColumnName : inputColumnNames) {
      if (inputColumnName != outputColumnName) {
        outputColumns.push_back(inputColumnName);

        // set the data
        mustache::data columnData;
        columnData.set("columnName", inputColumnName);
        columnData.set("isLast", false);

        // add the data entry
        outputColumnsData.push_back(columnData);
      }
    }

    // add the output column
    outputColumns.push_back(outputColumnName);

    // add the last output column data entry
    mustache::data lastColumnData;
    lastColumnData.set("columnName", outputColumnName);
    lastColumnData.set("isLast", true);
    outputColumnsData.push_back(lastColumnData);

    // create the data for the column names
    mustache::data inputColumnsToApplyData = mustache::data::type::list;
    for(int i = 0; i < inputColumnsToApply.size(); i++) {

      // fill in the column data
      mustache::data columnData;
      columnData.set("columnName", inputColumnsToApply[i]);
      columnData.set("isLast", i == inputColumnsToApply.size()-1);

      // add the data entry
      inputColumnsToApplyData.push_back(columnData);
    }

    // form the input columns to keep
    std::vector<std::string> inputColumnsToKeep;
    for (const auto &inputColumnName : inputColumnNames) {
      if(std::find(inputColumnsToApply.begin(), inputColumnsToApply.end(), inputColumnName) == inputColumnsToApply.end()) {
        // add the data
        inputColumnsToKeep.push_back(inputColumnName);
      }
    }

    // fill in the data
    mustache::data inputColumnsToKeepData = mustache::data::type::list;
    for(int i = 0; i < inputColumnsToKeep.size(); i++) {

      // fill in the column data
      mustache::data columnData;
      columnData.set("columnName", inputColumnsToKeep[i]);
      columnData.set("isLast", i == inputColumnsToKeep.size()-1);

      inputColumnsToKeepData.push_back(columnData);
    }

    // fill in the data
    lambdaData.set("outputTupleSetName", outputTupleSetName);
    lambdaData.set("outputColumns", outputColumnsData);
    lambdaData.set("inputTupleSetName", inputTupleSetName);
    lambdaData.set("inputColumnsToApply", inputColumnsToApplyData);
    lambdaData.set("inputColumnsToKeep", inputColumnsToKeepData);
    lambdaData.set("lambdaName", lambdaName);

    // apply template
    mustache::mustache ApplyTemplate{"{{outputTupleSetName}}"
                                     "({{#outputColumns}}{{columnName}}{{^isLast}}, {{/isLast}}{{/outputColumns}})"
                                     " <= APPLY ({{inputTupleSetName}}({{#inputColumnsToApply}}{{columnName}}{{^isLast}}, {{/isLast}}{{/inputColumnsToApply}}), "
                                     "{{inputTupleSetName}}({{#inputColumnsToKeep}}{{columnName}}{{^isLast}}, {{/isLast}}{{/inputColumnsToKeep}}), "
                                     "'{{computationName}}_{{computationLabel}}', '{{lambdaName}}')\n"};

    // the tcap string
    std::string tcapString = ApplyTemplate.render(lambdaData);

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

        for (const auto &inputColumnName : inputColumnNames) {
          outputColumns.push_back(inputColumnName);
        }
        outputColumns.push_back(outputColumnName);
        std::string computationNameWithLabel = computationName + std::to_string(computationLabel);

        tcapString += this->getTCAPString(inputTupleSetName,
                                          inputColumnNames,
                                          inputColumnsToApply,
                                          outputTupleSetName,
                                          outputColumns,
                                          outputColumnName,
                                          hashOperator,
                                          computationNameWithLabel,
                                          parentLambdaName,
                                          std::map<std::string, std::string>());
      }
      if (!isSelfJoin) {
        for (unsigned int i = 0; i < multiInputsComp->getNumInputs(); i++) {
          std::string curInput = multiInputsComp->getNameForIthInput(i);
          auto iter = std::find(outputColumns.begin(), outputColumns.end(), curInput);
          if (iter != outputColumns.end()) {
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

  /**
  * Returns the additional information about this lambda currently lambda type
  * @return the map
  */
  std::map<std::string, std::string> getInfo() override {

    // fill in the info
    return std::map<std::string, std::string>{
        std::make_pair ("lambdaType", getTypeOfLambda()),
    };
  };

  int getNumChildren() override {
    return 1;
  }

  GenericLambdaObjectPtr getChild(int which) override {
    if (which == 0)
      return input.getPtr();
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
          std::vector<Ptr<OutType>> &inColumn = input->getColumn<Ptr<OutType>>(firstAtt);

          // create the output attribute, if needed
          if (!output->hasColumn(outAtt)) {
            std::vector<OutType> *outColumn = new std::vector<OutType>;
            output->addColumn(outAtt, outColumn, true);
          }

          // get the output column
          std::vector<OutType> &outColumn = output->getColumn<OutType>(outAtt);

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
