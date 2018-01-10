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

#ifndef SELECTION_COMP
#define SELECTION_COMP

#include "Computation.h"
#include "VectorSink.h"
#include "ScanUserSet.h"
#include "TypeName.h"

namespace pdb {

template<class OutputClass, class InputClass>
class SelectionComp : public Computation {

 public:
  // the computation returned by this method is called to see if a data item should be returned in
  // the output set
  virtual Lambda<bool> getSelection(Handle<InputClass> checkMe) = 0;

  // the computation returned by this method is called to perfom a transformation on the input
  // item before it
  // is inserted into the output set
  virtual Lambda<Handle<OutputClass>> getProjection(Handle<InputClass> checkMe) = 0;

  // calls getProjection and getSelection to extract the lambdas
  void extractLambdas(std::map<std::string, GenericLambdaObjectPtr> &returnVal) override {
    int suffix = 0;
    Handle<InputClass> checkMe = nullptr;
    Lambda<bool> selectionLambda = getSelection(checkMe);
    Lambda<Handle<OutputClass>> projectionLambda = getProjection(checkMe);
    selectionLambda.toMap(returnVal, suffix);
    projectionLambda.toMap(returnVal, suffix);
  }

  // this is a selection computation
  std::string getComputationType() override {
    return std::string("SelectionComp");
  }

  // to return the type if of this computation
  ComputationTypeID getComputationTypeID() override {
    return SelectionCompTypeID;
  }

  // gets the name of the i^th input type...
  std::string getIthInputType(int i) override {
    if (i == 0) {
      return getTypeName<InputClass>();
    } else {
      return "";
    }
  }

  // get the number of inputs to this query type
  int getNumInputs() override {
    return 1;
  }

  // gets the output type of this query as a string
  std::string getOutputType() override {
    return getTypeName<OutputClass>();
  }

  // below function implements the interface for parsing computation into a TCAP string
  std::string toTCAPString(std::vector<InputTupleSetSpecifier> &inputTupleSets,
                           int computationLabel,
                           std::string &outputTupleSetName,
                           std::vector<std::string> &outputColumnNames,
                           std::string &addedOutputColumnName) override {

    if (inputTupleSets.size() == 0) {
      return "";
    }
    InputTupleSetSpecifier inputTupleSet = inputTupleSets[0];
    std::vector<std::string> childrenLambdaNames;
    std::string myLambdaName;
    return toTCAPString(inputTupleSet.getTupleSetName(),
                        inputTupleSet.getColumnNamesToKeep(),
                        inputTupleSet.getColumnNamesToApply(),
                        childrenLambdaNames,
                        computationLabel,
                        outputTupleSetName,
                        outputColumnNames,
                        addedOutputColumnName,
                        myLambdaName);
  }

  // to return Selection tcap string
  std::string toTCAPString(std::string inputTupleSetName,
                           std::vector<std::string> &inputColumnNames,
                           std::vector<std::string> &inputColumnsToApply,
                           std::vector<std::string> &childrenLambdaNames,
                           int computationLabel,
                           std::string &outputTupleSetName,
                           std::vector<std::string> &outputColumnNames,
                           std::string &addedOutputColumnName,
                           std::string &myLambdaName) {

    PDB_COUT << "ABOUT TO GET TCAP STRING FOR SELECTION" << std::endl;
    Handle<InputClass> checkMe = nullptr;
    std::string tupleSetName;
    std::vector<std::string> columnNames;
    std::string addedColumnName;
    int lambdaLabel = 0;

    PDB_COUT << "ABOUT TO GET TCAP STRING FOR SELECTION LAMBDA" << std::endl;
    Lambda<bool> selectionLambda = getSelection(checkMe);

    std::string tcapString;
    tcapString += "\n/* Apply selection filtering */\n";
    tcapString += selectionLambda.toTCAPString(inputTupleSetName,
                                               inputColumnNames,
                                               inputColumnsToApply,
                                               childrenLambdaNames,
                                               lambdaLabel,
                                               getComputationType(),
                                               computationLabel,
                                               tupleSetName,
                                               columnNames,
                                               addedColumnName,
                                               myLambdaName,
                                               false);

    PDB_COUT << "The tcapString after parsing selection lambda: " << tcapString << "\n";
    PDB_COUT << "lambdaLabel=" << lambdaLabel << "\n";

    // create the data for the column names
    mustache::data inputColumnData = mustache::data::type::list;
    for(int i = 0; i < inputColumnNames.size(); i++) {

      mustache::data columnData;

      // fill in the column data
      columnData.set("columnName", inputColumnNames[i]);
      columnData.set("isLast", i == inputColumnNames.size()-1);

      inputColumnData.push_back(columnData);
    }

    // create the data for the filter
    mustache::data selectionCompData;
    selectionCompData.set("computationType", getComputationType());
    selectionCompData.set("computationLabel", std::to_string(computationLabel));
    selectionCompData.set("inputColumns", inputColumnData);
    selectionCompData.set("tupleSetName", tupleSetName);
    selectionCompData.set("addedColumnName", addedColumnName);

    // tupleSetName1(att1, att2, ...) <= FILTER (tupleSetName(methodCall_0OutFor_isFrank), methodCall_0OutFor_SelectionComp1(in0), 'SelectionComp_1')
    mustache::mustache scanSetTemplate{"filteredInputFor{{computationType}}{{computationLabel}}({{#inputColumns}}{{columnName}}{{^isLast}}, {{/isLast}}{{/inputColumns}}) "
                                       "<= FILTER ({{tupleSetName}}({{addedColumnName}}), {{tupleSetName}}({{#inputColumns}}{{columnName}}{{^isLast}}, {{/isLast}}{{/inputColumns}}), '{{computationType}}_{{computationLabel}}')\n"};

    // generate the TCAP string for the FILTER
    tcapString += scanSetTemplate.render(selectionCompData);

    // template for the new tuple set name
    mustache::mustache newTupleSetNameTemplate{"filteredInputFor{{computationType}}{{computationLabel}}"};

    // generate the new tuple set name
    std::string newTupleSetName = newTupleSetNameTemplate.render(selectionCompData);

    PDB_COUT << "TO GET TCAP STRING FOR PROJECTION LAMBDA\n";
    Lambda<Handle<OutputClass>> projectionLambda = getProjection(checkMe);

    // generate the TCAP string for the FILTER
    tcapString += "\n/* Apply selection projection */\n";
    tcapString += projectionLambda.toTCAPString(newTupleSetName,
                                                inputColumnNames,
                                                inputColumnsToApply,
                                                childrenLambdaNames,
                                                lambdaLabel,
                                                getComputationType(),
                                                computationLabel,
                                                outputTupleSetName,
                                                outputColumnNames,
                                                addedOutputColumnName,
                                                myLambdaName,
                                                true);

    // update the state of the computation
    this->setTraversed(true);
    this->setOutputTupleSetName(outputTupleSetName);
    this->setOutputColumnToApply(addedOutputColumnName);

    // return the TCAP string
    return tcapString;
  }

  void setOutput(std::string dbName, std::string setName) override {
    this->materializeSelectionOut = true;
    this->outputSetScanner = makeObject<ScanUserSet<OutputClass>>();
    this->outputSetScanner->setDatabaseName(dbName);
    this->outputSetScanner->setSetName(setName);
  }

  void setBatchSize(int batchSize) override {
    if (this->outputSetScanner != nullptr) {
      this->outputSetScanner->setBatchSize(batchSize);
    }
  }

  // to return the database name
  std::string getDatabaseName() override {
    return this->outputSetScanner->getDatabaseName();
  }

  // to return the set name
  std::string getSetName() override {
    return this->outputSetScanner->getSetName();
  }

  // source for consumer to read selection output, which has been written to a user set
  ComputeSourcePtr getComputeSource(TupleSpec &outputScheme, ComputePlan &plan) override {

    if (this->materializeSelectionOut == true) {
      if (this->outputSetScanner != nullptr) {
        return outputSetScanner->getComputeSource(outputScheme, plan);
      }
    }
    std::cout << "ERROR: get compute source for " << outputScheme << " returns nullptr"
              << std::endl;
    return nullptr;
  }

  // sink to write selection output
  ComputeSinkPtr getComputeSink(TupleSpec &consumeMe,
                                TupleSpec &projection,
                                ComputePlan &plan) override {

    if (this->materializeSelectionOut == true) {
      return std::make_shared<VectorSink<OutputClass>>(consumeMe, projection);
    }
    return nullptr;
  }

  bool needsMaterializeOutput() override {
    return materializeSelectionOut;
  }

  Handle<ScanUserSet<OutputClass>> &getOutputSetScanner() {
    return outputSetScanner;
  }

 private:
  bool materializeSelectionOut = false;
  Handle<ScanUserSet<OutputClass>> outputSetScanner = nullptr;
};
}

#endif
