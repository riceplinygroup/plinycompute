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

#ifndef PDB_MULTISELECTIONCOMPBASE_H
#define PDB_MULTISELECTIONCOMPBASE_H

#include "Computation.h"
#include "ComputePlan.h"
#include "VectorSink.h"
#include "ScanUserSet.h"
#include "TypeName.h"

namespace pdb {

/**
 * TODO add proper description
 * @tparam OutputClass
 * @tparam InputClass
 */
template<class OutputClass, class InputClass>
class MultiSelectionCompBase : public Computation {

 public:

  /**
   * the computation returned by this method is called to see if a data item should be returned in the output set
   * @param checkMe
   * @return
   */
  virtual pdb::Lambda<bool> getSelection(pdb::Handle<InputClass> checkMe) = 0;

  /**
   * the computation returned by this method is called to produce output tuples from this method
   * @param checkMe
   * @return
   */
  virtual pdb::Lambda<pdb::Vector<pdb::Handle<OutputClass>>> getProjection(pdb::Handle<InputClass> checkMe) = 0;

  /**
   * calls getProjection and getSelection to extract the lambdas
   * @param returnVal
   */
  void extractLambdas(std::map<std::string, GenericLambdaObjectPtr> &returnVal) override {
    int suffix = 0;
    Handle<InputClass> checkMe = nullptr;
    Lambda<bool> selectionLambda = getSelection(checkMe);
    Lambda<Vector<Handle<OutputClass>>> projectionLambda = getProjection(checkMe);
    selectionLambda.toMap(returnVal, suffix);
    projectionLambda.toMap(returnVal, suffix);
  }

  /**
   * this is a MultiSelection computation
   * @return
   */
  std::string getComputationType() override {
    return std::string("MultiSelectionComp");
  }

  /**
   * to return the type if of this computation
   * @return
   */
  ComputationTypeID getComputationTypeID() override {
    return MultiSelectionCompTypeID;
  }

  /**
   * gets the name of the i^th input type...
   * @param i
   * @return
   */
  std::string getIthInputType(int i) override {
    if (i == 0) {
      return getTypeName<InputClass>();
    } else {
      return "";
    }
  }

  /**
   * get the number of inputs to this query type
   * @return
   */
  int getNumInputs() override {
    return 1;
  }

  /**
   * return the output type
   * @return
   */
  std::string getOutputType() override {
    return getTypeName<OutputClass>();
  }

  /**
   * below function implements the interface for parsing computation into a TCAP string
   * @param inputTupleSets
   * @param computationLabel
   * @param outputTupleSetName
   * @param outputColumnNames
   * @param addedOutputColumnName
   * @return
   */
  std::string toTCAPString(std::vector<InputTupleSetSpecifier> &inputTupleSets,
                           int computationLabel,
                           std::string &outputTupleSetName,
                           std::vector<std::string> &outputColumnNames,
                           std::string &addedOutputColumnName) override {

    if (inputTupleSets.empty()) {
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
    PDB_COUT << "To GET TCAP STRING FOR SELECTION" << std::endl;

    Handle<InputClass> checkMe = nullptr;
    PDB_COUT << "TO GET TCAP STRING FOR SELECTION LAMBDA" << std::endl;
    Lambda<bool> selectionLambda = getSelection(checkMe);
    std::string tupleSetName;
    std::vector<std::string> columnNames;
    std::string addedColumnName;
    int lambdaLabel = 0;

    std::string tcapString;
    tcapString += "\n/* Apply MultiSelection filtering */\n";
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

    PDB_COUT << "tcapString after parsing selection lambda: " << tcapString << std::endl;
    PDB_COUT << "lambdaLabel=" << lambdaLabel << std::endl;

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

    // set the new tuple set name
    mustache::mustache newTupleSetNameTemplate{"filteredInputFor{{computationType}}{{computationLabel}}"};
    std::string newTupleSetName = newTupleSetNameTemplate.render(selectionCompData);

    mustache::mustache filterTemplate{"filteredInputFor{{computationType}}{{computationLabel}}"
                                      "({{#inputColumns}}{{columnName}}{{^isLast}}, {{/isLast}}{{/inputColumns}}) "
                                      "<= FILTER ({{tupleSetName}}({{addedColumnName}}), {{tupleSetName}}"
                                      "({{#inputColumns}}{{columnName}}{{^isLast}}, {{/isLast}}{{/inputColumns}}), "
                                      "'{{computationType}}_{{computationLabel}}')\n"};

    tcapString += filterTemplate.render(selectionCompData);

    PDB_COUT << "tcapString after adding filter operation: " << tcapString << std::endl;
    PDB_COUT << "TO GET TCAP STRING FOR PROJECTION LAMBDA" << std::endl;
    PDB_COUT << "lambdaLabel=" << lambdaLabel << std::endl;

    Lambda<Vector<Handle<OutputClass>>> projectionLambda = getProjection(checkMe);
    tcapString += "\n/* Apply MultiSelection projection */\n";
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

    // add the new data
    selectionCompData.set("addedOutputColumnName", addedOutputColumnName);
    selectionCompData.set("computationType", getComputationType());
    selectionCompData.set("computationLabel", std::to_string(computationLabel));
    selectionCompData.set("outputTupleSetName", outputTupleSetName);


    // create the new tuple set name
    newTupleSetNameTemplate = {"flattenedOutFor{{computationType}}{{computationLabel}}"};
    newTupleSetName = newTupleSetNameTemplate.render(selectionCompData);

    // create the new output column name
    mustache::mustache newOutputColumnNameTemplate = {"flattened_{{addedOutputColumnName}}"};
    std::string newOutputColumnName = newOutputColumnNameTemplate.render(selectionCompData);

    // add flatten
    mustache::mustache flattenTemplate{"flattenedOutFor{{computationType}}{{computationLabel}}(flattened_{{addedOutputColumnName}})"
                                       " <= FLATTEN ({{outputTupleSetName}}({{addedOutputColumnName}}), "
                                       "{{outputTupleSetName}}(), '{{computationType}}_{{computationLabel}}')\n"};
    tcapString += flattenTemplate.render(selectionCompData);

    this->setTraversed(true);
    this->setOutputTupleSetName(newTupleSetName);
    outputTupleSetName = newTupleSetName;
    this->setOutputColumnToApply(newOutputColumnName);
    addedOutputColumnName = newOutputColumnName;
    outputColumnNames.clear();
    outputColumnNames.push_back(addedOutputColumnName);

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

    if (this->materializeSelectionOut) {
      if (this->outputSetScanner != nullptr) {
        return outputSetScanner->getComputeSource(outputScheme, plan);
      }
    }
    return nullptr;
  }

  // sink to write selection output
  ComputeSinkPtr getComputeSink(TupleSpec &consumeMe,
                                TupleSpec &projection,
                                ComputePlan &plan) override {

    if (this->materializeSelectionOut) {
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

#endif //PDB_MULTISELECTIONCOMPBASE_H
