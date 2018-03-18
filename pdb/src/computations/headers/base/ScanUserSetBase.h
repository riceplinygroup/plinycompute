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

#ifndef PDB_SCANUSERSETBASE_H
#define PDB_SCANUSERSETBASE_H

#include "TypeName.h"
#include "Computation.h"
#include "PageCircularBufferIterator.h"
#include "VectorTupleSetIterator.h"
#include "PDBString.h"
#include "DataTypes.h"
#include "DataProxy.h"
#include "Configuration.h"
#include "mustache.h"

namespace pdb {

template<class OutputClass>
class ScanUserSetBase : public Computation {

 public:

  /**
   * This constructor is for constructing builtin object
   */
  ScanUserSetBase() = default;

  /**
   * User should only use following constructor
   * @param dbName
   * @param setName
   */
  ScanUserSetBase(std::string dbName, std::string setName) {
    this->dbName = dbName;
    this->setName = setName;
    this->outputType = getTypeName<OutputClass>();
    this->batchSize = -1;
  }

  ~ScanUserSetBase() {
    this->iterator = nullptr;
    this->proxy = nullptr;
  }

  /**
   * Normally these would be defined by the ENABLE_DEEP_COPY macro, but because Array is the one variable-sized
   * type that we allow, we need to manually override these methods
   */
  void setUpAndCopyFrom(void *target, void *source) const override {
    new(target) ScanUserSetBase<OutputClass>();
    ScanUserSetBase<OutputClass> &fromMe = *((ScanUserSetBase<OutputClass> *) source);
    ScanUserSetBase<OutputClass> &toMe = *((ScanUserSetBase<OutputClass> *) target);
    toMe.iterator = fromMe.iterator;
    toMe.proxy = fromMe.proxy;
    toMe.batchSize = fromMe.batchSize;
    toMe.dbName = fromMe.dbName;
    toMe.setName = fromMe.setName;
    toMe.outputType = fromMe.outputType;

  }

  void deleteObject(void *deleteMe) override {
    deleter(deleteMe, this);
  }

  size_t getSize(void *forMe) override {
    return sizeof(ScanUserSetBase<OutputClass>);
  }

  ComputeSourcePtr getComputeSource(TupleSpec &schema, ComputePlan &plan) override {
    return std::make_shared<VectorTupleSetIterator>(

        [&]() -> void * {
          if (this->iterator == nullptr) {
            return nullptr;
          }
          while (this->iterator->hasNext()) {

            PDBPagePtr page = this->iterator->next();
            if (page != nullptr) {
              return page->getBytes();
            }
          }

          return nullptr;

        },

        [&](void *freeMe) -> void {
          if (this->proxy != nullptr) {
            char *pageRawBytes = (char *) freeMe -
                (sizeof(NodeID) + sizeof(DatabaseID) + sizeof(UserTypeID) + sizeof(SetID) +
                    sizeof(PageID) + sizeof(int) + sizeof(size_t));

            PDBPagePtr page = make_shared<PDBPage>(pageRawBytes, 0, 0);
            NodeID nodeId = page->getNodeID();
            DatabaseID dbId = page->getDbID();
            UserTypeID typeId = page->getTypeID();
            SetID setId = page->getSetID();
            try {
              this->proxy->unpinUserPage(nodeId, dbId, typeId, setId, page, false);
            } catch (NotEnoughSpace &n) {
              makeObjectAllocatorBlock(4096, true);
              this->proxy->unpinUserPage(nodeId, dbId, typeId, setId, page, false);
              throw n;
            }
          }
        },

        this->batchSize

    );
  }

  /**
   * Be careful here that we put PageCircularBufferIteratorPtr and DataProxyPtr in a pdb object
   * @param iterator
   */
  void setIterator(PageCircularBufferIteratorPtr iterator) {
    this->iterator = iterator;
  }

  void setProxy(DataProxyPtr proxy) {
    this->proxy = proxy;
  }

  void setBatchSize(int batchSize) override {
    this->batchSize = batchSize;
  }

  int getBatchSize() {
    return this->batchSize;
  }

  void setOutput(std::string dbName, std::string setName) override {
    this->dbName = dbName;
    this->setName = setName;
  }

  void setDatabaseName(std::string dbName) {
    this->dbName = dbName;
  }

  void setSetName(std::string setName) {
    this->setName = setName;
  }

  std::string getDatabaseName() override {
    return dbName;
  }

  std::string getSetName() override {
    return setName;
  }

  std::string getComputationType() override {
    return std::string("ScanUserSet");
  }

  /**
   * to return the type if of this computation
   * @return
   */
  ComputationTypeID getComputationTypeID() override {
    return ScanSetTypeID;
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

    InputTupleSetSpecifier inputTupleSet;
    if (!inputTupleSets.empty()) {
      inputTupleSet = inputTupleSets[0];
    }
    return toTCAPString(inputTupleSet.getTupleSetName(),
                        inputTupleSet.getColumnNamesToKeep(),
                        inputTupleSet.getColumnNamesToApply(),
                        computationLabel,
                        outputTupleSetName,
                        outputColumnNames,
                        addedOutputColumnName);
  }

  /**
   * Below function returns a TCAP string for this Computation
   * @param inputTupleSetName
   * @param inputColumnNames
   * @param inputColumnsToApply
   * @param computationLabel
   * @param outputTupleSetName
   * @param outputColumnNames
   * @param addedOutputColumnName
   * @return
   */
  std::string toTCAPString(std::string inputTupleSetName,
                           std::vector<std::string> &inputColumnNames,
                           std::vector<std::string> &inputColumnsToApply,
                           int computationLabel,
                           std::string &outputTupleSetName,
                           std::vector<std::string> &outputColumnNames,
                           std::string &addedOutputColumnName) {

    // the template we are going to use to create the TCAP string for this ScanUserSet
    mustache::mustache scanSetTemplate{"inputDataFor{{computationType}}_{{computationLabel}}(in{{computationLabel}})"
                                       " <= SCAN ('{{setName}}', '{{dbName}}', '{{computationType}}_{{computationLabel}}')\n"};

    // the data required to fill in the template
    mustache::data scanSetData;
    scanSetData.set("computationType", getComputationType());
    scanSetData.set("computationLabel", std::to_string(computationLabel));
    scanSetData.set("setName", std::string(setName));
    scanSetData.set("dbName", std::string(dbName));

    // output column name
    mustache::mustache outputColumnNameTemplate{"in{{computationLabel}}"};

    //  set the output column name
    addedOutputColumnName = outputColumnNameTemplate.render(scanSetData);
    outputColumnNames.push_back(addedOutputColumnName);

    // output tuple set name template
    mustache::mustache outputTupleSetTemplate{"inputDataFor{{computationType}}_{{computationLabel}}"};
    outputTupleSetName = outputTupleSetTemplate.render(scanSetData);

    // update the state of the computation
    this->setTraversed(true);
    this->setOutputTupleSetName(outputTupleSetName);
    this->setOutputColumnToApply(addedOutputColumnName);

    // return the TCAP string
    return scanSetTemplate.render(scanSetData);
  }

  int getNumInputs() override {
    return 0;
  }

  std::string getIthInputType(int i) override {
    return "";
  }

  std::string getOutputType() override {
    if (outputType == "") {
      outputType = getTypeName<OutputClass>();
    }
    return this->outputType;
  }

  bool needsMaterializeOutput() override {
    return false;
  }

 protected:

  /**
   * Be careful here that we put PageCircularBufferIteratorPtr and DataProxyPtr in a pdb object.
   */
  PageCircularBufferIteratorPtr iterator = nullptr;

  DataProxyPtr proxy = nullptr;

  String dbName;

  String setName;

  int batchSize{};

  String outputType = "";
};

}



#endif //PDB_SCANUSERSETBASE_H
