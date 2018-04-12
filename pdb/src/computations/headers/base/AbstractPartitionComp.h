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

#ifndef ABSTRACT_PARTITION_COMP_H
#define ABSTRACT_PARTITION_COMP_H

#include "Computation.h"
#include "VectorSink.h"
#include "ScanUserSet.h"
#include "TypeName.h"

namespace pdb {
template<class KeyClass, class ValueClass>
class AbstractPartitionComp : public Computation {

 public:


  /**
   * @return the type of this computation
   */
  virtual std::string getComputationType() override { 
    return "AbstractPartitionComp"; 
  }

  /**
   * @return: the type id of this computation
   */
  virtual ComputationTypeID getComputationTypeID() override {
    return AbstractPartitionCompID;
  }

  /**
   * @param i: the index of the input element that we want to know the type
   * @return: the name of the i^th input type...
   */
  std::string getIthInputType(int i) override {
    if (i == 0) {
      return getTypeName<ValueClass>();
    } else {
      return "";
    }
  }

  /**
   * @return: the number of inputs to this query type
   */
  int getNumInputs() override {
    return 1;
  }

  /**
   * @return: the output type of this query as a string
   */
  virtual std::string getOutputType() override {
    return "";
  };

  /**
   * below function implements the interface for parsing computation into a TCAP string
   * @param inputTupleSets: identifiers to input tuple sets
   * @param computationLabel: the sequence number of this computation
   * @param outputTupleSetName: the name of the tuple set that is resulted from this computation
   * @param outputColumnNames: the column names in the tuple set that is resulted from this computation
   * @param addedOutputColumnName: the appended column name in the tuple set that is resulted from this computation
   * @return: the TCAP string represent this computation
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

  /**
   * @param inputTupleSetName: identifiers to input tuple sets
   * @param inputColumnNames: the sequence number of this computation
   * @param inputColumnsToApply: the names of the input columns in the input tuple sets that will be used in the computation
   * @param childrenLambdaNames: the extracted lambda names for this TCAP
   * @param computationLabel: the sequence number of this computation
   * @param outputTupleSetName: the name of the output tuple set that is resulted from this computation
   * @param outputColumnNames: the column names of the output tuple set that is resulted from this computation
   * @param addedOutputColumnName: the name of the newly appended column 
   * @param myLambdaName
   * @return: the tcap string for this computation
   */
  virtual std::string toTCAPString(std::string inputTupleSetName,
                           std::vector<std::string> &inputColumnNames,
                           std::vector<std::string> &inputColumnsToApply,
                           std::vector<std::string> &childrenLambdaNames,
                           int computationLabel,
                           std::string &outputTupleSetName,
                           std::vector<std::string> &outputColumnNames,
                           std::string &addedOutputColumnName,
                           std::string &myLambdaName) = 0;

  /**
   * to specify the output set for this computation
   * @param dbName: the name of the database
   * @param setName: the name of the set
   */
  virtual void setOutput(std::string dbName, std::string setName) override {}

  /**
   * to set the batch size for this computation
   * @param batchSize: the size of processing batches for this computation
   */

  virtual void setBatchSize(int batchSize) override {};

  /**
   * @return: the database name
   */
  virtual std::string getDatabaseName() override {
    return "";
  }

  /**
   * @return: the set name
   */
  std::string getSetName() override {
    return "";
  }

  /**
   * source for consumer to read selection output, which has been written to a user set
   * @param outputScheme: the tuple set scheme of the compute source
   * @param plan: the computation plan
   * @return: the source to this computation
   */
 virtual  ComputeSourcePtr getComputeSource(TupleSpec &outputScheme, ComputePlan &plan) override {
   return nullptr;
 }

  /**
   * Sink to write partition output
   * @param consumeMe: the tuple set scheme of the compute sink
   * @param projection: the tuple set scheme of the projection
   * @param plan: the computation plan
   * @return: the sink to this computation
   */
  virtual ComputeSinkPtr getComputeSink(TupleSpec &consumeMe,
                                TupleSpec &projection,
                                ComputePlan &plan) override {
    return nullptr;
  }


  /**
   * @return: whether the output of this computation is required to be materialized
   */
  bool needsMaterializeOutput() override {
    return materializeSelectionOut;
  }


  /**
   * @param numPartitions: to set the number of partitions for this computation
   */
 void setNumPartitions (int numPartitions) {
    this->numPartitions = numPartitions;
 }

 /**
  * @return: number of partitions for this partition computation
  */
 int getNumPartitions () {
    return this->numPartitions;
 }


 protected:
  bool materializeSelectionOut = false;
  int numPartitions = 16;


};

}

#endif //ABSTRACT_PARTITION_COMP_H
