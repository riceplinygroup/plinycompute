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

#ifndef PARTITION_TRANSFORMATION_COMP_BASE_H
#define PARTITION_TRANSFORMATION_COMP_BASE_H

#include "Computation.h"
#include "VectorSink.h"
#include "ScanUserSet.h"
#include "TypeName.h"
#include "AbstractPartitionComp.h"
#include "HashPartitionTransformationSink.h"

namespace pdb {
template<class KeyClass, class ValueClass>
class PartitionTransformationCompBase : public AbstractPartitionComp<KeyClass, ValueClass> {

 public:


  /**
   * the computation returned by this method is called to perfom a transformation on the input
   * item before it is inserted into the output set
   * @param checkMe: the input data element
   * @return: a Lambda in tree form to be called to perform transformation on the input to an output, which must have hash function defined.
   */
  virtual Lambda<KeyClass> getProjection(Handle<ValueClass> checkMe) = 0;


  /**
   * calls getProjection to extract the lambdas
   * @param returnVal: the populated lambda map
   */
  void extractLambdas(std::map<std::string, GenericLambdaObjectPtr> &returnVal) override {
    int suffix = 0;
    Handle<ValueClass> checkMe = nullptr;
    Lambda<KeyClass> projectionLambda = getProjection(checkMe);
    projectionLambda.toMap(returnVal, suffix);
  }



  /**
   * @return the type of this computation
   */
  std::string getComputationType() override {
    return std::string("PartitionTransformationComp");
  }

  /**
   * @return: the type id of this computation
   */
  ComputationTypeID getComputationTypeID() override {
    return PartitionTransformationCompTypeID;
  }


  /**
   * @return: the output type of this query as a string
   */
  std::string getOutputType() override {
    return getTypeName<KeyClass>();
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
  std::string toTCAPString(std::string inputTupleSetName,
                           std::vector<std::string> &inputColumnNames,
                           std::vector<std::string> &inputColumnsToApply,
                           std::vector<std::string> &childrenLambdaNames,
                           int computationLabel,
                           std::string &outputTupleSetName,
                           std::vector<std::string> &outputColumnNames,
                           std::string &addedOutputColumnName,
                           std::string &myLambdaName) override {

    PDB_COUT << "ABOUT TO GET TCAP STRING FOR SELECTION" << std::endl;
    Handle<ValueClass> checkMe = nullptr;
    std::string tupleSetName;
    std::vector<std::string> columnNames;
    std::string addedColumnName;
    int lambdaLabel = 0;


    std::string tcapString;

    PDB_COUT << "TO GET TCAP STRING FOR PROJECTION LAMBDA\n";
    Lambda<KeyClass> projectionLambda = getProjection(checkMe);

    tcapString += "\n/* Apply projection */\n";
    tcapString += projectionLambda.toTCAPString(inputTupleSetName,
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


  /**
   * to specify the output set for this computation
   * @param dbName: the name of the database
   * @param setName: the name of the set
   */
  void setOutput(std::string dbName, std::string setName) override {
    this->materializeSelectionOut = true;
    this->outputSetScanner = makeObject<ScanUserSet<KeyClass>>();
    this->outputSetScanner->setDatabaseName(dbName);
    this->outputSetScanner->setSetName(setName);
  }

  /**
   * to set the batch size for this computation
   * @param batchSize: the size of processing batches for this computation
   */

  void setBatchSize(int batchSize) override {
    if (this->outputSetScanner != nullptr) {
      this->outputSetScanner->setBatchSize(batchSize);
    }
  }

  /**
   * @return: the database name
   */
  std::string getDatabaseName() override {
    if (this->outputSetScanner != nullptr) {
      return this->outputSetScanner->getDatabaseName();
    } else {
      return "";
    }
  }

  /**
   * @return: the set name
   */
  std::string getSetName() override {
    if (this->outputSetScanner != nullptr) {
      return this->outputSetScanner->getSetName();
    } else {
      return "";
    }
  }

  /**
   * source for consumer to read selection output, which has been written to a user set
   * @param outputScheme: the tuple set scheme of the compute source
   * @param plan: the computation plan
   * @return: the source to this computation
   */
  ComputeSourcePtr getComputeSource(TupleSpec &outputScheme, ComputePlan &plan) override {

    if (this->materializeSelectionOut) {
      if (this->outputSetScanner != nullptr) {
        return outputSetScanner->getComputeSource(outputScheme, plan);
      }
    }
    std::cout << "ERROR: get compute source for " << outputScheme << " returns nullptr" << std::endl;
    return nullptr;
  }


  /**
   * Sink to write partition output
   * @param consumeMe: the tuple set scheme of the compute sink
   * @param projection: the tuple set scheme of the projection
   * @param plan: the computation plan
   * @return: the sink to this computation
   */
  ComputeSinkPtr getComputeSink(TupleSpec &consumeMe,
                                TupleSpec &projection,
                                ComputePlan &plan) override {

    if (this->materializeSelectionOut) {
      return std::make_shared<HashPartitionTransformationSink<KeyClass>>(this->numPartitions, this->numNodes, consumeMe, projection);
    }
    return nullptr;
  }


  /**
   * @return: a scanner that can be used to read the output of this computation
   */
  Handle<ScanUserSet<KeyClass>> &getOutputSetScanner() {
    return outputSetScanner;
  }

private:

  Handle<ScanUserSet<KeyClass>> outputSetScanner = nullptr;

};

}

#endif //PDB_PARTITIONCOMPBASE_H
