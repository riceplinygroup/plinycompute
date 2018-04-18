//
// Created by dimitrije on 3/16/18.
//

#ifndef PDB_BASEAGGREGATECOMP_H
#define PDB_BASEAGGREGATECOMP_H

#include "AbstractAggregateComp.h"

/**
 * This class is used to encapsulate the inner workings of the aggregate computation.
 * Do not derive from this class, when crating compute plans!
 */
namespace pdb {
template<class OutputClass, class InputClass, class KeyClass, class ValueClass>
class AggregateCompBase : public AbstractAggregateComp {
 public:

  /**
   * Gets the operation the extracts a key from an input object
   * @param aggMe - the object we want to the the operation from
   * @return the projection lambda
   */
  virtual Lambda<KeyClass> getKeyProjection(Handle<InputClass> aggMe) = 0;

  /**
   * Gets the operation the extracts a value from an input object
   * @param aggMe - the object we want to the the operation from
   * @return the projection lambda
   */
  virtual Lambda<ValueClass> getValueProjection(Handle<InputClass> aggMe) = 0;

  /**
   * Materialize aggregation output, use ScanUserSet to obtain consumer's ComputeSource
   * @param dbName - the name of the database we are materializing to
   * @param setName - the name of the set we are materializing to
   */
  void setOutput(std::string dbName, std::string setName) override {
    this->materializeAggOut = true;
    this->outputSetScanner = makeObject<ScanUserSet<OutputClass>>();
    this->outputSetScanner->setBatchSize(batchSize);
    this->outputSetScanner->setDatabaseName(dbName);
    this->outputSetScanner->setSetName(setName);
    this->whereHashTableSitsForThePartition = nullptr;
  }

  /**
   * Set hash table pointer
   * @param hashTablePointer - a pointer to the hash table for this aggregation
   */
  void setHashTablePointer(void *hashTablePointer) {
    this->whereHashTableSitsForThePartition = hashTablePointer;
  }

  /**
   * Extract the key projection and value projection lambdas
   * @param returnVal - the extracted lambdas
   */
  void extractLambdas(std::map<std::string, GenericLambdaObjectPtr> &returnVal) override {
    int suffix = 0;
    Handle<InputClass> checkMe = nullptr;
    Lambda<KeyClass> keyLambda = getKeyProjection(checkMe);
    Lambda<ValueClass> valueLambda = getValueProjection(checkMe);
    keyLambda.toMap(returnVal, suffix);
    valueLambda.toMap(returnVal, suffix);
  }

  /**
   * Sink for aggregation producing phase output, shuffle data will be combined from the sink
   * @param consumeMe -
   * @param projection -
   * @param plan -
   * @return
   */
  ComputeSinkPtr getComputeSink(TupleSpec &consumeMe,
                                TupleSpec &projection,
                                ComputePlan &plan) override {

    if (this->isUsingCombiner()) {
      return std::make_shared<ShuffleSink<KeyClass, ValueClass>>(numPartitions, consumeMe, projection);
    } else {
      if (numNodes == 0) {
        std::cout << "ERROR: cluster has 0 node" << std::endl;
        return nullptr;
      }
      if (numPartitions < numNodes) {
        std::cout << "ERROR: each node must have at least one partition" << std::endl;
        return nullptr;
      }
      return std::make_shared<CombinedShuffleSink<KeyClass, ValueClass>>(
          numPartitions / numNodes, numNodes, consumeMe, projection);
    }
  }

  /**
   * source for consumer to read aggregation results, aggregation results are written to user set
   * @param outputScheme
   * @param plan
   * @return
   */
  ComputeSourcePtr getComputeSource(TupleSpec &outputScheme, ComputePlan &plan) override {
    // materialize aggregation result to user set
    if (this->materializeAggOut) {
      if (outputSetScanner != nullptr) {
        return outputSetScanner->getComputeSource(outputScheme, plan);
      }
      return nullptr;

      // not materialize aggregation result, keep them in hash table
    } else {
      if (whereHashTableSitsForThePartition != nullptr) {
        Handle<Object> myHashTable =
            ((Record<Object> *) whereHashTableSitsForThePartition)->getRootObject();
        std::cout << "ClusterAggregate: getComputeSource: BATCHSIZE=" << batchSize
                  << std::endl;
        return std::make_shared<MapTupleSetIterator<KeyClass, ValueClass, OutputClass>>(
            myHashTable, batchSize);
      }
      return nullptr;
    }
  }

  /**
   * Used to return processor for combining data written to shuffle sink
   * the combiner processor is used in the end of aggregation producing phase the input is data written to shuffle sink
   * the output is data for shuffling
   * @param partitions
   * @return
   */
  SimpleSingleTableQueryProcessorPtr getCombinerProcessor(
      std::vector<HashPartitionID> partitions) override {
    return make_shared<CombinerProcessor<KeyClass, ValueClass>>(partitions);
  }

  /**
   * Used to return processor for aggregating on shuffle data
   * the aggregation processor is used in the  aggregation consuming phase
   * the input is shuffle data
   * the output are intermediate pages of arbitrary size allocated on heap
   * @param id
   * @return
   */
  SimpleSingleTableQueryProcessorPtr getAggregationProcessor(HashPartitionID id) override {
    return make_shared<AggregationProcessor<KeyClass, ValueClass>>(id);
  }

  /**
   * Used to return processor for writing aggregation results to a user set
   * the agg out processor is used in the aggregation consuming phase for materializing
   * aggregation results to user set
   * the input is the output of aggregation processor
   * the output is written to a user set
   */
  SimpleSingleTableQueryProcessorPtr getAggOutProcessor() override {
    return make_shared<AggOutProcessor<OutputClass, KeyClass, ValueClass>>();
  }

  /**
   * Used to set iterator for scanning the materialized aggregation output that is stored in a user set
   * @param iterator
   */
  void setIterator(PageCircularBufferIteratorPtr iterator) override {
    this->outputSetScanner->setIterator(iterator);
  }

  /**
   * Used to set data proxy for scanning the materialized aggregation output that is stored in a user set
   * @param proxy
   */
  void setProxy(DataProxyPtr proxy) override {
    this->outputSetScanner->setProxy(proxy);
  }

  /**
   * Used to set the database name
   * @param dbName
   */
  void setDatabaseName(std::string dbName) override {
    this->outputSetScanner->setDatabaseName(dbName);
  }

  /**
   * Used to set the set name
   * @param setName
   */
  void setSetName(std::string setName) override {
    this->outputSetScanner->setSetName(setName);
  }

  /**
   * Used to return the database name
   * @return
   */
  std::string getDatabaseName() override {
    return this->outputSetScanner->getDatabaseName();
  }

  /**
   * Used to return the set name
   * @return
   */
  std::string getSetName() override {
    return this->outputSetScanner->getSetName();
  }

  /**
   * this is an aggregation comp
   * @return
   */
  std::string getComputationType() override {
    return std::string("ClusterAggregationComp");
  }

  /**
   * Used to return the type if of this computation
   * @return
   */
  ComputationTypeID getComputationTypeID() override {
    return ClusterAggregationCompTypeID;
  }

  /**
   * Used to get output type
   * @return
   */
  std::string getOutputType() override {
    return getTypeName<OutputClass>();
  }

  /**
   * Used to get the number of inputs to this query type
   * @return
   */
  int getNumInputs() override {
    return 1;
  }

  /**
   * Used to get the name of the i^th input type...
   * @param i - return the i-th input type
   * @return the type
   */
  std::string getIthInputType(int i) override {
    if (i == 0) {
      return getTypeName<InputClass>();
    } else {
      return "";
    }
  }

  /**
   * Should the aggregation result be collected on the first 0 to numNodesToCollect
   * @param collectAsMapOrNot - true if it should false otherwise
   */
  void setCollectAsMap(bool collectAsMapOrNot) override {
    this->collectAsMapOrNot = collectAsMapOrNot;
  }

  /**
   * Is the aggregation collected on the first 0 to numNodesToCollect
   * @return true if it should, false otherwise
   */
  bool isCollectAsMap() override {
    return this->collectAsMapOrNot;
  }

  int getNumNodesToCollect() override {
    return this->numNodesToCollect;
  }

  void setNumNodesToCollect(int numNodesToCollect) override {
    this->numNodesToCollect = numNodesToCollect;
  }

  /**
   * Below function implements the interface for parsing computation into a TCAP string
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

  /**
   * Used to return Aggregate tcap string
   * @param inputTupleSetName
   * @param inputColumnNames
   * @param inputColumnsToApply
   * @param childrenLambdaNames
   * @param computationLabel
   * @param outputTupleSetName
   * @param outputColumnNames
   * @param addedOutputColumnName
   * @param myLambdaName
   * @return
   */
  std::string toTCAPString(std::string inputTupleSetName,
                           std::vector<std::string> &inputColumnNames,
                           std::vector<std::string> &inputColumnsToApply,
                           std::vector<std::string> &childrenLambdaNames,
                           int computationLabel,
                           std::string &outputTupleSetName,
                           std::vector<std::string> &outputColumnNames,
                           std::string &addedOutputColumnName,
                           std::string &myLambdaName) {

    PDB_COUT << "To GET TCAP STRING FOR CLUSTER AGGREGATE COMP" << std::endl;

    PDB_COUT << "To GET TCAP STRING FOR AGGREGATE KEY" << std::endl;
    Handle<InputClass> checkMe = nullptr;
    Lambda<KeyClass> keyLambda = getKeyProjection(checkMe);
    std::string tupleSetName;
    std::vector<std::string> columnNames;
    std::string addedColumnName;
    int lambdaLabel = 0;

    std::vector<std::string> columnsToApply;
    for (const auto &i : inputColumnsToApply) {
      columnsToApply.push_back(i);
    }

    std::string tcapString;
    tcapString += "\n/* Extract key for aggregation */\n";
    tcapString += keyLambda.toTCAPString(inputTupleSetName,
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

    PDB_COUT << "To GET TCAP STRING FOR AGGREGATE VALUE" << std::endl;

    Lambda<ValueClass> valueLambda = getValueProjection(checkMe);
    std::vector<std::string> columnsToKeep;
    columnsToKeep.push_back(addedColumnName);

    tcapString += "\n/* Extract value for aggregation */\n";
    tcapString += valueLambda.toTCAPString(tupleSetName,
                                           columnsToKeep,
                                           columnsToApply,
                                           childrenLambdaNames,
                                           lambdaLabel,
                                           getComputationType(),
                                           computationLabel,
                                           outputTupleSetName,
                                           outputColumnNames,
                                           addedOutputColumnName,
                                           myLambdaName,
                                           false);


    // create the data for the filter
    mustache::data clusterAggCompData;
    clusterAggCompData.set("computationType", getComputationType());
    clusterAggCompData.set("computationLabel", std::to_string(computationLabel));
    clusterAggCompData.set("outputTupleSetName", outputTupleSetName);
    clusterAggCompData.set("addedColumnName", addedColumnName);
    clusterAggCompData.set("addedOutputColumnName", addedOutputColumnName);

    // set the new tuple set name
    mustache::mustache newTupleSetNameTemplate{"aggOutFor{{computationType}}{{computationLabel}}"};
    std::string newTupleSetName = newTupleSetNameTemplate.render(clusterAggCompData);

    // set new added output columnName 1
    mustache::mustache newAddedOutputColumnName1Template{"aggOutFor{{computationLabel}}"};
    std::string addedOutputColumnName1 = newAddedOutputColumnName1Template.render(clusterAggCompData);

    clusterAggCompData.set("addedOutputColumnName1", addedOutputColumnName1);

    tcapString += "\n/* Apply aggregation */\n";

    mustache::mustache aggregateTemplate{"aggOutFor{{computationType}}{{computationLabel}} ({{addedOutputColumnName1}})"
                                         "<= AGGREGATE ({{outputTupleSetName}}({{addedColumnName}}, {{addedOutputColumnName}}),"
                                         "'{{computationType}}_{{computationLabel}}')\n"};

    tcapString += aggregateTemplate.render(clusterAggCompData);

    // update the state of the computation
    outputTupleSetName = newTupleSetName;
    outputColumnNames.clear();
    outputColumnNames.push_back(addedOutputColumnName1);

    this->setTraversed(true);
    this->setOutputTupleSetName(outputTupleSetName);
    this->setOutputColumnToApply(addedOutputColumnName1);
    addedOutputColumnName = addedOutputColumnName1;

    return tcapString;
  }

  Handle<ScanUserSet<OutputClass>> &getOutputSetScanner() {
    return outputSetScanner;
  }

 protected:

  /**
   * Output set scanner
   */
  Handle<ScanUserSet<OutputClass>> outputSetScanner = nullptr;

  /**
   * If this parameter is set the aggregation result will be collected on the first 0 to numNodesToCollect
   */
  bool collectAsMapOrNot = false;

  /**
   * The number of nodes we want to collect the aggregation results on, ignored if @see collectAsMap is false
   */
  int numNodesToCollect = 1;
};

}

#endif //PDB_BASEAGGREGATECOMP_H
