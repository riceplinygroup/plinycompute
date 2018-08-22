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

#ifndef PDB_JOINCOMPBASE_H
#define PDB_JOINCOMPBASE_H

#include "JoinTuple.h"
#include "DataProxy.h"
#include "AbstractJoinComp.h"
#include "ComputeInfo.h"
#include "ComputePlan.h"

namespace pdb {

/**
 * Used to parameterize joins that are run as part of a pipeline
 */
class JoinArg : public ComputeInfo {

 public:
  // this is the compute plan that we are part of
  ComputePlan& plan;

  // the location of the hash table
  void* pageWhereHashTableIs;

  JoinArg(ComputePlan& plan, void* pageWhereHashTableIs) : plan(plan), pageWhereHashTableIs(pageWhereHashTableIs) {}

  ~JoinArg() override = default;
};


/**
 * The possible types of joins
 */
typedef enum {

  HashPartitionedJoin,
  BroadcastJoin

} JoinType;

template <typename Out, typename In1, typename In2, typename... Rest>
class JoinCompBase : public AbstractJoinComp {
 private:
  // JiaNote: this is used to pass to lambda tree to update pipeline information for each input
  MultiInputsBase* multiInputsBase = nullptr;

  // JiaNote: this is to specify the JoinType, by default we use broadcast join
  JoinType joinType = BroadcastJoin;

  // JiaNote: partition number in the cluster, used by hash partition join
  int numPartitions = 0;

  // JiaNote: number of nodes, used by hash partition join
  int numNodes = 0;

  // JiaNote: partitionId for JoinSource, used by hash partition join
  size_t myPartitionId = 0;

  // JiaNote: the iterator for retrieving TupleSets from JoinMaps in pages
  // be careful here that we put PageCircularBufferIteratorPtr and DataProxyPtr in a pdb object.
  PageCircularBufferIteratorPtr iterator = nullptr;

  // JiaNote: the data proxy for accessing pages in frontend storage server.
  DataProxyPtr proxy = nullptr;

  // batch size
  int batchSize = -1;

 public:

  virtual ~JoinCompBase() {
    delete multiInputsBase;
    this->iterator = nullptr;
    this->proxy = nullptr;
  }

  // set join type
  void setJoinType(JoinType joinType) {
    this->joinType = joinType;
  }

  // get join type
  JoinType getJoinType() {
    return this->joinType;
  }

  // set number of partitions  (used in hash partition join)
  void setNumPartitions(int numPartitions) {
    this->numPartitions = numPartitions;
  }

  // return my number of partitions  (used in hash partition join)
  int getNumPartitions() {
    return numPartitions;
  }

  // set number of nodes  (used in hash partition join)
  void setNumNodes(int numNodes) {
    this->numNodes = numNodes;
  }

  // return my number of nodes  (used in hash partition join)
  int getNumNodes() {
    return numNodes;
  }

  // set my partition id for obtaining JoinSource for one partition  (used in hash partition join)
  void setPartitionId(size_t myPartitionId) {
    this->myPartitionId = myPartitionId;
  }

  // return my partition id for obtaining JoinSource for one partition  (used in hash partition
  // join)
  size_t getPartitionId() {
    return this->myPartitionId;
  }

  // JiaNote: be careful here that we put PageCircularBufferIteratorPtr and DataProxyPtr in a pdb
  // object (used in hash partition join)
  void setIterator(PageCircularBufferIteratorPtr iterator) {
    this->iterator = iterator;
  }

  // to set proxy for communicating with frontend storage server (used in hash partition join)
  void setProxy(DataProxyPtr proxy) {
    this->proxy = proxy;
  }

  // to set chunk size for JoinSource (used in hash partition join)
  void setBatchSize(int batchSize) override {
    this->batchSize = batchSize;
  }

  // to get batch size for JoinSource (used in hash partition join)
  int getBatchSize() {
    return this->batchSize;
  }

  MultiInputsBase* getMultiInputsBase() {
    if (multiInputsBase == nullptr) {
      multiInputsBase = new MultiInputsBase();
    }
    return multiInputsBase;
  }

  void setMultiInputsBaseToNull() {
    delete multiInputsBase;
    multiInputsBase = nullptr;
  }

  void analyzeInputSets(std::vector<std::string>& inputNames) {
    if (multiInputsBase == nullptr) {
      multiInputsBase = new MultiInputsBase();
    }
    // Step 1. setup all input names (the column name corresponding to input in tuple set)
    for (int i = 0; i < inputNames.size(); i++) {
      this->multiInputsBase->setNameForIthInput(i, inputNames[i]);
    }

    // Step 2. analyze selectionLambda to find all inputs in predicates
    Lambda<bool> selectionLambda = callGetSelection(*this);
    std::vector<std::string> inputsInPredicates =
        selectionLambda.getAllInputs(this->multiInputsBase);
    for (auto &inputsInPredicate : inputsInPredicates) {
      this->multiInputsBase->addInputNameToPredicates(inputsInPredicate);
    }
    // Step 3. analyze projectionLambda to find all inputs in projection
    Lambda<Handle<Out>> projectionLambda = callGetProjection(*this);
    std::vector<std::string> inputsInProjection =
        projectionLambda.getAllInputs(this->multiInputsBase);
    for (auto &i : inputsInProjection) {
      this->multiInputsBase->addInputNameToProjection(i);
    }
  }

  // the computation returned by this method is called to see if a data item should be returned in
  // the output set
  virtual Lambda<bool> getSelection(Handle<In1> in1,
                                    Handle<In2> in2,
                                    Handle<Rest>... otherArgs) = 0;

  // the computation returned by this method is called to produce output tuples from this method
  virtual Lambda<Handle<Out>> getProjection(Handle<In1> in1,
                                            Handle<In2> in2,
                                            Handle<Rest>... otherArgs) = 0;

  // calls getProjection and getSelection to extract the lambdas
  void extractLambdas(std::map<std::string, GenericLambdaObjectPtr>& returnVal) override {
    int suffix = 0;
    Lambda<bool> selectionLambda = callGetSelection(*this);
    Lambda<Handle<Out>> projectionLambda = callGetProjection(*this);
    selectionLambda.toMap(returnVal, suffix);
    projectionLambda.toMap(returnVal, suffix);
  }

  // return the output type
  std::string getOutputType() override {
    return getTypeName<Out>();
  }

  // count the number of inputs
  int getNumInputs() final {
    const int extras = sizeof...(Rest);
    return extras + 2;
  }

  template <typename First, typename... Others>
  typename std::enable_if<sizeof...(Others) == 0, std::string>::type getIthInputType(int i) {
    if (i == 0) {
      return getTypeName<First>();
    } else {
      std::cout << "Asked for an input type that didn't exist!";
      exit(1);
    }
  }

  // helper function to get a particular intput type
  template <typename First, typename... Others>
  typename std::enable_if<sizeof...(Others) != 0, std::string>::type getIthInputType(int i) {
    if (i == 0) {
      return getTypeName<First>();
    } else {
      return getIthInputType<Others...>(i - 1);
    }
  }

  // from the interface: get the i^th input type
  std::string getIthInputType(int i) final {
    return getIthInputType<In1, In2, Rest...>(i);
  }

  // JiaNote: TODO: encapsulate and reuse code for getting correctJoinTuple

  // JiaNote: this gets a sink merger
  SinkMergerPtr getSinkMerger(TupleSpec& consumeMe,
                              TupleSpec& attsToOpOn,
                              TupleSpec& projection,
                              ComputePlan& plan) override {

    // loop through each of the attributes that we are supposed to accept, and for each of them,
    // find the type
    std::vector<std::string> typeList;
    AtomicComputationPtr producer = plan.getPlan()->getComputations().getProducingAtomicComputation(consumeMe.getSetName());

    for (auto& a : projection.getAtts()) {

      // find the identity of the producing computation
      std::pair<std::string, std::string> res = producer->findSource(a, plan.getPlan()->getComputations());

      // and find its type... in the first case, there is not a particular lambda that we need to ask for
      if (res.second.empty()) {
        typeList.push_back("pdb::Handle<" + plan.getPlan()->getNode(res.first).getComputation().getOutputType() + ">");
      } else {
        std::string myType =
            plan.getPlan()->getNode(res.first).getLambda(res.second)->getOutputType();
        // std :: cout << "my type is " << myType << std :: endl;

        if (myType.find_first_of("pdb::Handle<") == 0) {
          typeList.push_back(myType);
        } else {
          typeList.push_back("pdb::Handle<" + myType + ">");
        }
      }
    }

    // now we get the correct join tuple, that will allow us to pack tuples from the join in a
    // hash table
    std::vector<int> whereEveryoneGoes;
    JoinTuplePtr correctJoinTuple = findCorrectJoinTuple<In1, In2, Rest...>(typeList, whereEveryoneGoes);

    return correctJoinTuple->getMerger();
  }

  // JiaNote: this gets a sink shuffler
  SinkShufflerPtr getSinkShuffler(TupleSpec& consumeMe,
                                  TupleSpec& attsToOpOn,
                                  TupleSpec& projection,
                                  ComputePlan& plan) override {

    // loop through each of the attributes that we are supposed to accept, and for each of them,
    // find the type
    std::vector<std::string> typeList;
    AtomicComputationPtr producer = plan.getPlan()->getComputations().getProducingAtomicComputation(consumeMe.getSetName());

    for (auto& a : projection.getAtts()) {

      // find the identity of the producing computation
      std::pair<std::string, std::string> res = producer->findSource(a, plan.getPlan()->getComputations());

      // and find its type... in the first case, there is not a particular lambda that we need to ask for
      if (res.second.empty()) {
        typeList.push_back("pdb::Handle<" + plan.getPlan()->getNode(res.first).getComputation().getOutputType() + ">");
      } else {
        std::string myType = plan.getPlan()->getNode(res.first).getLambda(res.second)->getOutputType();

        if (myType.find_first_of("pdb::Handle<") == 0) {
          typeList.push_back(myType);
        } else {
          typeList.push_back("pdb::Handle<" + myType + ">");
        }
      }
    }

    // now we get the correct join tuple, that will allow us to pack tuples from the join in a hash table
    std::vector<int> whereEveryoneGoes;
    JoinTuplePtr correctJoinTuple = findCorrectJoinTuple<In1, In2, Rest...>(typeList, whereEveryoneGoes);

    return correctJoinTuple->getShuffler();
  }


  // this gets a compute sink
  ComputeSinkPtr getComputeSink(TupleSpec& consumeMe,
                                TupleSpec& attsToOpOn,
                                TupleSpec& projection,
                                ComputePlan& plan) override {

    // loop through each of the attributes that we are supposed to accept, and for each of them, find the type
    std::vector<std::string> typeList;
    AtomicComputationPtr producer = plan.getPlan()->getComputations().getProducingAtomicComputation(consumeMe.getSetName());

    for (auto& a : projection.getAtts()) {

      // find the identity of the producing computation

      std::pair<std::string, std::string> res = producer->findSource(a, plan.getPlan()->getComputations());

      // and find its type... in the first case, there is not a particular lambda that we need to ask for
      if (res.second.empty()) {
        typeList.push_back(
            "pdb::Handle<" +
                plan.getPlan()->getNode(res.first).getComputation().getOutputType() + ">");
      } else {

        std::string myType = plan.getPlan()->getNode(res.first).getLambda(res.second)->getOutputType();

        if (myType.find_first_of("pdb::Handle<") == 0) {
          typeList.push_back(myType);
        } else {
          typeList.push_back("pdb::Handle<" + myType + ">");
        }
      }
    }

    // now we get the correct join tuple, that will allow us to pack tuples from the join in a hash table
    std::vector<int> whereEveryoneGoes;
    JoinTuplePtr correctJoinTuple = findCorrectJoinTuple<In1, In2, Rest...>(typeList, whereEveryoneGoes);

    if (this->joinType == BroadcastJoin) {
      return correctJoinTuple->getSink(consumeMe, attsToOpOn, projection, whereEveryoneGoes);
    } else if (this->joinType == HashPartitionedJoin) {
      return correctJoinTuple->getPartitionedSink(numPartitions / numNodes,
                                                  numNodes,
                                                  consumeMe,
                                                  attsToOpOn,
                                                  projection,
                                                  whereEveryoneGoes);
    }

    return nullptr;
  }

  // JiaNote: to get compute source for HashPartitionedJoin
  ComputeSourcePtr getComputeSource(TupleSpec& outputScheme, ComputePlan& plan) override {

    if (this->joinType != HashPartitionedJoin) {
      return nullptr;
    }
    // loop through each of the attributes that we are supposed to accept, and for each of them, find the type
    std::vector<std::string> typeList;
    AtomicComputationPtr producer = plan.getPlan()->getComputations().getProducingAtomicComputation(outputScheme.getSetName());

    for (auto& a : outputScheme.getAtts()) {
      if (a.find("hash") != std::string::npos) {
        continue;
      }

      // find the identity of the producing computation
      std::pair<std::string, std::string> res = producer->findSource(a, plan.getPlan()->getComputations());

      // and find its type... in the first case, there is not a particular lambda that we need to ask for
      if (res.second.empty()) {
        typeList.push_back("pdb::Handle<" + plan.getPlan()->getNode(res.first).getComputation().getOutputType() + ">");
      } else {

        std::string myType = plan.getPlan()->getNode(res.first).getLambda(res.second)->getOutputType();

        if (myType.find_first_of("pdb::Handle<") == 0) {
          typeList.push_back(myType);
        } else {
          typeList.push_back("pdb::Handle<" + myType + ">");
        }
      }
    }


    // now we get the correct join tuple, that will allow us to pack tuples from the join in a hash table
    std::vector<int> whereEveryoneGoes;
    JoinTuplePtr correctJoinTuple = findCorrectJoinTuple<In1, In2, Rest...>(typeList, whereEveryoneGoes);

    return correctJoinTuple->getPartitionedSource(
        this->myPartitionId,

        [&]() -> PDBPagePtr {

          if (this->iterator == nullptr) {
            std::cout << "Error in JoinComp: partitioned join source has a null iterator"
                      << std::endl;
            return nullptr;
          }

          while (this->iterator->hasNext()) {

            PDBPagePtr page = this->iterator->next();
            if (page != nullptr) {
              return page;
            }
          }

          return nullptr;

        },

        [&](PDBPagePtr freeMe) -> void {
          if (this->proxy != nullptr) {
            char* curBytes = freeMe->getRawBytes();
            NodeID nodeId = (NodeID)(*((NodeID*)(curBytes)));
            curBytes = curBytes + sizeof(NodeID);
            DatabaseID dbId = (DatabaseID)(*((DatabaseID*)(curBytes)));
            curBytes = curBytes + sizeof(DatabaseID);
            UserTypeID typeId = (UserTypeID)(*((UserTypeID*)(curBytes)));
            curBytes = curBytes + sizeof(UserTypeID);
            SetID setId = (SetID)(*((SetID*)(curBytes)));
            freeMe->decRefCount();
            if (freeMe->getRefCount() == 0) {
#ifdef PROFILING_CACHE
              std::cout << "To unpin Join source page with DatabaseID=" << dbId
                                  << ", UserTypeID=" << typeId << ", SetID=" << setId
                                  << ", PageID=" << freeMe->getPageID() << std::endl;
#endif
              try {
                this->proxy->unpinUserPage(nodeId, dbId, typeId, setId, freeMe, false);
              } catch (NotEnoughSpace& n) {
                makeObjectAllocatorBlock(4096, true);
                this->proxy->unpinUserPage(nodeId, dbId, typeId, setId, freeMe, false);
                throw n;
              }
            }
#ifdef PROFILING_CACHE
            else {
                        std::cout << "Can't unpin Join source page with DatabaseID=" << dbId
                                  << ", UserTypeID=" << typeId << ", SetID=" << setId
                                  << ", PageID=" << freeMe->getPageID()
                                  << ", reference count=" << freeMe->getRefCount() << std::endl;
                    }
#endif
          }
        },

        (size_t)this->batchSize,

        whereEveryoneGoes

    );
  }


  // this is a join computation
  std::string getComputationType() override {
    return std::string("JoinComp");
  }

  // to return the type if of this computation
  ComputationTypeID getComputationTypeID() override {
    return JoinCompTypeID;
  }

  // JiaNote: Returning a TCAP string for this Join computation
  std::string toTCAPString(std::vector<InputTupleSetSpecifier>& inputTupleSets,
                           int computationLabel,
                           std::string& outputTupleSetName,
                           std::vector<std::string>& outputColumnNames,
                           std::string& addedOutputColumnName) override {

    if (inputTupleSets.size() == getNumInputs()) {
      std::string tcapString;
      if (multiInputsBase == nullptr) {
        multiInputsBase = new MultiInputsBase();
      }
      multiInputsBase->setNumInputs(this->getNumInputs());
      std::vector<std::string> inputNames;

      // update tupleset name for input sets
      for (unsigned int i = 0; i < inputTupleSets.size(); i++) {
        this->multiInputsBase->setTupleSetNameForIthInput(i, inputTupleSets[i].getTupleSetName());
        this->multiInputsBase->setInputColumnsForIthInput(i, inputTupleSets[i].getColumnNamesToKeep());
        this->multiInputsBase->setInputColumnsToApplyForIthInput(i, inputTupleSets[i].getColumnNamesToApply());
        inputNames.push_back(inputTupleSets[i].getColumnNamesToApply()[0]);
      }

      analyzeInputSets(inputNames);
      Lambda<bool> selectionLambda = callGetSelection(*this);
      std::string inputTupleSetName;
      std::vector<std::string> inputColumnNames;
      std::vector<std::string> inputColumnsToApply;
      std::vector<std::string> childrenLambdaNames;
      int lambdaLabel = 0;
      std::string myLambdaName;
      MultiInputsBase* multiInputsComp = this->getMultiInputsBase();
      tcapString += selectionLambda.toTCAPString(inputTupleSetName,
                                                 inputColumnNames,
                                                 inputColumnsToApply,
                                                 childrenLambdaNames,
                                                 lambdaLabel,
                                                 "JoinComp",
                                                 computationLabel,
                                                 outputTupleSetName,
                                                 outputColumnNames,
                                                 addedOutputColumnName,
                                                 myLambdaName,
                                                 false,
                                                 multiInputsComp,
                                                 true);

      std::vector<std::string> inputsInProjection = multiInputsComp->getInputsInProjection();
      tcapString += "\n/* run Join projection on ( " + inputsInProjection[0];
      for (unsigned int i = 1; i < inputsInProjection.size(); i++) {
        tcapString += " " + inputsInProjection[i];
      }
      tcapString += " )*/\n";
      Lambda<Handle<Out>> projectionLambda = callGetProjection(*this);
      inputTupleSetName = outputTupleSetName;
      inputColumnNames.clear();
      inputColumnsToApply.clear();
      childrenLambdaNames.clear();
      for (unsigned int index = 0; index < multiInputsComp->getNumInputs(); index++) {
        multiInputsComp->setInputColumnsForIthInput(index, inputColumnNames);
      }

      tcapString += projectionLambda.toTCAPString(inputTupleSetName,
                                                  inputColumnNames,
                                                  inputColumnsToApply,
                                                  childrenLambdaNames,
                                                  lambdaLabel,
                                                  "JoinComp",
                                                  computationLabel,
                                                  outputTupleSetName,
                                                  outputColumnNames,
                                                  addedOutputColumnName,
                                                  myLambdaName,
                                                  true,
                                                  multiInputsComp,
                                                  false);

      this->setOutputTupleSetName(outputTupleSetName);
      this->setOutputColumnToApply(addedOutputColumnName);
      setMultiInputsBaseToNull();
      return tcapString;

    } else {
      std::cout << "ERROR: inputTupleSet size is " << inputTupleSets.size()
                << " and not equivalent with Join's inputs " << getNumInputs() << std::endl;
      return "";
    }
  }


  // gets an execute that can run a scan join... needToSwapAtts is true if the atts that are
  // currently stored in the hash table need to
  // come SECOND in the output tuple sets... hashedInputSchema tells us the schema for the
  // attributes that are currently stored in the
  // hash table... pipelinedInputSchema tells us the schema for the attributes that will be coming
  // through the pipeline...
  // pipelinedAttsToOperateOn is the identity of the hash attribute...
  // pipelinedAttsToIncludeInOutput tells us the set of attributes
  // that are coming through the pipeline that we actually have to write to the output stream
  ComputeExecutorPtr getExecutor(bool needToSwapAtts,
                                 TupleSpec& hashedInputSchema,
                                 TupleSpec& pipelinedInputSchema,
                                 TupleSpec& pipelinedAttsToOperateOn,
                                 TupleSpec& pipelinedAttsToIncludeInOutput,
                                 ComputeInfoPtr arg) override {
    // get the argument to the join
    JoinArg& joinArg = *((JoinArg*)arg.get());


    // loop through each of the attributes that we are supposed to accept, and for each of them,
    // find the type
    std::vector<std::string> typeList;
    AtomicComputationPtr producer = joinArg.plan.getPlan()->getComputations().getProducingAtomicComputation(hashedInputSchema.getSetName());
    for (auto& a : (hashedInputSchema.getAtts())) {

      // find the identity of the producing computation
      std::pair<std::string, std::string> res = producer->findSource(a, joinArg.plan.getPlan()->getComputations());

      // and find its type... in the first case, there is not a particular lambda that we need to ask for
      if (res.second.empty()) {
        typeList.push_back("pdb::Handle<" + joinArg.plan.getPlan()->getNode(res.first).getComputation().getOutputType() + ">");
      } else {

        std::string myType = joinArg.plan.getPlan()->getNode(res.first).getLambda(res.second)->getOutputType();

        if (myType.find_first_of("pdb::Handle<") == 0) {
          typeList.push_back(myType);
        } else {
          typeList.push_back("pdb::Handle<" + myType + ">");
        }
      }
    }

    // now we get the correct join tuple, that will allow us to pack tuples from the join in a hash table
    std::vector<int> whereEveryoneGoes;
    JoinTuplePtr correctJoinTuple = findCorrectJoinTuple<In1, In2, Rest...>(typeList, whereEveryoneGoes);

    // and return the correct probing code
    return correctJoinTuple->getProber(joinArg.pageWhereHashTableIs,
                                       whereEveryoneGoes,
                                       pipelinedInputSchema,
                                       pipelinedAttsToOperateOn,
                                       pipelinedAttsToIncludeInOutput,
                                       needToSwapAtts);
  }

  ComputeExecutorPtr getExecutor(bool needToSwapAtts,
                                 TupleSpec& hashedInputSchema,
                                 TupleSpec& pipelinedInputSchema,
                                 TupleSpec& pipelinedAttsToOperateOn,
                                 TupleSpec& pipelinedAttsToIncludeInOutput) override {
    std::cout << "Currently, no pipelined version of the join doesn't take an arg.\n";
    exit(1);
  }
};

}


#endif //PDB_JOINCOMPBASE_H
