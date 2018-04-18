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
#include "SimpleTCAPAnalyzer/SimpleTCAPAnalyzerNodeFactory.h"
#include "TCAPAnalyzer.h"

namespace pdb {

TCAPAnalyzer::TCAPAnalyzer(std::string &jobId,
                                 PDBLoggerPtr logger,
                                 ConfigurationPtr &conf,
                                 std::string TCAPString,
                                 pdb::Handle<pdb::Vector<pdb::Handle<pdb::Computation>>> computations) {
  try {
    // parse the plan and initialize the values we need
    this->computePlan = makeObject<ComputePlan>(String(TCAPString), *computations);
    this->logicalPlan = this->computePlan->getPlan();
    this->computationGraph = this->logicalPlan->getComputations();
    this->sourcesComputations = this->computationGraph.getAllScanSets();

    // create the analyzer factory
    AbstractTCAPAnalyzerNodeFactoryPtr analyzerNodeFactory = make_shared<SimpleTCAPAnalyzerNodeFactory>(jobId,
                                                                                                        computePlan,
                                                                                                        conf);
    // generate the graph
    auto sources = analyzerNodeFactory->generateAnalyzerGraph(this->sourcesComputations);

    // form the map of source nodes
    for(const auto &i : sources) {
      sourceNodes[i->getSourceSetIdentifier()->toSourceSetName()] = i;
    }

  } catch (pdb::NotEnoughSpace &n) {
    PDB_COUT << "FATAL ERROR in TCAPAnalyzer: Not enough memory to allocate the computePlan object";
    logger->fatal("FATAL ERROR in TCAPAnalyzer: Not enough memory to allocate the computePlan object");
    this->computePlan = nullptr;
    this->logicalPlan = nullptr;
    this->sourcesComputations.clear();
  }
}

bool TCAPAnalyzer::getNextStagesOptimized(vector<Handle<AbstractJobStage>> &physicalPlanToOutput,
                                             vector<Handle<SetIdentifier>> &interGlobalSets,
                                             StatisticsPtr &stats,
                                             int &jobStageId) {
  // grab the best node
  auto source = getBestNode(stats);

  // analyze this source node and do physical planning
  auto result = source->analyze(stats, jobStageId);

  // copy the physical plan
  physicalPlanToOutput.insert(physicalPlanToOutput.end(),
                              result->physicalPlanToOutput.begin(),
                              result->physicalPlanToOutput.end());

  // copy the intermediate sets
  interGlobalSets.insert(interGlobalSets.end(),
                         result->interGlobalSets.begin(),
                         result->interGlobalSets.end());

  // did we succeed
  if(result->success) {

    // increase the job stage
    jobStageId += result->physicalPlanToOutput.size();

    // did we create a new source set
    if(result->newSourceComputation != nullptr) {
      sourceNodes[result->newSourceComputation->getSourceSetIdentifier()->toSourceSetName()] = result->newSourceComputation;
    }

    // does this source have any consumers
    if(!source->hasConsumers()) {
      sourceNodes.erase(source->getSourceSetIdentifier()->toSourceSetName());
    }
  }
  // if we did not penalize this set
  else {
    penalizedSets.insert(source->getSourceSetIdentifier()->toSourceSetName());
  }

  return result->success;
}

bool TCAPAnalyzer::hasSources() {
  return !sourceNodes.empty();
}

bool TCAPAnalyzer::hasConsumers(std::string &name) {

  // do we even have this node if not return false
  if(sourceNodes.find(name) == sourceNodes.end()){
    return false;
  }

  return sourceNodes[name]->hasConsumers();
}

AbstractTCAPAnalyzerNodePtr TCAPAnalyzer::getBestNode(StatisticsPtr &ptr) {

  // the default is to just use the first node
  AbstractTCAPAnalyzerNodePtr ret = sourceNodes.begin()->second;
  double cost = std::numeric_limits<double>::max();

  // go through all source nodes
  for(const auto &it : sourceNodes) {

    // grab the cost of the source
    double sourceCost = it.second->getCost(ptr);
    
    // is this set in the penalized sets, increase the cost by a factor of 1000
    if(penalizedSets.find(it.second->getSourceSetIdentifier()->toSourceSetName()) != penalizedSets.end()){
      sourceCost *= SOURCE_PENALIZE_FACTOR;
    }

    // if the cost is less the the current cost this is our new best source
    if(sourceCost < cost) {
      cost = sourceCost;
      ret = it.second;
    }
  }

  return ret;
}

}