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

#include "SimplePhysicalOptimizer/SimplePhysicalNodeFactory.h"
#include "PhysicalOptimizer.h"


namespace pdb {

PhysicalOptimizer::PhysicalOptimizer(std::vector<AbstractTCAPAnalyzerNodePtr> &sources, PDBLoggerPtr &logger) {

  // this is the logger
  this->logger = logger;

  // form the map of source nodes
  for(const auto &i : sources) {
    sourceNodes[i->getSourceSetIdentifier()->toSourceSetName()] = i;
  }
}

bool PhysicalOptimizer::getNextStagesOptimized(vector<Handle<AbstractJobStage>> &physicalPlanToOutput,
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

bool PhysicalOptimizer::hasSources() {
  return !sourceNodes.empty();
}

bool PhysicalOptimizer::hasConsumers(std::string &name) {

  // do we even have this node if not return false
  if(sourceNodes.find(name) == sourceNodes.end()){
    return false;
  }

  return sourceNodes[name]->hasConsumers();
}

AbstractTCAPAnalyzerNodePtr PhysicalOptimizer::getBestNode(StatisticsPtr &ptr) {

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