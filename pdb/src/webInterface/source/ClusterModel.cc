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

#include <mustache.h>
#include <UseTemporaryAllocationBlock.h>
#include "ClusterModel.h"

pdb::ClusterModel::ClusterModel(bool isPseudoCluster, pdb::ResourceManagerServer *resourceManager) : isPseudoCluster(
    isPseudoCluster), resourceManager(resourceManager) {}

mustache::data pdb::ClusterModel::getClusterInfo() {

  // all the stuff we create will be stored here
  const UseTemporaryAllocationBlock block(2 * 1024 * 1024);

  // the return data
  mustache::data ret;

  // we put all the node data here
  mustache::data nodesData = mustache::data::type::list;

  // the total stats
  size_t totalMemory = 0;
  size_t totalCores = 0;

  if(isPseudoCluster) {

    // grab the info about all the nodes in the pseudo cluster
    auto nodeObjects = this->resourceManager->getAllNodes();

    // set the number of nodes
    ret.set("number-of-nodes", std::to_string(nodeObjects->size()));

    // go through the nodes
    for (int i = 0; i < nodeObjects->size(); i++) {

      // the data of this node
      mustache::data nodeData;

      // set the data
      nodeData.set("node-id", std::to_string((*(nodeObjects))[i]->getNodeId()));
      nodeData.set("number-of-cores", std::to_string(DEFAULT_NUM_CORES / (nodeObjects->size())));
      nodeData.set("memory-size", std::to_string(DEFAULT_MEM_SIZE / (nodeObjects->size())));
      nodeData.set("address", (std::string)(*(nodeObjects))[i]->getAddress());
      nodeData.set("port", std::to_string((*(nodeObjects))[i]->getPort()));
      nodeData.set("is-last", i == nodeObjects->size() - 1);

      // update the total stats
      totalMemory += DEFAULT_MEM_SIZE / (nodeObjects->size());
      totalCores += DEFAULT_NUM_CORES / (nodeObjects->size());

      // add the node to the list
      nodesData.push_back(nodeData);
    }
  }
  else {

    // grab all the resources
    auto resourceObjects = this->resourceManager->getAllResources();

    // set the number of nodes
    ret.set("number-of-nodes", std::to_string(resourceObjects->size()));

    // go through resources
    for (int i = 0; i < resourceObjects->size(); i++) {

      // the data of this node
      mustache::data nodeData;

      // set the data
      nodeData.set("node-id", std::to_string((*(resourceObjects))[i]->getNodeId()));
      nodeData.set("number-of-cores", std::to_string((*(resourceObjects))[i]->getNumCores()));
      nodeData.set("memory-size", std::to_string((*(resourceObjects))[i]->getMemSize()));
      nodeData.set("address", (std::string)(*(resourceObjects))[i]->getAddress());
      nodeData.set("port", std::to_string((*(resourceObjects))[i]->getPort()));
      nodeData.set("is-last", i == resourceObjects->size() - 1);

      // update the total stats
      totalMemory += (*(resourceObjects))[i]->getMemSize();
      totalCores += (*(resourceObjects))[i]->getNumCores();

      // add the node to the list
      nodesData.push_back(nodeData);
    }
  }

  // set the data
  ret.set("nodes", nodesData);
  ret.set("total-memory", std::to_string(totalMemory));
  ret.set("total-cores", std::to_string(totalCores));
  ret.set("is-pseudo-cluster", isPseudoCluster ? "true" : "false");
  ret.set("success", true);

  return ret;
}
