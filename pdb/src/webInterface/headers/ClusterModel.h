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

#ifndef PDB_CLUSTERMODEL_H
#define PDB_CLUSTERMODEL_H

#include <ResourceManagerServer.h>
#include <mustache.h>

namespace pdb {

class ClusterModel;
typedef std::shared_ptr<ClusterModel> ClusterModelPtr;

class ClusterModel {
public:

  /**
   * The model for the cluster data
   * @param isPseudoCluster - true if we are running the a pseudo cluster
   * @param resourceManager - the resource manager server
   */
  ClusterModel(bool isPseudoCluster, ResourceManagerServer *resourceManager);

  /**
   * Returns the info about the clusters in the mutache data for
   * @return - the data
   */
  mustache::data getClusterInfo();

private:

  /**
   * true if we are running the a pseudo cluster
   */
  bool isPseudoCluster;

  /**
   * resource manager
   */
  pdb::ResourceManagerServer *resourceManager;
};

}


#endif //PDB_CLUSTERMODEL_H
