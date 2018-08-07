//
// Created by dimitrije on 8/5/18.
//

#ifndef PDB_CLUSTERMODEL_H
#define PDB_CLUSTERMODEL_H

#include <ResourceManagerServer.h>
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
