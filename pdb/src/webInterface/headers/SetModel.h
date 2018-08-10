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

#ifndef PDB_SETMODEL_H
#define PDB_SETMODEL_H

#include "CatalogServer.h"
#include "ResourceManagerServer.h"
#include <mustache.h>

namespace pdb {

// predefine the web interface
class WebInterface;

class SetModel;
typedef std::shared_ptr<SetModel> SetModelPtr;

class SetModel {

public:

  SetModel(bool isPseudoCluster, CatalogServer *catalogServer, pdb::ResourceManagerServer *resourceManager, pdb::WebInterface* webInterface);

  /**
   * Returns info about all the sets in the database
   * @return - the data
   */
  mustache::data getSets();

  /**
   *
   * @param dbName
   * @param setName
   * @return
   */
  mustache::data getSet(std::string& dbName, std::string& setName);

  /**
   *
   * @param node
   * @param numNodes
   * @param dbName
   * @param setName
   * @param address
   * @param port
   * @return
   */
  mustache::data getSetFromNode(int node, size_t numNodes, std::string &dbName, std::string &setName, std::string &address, int32_t port);

private:

  /**
   * Returns all the nodes as pairs of <std::string, int32_t>
   * @return the nodes
   */
  std::vector<std::pair<std::string, int32_t>> getNodes();

  /**
   * Catalog server pointer
   */
  pdb::CatalogServer *catalogServer;

  /**
   * resource manager
   */
  pdb::ResourceManagerServer *resourceManager;

  /**
   * web interface
   */
  pdb::WebInterface* webInterface;

  /**
   * true if we are running the a pseudo cluster
   */
  bool isPseudoCluster;

};

}


#endif //PDB_SETMODEL_H
