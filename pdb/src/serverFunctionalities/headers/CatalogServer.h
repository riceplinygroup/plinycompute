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
#ifndef CATALOG_SERVER_H
#define CATALOG_SERVER_H

#include <mutex>

#include "CatalogClient.h"
#include "PDBCatalog.h"
#include "PDBDebug.h"
#include "PDBServer.h"
#include "ServerFunctionality.h"

/**
 * Class for handling requests regarding the Catalog Server functionality.
 * This can be used either by the Catalog Manager Server or a Catalog in any
 * Worker Node in the cluster. All metadata is stored and retrieved via
 * the pdbCatalog class, which has an SQLite database as the underlying
 * persistent storage.
 *
 * If this is the manager catalog server, it will receive a metadata
 * registration request and perform the following operations:
 *
 *    1) update metadata in the local manager catalog SQLite database
 *    2) iterate over all registered nodes in the cluster and send the metadata
 * object
 *    3) update catalog version
 *
 *  if this is the catalog instance of a worker node, it will receive a metadata
 * registration
 *  request from the manager catalog server and perform the following operations:
 *
 *    1) update metadata in the local catalog SQLite database
 *    2) update catalog version
 *    3) send acknowledgement to manager catalog server
 */

namespace pdb {

class CatalogServer : public ServerFunctionality {

public:

  /**
   * Creates a Catalog Server
   * @param catalogDirectory : the path of the location of the catalog
   * @param isManagerCatalogServer : true if this is the Manager Catalog Server, workers nodes have this parameter set to false
   * @param managerIP : the IP address of the Manager Catalog
   * @param managerPort : the port number of the Manager Catalog
   */
  CatalogServer(const string &catalogDirectory, bool isManagerCatalogServer,
                const string &managerIP, int managerPort);

  /**
   * Default destructor
   */
  ~CatalogServer() = default;

  /**
   * From the ServerFunctionality interface
   * @param forMe
   */
  void registerHandlers(PDBServer &forMe) override;

  /**
   * Returns a reference to the underlying class that manages catalog metadata
   * the metadata is stored in a SQLite database
   * @return
   */
  PDBCatalogPtr getCatalog();

private:

  /**
   * Interface to a persistent catalog storage for storing and retrieving PDB
   * metadata.
   * All metadata is stored in an SQLite database.
   */
  PDBCatalogPtr pdbCatalog;

  /**
   * Path where the catalog file is located
   */
  std::string catalogDirectory;

  /**
   * Path where we store the .so files
   */
  std::string tempPath;

  /**
   * True if this is the Manager Catalog Server
   */
  bool isManagerCatalogServer;

  /**
   * The ip address of the manager
   */
  string managerIP;

  /**
   * Default port of the Catalog Server, can be changed in the constructor
   */
  int managerPort;

  /**
   * Logger to capture debug information for the Catalog Server
   */
  PDBLoggerPtr catServerLogger;

  /**
   * To ensure serialized access
   */
  std::mutex serverMutex;

  /**
   * Adds a new object type... return -1 on failure, this is done on a worker node catalog
   * the typeID is given by the manager catalog
   * @param typeID
   * @param soFile
   * @param errMsg
   * @return
   */
  int16_t loadAndRegisterType(int16_t typeID, const char* &soFile, size_t soFileSize, string &errMsg);

  /**
   * Broadcasts a metadata item to all available nodes in a cluster, when an update has occurred
   * returns a map with the results from updating each node in the cluster
   * @tparam Type
   * @param metadataToSend
   * @param broadcastResults
   * @param errMsg
   * @return
   */
  template <class Type>
  bool broadcastCatalogUpdate(Handle<Type> metadataToSend, map<string, pair<bool, string>> &broadcastResults, string &errMsg);

  /**
   * Broadcasts a metadata item to all available nodes in a cluster, when an delete has occurred returns a map with
   * the results from updating each node in the cluster
   * @tparam Type
   * @param metadataToSend
   * @param broadcastResults
   * @param errMsg
   * @return
   */
  template <class Type>
  bool broadcastCatalogDelete(Handle<Type> metadataToSend, map<string, pair<bool, string>> &broadcastResults, string &errMsg);
};
}

#endif
