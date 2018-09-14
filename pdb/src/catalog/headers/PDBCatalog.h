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
#ifndef PDB_CATALOG_H_
#define PDB_CATALOG_H_

#include <sqlite3.h>
#include <sqlite_orm.h>

#include <algorithm>
#include <ctime>
#include <dirent.h>
#include <dlfcn.h>
#include <fstream>
#include <iostream>
#include <map>
#include <pthread.h>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>
#include <memory>
#include <mutex>
#include "PDBCatalogStorage.h"

namespace pdb {
class PDBCatalog;
typedef std::shared_ptr<PDBCatalog> PDBCatalogPtr;



class PDBCatalog {

 public:

  /**
   * Constructor
   *
   * //TODO need to add @param logger the catalog logger
   * @param location the path to the location of the catalog (is relative to the
   * pdb root directory
   *
   */
  explicit PDBCatalog(std::string location);

  /**
   * Destructor, deletes temporary files.
   *
   */
  ~PDBCatalog() = default;

  bool registerSet(PDBCatalogSetPtr set, std::string &error);

  bool registerDatabase(PDBCatalogDatabasePtr db, std::string &error);

  bool registerType(PDBCatalogTypePtr type, std::string &error);

  bool registerNode(PDBCatalogNodePtr node, std::string &error);

  bool addNodeToSet(const std::string &nodeID, const std::string &dbName, const std::string &setName, std::string &error);

  bool databaseExists(const std::string &name);

  bool setExists(const std::string &dbName, const std::string &setName);

  bool typeExists(const std::string &name);

  bool nodeExists(const std::string &nodeID);

  PDBCatalogSetPtr getSet(const std::string &dbName, const std::string &setName);

  PDBCatalogDatabasePtr getDatabase(const std::string &dbName);

  PDBCatalogNodePtr getNode(const std::string &nodeID);

  PDBCatalogTypePtr getType(long id);

  PDBCatalogTypePtr getType(const std::string &name);

  PDBCatalogTypePtr getTypeWithoutLibrary(long id);

  PDBCatalogTypePtr getTypeWithoutLibrary(const std::string &name);

  int32_t numRegisteredTypes();

  std::vector<PDBCatalogNode> getNodesWithSet(const std::string &dbName, const std::string &setName);

  std::vector<PDBCatalogNode> getNodesWithDatabase(const std::string &dbName);

  std::vector<PDBCatalogSet> getSetForDatabase(const std::string &dbName);

  std::vector<PDBCatalogNode> getNodes();

  std::vector<PDBCatalogDatabase> getDatabases();

  std::vector<PDBCatalogType> getTypesWithoutLibrary();

  std::string listNodesInCluster();

  std::string listRegisteredDatabases();

  std::string listRegisteredSetsForDatabase(const std::string &dbName);

  std::string listUserDefinedTypes();

  bool removeDatabase(const std::string &dbName, std::string &error);

  bool removeSet(const std::string &dbName, const std::string &setName, std::string &error);

  bool removeNodeFromSet(const std::string &nodeID, const std::string &dbName, const std::string &setName, std::string &error);

 private:

  PDBCatalogStorage storage;

  /**
   * To ensure serialized access
   */
  std::mutex serverMutex;
};

}


#endif /* PDB_CATALOG_H_ */