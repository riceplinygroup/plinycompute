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
   * Constructor of the catalog opens the location if it exists, otherwise creates a new empty catalog.
   * @param location - the path where our sqlite is/will be stored
   */
  explicit PDBCatalog(std::string location);

  /**
   * Destructor, deletes temporary files.
   *
   */
  ~PDBCatalog() = default;

  std::vector<unsigned char> serializeToBytes();

  /**
   * Registers the provided set
   * @param set - the set we want to register
   * @param error - the error if we fail
   * @return true if the set is registered successfully
   */
  bool registerSet(PDBCatalogSetPtr set, std::string &error);

  /**
   * Registers the provided database
   * @param db - the database we want to register
   * @param error - the error if we fail
   * @return true if the database is registered successfully
   */
  bool registerDatabase(PDBCatalogDatabasePtr db, std::string &error);

  /**
   * Registers the provided type
   * @param type - the type we want to register
   * @param error - the error if we fail
   * @return true if the type is registered successfully
   */
  bool registerType(PDBCatalogTypePtr type, std::string &error);

  /**
   * Registers the node
   * @param node - the node we want to register
   * @param error - the error if we fail
   * @return true if the node is registered successfully
   */
  bool registerNode(PDBCatalogNodePtr node, std::string &error);

  /**
   * Check if the database with the provided name exists
   * @param name - the name of the database
   * @return - true if it does false otherwise
   */
  bool databaseExists(const std::string &name);

  /**
   * Check if the set exists
   * @param dbName - the name of the database the set belongs to
   * @param setName - the name of the set
   * @return true if it does false otherwise
   */
  bool setExists(const std::string &dbName, const std::string &setName);

  /**
   * Check if the type exists
   * @param name - the name of the type
   * @return - true if it does false otherwise
   */
  bool typeExists(const std::string &name);

  /**
   * Check if the node exits
   * @param nodeID - the id of the node by aggrement it is a string of the form "ip:port"
   * @return - true if it does false otherwise
   */
  bool nodeExists(const std::string &nodeID);

  /**
   * Get the set with the of the provided name in the provided database
   * @param dbName - the name of the database the set belongs to
   * @param setName - the name of the set
   * @return - null if we do not find it, a copy of the set if we find it
   */
  PDBCatalogSetPtr getSet(const std::string &dbName, const std::string &setName);

  /**
   * Get the database with the name provided
   * @param dbName - the name of the database
   * @return - null if we do not find it, a copy of the database if we find it
   */
  PDBCatalogDatabasePtr getDatabase(const std::string &dbName);

  /**
   * Get the node with the identifier provided
   * @param nodeID - the node identifier is a string of the format "ip:port"
   * @return - null if we do not find it, a copy of the node info if we find it
   */
  PDBCatalogNodePtr getNode(const std::string &nodeID);

  /**
   * Get the type with the id provided
   * @param id - the id of the type
   * @return - null if we do not find it, a copy of the type with the shared library if we do. If the type is a builtin
   * type the shared library will be an empty vector<char>
   */
  PDBCatalogTypePtr getType(long id);

  /**
   * Get the type with the name provided
   * @param name - the name of the type
   * @return - null if we do not find it, a copy of the type with the shared library if we do. If the type is a builtin
   * type the shared library will be an empty vector<char>
   */
  PDBCatalogTypePtr getType(const std::string &name);

  /**
   * Get the type with the id provided
   * @param id - the id of the type
   * @return - null if we do not find it, a copy of the type without the shared library if we do. If the type is a builtin
   * type the shared library will be an empty vector<char>
   */
  PDBCatalogTypePtr getTypeWithoutLibrary(long id);

  /**
   * Get the type with the name provided
   * @param name - the name of the type
   * @return - null if we do not find it, a copy of the type without the shared library if we do. If the type is a builtin
   * type the shared library will be an empty vector<char>
   */
  PDBCatalogTypePtr getTypeWithoutLibrary(const std::string &name);

  /**
   * Returns the total number of registered types inside the catalog
   * @return - the number of types in the catalog
   */
  int32_t numRegisteredTypes();

  /**
   * Returns all the sets inside a database
   * @param dbName - the database we want the sets for
   * @return - a list of sets for that database, empty vector if there are none
   */
  std::vector<PDBCatalogSet> getSetsInDatabase(const std::string &dbName);

  /**
   * Returns all the nodes
   * @return - all the nodes inside the catalog
   */
  std::vector<PDBCatalogNode> getNodes();

  /**
   * Returns all the database
   * @return - a list of all the databases
   */
  std::vector<PDBCatalogDatabase> getDatabases();

  /**
   * Returns all the types inside the catalog without the shared library
   * @return - a list of all the types without the shared libraray
   */
  std::vector<PDBCatalogType> getTypesWithoutLibrary();

  /**
   * Returns a text that summarizes the nodes in the cluster
   * @return - the summary string
   */
  std::string listNodesInCluster();

  /**
   * Returns a text that summarizes the databases in the catalog
   * @return - the summary string
   */
  std::string listRegisteredDatabases();

  /**
   * Returns a text that summarizes the sets in a particular database
   * @param dbName - the name of the database
   * @return - the summary string
   */
  std::string listRegisteredSetsForDatabase(const std::string &dbName);

  /**
   * A text that summarizes the types inside the set
   * @return - the summary string
   */
  std::string listUserDefinedTypes();

  /**
   * Removes a particular database from the catalog
   * @param dbName - the name of the database we want to remove
   * @param error - the error if any
   * @return true if we removed it, false otherwise
   */
  bool removeDatabase(const std::string &dbName, std::string &error);

  /**
   * Removes a particular set from the catalog
   * @param dbName - the name of the database the set belongs to
   * @param setName - the name of the set
   * @param error - the error if any
   * @return true if we removed it, false otherwise
   */
  bool removeSet(const std::string &dbName, const std::string &setName, std::string &error);

 private:

  /**
   * The storage of the catalog - an orm mapped sqlite database
   */
  PDBCatalogStorage storage;

};

}


#endif /* PDB_CATALOG_H_ */