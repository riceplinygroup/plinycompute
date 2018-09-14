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

#include "PDBCatalog.h"

using namespace sqlite_orm;

pdb::PDBCatalog::PDBCatalog(std::string location) : storage(makeStorage(&location)) {

  // sync the schema
  storage.sync_schema();
}

bool pdb::PDBCatalog::registerSet(pdb::PDBCatalogSetPtr set, std::string &error) {

  try {

    // check if the set already exists
    if(setExists(set->database, set->setIdentifier)) {

      // set the error
      error = "The set is already registered\n";

      // we failed return false
      return false;
    }

    // insert the database
    storage.replace(*set);

    // return true
    return true;

  } catch(std::system_error &e){

    // set the error we failed
    error = "Could not register the set with the identifier : " + set->setIdentifier + " and type " + *set->type + "! The SQL error is : "  + std::string(e.what());

    // we failed
    return false;
  }
}

bool pdb::PDBCatalog::registerDatabase(pdb::PDBCatalogDatabasePtr db, std::string &error) {

  try {

    // if the database exists don't create it
    if(databaseExists(db->name)) {

      // set the error
      error = "The database is already registered\n";

      // we failed return false
      return false;
    }

    // set the created time
    db->createdOn = std::time(0);

    // insert the database
    storage.replace(*db);

    // return true
    return true;

  } catch(std::system_error &e){

    // set the error we failed
    error = "Could not register the database : " + db->name +  "! The SQL error is : "  + std::string(e.what());

    // we failed
    return false;
  }
}

bool pdb::PDBCatalog::registerType(pdb::PDBCatalogTypePtr type, std::string &error) {

  try {

    // if the type exists don't create it
    if(typeExists(type->name)) {

      // set the error
      error = "The type is already registered\n";

      // we failed return false
      return false;
    }

    // insert the the set
    storage.replace(*type);

    // return true
    return true;

  } catch(std::system_error &e){

    // set the error we failed
    error = "Could not register a type : " + type->name +  "! The SQL error is : "  + std::string(e.what());

    // we failed
    return false;
  }
}

bool pdb::PDBCatalog::registerNode(pdb::PDBCatalogNodePtr node, std::string &error) {

  try {

    // if the node exists don't create it
    if(nodeExists(node->nodeID)){

      // set the error
      error = "The name is already registered\n";

      // we failed return false
      return false;
    }

    // insert the the set
    storage.replace(*node);

    // return true
    return true;

  } catch(std::system_error &e){

    // set the error we failed
    error = "Could not register the node with the address : " + node->nodeID +  "! The SQL error is : "  + std::string(e.what());

    // we failed
    return false;
  }
}

bool pdb::PDBCatalog::addNodeToSet(const std::string &nodeID, const std::string &dbName, const std::string &setName, std::string &error) {

  try {

    // check if the set exists
    if(!setExists(dbName, setName)) {

      // log the error
      error = "The set with the identifier : " + dbName + ":" + setName + " we are supposed to add to the node does not exist\n";
      return false;
    }

    if(!nodeExists(nodeID)) {

      // log the error
      error = "The node with the id : " + nodeID +" does not exist\n";
      return false;
    }

    // create the object that maps the set to the node
    PDBCatalogSetOnNodes setToNode(dbName + ":" + setName, nodeID);

    // insert the the set
    storage.insert(setToNode);

    // return true
    return true;

  } catch(std::system_error &e){

    // set the error we failed
    error = "Could add the set : " + dbName  + ":" + setName + " to the node with the address : " + nodeID +  "! The SQL error is : "  + std::string(e.what());

    // we failed
    return false;
  }
}

bool pdb::PDBCatalog::databaseExists(const std::string &name) {

  // try to find the database
  auto db = storage.get_no_throw<PDBCatalogDatabase>(name);

  // did we find it?
  return db != nullptr;
}

bool pdb::PDBCatalog::setExists(const std::string &dbName, const std::string &setName) {
  // try to find the database
  auto set = storage.get_no_throw<PDBCatalogSet>(dbName + ":" + setName);

  // did we find it?
  return set != nullptr;
}

bool pdb::PDBCatalog::typeExists(const std::string &name) {
  // try to find the database
  auto type = storage.get_no_throw<PDBCatalogType>(name);

  // did we find it?
  return type != nullptr;
}

bool pdb::PDBCatalog::nodeExists(const std::string &nodeID) {
  // try to find the database
  auto node = storage.get_no_throw<PDBCatalogNode>(nodeID);

  // did we find it?
  return node != nullptr;
}

pdb::PDBCatalogSetPtr pdb::PDBCatalog::getSet(const std::string &dbName, const std::string &setName) {
  return storage.get_no_throw<PDBCatalogSet>(dbName + ":" + setName);
}

pdb::PDBCatalogDatabasePtr pdb::PDBCatalog::getDatabase(const std::string &dbName) {
  return storage.get_no_throw<PDBCatalogDatabase>(dbName);
}

pdb::PDBCatalogNodePtr pdb::PDBCatalog::getNode(const std::string &nodeID) {
  return storage.get_no_throw<PDBCatalogNode>(nodeID);
}

pdb::PDBCatalogTypePtr pdb::PDBCatalog::getType(long id) {

  // select all the nodes we need
  auto rows = storage.select(columns(&PDBCatalogType::id, &PDBCatalogType::typeCategory, &PDBCatalogType::name, &PDBCatalogType::soBytes),
                             where(c(&PDBCatalogType::id) == id));

  // did we find the type
  if(rows.empty()){
    return nullptr;
  }

  // grab the row
  auto &r = rows.front();

  // make a return value
  auto ret = std::make_shared<PDBCatalogType>(std::get<0>(r), std::get<1>(r), std::get<2>(r), std::vector<char>());

  // move the so bytes
  ret->soBytes = std::move(std::get<3>(r));

  // return the value
  return ret;

}

pdb::PDBCatalogTypePtr pdb::PDBCatalog::getType(const std::string &name) {
  return storage.get_no_throw<PDBCatalogType>(name);
}

pdb::PDBCatalogTypePtr pdb::PDBCatalog::getTypeWithoutLibrary(long id) {

  // select all the nodes we need
  auto rows = storage.select(columns(&PDBCatalogType::id, &PDBCatalogType::typeCategory, &PDBCatalogType::name),
                             where(c(&PDBCatalogType::id) == id));

  // did we find the type
  if(rows.empty()){
    return nullptr;
  }

  // return the type
  auto &r = rows.front();
  return std::make_shared<pdb::PDBCatalogType>(std::get<0>(r),
                                               std::get<1>(r),
                                               std::get<2>(r),
                                               std::vector<char>());
}

pdb::PDBCatalogTypePtr pdb::PDBCatalog::getTypeWithoutLibrary(const std::string &name) {

  // select all the nodes we need
  auto rows = storage.select(columns(&PDBCatalogType::id, &PDBCatalogType::typeCategory, &PDBCatalogType::name),
                             where(c(&PDBCatalogType::name) == name));

  // did we find the type
  if(rows.empty()){
    return nullptr;
  }

  // return the type
  auto &r = rows.front();
  return std::make_shared<pdb::PDBCatalogType>(std::get<0>(r),
                                               std::get<1>(r),
                                               std::get<2>(r),
                                               std::vector<char>());
}

int32_t pdb::PDBCatalog::numRegisteredTypes() {

  // return the number of types in the catalog
  return storage.count<PDBCatalogType>();
}

std::vector<pdb::PDBCatalogNode> pdb::PDBCatalog::getNodesWithSet(const std::string &dbName, const std::string &setName) {

  // select all the nodes we need
  auto rows = storage.select(columns(&PDBCatalogSetOnNodes::node, &PDBCatalogNode::address, &PDBCatalogNode::port, &PDBCatalogNode::nodeType),
                             where(c(&PDBCatalogSetOnNodes::setIdentifier) == (dbName + ":" + setName) and c(&PDBCatalogSetOnNodes::node) == &PDBCatalogNode::nodeID));

  // create the return value
  std::vector<pdb::PDBCatalogNode> ret;

  // preallocate
  ret.reserve(rows.size());

  // store them
  for(auto &r : rows) {
    ret.emplace_back(PDBCatalogNode(std::get<0>(r), std::get<1>(r), std::get<2>(r), std::get<3>(r)));
  }

  return std::move(ret);
}

std::vector<pdb::PDBCatalogNode> pdb::PDBCatalog::getNodesWithDatabase(const std::string &dbName) {
  // select all the nodes we need
  auto rows = storage.select(columns(&PDBCatalogSetOnNodes::node, &PDBCatalogNode::address, &PDBCatalogNode::port, &PDBCatalogNode::nodeType, &PDBCatalogSet::setIdentifier),
                             where(c(&PDBCatalogSetOnNodes::setIdentifier) == &PDBCatalogSet::setIdentifier and
                                 c(&PDBCatalogSetOnNodes::node) == &PDBCatalogNode::nodeID and
                                 c(&PDBCatalogSet::database) == dbName));

  // create the return value
  std::vector<pdb::PDBCatalogNode> ret;

  // preallocate
  ret.reserve(rows.size());

  // store them
  for(auto &r : rows) {
    ret.emplace_back(PDBCatalogNode(std::get<0>(r), std::get<1>(r), std::get<2>(r), std::get<3>(r)));
  }

  return std::move(ret);
}

std::vector<pdb::PDBCatalogSet> pdb::PDBCatalog::getSetForDatabase(const std::string &dbName) {

  // select all the sets
  auto rows = storage.select(columns(&PDBCatalogSet::name, &PDBCatalogSet::database, &PDBCatalogSet::type),
                             where(c(&PDBCatalogSet::database) == dbName));

  // create a return value
  std::vector<pdb::PDBCatalogSet> ret;

  // preallocate
  ret.reserve(rows.size());

  // create the objects
  for(auto &r : rows) {
    ret.emplace_back(pdb::PDBCatalogSet(std::get<0>(r), std::get<1>(r), *std::get<2>(r)));
  }

  return std::move(ret);
}

std::vector<pdb::PDBCatalogNode> pdb::PDBCatalog::getNodes() {
  return std::move(storage.get_all<PDBCatalogNode>());
}

std::vector<pdb::PDBCatalogDatabase> pdb::PDBCatalog::getDatabases() {
  return std::move(storage.get_all<PDBCatalogDatabase>());
}

std::vector<pdb::PDBCatalogType> pdb::PDBCatalog::getTypesWithoutLibrary() {

  // select all the types
  auto rows = storage.select(columns(&PDBCatalogType::id, &PDBCatalogType::typeCategory, &PDBCatalogType::name));

  // create a return value
  std::vector<pdb::PDBCatalogType> ret;

  // preallocate
  ret.reserve(rows.size());

  // create the objects
  for(auto &r : rows) {
    ret.emplace_back(pdb::PDBCatalogType(std::get<0>(r), std::get<1>(r), std::get<2>(r), std::vector<char>()));
  }

  // move this to the return
  return std::move(ret);
}

std::string pdb::PDBCatalog::listNodesInCluster() {

  // create the output string
  std::string ret = "NODES : \n";

  // add all the nodes
  for(const auto &node : getNodes()) {
    ret.append("Node : " + node.nodeID + ", Type : " + node.nodeType + "\n");
  }

  // move the return value
  return std::move(ret);
}

std::string pdb::PDBCatalog::listRegisteredDatabases() {

  // create the output string
  std::string ret = "DATABASE SETS: \n";

  // go through each database
  for(const auto &db : getDatabases()) {

    // go through each set of this database
    for(const auto &set : getSetForDatabase(db.name)){
      ret.append("Name " + set.name + ", Database " +  set.database + ", Type : " + *set.type + "\n");
    }
  }

  // move the return value
  return std::move(ret);
}
std::string pdb::PDBCatalog::listRegisteredSetsForDatabase(const std::string &dbName) {

  // create the output string
  std::string ret = "SETS: \n";

  // go through each set of this database
  for(const auto &set : getSetForDatabase(dbName)){
    ret.append("Name " + set.name + ", Database " +  set.database + ", Type : " + *set.type + "\n");
  }

  // move the return value
  return std::move(ret);
}

std::string pdb::PDBCatalog::listUserDefinedTypes() {

  // create the output string
  std::string ret = "TYPES : \n";

  // add all the nodes
  for(const auto &type : getTypesWithoutLibrary()) {
    ret.append("ID : " + std::to_string(type.id) + ", Category : " + type.typeCategory + ", Name : " + type.name + "\n");
  }

  // move the return value
  return std::move(ret);
}

bool pdb::PDBCatalog::removeDatabase(const std::string &dbName, std::string &error) {

  // grab the database
  auto db = getDatabase(dbName);

  // if the database does not exist return false
  if(db == nullptr) {

    // set the error and return false
    error = "Database to be removed does not exist!";
    return false;
  }

  // remove each set from every node
  auto setIdentifiers = storage.select(columns(&PDBCatalogSet::setIdentifier), where(c(&PDBCatalogSet::database) == dbName));

  // go through each set and clean up the PDBCatalogSetOnNodes
  for(const auto &setIdentifier : setIdentifiers) {
    storage.remove_all<PDBCatalogSetOnNodes>(where(c(&PDBCatalogSetOnNodes::setIdentifier) == std::get<0>(setIdentifier)));
  }

  // remove all the sets
  storage.remove_all<PDBCatalogSet>(where(c(&PDBCatalogSet::database) == dbName));

  // remove the database
  storage.remove_all<PDBCatalogDatabase>(where(c(&PDBCatalogDatabase::name) == dbName));

  return true;
}

bool pdb::PDBCatalog::removeSet(const std::string &dbName, const std::string &setName, std::string &error) {

  // get the set
  auto set = getSet(dbName, setName);

  // create the set identifier (this is how it is created inside the catalog)
  std::string setIdentifier = dbName + ":" + setName;

  // if the set does not exist indicate an error
  if(set == nullptr) {
    error = "Set with the identifier " + setIdentifier + " does not exist\n";
    return false;
  }

  // remove the node associations
  storage.remove_all<PDBCatalogSetOnNodes>(where(c(&PDBCatalogSetOnNodes::setIdentifier) == setIdentifier));

  // remove the set
  storage.remove_all<PDBCatalogSet>(where(c(&PDBCatalogSet::setIdentifier) == setIdentifier));

  return false;
}

bool pdb::PDBCatalog::removeNodeFromSet(const std::string &nodeID, const std::string &dbName, const std::string &setName, std::string &error) {

  // check if the node actually exists
  if(!nodeExists(nodeID)) {

    // set the error
    error = "Could not find the node with the identifier : " + nodeID + "\n";

    // indicate an error
    return false;
  }

  // check if the set exists
  if(!setExists(dbName, setName)) {

    // set the error
    error = "Could not find the set with the identifier : " + dbName + ":" + setName;

    // indicate an error
    return false;
  }

  // create the set identifier
  std::string setIdentifier = dbName + ":" + setName;

  // remove the node associations
  storage.remove_all<PDBCatalogSetOnNodes>(where(c(&PDBCatalogSetOnNodes::setIdentifier) == setIdentifier and
      c(&PDBCatalogSetOnNodes::node) == nodeID));

  return true;
}

