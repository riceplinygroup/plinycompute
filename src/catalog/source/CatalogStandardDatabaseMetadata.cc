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
/*
 * CatalogStandardDatabaseMetadata.cc
 *
 *  Created on: Dec 8, 2016
 *      Author: carlos
 */

#include "CatalogStandardDatabaseMetadata.h"

using namespace std;

// This class serves to store information about the databases in a given
// instance of PDB
// and provide methods for maintaining their associated metadata.
// Clients of this class will access this information using a handler to the
// catalog.

CatalogStandardDatabaseMetadata::CatalogStandardDatabaseMetadata() {}

CatalogStandardDatabaseMetadata::CatalogStandardDatabaseMetadata(
    string dbIdIn, string dbNameIn, string userCreatorIn, string createdOnIn,
    string lastModifiedIn)
    : dbId(dbIdIn), dbName(dbNameIn), userCreator(userCreatorIn),
      createdOn(createdOnIn), lastModified(lastModifiedIn) {}

CatalogStandardDatabaseMetadata::CatalogStandardDatabaseMetadata(
    const CatalogStandardDatabaseMetadata &pdbDatabaseToCopy) {
  dbId = pdbDatabaseToCopy.dbId;
  dbName = pdbDatabaseToCopy.dbName;
  userCreator = pdbDatabaseToCopy.userCreator;
  createdOn = pdbDatabaseToCopy.createdOn;
  lastModified = pdbDatabaseToCopy.lastModified;
  listOfNodes = pdbDatabaseToCopy.listOfNodes;
  listOfSets = pdbDatabaseToCopy.listOfSets;
  listOfTypes = pdbDatabaseToCopy.listOfTypes;
  setsInDB = pdbDatabaseToCopy.setsInDB;
  nodesInDB = pdbDatabaseToCopy.nodesInDB;
}

void CatalogStandardDatabaseMetadata::setValues(string dbIdIn, string dbNameIn,
                                                string userCreatorIn,
                                                string createdOnIn,
                                                string lastModifiedIn) {
  dbId = dbIdIn;
  dbName = dbNameIn;
  userCreator = userCreatorIn;
  createdOn = createdOnIn;
  lastModified = lastModifiedIn;
}

CatalogStandardDatabaseMetadata::~CatalogStandardDatabaseMetadata() {}

void CatalogStandardDatabaseMetadata::addPermission(
    CatalogStandardPermissionsMetadata &permissionsIn) {
  listOfPermissions.push_back(permissionsIn);
}

void CatalogStandardDatabaseMetadata::addNode(string &nodeIn) {
  PDB_COUT << "Adding node " << nodeIn.c_str() << endl;
  listOfNodes.push_back(nodeIn);
}

void CatalogStandardDatabaseMetadata::addSet(string &setIn) {
  PDB_COUT << "Adding node " << setIn.c_str() << endl;
  listOfSets.push_back(setIn);
}

void CatalogStandardDatabaseMetadata::addSetToMap(string &setName,
                                                  string &nodeIP) {
  PDB_COUT << "first: " << setName.c_str()
           << " push_back node: " << nodeIP.c_str();
  setsInDB[setName].push_back(nodeIP);
}

void CatalogStandardDatabaseMetadata::addNodeToMap(string &nodeIP,
                                                   string &setName) {
  PDB_COUT << "first: " << nodeIP.c_str()
           << " push_back set: " << setName.c_str();
  nodesInDB[nodeIP].push_back(setName);
}

void CatalogStandardDatabaseMetadata::addType(string &typeIn) {
  listOfTypes.push_back(typeIn);
}

void CatalogStandardDatabaseMetadata::replaceListOfSets(
    vector<string> &newList) {
  listOfSets = newList;
}

void CatalogStandardDatabaseMetadata::replaceListOfNodes(
    vector<string> &newList) {
  listOfNodes = newList;
}

void CatalogStandardDatabaseMetadata::replaceMapOfSets(
    map<string, vector<string>> &newMap) {
  setsInDB = newMap;
}

void CatalogStandardDatabaseMetadata::replaceMapOfNodes(
    map<string, vector<string>> &newMap) {
  nodesInDB = newMap;
}

/**
 * Deletes a set from the listOfSets, along with the set.nodes map and the
 * nodes.set map
 * @param whichSet
 */
void CatalogStandardDatabaseMetadata::deleteSet(string setName) {
  deleteSetFromSetList(setName);
  deleteSetFromSetMap(setName);
  deleteSetFromNodeMap(setName);
}

void CatalogStandardDatabaseMetadata::removeNodeFromSet(string node,
                                                        string set) {
  deleteNodeFromSingleSet(node, set);
  deleteSetFromSingleNode(set, node);
}

void CatalogStandardDatabaseMetadata::deleteNodeFromMap(string &nodeIP,
                                                        string &setName) {
  // creates a temp vector
  vector<string> tempListOfNodes;

  for (int i = 0; i < getListOfNodes().size(); i++) {
    string itemValue = getListOfNodes()[i];
    if (itemValue != setName) {
      tempListOfNodes.push_back(itemValue);
    }
  }
  replaceListOfNodes(tempListOfNodes);
}

vector<string> CatalogStandardDatabaseMetadata::getListOfNodes() {
  return listOfNodes;
}

vector<string> CatalogStandardDatabaseMetadata::getListOfSets() {
  return listOfSets;
}

vector<string> CatalogStandardDatabaseMetadata::getListOfTypes() {
  return listOfTypes;
}

vector<CatalogStandardPermissionsMetadata>
CatalogStandardDatabaseMetadata::getListOfPermissions() {
  return listOfPermissions;
}

string CatalogStandardDatabaseMetadata::getItemId() { return dbId; }

string CatalogStandardDatabaseMetadata::getItemName() { return dbName; }

string CatalogStandardDatabaseMetadata::getItemKey() { return dbName; }

string CatalogStandardDatabaseMetadata::getUserCreator() { return userCreator; }

string CatalogStandardDatabaseMetadata::getCreatedOn() { return createdOn; }

string CatalogStandardDatabaseMetadata::getLastModified() {
  return lastModified;
}

void CatalogStandardDatabaseMetadata::setItemKey(string &itemKeyIn) {
  dbName = itemKeyIn;
}

void CatalogStandardDatabaseMetadata::setItemId(string &idIn) { dbId = idIn; }

void CatalogStandardDatabaseMetadata::setItemName(string &itemNameIn) {
  dbName = itemNameIn;
}

map<string, vector<string>> CatalogStandardDatabaseMetadata::getSetsInDB() {
  return setsInDB;
}

map<string, vector<string>> CatalogStandardDatabaseMetadata::getNodesInDB() {
  return nodesInDB;
}

string CatalogStandardDatabaseMetadata::printShort() {
  string output;
  string spaces("");
  output = "   \nDB ";
  output.append(getItemId().c_str()).append(":").append(getItemKey().c_str());
  //.append(" has (").append(to_string(getListOfSets().size())).append(") sets:
  //[ ");
  //            for (int i=0; i < getListOfSets().size(); i++){
  //                if (i>0) output.append(",
  //                ").append(spaces).append((*getListOfSets())[i].c_str());
  //                else output.append((*getListOfSets())[i].c_str());
  //            }
  //            output.append(" ]");
  //            for (auto &item : (*nodesInDB)){
  //                output.append("\n -Set:
  //                ").append(item.first.c_str()).append(" is stored in
  //                (").append(to_string(item.second.size())).append(") Nodes: [
  //                ");
  //                for (int i=0; i < item.second.size(); i++){
  //                    if (i>0) output.append(",
  //                    ").append(spaces).append(item.second[i].c_str());
  //                    else output.append(item.second[i].c_str());
  //                }
  //            }
  //            output.append(" ]");

  int i = 0;
  output.append("\n is stored in (")
      .append(to_string(nodesInDB.size()))
      .append(")nodes: [ ");
  for (auto &item : nodesInDB) {
    if (i > 0)
      output.append(", ").append(spaces).append(item.first);
    else
      output.append(item.first);
    i++;
  }

  output.append(" ]\n and has (")
      .append(to_string(setsInDB.size()))
      .append(")sets: [ ");
  i = 0;
  for (auto &item : setsInDB) {
    if (i > 0)
      output.append(", ").append(spaces).append(item.first.c_str());
    else
      output.append(item.first.c_str());
    i++;
  }
  output.append(" ]");

  for (auto &item : setsInDB) {
    output.append("\n  * Set: ")
        .append(item.first.c_str())
        .append(" is stored in (")
        .append(to_string(item.second.size()))
        .append(")nodes: [ ");
    for (int i = 0; i < item.second.size(); i++) {
      if (i > 0)
        output.append(", ").append(spaces).append(item.second[i].c_str());
      else
        output.append(item.second[i].c_str());
    }
    output.append(" ]");
  }

  return output;
}

void CatalogStandardDatabaseMetadata::deleteSetFromSetList(string &setName) {
  // creates a temp vector
  vector<string> tempListOfSets;
  for (int i = 0; i < getListOfSets().size(); i++) {
    string itemValue = getListOfSets()[i];
    if (itemValue != setName) {
      tempListOfSets.push_back(itemValue);
    }
  }
  replaceListOfSets(tempListOfSets);
}

void CatalogStandardDatabaseMetadata::deleteSetFromSetMap(string &setName) {
  // creates a temp vector
  map<string, vector<string>> tempSetsInDB;
  for (auto &a : getSetsInDB()) {
    if (a.first != setName) {
      tempSetsInDB[a.first] = a.second;
    }
  }
  replaceMapOfSets(tempSetsInDB);
}

void CatalogStandardDatabaseMetadata::deleteNodeFromSingleSet(string &node,
                                                              string &setName) {
  getSetsInDB().erase(node);
}

void CatalogStandardDatabaseMetadata::deleteSetFromSingleNode(string &setName,
                                                              string &node) {
  getNodesInDB().erase(setName);
}

void CatalogStandardDatabaseMetadata::deleteSetFromNodeMap(string &setName) {
  map<string, vector<string>> tempNodesInDB;
  for (const auto &setsInNode : nodesInDB) {
    auto node = setsInNode.first;
    auto sets = setsInNode.second;
    auto newSetsInNode = tempNodesInDB[node];
    for (int i = 0; i < sets.size(); i++) {
      if (sets[i] != setName) {
        newSetsInNode.push_back(sets[i]);
      }
    }
  }
  replaceMapOfNodes(tempNodesInDB);
}
