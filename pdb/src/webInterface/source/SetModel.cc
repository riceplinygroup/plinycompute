//
// Created by dimitrije on 8/5/18.
//

#include <mustache.h>
#include <UseTemporaryAllocationBlock.h>
#include <CatalogServer.h>
#include <WebInterface.h>
#include <StorageCollectStats.h>
#include "CollectSetStats.h"
#include "CollectSetStatsResponse.h"

SetModel::SetModel(bool isPseudoCluster, CatalogServer *catalogServer,
                   pdb::ResourceManagerServer *resourceManager,
                   pdb::WebInterface* webInterface) : isPseudoCluster(isPseudoCluster),
                                                      catalogServer(catalogServer),
                                                      resourceManager(resourceManager),
                                                      webInterface(webInterface) {}

mustache::data pdb::SetModel::getSets() {

  // all the stuff we create will be stored here
  const UseTemporaryAllocationBlock block(2 * 1024 * 1024);

  // the return value
  mustache::data ret = mustache::data();

  // grab the sets
  pdb::Handle<Vector<CatalogSetMetadata>> listOfSets = makeObject<Vector<CatalogSetMetadata>>();
  this->catalogServer->getSets(listOfSets);

  // we put all the node data here
  mustache::data setsData = mustache::data::type::list;

  // go through each set
  for(int i = 0; i < listOfSets->size(); ++i) {

    // the data of this node
    mustache::data setData;

    // set the data
    setData.set("set-name", (std::string)(*listOfSets)[i].getItemName());
    setData.set("set-id", (std::string)(*listOfSets)[i].getItemId());
    setData.set("database-name", (std::string)(*listOfSets)[i].getDBName());
    setData.set("database-id", (std::string)(*listOfSets)[i].getDBId());
    setData.set("type-name", (std::string)(*listOfSets)[i].getObjectTypeName());
    setData.set("type-id", (std::string)(*listOfSets)[i].getObjectTypeId());
    setData.set("is-last", i == listOfSets->size() - 1);

    //TODO there is not timestamp for the set
    setData.set("timestamp", std::to_string(1533453548));

    setsData.push_back(setData);
  }

  // set the return result
  ret.set("sets", setsData);
  ret.set("success", true);

  return ret;
}

mustache::data pdb::SetModel::getSet(std::string& dbName, std::string& setName) {

  // the return value
  mustache::data ret = mustache::data();

  // grab all the nodes
  auto nodes = getNodes();

  // we put all the node data here
  mustache::data partitions = mustache::data::type::list;

  // used to calculate the total size
  size_t totalSize = 0;

  int i = 0;
  for (auto &node : nodes) {

    // grab the address
    std::string address = node.first;
    int32_t port = node.second;

    // grab the partition TODO maybe do a bit of better error handling
    auto partition = getSetFromNode(i++, nodes.size(), dbName, setName, address, port);

    // sum the size up
    totalSize += std::stoi(partition["size"].string_value());

    // add the partition
    partitions.push_back(partition);
  }

  // set the values
  ret.set("partitions", partitions);
  ret.set("set-name", setName);
  ret.set("db-name", dbName);
  ret.set("set-size", std::to_string(totalSize));

  // grab the sets
  const UseTemporaryAllocationBlock block(2 * 1024 * 1024);
  pdb::Handle<Vector<CatalogSetMetadata>> mySet = makeObject<Vector<CatalogSetMetadata>>();
  this->catalogServer->getSet(mySet, dbName + "." + setName);

  if(mySet->size() == 1) {
    ret.set("success", true);
    ret.set("type-name", (std::string)(*mySet)[0].getObjectTypeName());
    return ret;
  }

  // set this as success
  ret.set("success", false);

  // return the value
  return ret;
}

mustache::data pdb::SetModel::getSetFromNode(int node,
                                             size_t numNodes,
                                             std::string &dbName,
                                             std::string &setName,
                                             std::string &address,
                                             int32_t port) {
  std::string errMsg;
  bool success;

  // the data of this node
  mustache::data ret;

  // set the data we know
  ret.set("node-id", std::to_string(node));
  ret.set("node-address", address);
  ret.set("node-port", std::to_string(port));
  ret.set("is-last", (numNodes - 1) == node);

  // all the stuff that we create in this method will be stored here
  const UseTemporaryAllocationBlock block(4 * 1024 * 1024);

  // create PDBCommunicator
  PDBCommunicatorPtr communicator = webInterface->getCommunicatorToNode(port, address);

  // send the request to grab the set data
  Handle<pdb::CollectSetStats> collectStatsMsg = makeObject<CollectSetStats>(setName, dbName);
  success = communicator->sendObject<pdb::CollectSetStats>(collectStatsMsg, errMsg);

  // we failed to request print the reason for the failure and signal an error
  if (!success) {

    // set all the values to 0
    ret.set("num-pages", "0");
    ret.set("size", "0");

    return ret;
  }

  // grab the result
  Handle<pdb::CollectSetStatsResponse> result = communicator->getNextObject<pdb::CollectSetStatsResponse>(success, errMsg);

  // we failed to receive the result, print out what happened and signal an error
  if (!result->isSuccess() || result == nullptr) {

    // set all the values to 0
    ret.set("num-pages", "0");
    ret.set("size", "0");

    return ret;
  }

  // grab the stats
  auto stats = result->getStats();

  // set the data;
  ret.set("num-pages", std::to_string(stats->getNumPages()));
  ret.set("size", std::to_string(stats->getNumPages() * stats->getPageSize()));

  return ret;
}


std::vector<std::pair<std::string, int32_t>> pdb::SetModel::getNodes() {

  // all the stuff we create will be stored here
  const UseTemporaryAllocationBlock block(2 * 1024 * 1024);

  // the info about the nodes
  std::vector<std::pair<std::string, int32_t>> nodeData;

  if(isPseudoCluster) {

    // grab the info about all the nodes in the pseudo cluster
    auto nodeObjects = this->resourceManager->getAllNodes();

    // go through the nodes
    for (int i = 0; i < nodeObjects->size(); i++) {

      // add the node to the list
      nodeData.emplace_back((std::string)(*(nodeObjects))[i]->getAddress(), (*(nodeObjects))[i]->getPort());
    }
  }
  else {

    // grab all the resources
    auto resourceObjects = this->resourceManager->getAllResources();

    // go through resources
    for (int i = 0; i < resourceObjects->size(); i++) {

      // add the node to the list
      nodeData.emplace_back((std::string)(*(resourceObjects))[i]->getAddress(), (*(resourceObjects))[i]->getPort());
    }
  }

  return nodeData;
}