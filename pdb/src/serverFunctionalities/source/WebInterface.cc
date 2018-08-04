//
// Created by dimitrije on 8/2/18.
//

#include <GenericWork.h>
#include <WebInterface.h>
#include <ResourceManagerServer.h>

#include "WebInterface.h"

pdb::WebInterface::WebInterface(const std::string &ip, int32_t port, bool pseudoClusterMode) : ip(ip), port(port), isPseudoCluster(pseudoClusterMode) {}

void pdb::WebInterface::registerHandlers(pdb::PDBServer &forMe) {

  server.set_base_dir("web-interface");

  server.Get(R"(/api/cluster-info)", [&](const httplib::Request& req, httplib::Response& res) {

    // the template for the output
    mustache::mustache outputTemplate{"{\n"
                                      "  \"number-of-nodes\" : {{number-of-nodes}},\n"
                                      "  \"total-memory\" : {{total-memory}},\n"
                                      "  \"total-cores\" : {{total-cores}},\n"
                                      "  \"is-pseudo-cluster\" : {{is-pseudo-cluster}},\n"
                                      "  \"nodes\" : [{{#nodes}} { \"node-id\" : {{node-id}}, \"number-of-cores\": {{number-of-cores}}, \"memory-size\" : {{memory-size}}, \"address\" : \"{{address}}\", \"port\" : {{port}} }{{^is-last}}, {{/is-last}} {{/nodes}}]\n"
                                      "}"};

    // output the template
    res.set_content(outputTemplate.render(getClusterInfo()), "text/plain");
  });

  // grab a we worker
  auto webWorker = getWorker();

  // grab a worker
  PDBWorkerPtr myWorker = getWorker();

  // create some work for it
  PDBWorkPtr myWork = make_shared<GenericWork>([&](PDBBuzzerPtr callerBuzzer) {
    server.listen(ip.c_str(), port);
  });

  // execute the work
  myWorker->execute(myWork, myWork->getLinkedBuzzer());
}

pdb::WebInterface::~WebInterface() {

  // stop the web server
  server.stop();
}

mustache::data pdb::WebInterface::getClusterInfo() {

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
    auto nodeObjects = getFunctionality<ResourceManagerServer>().getAllNodes();

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
    auto resourceObjects = getFunctionality<ResourceManagerServer>().getAllResources();

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

  return ret;
}