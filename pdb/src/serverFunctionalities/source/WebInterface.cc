//
// Created by dimitrije on 8/2/18.
//

#include <GenericWork.h>
#include <WebInterface.h>
#include <ResourceManagerServer.h>
#include <CatalogServer.h>

#include "WebInterface.h"

pdb::WebInterface::WebInterface(const std::string &ip,
                                int32_t port,
                                bool pseudoClusterMode,
                                PDBLoggerPtr &logger) : ip(ip), port(port), isPseudoCluster(pseudoClusterMode), logger(logger) {

  // lock the mutex
  pthread_mutex_init(&connection_mutex, nullptr);
}

void pdb::WebInterface::registerHandlers(pdb::PDBServer &forMe) {

  // initialize the cluster model
  clusterModel = std::make_shared<ClusterModel>(isPseudoCluster, &getFunctionality<pdb::ResourceManagerServer>());
  setModel = std::make_shared<SetModel>(isPseudoCluster, &getFunctionality<pdb::CatalogServer>(), &getFunctionality<pdb::ResourceManagerServer>(), this);
  typeModel = std::make_shared<TypeModel>(&getFunctionality<pdb::CatalogServer>());

  // set the base directory for the html stuff
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

    // grab the cluster info data
    auto clusterInfo = this->clusterModel->getClusterInfo();

    // output the template
    res.set_content(outputTemplate.render(clusterInfo), "text/plain");
    res.status = clusterInfo["success"].is_true() ? 200 : 400;
  });

  server.Get(R"(/api/sets)", [&](const httplib::Request& req, httplib::Response& res) {

    // the template for the output
    mustache::mustache outputTemplate{"[\n"
                                      " {{#sets}} { \n   \"set-name\" : \"{{set-name}}\",\n"
                                      "   \"set-id\" : \"{{set-id}}\",\n"
                                      "   \"database-name\" : \"{{database-name}}\",\n"
                                      "   \"database-id\" : \"{{database-name}}\",\n"
                                      "   \"type-name\" : \"{{type-name}}\",\n"
                                      "   \"type-id\" : \"{{type-name}}\",\n"
                                      "   \"created\" : {{timestamp}}\n"
                                      "  }{{^is-last}}, {{/is-last}}\n {{/sets}}"
                                      "]"};

    // grab the sets data
    auto sets = this->setModel->getSets();

    // output the template
    res.set_content(outputTemplate.render(sets), "text/plain");
    res.status = sets["success"].is_true() ? 200 : 400;
  });

  server.Get(R"(/api/set/([^\.]+)\.([^\.]+))", [&](const httplib::Request& req, httplib::Response& res) {

    // grab the set id
    std::string dbID = req.matches[1];
    std::string setID = req.matches[2];

    // the template for the output
    mustache::mustache outputTemplate{"{\n"
                                      " \"set-name\" : \"{{set-name}}\",\n"
                                      " \"db-name\" : \"{{db-name}}\",\n"
                                      " \"type-name\" : \"{{type-name}}\",\n"
                                      " \"set-size\" : {{set-size}},\n"
                                      " \"partitions\" : [{{#partitions}} { \"node-id\" : \"{{node-id}}\", \"node-port\" : {{node-port}}, \"node-address\" : \"{{node-address}}\", \n"
                                      " \"num-pages\" : {{num-pages}}, \"size\" : {{size}} }{{^is-last}},{{/is-last}} {{/partitions}}]\n"
                                      "}"};

    // grab the cluster info data
    auto clusterInfo = this->setModel->getSet(dbID, setID);


    if(clusterInfo["success"].is_true()) {
      // output the template
      res.set_content(outputTemplate.render(clusterInfo), "text/plain");
    }
    else {

      res.set_content(R"({ "error" : "Could not find the requested set"})", "text/plain");
      res.status = 400;
    }
  });

  server.Get(R"(/api/types)", [&](const httplib::Request& req, httplib::Response& res) {

    // the template for the output
    mustache::mustache outputTemplate{"[\n"
                                      " {{#types}} { \n"
                                      "   \"type-name\" : \"{{type-name}}\",\n"
                                      "   \"type-id\" : \"{{type-id}}\",\n"
                                      "   \"registered\" : {{registered}}\n"
                                      "  }{{^is-last}}, {{/is-last}}\n{{/types}}"
                                      "]"};

    // grab the cluster info data
    auto types = this->typeModel->getTypes();

    // output the template
    res.set_content(outputTemplate.render(types), "text/plain");
    res.status = types["success"].is_true() ? 200 : 400;
  });

  server.Get(R"(/api/type/([^\.]+))", [&](const httplib::Request& req, httplib::Response& res) {

    std::string typeID = req.matches[1];

    // the template for the output
    mustache::mustache outputTemplate{"{\n"
                                      "  \"type-name\" : \"{{type-name}}\",\n"
                                      "  \"type-id\" : {{type-id}},\n"
                                      "  \"methods\" : [ {{#methods}} { \"method-name\": \"{{method-name}}\", \"method-symbol\" : \"{{method-symbol}}\", \"return-type\" : \"{{return-type}}\", \"return-type-size\" : {{return-type-size}}, \n"
                                      "  \"parameters\" : [ {{#parameters}}{ \"name\" : \"{{name}}\", \"size\" : {{size}} }{{^is-last}}, {{/is-last}}{{/parameters}} ] }{{^is-last}}, {{/is-last}} {{/methods}}],\n"
                                      "  \"attributes\" : [ {{#attributes}}{ \"type-name\" : \"{{type-name}}\", \"attribute-name\" : \"{{attribute-name}}\", \"size\" : {{size}}, \"offset\" : {{offset}} }{{^is-last}}, {{/is-last}} {{/attributes}}]\n"
                                      "}"};

    // grab the cluster info data
    auto types = this->typeModel->getType(typeID);

    // output the template if we succeeded
    if(types["success"].is_true()) {
      // output the template
      res.set_content(outputTemplate.render(types), "text/plain");
    }
    else {

      // output the error
      res.set_content(R"({ "error" : "Could not find the requested set"})", "text/plain");
      res.status = 400;
    }
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

PDBCommunicatorPtr pdb::WebInterface::getCommunicatorToNode(int port, std::string &ip) {

  // lock the connection mutex so no other thread tries to connect to a node
  pthread_mutex_lock(&connection_mutex);
  PDBCommunicatorPtr communicator = std::make_shared<PDBCommunicator>();

  // log what we are doing
  PDB_COUT << "Connecting to remote node connect to the remote node with address : " << ip  << ":" << port << std::endl;

  // try to connect to the node
  string errMsg;
  bool failure = communicator->connectToInternetServer(logger, port, ip, errMsg);

  // we are don connecting somebody else can do it now
  pthread_mutex_unlock(&connection_mutex);

  // if we succeeded return the communicator
  if (!failure) {
    return communicator;
  }

  // otherwise log the error message
  std::cout << errMsg << std::endl;

  // return a null pointer
  return nullptr;
}