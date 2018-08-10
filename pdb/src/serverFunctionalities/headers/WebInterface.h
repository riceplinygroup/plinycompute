//
// Created by dimitrije on 8/2/18.
//

#ifndef PDB_WEBSERVER_H
#define PDB_WEBSERVER_H

#include <ServerFunctionality.h>
#include <httplib.h>
#include <mustache.h>
#include <ClusterModel.h>
#include <SetModel.h>
#include <TypeModel.h>
#include <JobModel.h>

namespace pdb {

// this class runs the web server for pdb monitoring
class WebInterface  : public ServerFunctionality  {

public:

  /**
   * Create the web server on the specified ip address and port
   * @param ip - the ip address to access the web server
   * @param port - the port to access the web interface
   */
  WebInterface(const std::string &ip, int32_t port, bool pseudoClusterMode, PDBLoggerPtr &logger);

  virtual ~WebInterface();

  /**
   * We use this method as a way to register the http handles
   * @param forMe
   */
  void registerHandlers(PDBServer& forMe) override;

  /**
   * Returns a communicator to a particular node
   * @param port - the port to which we are connecting to
   * @param ip - the ip address
   * @return - the communicator
   */
  PDBCommunicatorPtr getCommunicatorToNode(int port, std::string &ip);

private:

  /**
   * The http server
   */
  httplib::Server server;

  /**
   * The ip address we are going to start the web interface
   */
  std::string ip;

  /**
   * The port of the web interface
   */
  int32_t port;

  /**
   * Is pseudo cluster mode
   */
  bool isPseudoCluster;

  /**
   * The model to get the cluster info data
   */
  ClusterModelPtr clusterModel;

  /**
   * The model to get the info about the sets
   */
  SetModelPtr setModel;

  /**
   * The model to get the info about types
   */
  TypeModelPtr typeModel;

  /**
   * The model to get the info about jobs
   */
  JobModelPtr jobModel;

  /**
   * This is used to synchronize the communicator
   * More specifically the part where we are creating them and connecting them to a remote node
   * with the connectToInternetServer method
   */
  pthread_mutex_t connection_mutex;

  /**
   * An instance of the PDBLogger set in the constructor
   */
  PDBLoggerPtr logger;
};

}


#endif //PDB_WEBSERVER_H
