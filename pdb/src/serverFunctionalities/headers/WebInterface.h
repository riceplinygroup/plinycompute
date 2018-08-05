//
// Created by dimitrije on 8/2/18.
//

#ifndef PDB_WEBSERVER_H
#define PDB_WEBSERVER_H

#include <ServerFunctionality.h>
#include <httplib.h>
#include <mustache.h>

namespace pdb {

// this class runs the web server for pdb monitoring
class WebInterface  : public ServerFunctionality  {

public:

  /**
   * Create the web server on the specified ip address and port
   * @param ip - the ip address to access the web server
   * @param port - the port to access the web interface
   */
  WebInterface(const std::string &ip, int32_t port, bool pseudoClusterMode);

  virtual ~WebInterface();

  /**
   * We use this method as a way to register the http handles
   * @param forMe
   */
  void registerHandlers(PDBServer& forMe) override;

private:


  mustache::data getClusterInfo();

  mustache::data getSetInfo();

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
};

}


#endif //PDB_WEBSERVER_H
