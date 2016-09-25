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
#ifndef RESOURCE_MANAGER_SERVER_H
#define RESOURCE_MANAGER_SERVER_H


#include "ServerFunctionality.h"
#include "ResourceInfo.h"
#include "Handle.h"
#include "PDBVector.h"
#include "NodeDispatcherData.h"


namespace pdb {

class ResourceManagerServer : public ServerFunctionality {

public:

       //destructor
       ~ResourceManagerServer ();

       //constructor, initialize from catalog
       ResourceManagerServer (std :: string catalogIp);

       //constructor, initialize from a server list file 
       ResourceManagerServer (std :: string pathToServerList, int port);

       Handle<Vector<Handle<ResourceInfo>>> getAllResources ();

       Handle<Vector<Handle<NodeDispatcherData>>> getAllNodes();

       //from the serverFunctionality interface... register the resource manager handlers
       void registerHandlers (PDBServer &forMe) override;

       void clean () override;

protected:

      // initialize from a serverlist file, by invoking shell scripts to connect to each server, and got that server's resource list 
      // now all resources shall be initialized when the cluster is started
      void initialize (std :: string pathToServerList);

      void analyzeResources(std :: string resourceFileName);       

      void analyzeNodes(std :: string serverList);

private:

      Handle<Vector<Handle<ResourceInfo>>> resources;

      Handle<Vector<Handle<NodeDispatcherData>>> nodes;

      int port;
};


}



#endif
