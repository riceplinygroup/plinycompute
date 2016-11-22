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

#ifndef CATALOG_SERVER_TESTS_CC
#define CATALOG_SERVER_TESTS_CC

#include "PDBServer.h"
#include "CatalogServer.h"
#include "CatalogClient.h"
#include "PangeaStorageServer.h"
#include "ResourceManagerServer.h"
#include "DistributedStorageManagerServer.h"

int main (int argc, char * argv[]) {
    bool isMasterCatalog = 0;
    string masterIP = "localhost";
    int masterPort = 8108;
    string localIP = "localhost";
    int localPort = 8108;
    string nodeName = "compute1";
    string nodeType = "worker";

    if (argc < 8){
        cout << "bin/CatalogServerTests isMasterCatalog masterIP masterPort localIP localPort nodeName nodeType" << endl;
        cout << "where isMasterCatalog takes a value of 0 to indicate that this is not a node with the master catalog, 1 otherwise" << endl;
        cout << "       masterIP is the IP address of the node with the master catalog" << endl;
        cout << "       masterPort is the port of the node with the master catalog" << endl;
        cout << "       localIP is the IP address of this node" << endl;
        cout << "       localPort is the port of this node" << endl;
        cout << "       nodeName is an arbitrary name given to this node, e.g. compute_1" << endl;
        cout << "       nodeType is a string that can take two values 'master' or 'worker'" << endl;

        cout << " example: bin/CatalogServerTests 1 1111.2222.3333.4444 8108 222.222.222.2222 8108 compute_1 master" << endl;
    }else{
        isMasterCatalog = atoi(argv[1]);
        masterIP = argv[2];
        masterPort = atoi(argv[3]);
        localIP = argv[4];
        localPort = atoi(argv[5]);
        nodeName = argv[6];
        nodeType = argv[7];
    }
    cout << "is master catalog " << isMasterCatalog << endl;
        // This code replaces /bin/test15
        //allocates 24Mb
       makeObjectAllocatorBlock (1024 * 1024 * 24, true);


       std :: cout << "Starting up a catalog/storage server!!\n";
       pdb :: PDBLoggerPtr myLogger = make_shared <pdb :: PDBLogger> ("frontendLogFile.log");
       pdb :: PDBServer frontEnd (masterPort, 10, myLogger);
       frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir", isMasterCatalog, masterIP, masterPort);
       // for this test make the catalog a master server
       frontEnd.addFunctionality <pdb :: CatalogClient> (masterPort, masterIP, myLogger);

       ConfigurationPtr conf = make_shared < Configuration > ();
       pdb :: PDBLoggerPtr logger = make_shared < pdb :: PDBLogger> (conf->getLogFile());
       SharedMemPtr shm = make_shared< SharedMem > (conf->getShmSize(), logger);
       //pdb :: PDBWorkerQueuePtr workers = make_shared < pdb :: PDBWorkerQueue > (logger, conf->getMaxConnections());
       frontEnd.addFunctionality<pdb :: PangeaStorageServer> (shm, frontEnd.getWorkerQueue(), logger, conf);
       frontEnd.getFunctionality<pdb :: PangeaStorageServer>().startFlushConsumerThreads();

       string errMsg = " ";
       pdb :: Handle<pdb :: CatalogNodeMetadata> nodeData = pdb :: makeObject<pdb :: CatalogNodeMetadata>(String(localIP + ":" + std::to_string(localPort)), String(localIP), localPort, String(nodeName), String(nodeType), 1);

       // if it's not the master catalog node, use a client to remotely register this node metadata
       if ( isMasterCatalog == false ) {
           pdb :: CatalogClient catClient (masterPort, masterIP, make_shared <pdb :: PDBLogger> ("clientCatalogLog"));

           if (!catClient.registerNodeMetadata (nodeData, errMsg)) {
               std :: cout << "Not able to register node metadata: " + errMsg << std::endl;
               std :: cout << "Please change the parameters: nodeIP, port, nodeName, nodeType, status."<<std::endl;
           } else {
               std :: cout << "Node metadata successfully added.\n";
           }
       } else {
           // if it's the master catalog node, register the metadata in the local catalog
           if (frontEnd.getFunctionality<pdb::CatalogServer>().addNodeMetadata( nodeData, errMsg)){
               std :: cout << "Not able to register node metadata: " + errMsg << std::endl;
               std :: cout << "Please change the parameters: nodeIP, port, nodeName, nodeType, status."<<std::endl;
           } else {
               std :: cout << "Node metadata successfully added.\n";
           }
       }

       frontEnd.startServer (nullptr);

}

#endif

