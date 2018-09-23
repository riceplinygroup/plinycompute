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
#include <cstddef>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <vector>
#include <CatalogServer.h>

#include "BuiltInObjectTypeIDs.h"
#include "CatSyncResult.h"
#include "CatSyncRequest.h"
#include "CatCreateDatabaseRequest.h"
#include "CatCreateSetRequest.h"
#include "CatDeleteDatabaseRequest.h"
#include "CatDeleteSetRequest.h"
#include "CatRegisterType.h"
#include "CatSetObjectTypeRequest.h"
#include "CatSharedLibraryByNameRequest.h"
#include "CatGetType.h"
#include "CatTypeNameSearchResult.h"
#include "CatGetTypeResult.h"
#include "CatGetDatabaseRequest.h"
#include "CatGetDatabaseResult.h"
#include "CatGetSetRequest.h"
#include "CatGetSetResult.h"
#include "CatalogUserTypeMetadata.h"
#include "CatPrintCatalogRequest.h"
#include "CatPrintCatalogResult.h"
#include "CatalogServer.h"
#include "SimpleRequestHandler.h"
#include "VTableMap.h"

namespace pdb {

// constructor
CatalogServer::CatalogServer(const string &catalogDirectoryIn,
                             bool isManagerCatalogServer,
                             const string &managerIP,
                             int managerPort,
                             const string &nodeIP,
                             int nodePort) {

  // create a logger for the catalog server
  this->logger = make_shared<pdb::PDBLogger>("catalogServer.log");

  // set the ip address and port
  this->managerIP = managerIP;
  this->managerPort = managerPort;
  this->nodeIP = nodeIP;
  this->nodePort = nodePort;
  this->catalogDirectory = catalogDirectoryIn;
  this->isManagerCatalogServer = isManagerCatalogServer;
  this->tempPath = catalogDirectory + "/tmp_so_files";

  // create the directories for the catalog
  initDirectories();

  // if I am a worker than I need to sync with the manager catalog.
  if(!isManagerCatalogServer) {
    syncWithManager();
  }

  // creates instance of catalog
  PDBLoggerPtr catalogLogger = make_shared<PDBLogger>("catalogLogger");
  this->pdbCatalog = make_shared<PDBCatalog>(catalogDirectory + "/catalog.sqlite");

  // initialize the types
  initBuiltInTypes();

  // if am a manager register me
  if(isManagerCatalogServer) {
    registerManager();
  }

  PDB_COUT << "Catalog Server successfully initialized!\n";
}

void CatalogServer::initDirectories() const {

  // creates the parent folder for the catalog if location exists, only opens it.
  if (mkdir(catalogDirectory.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
    PDB_COUT << "Parent catalog folder " << catalogDirectory << " was not created, it already exists.\n";
  } else {
    PDB_COUT << "Parent catalog folder " << catalogDirectory << "  was created/opened.\n";
  }

  // creates temp folder for extracting so_files (only if folder doesn't exist)
  const int folder = mkdir(tempPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  if (folder == -1) {
    PDB_COUT << "Folder " << tempPath << " was not created, it already exists.\n";
  } else {
    PDB_COUT << "Folder " << tempPath << " for temporary shared libraries was created/opened.\n";
  }
}

void CatalogServer::initBuiltInTypes() {

  // go through each built in type
  for (const auto &type : VTableMap::getBuiltInTypes()) {

    // check if the type does not exist
    if(!pdbCatalog->typeExists(type.first)) {

      // register the type
      std::string error;
      if(!pdbCatalog->registerType(std::make_shared<pdb::PDBCatalogType>(type.second, "built-in", type.first, std::vector<char>()), error)) {

        // we failed log what happened
        PDB_COUT << error;
      }
    }
  }
}

// register handlers for processing requests to the Catalog Server
void CatalogServer::registerHandlers(PDBServer &forMe) {
  PDB_COUT << "Catalog Server registering handlers" << endl;

  // handles a request to register metadata of a new cluster Node in the catalog
  forMe.registerHandler(
      CatSyncRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatSyncRequest>>([&](Handle<CatSyncRequest> request, PDBCommunicatorPtr sendUsingMe) {

        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // grab the relevant node attributes
        auto nodeID = (std::string) request->nodeIP+ ":" + std::to_string(request->nodePort);
        auto address = (std::string) request->nodeIP;
        auto port = request->nodePort;
        auto type = (std::string) request->nodeType;

        // log what is happening
        PDB_COUT << "CatalogServer handler CatalogNodeMetadata_TYPEID adding the node with the address " << nodeID << "\n";

        // adds the node to the catalog
        bool res = true;
        std::string errMsg;

        // if we are a worker not we simply register the node and that is it.
        if (!isManagerCatalogServer) {

          // add the guy that made the request as a registered node
          res = pdbCatalog->registerNode(std::make_shared<pdb::PDBCatalogNode>(nodeID, address, port, type), errMsg);

          // create an allocation block to hold the response
          const UseTemporaryAllocationBlock tempBlock{1024};
          Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

          // sends result to requester
          res = sendUsingMe->sendObject(response, errMsg) && res;
          return make_pair(res, errMsg);
        }

        // to get the results of each broadcast
        map<string, pair<bool, string>> updateResults;

        // broadcast the update
        broadcastRequest(request, updateResults, errMsg);

        for (auto &item : updateResults) {

          // if we failed res would be set to false
          res = item.second.first && res;

          // log what is happening
          PDB_COUT << "Node IP: " << item.first + (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ") << item.second.second << "\n";
        }

        // add the guy that made the request as a registered node
        res = pdbCatalog->registerNode(std::make_shared<pdb::PDBCatalogNode>(nodeID, address, port, type), errMsg);

        // grab the catalog bytes
        auto catalogDump = pdbCatalog->serializeToBytes();

        // make an allocation block
        const UseTemporaryAllocationBlock tempBlock{catalogDump.size() + 1024};
        Handle<CatSyncResult> response = makeObject<CatSyncResult>(catalogDump);

        // sends result to requester
        res = sendUsingMe->sendObject(response, errMsg);
        return make_pair(res, errMsg);
      }));

  // handles a request to display the contents of the Catalog that have changed since a given timestamp
  forMe.registerHandler(CatPrintCatalogRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatPrintCatalogRequest>>([&](Handle<CatPrintCatalogRequest> request,
                                                                  PDBCommunicatorPtr sendUsingMe) {

        // print the catalog
        string categoryToPrint = request->category.c_str();
        string resultToPrint;

        if (categoryToPrint == "all") {

          resultToPrint.append(pdbCatalog->listNodesInCluster());
          resultToPrint.append(pdbCatalog->listRegisteredDatabases());
          resultToPrint.append(pdbCatalog->listUserDefinedTypes());

        } else {

          if (categoryToPrint == "databases"){
            resultToPrint.append(pdbCatalog->listRegisteredDatabases());
          }

          if (categoryToPrint == "sets"){
            string dbName = request->itemName;
            resultToPrint.append(pdbCatalog->listRegisteredSetsForDatabase(dbName));
          }

          if (categoryToPrint == "nodes"){
            resultToPrint.append(pdbCatalog->listNodesInCluster());
          }

          if (categoryToPrint == "udts"){
            resultToPrint.append(pdbCatalog->listUserDefinedTypes());
          }
        }

        // there is no error we just need this to send that back
        std::string errMsg;

        // make an allocation block 1 MB + the result we want to print.
        const UseTemporaryAllocationBlock tempBlock{1024 * 1024 + resultToPrint.size()};

        // copy the request we want to forward and set the text
        Handle<CatPrintCatalogResult> response = makeObject<CatPrintCatalogResult>(resultToPrint);
        response->output = resultToPrint;

        // sends result to requester
        bool res = sendUsingMe->sendObject(response, errMsg);
        return make_pair(res, errMsg);
      }));

  // handles a request to return the typeID of a Type given its name [DONE]
  forMe.registerHandler(
      CatGetType_TYPEID,
      make_shared<SimpleRequestHandler<CatGetType>>([&](Handle<CatGetType> request,
                                                               PDBCommunicatorPtr sendUsingMe) {

        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // log what is happening
        PDB_COUT << "Received CatGetType message \n";

        // ask the catalog server for the type ID given the type name
        auto type = this->pdbCatalog->getTypeWithoutLibrary(request->typeName);

        // make an allocation block for the response
        const UseTemporaryAllocationBlock tempBlock{1024};
        Handle<CatGetTypeResult> response;

        // did we find the type
        if(type != nullptr) {

          // log what is happening
          PDB_COUT << "Searched for object type name " + (std::string) request->typeName + " got " + std::to_string(type->id) << "\n";

          // make a response object
          response = makeObject<CatGetTypeResult>((int16_t) type->id, type->name, type->typeCategory);
        }
        else {

          // create an empty response since we haven't found it
          response = makeObject<CatGetTypeResult>();
        }

        // sends result to requester
        std::string errMsg;
        bool res = sendUsingMe->sendObject(response, errMsg);

        // return result
        return make_pair(res, errMsg);
      }));

  // handles a request to retrieve an .so library given a Type Name along with its metadata (stored as a serialized CatalogUserTypeMetadata object)
  forMe.registerHandler(
      CatSharedLibraryByNameRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatSharedLibraryByNameRequest>>([&](Handle<CatSharedLibraryByNameRequest> request,
                                                                           PDBCommunicatorPtr sendUsingMe) {

        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // grab the type
        auto type = pdbCatalog->getType(request->getTypeLibraryId());

        // log what is happening
        PDB_COUT << "Triggering Handler CatalogServer CatSharedLibraryByNameRequest for typeID=" << request->getTypeLibraryId() << "\n";

        // check if the type is in the local catalog!
        if(type != nullptr) {

          // create an allocation block that can fit .so library and allocate a response and init the response data
          const UseTemporaryAllocationBlock tempBlock{ type->soBytes.size() + 1024 * 1024 };
          Handle<CatalogUserTypeMetadata> response = makeObject<CatalogUserTypeMetadata>(type->id,
                                                                                         type->name,
                                                                                         type->typeCategory,
                                                                                         type->soBytes.data(),
                                                                                         type->soBytes.size());

          // low what is happening
          PDB_COUT << "Found the type with typeName " << type->name << " for typeId=" + std::to_string(type->id) << "\n";
          PDB_COUT << "The size of the .so library is : " + std::to_string(type->soBytes.size()) << "\n";

          // sends result to requester
          std::string errMsg;
          bool res = sendUsingMe->sendObject(response, errMsg);

          // return the result
          return make_pair(res, errMsg);
        }

        // ok the type is not in the local catalog, this is fine if we are not the manager!
        if(!this->isManagerCatalogServer) {

          // process the case where the type is not registered in this local
          PDB_COUT << "Connecting to the Remote Catalog Server via Catalog Client\n";
          PDB_COUT << "Invoking CatalogClient.getSharedLibraryByName(typeName) from CatalogServer b/c this is Local Catalog \n";

          // uses a dummyObjectFile since this is just making a remote call to
          // the Catalog Manager Server and what matters is the returned bytes.
          string dummyObjectFile = catalogDirectory + "/tmp_so_files/temp.so";

          // grab the type id from the request
          auto typeId = request->getTypeLibraryId();

          // create
          std::string errMsg;
          string bytes;
          string outTypeName;

          // retrieves from remote catalog the Shared Library bytes in "returnedBytes" and metadata in the "response" object
          auto client = CatalogClient(managerPort, managerIP, make_shared<pdb::PDBLogger>("clientCatalogToServerLog"));
          bool res = client.getSharedLibraryByTypeName(typeId, outTypeName, dummyObjectFile, bytes, errMsg);

          PDB_COUT << "Bytes returned from manager: "  << std::to_string(bytes.size()) << "\n";

          // if the library was successfully retrieved, go ahead and resolve
          // vtable fixing in the local catalog, given the library and
          // metadata retrieved from the remote Manager Catalog
          if (res) {

            // grab the bytes
            auto bytesPointer = bytes.data();

            // do the vtable fix and update res
            res = loadAndRegisterType(typeId, bytesPointer, bytes.size(), errMsg) && res;
          }

          if (!res) {

            // Ok we had an error either we could not grab the .so or we could not fix the vtable log that
            PDB_COUT << res  << "\n";

            // create a not found response
            const UseTemporaryAllocationBlock tempBlock{1024};
            Handle<CatalogUserTypeMetadata> notFoundResponse = makeObject<CatalogUserTypeMetadata>();

            // send it
            res = sendUsingMe->sendObject(notFoundResponse, errMsg);

            // finish this
            return make_pair(res, errMsg);

          }

          // now grab the type and it better be there
          type = this->pdbCatalog->getTypeWithoutLibrary(typeId);

          if (type != nullptr) {

            // if retrieval was successful prepare and send object to caller
            PDB_COUT << "Fixed the vtable successfully, sending the response !!!!" << endl;

            // create a block to put the response in..
            const UseTemporaryAllocationBlock tempBlock{1024 * 1024 + bytes.size()};

            // prepares Object to be sent to caller
            Handle<CatalogUserTypeMetadata> objectToBeSent = makeObject<CatalogUserTypeMetadata>(type->id, type->name, type->typeCategory, bytes.data(), bytes.size());

            // sends result to requester
            res = sendUsingMe->sendObject(objectToBeSent, errMsg);

            // finish this
            return make_pair(res, errMsg);
          }
        }

        // so we are the manager and we can not find the type, this means the type does not exist!
        // create an allocation block
        const UseTemporaryAllocationBlock tempBlock{1024};

        // set the error
        std::string errMsg = "CatSharedLibraryByNameRequest Handler Could not find the type with the name" + type->name + "\n";

        // log what is happening
        PDB_COUT << errMsg;

        // Creates an empty Object just to send the response to caller
        Handle<CatalogUserTypeMetadata> notFoundResponse = makeObject<CatalogUserTypeMetadata>();

        // send the object
        bool res = sendUsingMe->sendObject(notFoundResponse, errMsg);

        // finish this
        return make_pair(res, errMsg);
      }));

  // handles a request to retrieve the name of a Type, if it's not registered returns -1
  forMe.registerHandler(
      CatSetObjectTypeRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatSetObjectTypeRequest>>(
          [&](Handle<CatSetObjectTypeRequest> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // grab the type if it exists
            auto set = pdbCatalog->getSet(request->getDatabaseName(), request->getSetName());

            // create an allocation block for the response
            const UseTemporaryAllocationBlock tempBlock{1024};

            // allocate the response
            Handle<CatTypeNameSearchResult> response;

            // if the set has a type we grab the typename from the catalog and create a response
            if (set != nullptr && set->type != nullptr) {

              // log what is happening
              PDB_COUT << "Type for set with dbName=" << request->getDatabaseName() << " and setName=" << request->getSetName() << " is " << *set->type << "\n";

              // return the name
              response = makeObject<CatTypeNameSearchResult>(*set->type, true, "success");
            }
            else {

              // otherwise create a response indicating the failure to find the type
              response = makeObject<CatTypeNameSearchResult>("", false, "could not find requested type");
            }

            // sends result to requester
            std::string errMsg;
            bool res = sendUsingMe->sendObject(response, errMsg);

            // return
            return make_pair(res, errMsg);
          }));

  // handle a request to register metadata for a new Database in the catalog
  forMe.registerHandler(
      CatCreateDatabaseRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatCreateDatabaseRequest>>(
          [&](Handle<CatCreateDatabaseRequest> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // register the database in the catalog
            std::string errMsg;

            // store it locally
            bool res = pdbCatalog->registerDatabase(make_shared<PDBCatalogDatabase>(request->dbToCreate()), errMsg);

            // after we added the set to the local catalog, if this is the
            // manager catalog iterate over all nodes in the cluster and broadcast the
            // request to the distributed copies of the catalog
            if (isManagerCatalogServer) {

              // get the results of each broadcast
              map<string, pair<bool, string>> updateResults;

              // broadcast the update
              broadcastRequest(request, updateResults, errMsg);

              for (auto &item : updateResults) {

                // if we failed res would be set to false
                res = item.second.first && res;

                // log what is happening
                PDB_COUT << "Node IP: " << item.first + (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ") << item.second.second << "\n";
              }

            } else {

              // log what happened
              PDB_COUT << "This is not Manager Catalog Node, thus metadata was only registered locally!\n";
            }

            // create an allocation block to hold the response
            const UseTemporaryAllocationBlock tempBlock{1024};

            // create the response
            Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

            // sends result to requester
            res = sendUsingMe->sendObject(response, errMsg) && res;
            return make_pair(res, errMsg);
          }));

  // handle a request to register metadata for a new Set in the catalog
  forMe.registerHandler(
      CatCreateSetRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatCreateSetRequest>>([&](Handle<CatCreateSetRequest> request,
                                                                 PDBCommunicatorPtr sendUsingMe) {
        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // just a place to put the error message
        std::string errMsg;

        // if something fails we set this to false
        bool res = true;

        // grab the info about the set
        auto dbName = request->dbName;
        auto setName = request->setName;
        auto typeID = request->typeID;
        auto type = request->typeName;
        auto internalTypeName = VTableMap::getInternalTypeName(type);

        // ok this is a bit of an oddity in the system, essentially if create a set of a type like int, char or something similar.
        // the typeID is going to be 8191 or TYPE_NOT_RECOGNIZED so we have to register a type with the provided name and this type
        if(typeID == TYPE_NOT_RECOGNIZED && !pdbCatalog->typeExists(type)) {
          res = pdbCatalog->registerType(make_shared<PDBCatalogType>(typeID, "built-in", type, vector<char>()), errMsg);
        }

        // register the set with the catalog
        res = pdbCatalog->registerSet(make_shared<PDBCatalogSet>(setName, dbName, internalTypeName), errMsg) && res;

        // after we added the set to the local catalog, if this is the
        // manager catalog iterate over all nodes in the cluster and broadcast the
        // request to the distributed copies of the catalog
        if (isManagerCatalogServer) {

          // get the results of each broadcast
          map<string, pair<bool, string>> updateResults;

          // broadcast the update
          broadcastRequest(request, updateResults, errMsg);

          for (auto &item : updateResults) {

            // if we failed res would be set to false
            res = item.second.first && res;

            // log what is happening
            PDB_COUT << "Node IP: " << item.first + (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ") << item.second.second << "\n";
          }

        } else {

          // log what happened
          PDB_COUT << "This is not Manager Catalog Node, thus metadata was only registered locally!\n";
        }

        // create an allocation block to hold the response
        const UseTemporaryAllocationBlock tempBlock{1024};
        Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

        // sends result to requester
        res = sendUsingMe->sendObject(response, errMsg) && res;

        // return from handler
        return make_pair(res, errMsg);
      }));

  // handle a request to delete metadata for an existing Database in the catalog
  forMe.registerHandler(
      CatDeleteDatabaseRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatDeleteDatabaseRequest>>(
          [&](Handle<CatDeleteDatabaseRequest> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // invokes deleting database metadata from catalog
            std::string errMsg;
            bool res = pdbCatalog->removeDatabase(request->dbToDelete(), errMsg);

            // after it deleted the database in the local catalog, if this is the
            // manager catalog iterate over all nodes in the cluster and broadcast the
            // delete to the distributed copies of the catalog
            if (isManagerCatalogServer) {

              // get the results of each broadcast
              map<string, pair<bool, string>> updateResults;

              // broadcast the update
              broadcastRequest(request, updateResults, errMsg);

              for (auto &item : updateResults) {

                // if we failed res would be set to false
                res = item.second.first && res;

                // log what is happening
                PDB_COUT << "Node IP: " << item.first + (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ") << item.second.second << "\n";
              }

            } else {

              // log what happened
              PDB_COUT << "This is not Manager Catalog Node, thus metadata was only registered locally!\n";
            }

            // allocate a block for the response
            const UseTemporaryAllocationBlock tempBlock{1024};

            // create the response
            Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

            // sends result to requester
            res = sendUsingMe->sendObject(response, errMsg) && res;
            return make_pair(res, errMsg);
          }));

  // handle a request to delete metadata for an existing Set in the catalog
  forMe.registerHandler(
      CatDeleteSetRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatDeleteSetRequest>>([&](Handle<CatDeleteSetRequest> request,
                                                                 PDBCommunicatorPtr sendUsingMe) {

        // invokes deleting Set metadata from catalog
        std::string errMsg;
        auto set = request->whichSet();

        // invokes deleting Set metadata from catalog
        bool res = pdbCatalog->removeSet(set.first, set.second, errMsg);

        // after we deleted the set in the local catalog, if this is the
        // manager catalog iterate over all nodes in the cluster and broadcast the
        // delete to the distributed copies of the catalog
        if (isManagerCatalogServer) {

          // get the results of each broadcast
          map<string, pair<bool, string>> updateResults;

          // broadcast the update
          broadcastRequest(request, updateResults, errMsg);

          for (auto &item : updateResults) {

            // if we failed res would be set to false
            res = item.second.first && res;

            // log what is happening
            PDB_COUT << "Node IP: " << item.first + (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ") << item.second.second << "\n";
          }

        } else {

          // log what happened
          PDB_COUT << "This is not Manager Catalog Node, thus metadata was only registered locally!\n";
        }

        // create an allocation block for the response
        const UseTemporaryAllocationBlock tempBlock{1024};

        // create the response
        Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

        // sends result to requester
        res =  sendUsingMe->sendObject(response, errMsg) && res;

        // return
        return make_pair(res, errMsg);
      }));

  // handles a request to register a shared library
  forMe.registerHandler(
      CatRegisterType_TYPEID,
      make_shared<SimpleRequestHandler<CatRegisterType>>(
          [&](Handle<CatRegisterType> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // register the type
            std::string errMsg;

            // grab the library bytes
            const char *bytes = request->getLibraryBytes();

            // store it locally
            bool res = loadAndRegisterType(-1, bytes, request->getLibrarySize(), errMsg);

            // after we added the set to the local catalog, if this is the
            // manager catalog iterate over all nodes in the cluster and broadcast the
            // request to the distributed copies of the catalog
            if (isManagerCatalogServer) {

              // get the results of each broadcast
              map<string, pair<bool, string>> updateResults;

              // broadcast the update
              broadcastRequest(request, updateResults, errMsg);

              for (auto &item : updateResults) {

                // if we failed res would be set to false
                res = item.second.first && res;

                // log what is happening
                PDB_COUT << "Node IP: " << item.first + (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ") << item.second.second << "\n";
              }

            } else {

              // log what happened
              PDB_COUT << "This is not Manager Catalog Node, thus metadata was only registered locally!\n";
            }

            // allocate a block for the response
            const UseTemporaryAllocationBlock tempBlock{1024};

            // create the response object
            Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

            // sends result to requester
            res = sendUsingMe->sendObject(response, errMsg) && res;
            return make_pair(res, errMsg);
          }));

  // handles a request to register a shared library
  forMe.registerHandler(
      CatGetDatabaseRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatGetDatabaseRequest>>(
          [&](Handle<CatGetDatabaseRequest> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // grab the database
            auto db = pdbCatalog->getDatabase(request->databaseName);

            // check if the thing exists
            bool res = db != nullptr;

            // this is where we put the error
            std::string errMsg;

            // allocate a block for the response
            const UseTemporaryAllocationBlock tempBlock{1024};
            Handle<CatGetDatabaseResult> response;

            if(res) {

              // create the response object
              response = makeObject<CatGetDatabaseResult>((std::string) db->name,  db->createdOn);

            } else {

              // set the error
              errMsg = "Could not find the database with the name " + (std::string) request->databaseName;

              // create the response object in case of an error
              response = makeObject<CatGetDatabaseResult>("", -1);
            }

            // sends result to requester
            res = sendUsingMe->sendObject(response, errMsg) && res;
            return make_pair(res, errMsg);

          }));

  // handles a request to register a shared library
  forMe.registerHandler(
      CatGetSetRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatGetSetRequest>>(
          [&](Handle<CatGetSetRequest> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // grab the database
            auto set = pdbCatalog->getSet(request->databaseName, request->setName);

            // check if the thing exists
            bool res = set != nullptr;

            // this is where we put the error
            std::string errMsg;

            // allocate a block for the response
            const UseTemporaryAllocationBlock tempBlock{1024};
            Handle<CatGetSetResult> response;

            if(res) {

              // create the response object
              response = makeObject<CatGetSetResult>(set->database, set->name, *set->type, *set->type);

            } else {

              // set the error
              errMsg = "Could not find the set with the name " + (std::string) request->databaseName + " and " + (std::string) request->setName;

              // create the response object in case of an error
              response = makeObject<CatGetSetResult>();
            }

            // sends result to requester
            res = sendUsingMe->sendObject(response, errMsg) && res;
            return make_pair(res, errMsg);
          }));
}

void CatalogServer::registerManager() {

  // lock the catalog server
  lock_guard<mutex> guard(serverMutex);

  // make the node identifier
  string nodeIdentifier = managerIP + ":" + to_string(managerPort);

  // register the node
  std::string error;
  pdbCatalog->registerNode(make_shared<PDBCatalogNode>(nodeIdentifier, managerIP, managerPort, "manager"), error);

  // log the error
  PDB_COUT << error << "\n";
}

void CatalogServer::syncWithManager() {

  // allocate a block for the response
  const UseTemporaryAllocationBlock tempBlock{1024};
  Handle<CatSyncRequest> request = makeObject<CatSyncRequest>(nodeIP, nodePort, "worker");

  // sends the request to a node in the cluster
  auto ret = simpleRequest<CatSyncRequest, CatSyncResult, std::shared_ptr<std::vector<unsigned char>> >(
      this->logger, managerPort, managerIP, nullptr, 1024,
      [&](Handle<CatSyncResult> result) {

        // if the result is something else null we got a response
        if (result != nullptr) {

          // create the result vector
          auto out = std::make_shared<std::vector<unsigned char>>();

          // copy the result to the return value
          out->reserve(result->bytes->size());
          out->assign(result->bytes->c_ptr(), result->bytes->c_ptr() + result->bytes->size());

          return out;
        }

        return (std::shared_ptr<std::vector<unsigned char>>) nullptr;
      }, nodeIP, nodePort, "worker");

  ofstream file (catalogDirectory + "/catalog.sqlite", ios::trunc | ios::binary);

  // check if the file is open
  if(!file.is_open()) {

    // log what happened
    PDB_COUT << "Could not open out the catalog\n";

    // just end
    return;
  }

  // write out the received catalog
  file.write((char*) ret->data(), ret->size());

  // close the file
  file.close();
}

// adds metadata and bytes of a shared library in the catalog and returns its typeId
bool CatalogServer::loadAndRegisterType(int16_t typeIDFromManagerCatalog, const char *&soFile, size_t soFileSize, string &errMsg) {

  // open a temporary file with the right permissions
  string tempFile = catalogDirectory + "/tmp_so_files/temp.so";
  int fileDesc = open(tempFile.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);

  // write out the shared library
  auto sizeWritten = write(fileDesc, soFile, soFileSize);
  PDB_COUT << "For type id : " <<  typeIDFromManagerCatalog << " shared library of size : " << sizeWritten  << "written to disk.\n";

  // close the damn thing
  close(fileDesc);

  // load the shared library
  void *so_handle = nullptr;
  so_handle = dlopen(tempFile.c_str(), RTLD_LOCAL | RTLD_LAZY);

  // check if we have loaded it
  if (!so_handle) {

    // grab the error
    const char *dlsym_error = dlerror();
    errMsg = "Could not load the shared library for the typeID : " ;
    errMsg.append(std::to_string(typeIDFromManagerCatalog));
    errMsg.append(", error was : ");
    errMsg.append(dlsym_error);
    errMsg.append("\n");

    // log what happened
    PDB_COUT << errMsg;

    // close the damn thing
    dlclose(so_handle);
    return false;
  }

  // grab the function from the file
  typedef char *getObjectTypeNameFunc();
  auto *myFunc = (getObjectTypeNameFunc *) dlsym(so_handle, "getObjectTypeName");

  // log what is happening
  PDB_COUT << "Tried to extract the getObjectTypeName function from the .so file \n";

  // if the function is not defined or there was an error return
  char *symError;
  if ((symError = dlerror())) {

    // ok we failed create the error and log it
    errMsg = "Error, can't load function getObjectTypeName from the shared library. ";
    errMsg.append(symError);
    errMsg.append("\n");

    PDB_COUT << errMsg << endl;

    return false;
  }

  // log what is happening
  PDB_COUT << "Successfully extracted the getObjectTypeName function from the .so file" << endl;

  // now, get the type name so we can write the appropriate file
  string typeName(myFunc());
  dlclose(so_handle);

  // log what is happening
  PDB_COUT << "typeName returned from SO file: " << typeName << endl;

  // rename temporary file
  string newName = catalogDirectory + "/tmp_so_files/" + typeName + ".so";
  int result = rename(tempFile.c_str(), newName.c_str());
  if (result == 0) {
    PDB_COUT << "Successfully renaming file " << newName << endl;
  } else {
    PDB_COUT << "Renaming temp file failed " << newName << endl;
    return false;
  }

  // add the new type name, if we don't already have it
  if(pdbCatalog->getTypeWithoutLibrary(typeName) == nullptr) {

    // ok we don't have it log that!
    PDB_COUT << "Fixing vtable ptr for type " << typeName << " with metadata retrieved from remote Catalog Server." << endl;

    // we are gonna put the type code here
    int16_t typeCode;

    // if the type received is -1 this is a type not registered and we set the new typeID increasing by 1,
    // otherwise we use the typeID received from the Manager Catalog
    if (typeIDFromManagerCatalog == -1) {
      typeCode = (int16_t) (8192 + pdbCatalog->numRegisteredTypes());
    }
    else {
      typeCode = typeIDFromManagerCatalog;
    }

    // log what is happening
    PDB_COUT << "ID Assigned to type " << typeName << " is " << std::to_string(typeCode) << endl;


    // copy the type into a vector
    std::vector<char> soFileVector;
    soFileVector.assign(soFile, soFile + soFileSize);

    // register the type in the catalog
    std::string error;
    pdbCatalog->registerType(std::make_shared<pdb::PDBCatalogType>(typeCode, "user-defined", typeName, std::move(soFileVector)), error);

    // return the new type code
    return true;

  } else {

    // otherwise return the already existing type code
    return true;
  }
}

} // namespace