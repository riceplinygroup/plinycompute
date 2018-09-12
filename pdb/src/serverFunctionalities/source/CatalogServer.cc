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

#include "BuiltInObjectTypeIDs.h"
#include "CatCreateDatabaseRequest.h"
#include "CatCreateSetRequest.h"
#include "CatDeleteDatabaseRequest.h"
#include "CatDeleteSetRequest.h"
#include "CatRegisterType.h"
#include "CatRemoveNodeFromSetRequest.h"
#include "CatSetObjectTypeRequest.h"
#include "CatSharedLibraryByNameRequest.h"
#include "CatAddNodeToSetRequest.h"
#include "CatSharedLibraryRequest.h"
#include "CatTypeNameSearch.h"
#include "CatTypeNameSearchResult.h"
#include "CatTypeSearchResult.h"
#include "CatalogDatabaseMetadata.h"
#include "CatalogPrintMetadata.h"
#include "CatalogServer.h"
#include "SimpleRequestHandler.h"

namespace pdb {

// constructor
CatalogServer::CatalogServer(const string &catalogDirectoryIn,
                             bool isManagerCatalogServer,
                             const string &managerIPValue,
                             int managerPortValue) {

  // create a logger for the catalog server
  this->catServerLogger = make_shared<pdb::PDBLogger>("catalogServer.log");

  // set the ip address and port
  this->managerIP = managerIPValue;
  this->managerPort = managerPortValue;
  this->catalogDirectory = catalogDirectoryIn;
  this->isManagerCatalogServer = isManagerCatalogServer;
  this->tempPath = catalogDirectory + "/tmp_so_files";

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

  // creates instance of catalog
  PDBLoggerPtr catalogLogger = make_shared<PDBLogger>("catalogLogger");
  this->pdbCatalog = make_shared<PDBCatalog>(catalogDirectory + "/catalog.sqlite");

  PDB_COUT << "Catalog Server successfully initialized!" << endl;
}

PDBCatalogPtr CatalogServer::getCatalog() { return pdbCatalog; }

// register handlers for processing requests to the Catalog Server
void CatalogServer::registerHandlers(PDBServer &forMe) {
  PDB_COUT << "Catalog Server registering handlers" << endl;

  // handles a request to register metadata of a new cluster Node in the catalog
  forMe.registerHandler(
      CatalogNodeMetadata_TYPEID,
      make_shared<SimpleRequestHandler<CatalogNodeMetadata>>([&](Handle<CatalogNodeMetadata> request,
                                                                 PDBCommunicatorPtr sendUsingMe) {

        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // grab the relevant node attributes
        auto nodeID = (std::string) request->getNodeIP() + ":" + std::to_string(request->getNodePort());
        auto address = (std::string) request->getNodeIP();
        auto port = request->getNodePort();
        auto type = (std::string) request->getNodeType();

        // log what is happening
        PDB_COUT << "CatalogServer handler CatalogNodeMetadata_TYPEID adding the node with the address " << nodeID << "\n";

        // adds the node metadata
        std::string errMsg;
        bool res = pdbCatalog->registerNode(std::make_shared<pdb::PDBCatalogNode>(nodeID, address, port, type) , errMsg);

        // make an allocation block
        const UseTemporaryAllocationBlock tempBlock{1024};
        Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

        // sends result to requester
        res = sendUsingMe->sendObject(response, errMsg);
        return make_pair(res, errMsg);
      }));

  // handles a request to register or update metadata for a Database in the catalog
  forMe.registerHandler(
      CatalogDatabaseMetadata_TYPEID,
      make_shared<SimpleRequestHandler<CatalogDatabaseMetadata>>(
          [&](Handle<CatalogDatabaseMetadata> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // log what is happening
            PDB_COUT << "CatalogServer handler CatalogDatabaseMetadata_TYPEID \n";

            // grab the database name
            std::string dbName = request->getItemName();

            // if database doesn't exist inserts metadata, otherwise only updates it
            std::string errMsg;
            bool res = pdbCatalog->registerDatabase(std::make_shared<PDBCatalogDatabase>(dbName), errMsg);

            // after it registered the Database metadata in the local catalog, if this is
            // the manager catalog iterate over all nodes in the cluster and broadcast the
            // insert to the distributed copies of the catalog
            if (isManagerCatalogServer) {

              // need these to get the result back
              map<string, pair<bool, string>> updateResults;
              broadcastCatalogUpdate(request, updateResults, errMsg);

              // check all the responses
              for (auto &item : updateResults) {

                // if we failed res would be set to false
                res = res || item.second.first;

                // log what is happening
                PDB_COUT << "Node IP: " << item.first << (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ") << item.second.second << "\n";
              }

            } else {
              PDB_COUT << "This is not Manager Catalog Node, thus metadata was only registered locally! \n";
            }

            // create a response
            const UseTemporaryAllocationBlock tempBlock{1024};
            Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

            // sends result to requester
            res = sendUsingMe->sendObject(response, errMsg);
            return make_pair(res, errMsg);
          }));

  // handles a request to register or update metadata of a Set in the catalog
  forMe.registerHandler(
      CatalogSetMetadata_TYPEID,
      make_shared<SimpleRequestHandler<CatalogSetMetadata>>([&](Handle<CatalogSetMetadata> request,
                                                                PDBCommunicatorPtr sendUsingMe) {

        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // log what is happening
        PDB_COUT << "CatalogServer handler CatalogSetMetadata_TYPEID \n";

        // grab the info about the set
        std::string setName = request->getItemName();
        std::string dbName = request->getDBName();
        int type = std::stoi(request->getObjectTypeId());

        // register the set locally
        std::string errMsg;
        bool res = pdbCatalog->registerSet(std::make_shared<pdb::PDBCatalogSet>(setName, dbName, type), errMsg);

        // after it registered the Set metadata in the local catalog, if this is the
        // manager catalog iterate over all nodes in the cluster and broadcast the
        // insert to the distributed copies of the catalog
        if (isManagerCatalogServer) {

          // get the results of each broadcast
          map<string, pair<bool, string>> updateResults;

          // broadcast the update
          broadcastCatalogUpdate(request, updateResults, errMsg);

          for (auto &item : updateResults) {

            // if we failed res would be set to false
            res = res || item.second.first;

            // log what is happening
            PDB_COUT << "Node IP: " << item.first + (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ") << item.second.second << "\n";
          }

        } else {

          // log what happened
          PDB_COUT << "This is not Manager Catalog Node, thus metadata was only registered locally!\n";
        }

        // make the response object
        const UseTemporaryAllocationBlock tempBlock{1024};
        Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

        // sends result to requester
        res = sendUsingMe->sendObject(response, errMsg);
        return make_pair(res, errMsg);
      }));

  // handles a request to display the contents of the Catalog that have changed since a given timestamp
  forMe.registerHandler(CatalogPrintMetadata_TYPEID,
      make_shared<SimpleRequestHandler<CatalogPrintMetadata>>([&](Handle<CatalogPrintMetadata> itemToPrint,
                                                                  PDBCommunicatorPtr sendUsingMe) {

        // log what is happening
        PDB_COUT << "Testing CatalogPrintMetadata handler with timeStamp " << itemToPrint->getTimeStamp() << "\n";

        // print the catalog
        string categoryToPrint = itemToPrint->getCategoryToPrint().c_str();
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
            string dbName = itemToPrint->getItemName();
            resultToPrint.append(pdbCatalog->listRegisteredSetsForDatabase(dbName));
          }

          if (categoryToPrint == "nodes"){
            resultToPrint.append(pdbCatalog->listNodesInCluster());
          }

          if (categoryToPrint == "udts"){
            resultToPrint.append(pdbCatalog->listUserDefinedTypes());
          }
        }

        // set the text
        itemToPrint->setMetadataToPrint(resultToPrint);

        // there is no error we just need this to send that back
        std::string errMsg;

        // sends result to requester
        bool res = sendUsingMe->sendObject(itemToPrint, errMsg);
        return make_pair(res, errMsg);
      }));

  // handles a request to return the typeID of a Type given its name [DONE]
  forMe.registerHandler(
      CatTypeNameSearch_TYPEID,
      make_shared<SimpleRequestHandler<CatTypeNameSearch>>([&](Handle<CatTypeNameSearch> request,
                                                               PDBCommunicatorPtr sendUsingMe) {

        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // log what is happening
        PDB_COUT << "Received CatTypeNameSearch message \n";

        // ask the catalog server for the type ID given the type name
        auto type = this->pdbCatalog->getTypeWithoutLibrary(request->getObjectTypeName());
        auto typeID = (type != nullptr ? this->pdbCatalog->getTypeWithoutLibrary(request->getObjectTypeName())->id : -1);

        // log what is happening
        PDB_COUT << "Searched for object type name " + request->getObjectTypeName() + " got " + std::to_string(typeID) << "\n";

        // make a response object
        const UseTemporaryAllocationBlock tempBlock{1024};
        Handle<CatTypeSearchResult> response = makeObject<CatTypeSearchResult>((int16_t) typeID);

        // sends result to requester
        std::string errMsg;
        bool res = sendUsingMe->sendObject(response, errMsg);

        // return result
        return make_pair(res, errMsg);
      }));

  // handles a request to retrieve an .so library given a Type Id
  forMe.registerHandler(
      CatSharedLibraryRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatSharedLibraryRequest>>([&](Handle<CatSharedLibraryRequest> request,
                                                                     PDBCommunicatorPtr sendUsingMe) {

        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // grab the type id
        int16_t typeID = request->getTypeID();

        // log what is happening
        PDB_COUT << "CatalogServer to handle CatSharedLibraryRequest to get shared library for typeID=" << std::to_string(typeID) << endl;

        // this is the error
        std::string errMsg;

        // the result of the sending
        bool res;

        // first, make sure we have this identifier
        if(!pdbCatalog->typeExists(typeID)){

          // set the error
          errMsg = "CatalogServer Error: Could not find the identifier it received";

          // create a response
          const UseTemporaryAllocationBlock tempBlock{1024};
          Handle<Vector<char>> response = makeObject<Vector<char>>();

          // sends the response in case of failure
          res = sendUsingMe->sendObject(response, errMsg);

          // log what happened
          PDB_COUT << errMsg << endl;

          // return the result
          return make_pair(res, errMsg);
        }

        // grab the type and extract the vector from it
        auto type = pdbCatalog->getType(typeID);

        // log what happened
        PDB_COUT << "The .so file is of size " + std::to_string(type->soBytes.size()) << "\n";

        // allocates memory for the .so library bytes
        const UseTemporaryAllocationBlock temp{ 1024 + type->soBytes.size() };
        Handle<Vector<char>> response = makeObject<Vector<char>>(type->soBytes.size(), type->soBytes.size());

        // copy the memory
        memmove(response->c_ptr(), type->soBytes.data(), type->soBytes.size());

        // sends result to requester
        res = sendUsingMe->sendObject(response, errMsg);

        // return the result
        return make_pair(res, errMsg);
      }));

  // handles a request to retrieve an .so library given a Type Name along with its metadata (stored as a serialized CatalogUserTypeMetadata object)
  forMe.registerHandler(
      CatSharedLibraryByNameRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatSharedLibraryByNameRequest>>([&](Handle<CatSharedLibraryByNameRequest> request,
                                                                           PDBCommunicatorPtr sendUsingMe) {

        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // grab the name of the library
        string typeName = request->getTypeLibraryName();

        // log what is happening
        PDB_COUT << "Triggering Handler CatalogServer CatSharedLibraryByNameRequest for typeName=" << typeName << "\n";

        // grab the type
        auto type = pdbCatalog->getType(typeName);

        // check if the type is in the local catalog!
        if(type != nullptr) {

          // create an allocation block that can fit .so library and allocate a response
          const UseTemporaryAllocationBlock tempBlock{ type->soBytes.size() + 1024 * 1024 };
          Handle<CatalogUserTypeMetadata> response = makeObject<CatalogUserTypeMetadata>();

          // low what is happening
          PDB_COUT << "Found the type with typeName " << type->name << " for typeId=" + std::to_string(type->id) << "\n";
          PDB_COUT << "The size of the .so library is : " + std::to_string(type->soBytes.size()) << "\n";

          // set the response data
          response->itemId = std::to_string(type->id);
          response->objectID = std::to_string(type->id);
          response->objectType = type->typeCategory;
          response->objectName = type->name;

          // copies the bytes of the Shared Library to the object to be sent back to the caller
          response->setLibraryBytes(type->soBytes.data());

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

          // retrieves from remote catalog the Shared Library bytes in "returnedBytes" and metadata in the "response" object
          auto client = CatalogClient(managerPort, managerIP, make_shared<pdb::PDBLogger>("clientCatalogToServerLog"));
          bool res = client.getSharedLibraryByTypeName(typeId, typeName, dummyObjectFile, bytes, errMsg);

          PDB_COUT << "Bytes returned from manager: "  << std::to_string(bytes.size()) << "\n";

          // if the library was successfully retrieved, go ahead and resolve
          // vtable fixing in the local catalog, given the library and
          // metadata retrieved from the remote Manager Catalog
          if (res) {

            // grab the bytes
            auto bytesPointer = bytes.data();

            // do the vtable fix and update res
            res = res && loadAndRegisterType(typeId, bytesPointer, bytes.size(), errMsg);
          }

          if (!res) {

            // Ok we had an error either we could not grab the .so or we could not fix the vtable log that
            PDB_COUT << res  << "\n";

            // create a not found response
            const UseTemporaryAllocationBlock tempBlock{1024};
            Handle<CatalogUserTypeMetadata> notFoundResponse = makeObject<CatalogUserTypeMetadata>();
            String newItemID("-1");
            notFoundResponse->setObjectId(newItemID);

            // send it
            res = sendUsingMe->sendObject(notFoundResponse, errMsg);

            // finish this
            return make_pair(res, errMsg);

          } else {

            // if retrieval was successful prepare and send object to caller
            PDB_COUT << "Fixed the vtable successfully, sending the response !!!!" << endl;

            // create a block to put the response in..
            const UseTemporaryAllocationBlock tempBlock{1024 * 1024 + bytes.size()};

            // prepares Object to be sent to caller
            Handle<CatalogUserTypeMetadata> objectToBeSent = makeObject<CatalogUserTypeMetadata>();

            // set the response parameters
            auto objectIDString = std::to_string(typeId);
            String newItemID(objectIDString);
            objectToBeSent->setObjectId(newItemID);
            objectToBeSent->setLibraryBytes(bytes.data());
            String newTypeName(typeName);
            objectToBeSent->setItemName(newTypeName);
            objectToBeSent->setItemKey(newTypeName);

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
        std::string errMsg = "CatSharedLibraryByNameRequest Handler Could not find the type with the name" + typeName + "\n";

        // log what is happening
        PDB_COUT << errMsg;

        // Creates an empty Object just to send the response to caller
        Handle<CatalogUserTypeMetadata> notFoundResponse = makeObject<CatalogUserTypeMetadata>();
        notFoundResponse->objectID = "-1";
        notFoundResponse->itemId = "-1";

        // send the object
        bool res = sendUsingMe->sendObject(notFoundResponse, errMsg);

        // finish this
        return make_pair(res, errMsg);
      }));

  // handles a request to retrieve the TypeId of a Type, if it's not registered returns -1
  forMe.registerHandler(
      CatSetObjectTypeRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatSetObjectTypeRequest>>(
          [&](Handle<CatSetObjectTypeRequest> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // grab the type if it exists
            auto set = pdbCatalog->getSet(request->getDatabaseName(), request->getSetName());

            // ask the catalog server for the type ID and then the name of the type
            auto typeID = set != nullptr ? *set->type : -1;

            // log what is happening
            PDB_COUT << "Type ID for set with dbName=" << request->getDatabaseName() << " and setName=" << request->getSetName() << " is " << std::to_string(typeID) << "\n";

            // create an allocation block for the response
            const UseTemporaryAllocationBlock tempBlock{1024};

            // allocate the response
            Handle<CatTypeNameSearchResult> response;

            // if the set has a type we grab the typename from the catalog and create a respnse
            if (typeID >= 0) {
              response = makeObject<CatTypeNameSearchResult>(pdbCatalog->getTypeWithoutLibrary(typeID)->name, true, "success");
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
            bool res = pdbCatalog->registerDatabase(std::make_shared<pdb::PDBCatalogDatabase>(request->dbToCreate()), errMsg);

            // create an allocation block to hold the response
            const UseTemporaryAllocationBlock tempBlock{1024};

            // create the response
            Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

            // sends result to requester
            res = sendUsingMe->sendObject(response, errMsg);
            return make_pair(res, errMsg);
          }));

  // handle a request to register metadata for a new Set in the catalog
  forMe.registerHandler(
      CatCreateSetRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatCreateSetRequest>>([&](Handle<CatCreateSetRequest> request,
                                                                 PDBCommunicatorPtr sendUsingMe) {
        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // grab the info about the set
        auto dbName = request->whichSet().first;
        auto setName = request->whichSet().second;
        auto type = request->whichType();

        // register the set with the catalog
        std::string errMsg;
        bool res =  pdbCatalog->registerSet(std::make_shared<pdb::PDBCatalogSet>(setName, dbName, type), errMsg);

        // create an allocation block to hold the response
        const UseTemporaryAllocationBlock tempBlock{1024};
        Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

        // sends result to requester
        res = sendUsingMe->sendObject(response, errMsg);

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
              broadcastCatalogDelete(request, updateResults, errMsg);

              for (auto &item : updateResults) {

                // if we failed res would be set to false
                res = res || item.second.first;

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
            res = sendUsingMe->sendObject(response, errMsg);
            return make_pair(res, errMsg);
          }));

  // handle a request to delete metadata for an existing Set in the catalog
  forMe.registerHandler(
      CatDeleteSetRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatDeleteSetRequest>>([&](Handle<CatDeleteSetRequest> request,
                                                                 PDBCommunicatorPtr sendUsingMe) {

        // lock the catalog server
        std::lock_guard<std::mutex> guard(serverMutex);

        // grab the set
        auto set = request->whichSet();

        // invokes deleting Set metadata from catalog
        std::string errMsg;
        bool res = pdbCatalog->removeSet(set.first, set.second, errMsg);

        // after it has deleted the Set metadata in the local catalog, if this is the
        // manager catalog iterate over all nodes in the cluster and broadcast the
        // delete to the distributed copies of the catalog
        if (isManagerCatalogServer) {

          // get the results of each broadcast
          map<string, pair<bool, string>> updateResults;

          // broadcast the update
          broadcastCatalogDelete(request, updateResults, errMsg);

          for (auto &item : updateResults) {

            // if we failed res would be set to false
            res = res || item.second.first;

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
        res = sendUsingMe->sendObject(response, errMsg);

        // return
        return make_pair(res, errMsg);
      }));

  // handle a request to add information of a set to an existing Database
  // this is invoked when the Distributed Storage Manager creates a set
  // in a given node for an existing Database
  forMe.registerHandler(
      CatAddNodeToSetRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatAddNodeToSetRequest>>(
          [&](Handle<CatAddNodeToSetRequest> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // invokes add Node information to a Set in a given Database
            std::string errMsg;
            bool res = pdbCatalog->addNodeToSet(request->nodeToAdd(), request->whichDB(), request->whichSet(), errMsg);

            // after it has added the node to the Set in the local catalog, if this is the
            // manager catalog iterate over all nodes in the cluster and broadcast the
            // request to the distributed copies of the catalog
            if (isManagerCatalogServer) {

              // get the results of each broadcast
              map<string, pair<bool, string>> updateResults;

              // broadcast the update
              broadcastCatalogUpdate<CatAddNodeToSetRequest>(request, updateResults, errMsg);

              for (auto &item : updateResults) {

                // if we failed res would be set to false
                res = res || item.second.first;

                // log what is happening
                PDB_COUT << "Node IP: " << item.first + (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ") << item.second.second << "\n";
              }

            } else {

              // log what happened
              PDB_COUT << "This is not Manager Catalog Node, thus metadata was only registered locally!\n";
            }

            // create the allocation block
            const UseTemporaryAllocationBlock tempBlock{1024};

            // create the response
            Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

            // sends result to requester
            res = sendUsingMe->sendObject(response, errMsg);
            return make_pair(res, errMsg);
          }));

  // handle a request to remove information of a set to an existing Database
  // this is invoked when the Distributed Storage Manager deletes a set
  // in a given node for an existing Database
  forMe.registerHandler(
      CatRemoveNodeFromSetRequest_TYPEID,
      make_shared<SimpleRequestHandler<CatRemoveNodeFromSetRequest>>(
          [&](Handle<CatRemoveNodeFromSetRequest> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // invokes remove node information from a Set in an existing database
            std::string errMsg;
            bool res = pdbCatalog->removeNodeFromSet(request->nodeToRemove(), request->whichDB(), request->whichSet(), errMsg);

            // after it registered the Set metadata in the local catalog, if this is the
            // manager catalog iterate over all nodes in the cluster and broadcast the
            // insert to the distributed copies of the catalog
            if (isManagerCatalogServer) {

              // get the results of each broadcast
              map<string, pair<bool, string>> updateResults;

              // broadcast the update
              broadcastCatalogUpdate(request, updateResults, errMsg);

              for (auto &item : updateResults) {

                // if we failed res would be set to false
                res = res || item.second.first;

                // log what is happening
                PDB_COUT << "Node IP: " << item.first + (item.second.first ? " updated correctly!" : " couldn't be updated due to error: ") << item.second.second << "\n";
              }

            } else {

              // log what happened
              PDB_COUT << "This is not Manager Catalog Node, thus metadata was only registered locally!\n";
            }

            // create the allocation block
            const UseTemporaryAllocationBlock tempBlock{1024};

            // create a response
            Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

            // sends result to requester
            res = sendUsingMe->sendObject(response, errMsg);
            return make_pair(res, errMsg);
          }));

  // handles a request to register a shared library
  forMe.registerHandler(
      CatRegisterType_TYPEID,
      make_shared<SimpleRequestHandler<CatRegisterType>>(
          [&](Handle<CatRegisterType> request, PDBCommunicatorPtr sendUsingMe) {

            // lock the catalog server
            std::lock_guard<std::mutex> guard(serverMutex);

            // log what is happening
            PDB_COUT << "Got a CatRegisterType request" << std::endl;

            // get the next object... this holds the shared library file... it could be big, so be careful!!
            size_t objectSize = sendUsingMe->getSizeOfNextObject();

            // log what is happening
            PDB_COUT << "Got objectSize=" << objectSize << "\n";

            // allocate the buffer where we will put the .so file
            void *memory = malloc(objectSize + 2048);

            bool res;
            std::string errMsg;
            Handle<Vector<char>> myFile = sendUsingMe->getNextObject<Vector<char>>(memory, res, errMsg);

            // if we succeeded in receiving the data load the library
            if (res) {

              // grab the bytes
              const char* bytesPointer = myFile->c_ptr();

              // register the type
              res = (loadAndRegisterType(-1, bytesPointer, objectSize, errMsg) >= 0);
            }

            // free the memory we just used
            free(memory);

            // allocate a block for the response
            const UseTemporaryAllocationBlock tempBlock{1024};

            // create the response object
            Handle<SimpleRequestResult> response = makeObject<SimpleRequestResult>(res, errMsg);

            // sends result to requester
            res = sendUsingMe->sendObject(response, errMsg);
            return make_pair(res, errMsg);
          }));
}

// adds metadata and bytes of a shared library in the catalog and returns its typeId
int16_t CatalogServer::loadAndRegisterType(int16_t typeIDFromManagerCatalog, const char* &soFile, size_t soFileSize, string &errMsg) {

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
    return -1;
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

    return -1;
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
    return -1;
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
    return typeCode;

  } else {

    // otherwise return the already existing type code
    return (int16_t) pdbCatalog->getTypeWithoutLibrary(typeName)->id;
  }
}

// templated method for broadcasting a Catalog Update to nodes in the cluster
template <class Type>
bool CatalogServer::broadcastCatalogUpdate(Handle<Type> metadataToSend, map<string, pair<bool, string>> &broadcastResults, string &errMsg) {

  PDBLoggerPtr catalogLogger = make_shared<PDBLogger>("distCatalogLogger");

  for (auto &node : pdbCatalog->getNodes()) {

    string nodeIP = node.address;
    int nodePort = node.port;

    bool res = false;
    CatalogClient clusterCatalogClient = CatalogClient(nodePort, nodeIP, catalogLogger);

    // if this node is not a manager forward the message to it
    if (node.nodeType != "manager") {

      // sends the request to a node in the cluster
      res = clusterCatalogClient.registerGenericMetadata(metadataToSend, errMsg);

      // adds the result of the update
      broadcastResults.insert(make_pair(nodeIP, make_pair(res, errMsg)));
    }
  }

  return true;
}

// templated method for broadcasting a Catalog Deletion to nodes in the cluster
template <class Type>
bool CatalogServer::broadcastCatalogDelete(Handle<Type> metadataToSend,
                                           map<string, pair<bool, string>> &broadcastResults,
                                           string &errMsg) {

  // grab a logger
  PDBLoggerPtr catalogLogger = make_shared<PDBLogger>("distCatalogLogger");

  // go through each node
  for (auto &node : pdbCatalog->getNodes()) {

    // grab the address and the port of the node
    string nodeIP = node.address;
    int nodePort = node.port;

    // grab a catalog client to broadcast the change
    CatalogClient clusterCatalogClient = CatalogClient(nodePort, nodeIP, catalogLogger);

    // is this node the manager if so skip it
    if (node.nodeType != "manager") {

      // sends the request to a node in the cluster
      bool res = clusterCatalogClient.deleteGenericMetadata(metadataToSend, errMsg);

      // adds the result of the update
      broadcastResults.insert(make_pair(nodeIP, make_pair(res, errMsg)));
    }
  }

  return true;
}

/* Explicit instantiation to broadcast Catalog Updates for a Database, needed for linking */
template bool CatalogServer::broadcastCatalogUpdate(Handle<CatalogDatabaseMetadata> metadataToSend,
                                                    map<string, pair<bool, string>> &broadcastResults,
                                                    string &errMsg);

/* Explicit instantiation to broadcast Catalog Updates for a Set, needed for linking */
template bool CatalogServer::broadcastCatalogUpdate(Handle<CatalogSetMetadata> metadataToSend,
                                                    map<string, pair<bool, string>> &broadcastResults,
                                                    string &errMsg);

/* Explicit instantiation to broadcast Catalog Updates for a Adding Node to Set, needed for linking */
template bool CatalogServer::broadcastCatalogUpdate(Handle<CatAddNodeToSetRequest> metadataToSend,
                                                    map<string, pair<bool, string>> &broadcastResults,
                                                    string &errMsg);

/* Explicit instantiation to broadcast Catalog Updates for a Removing a Node from Set, needed for linking */
template bool CatalogServer::broadcastCatalogUpdate(Handle<CatRemoveNodeFromSetRequest> metadataToSend,
                                                    map<string, pair<bool, string>> &broadcastResults,
                                                    string &errMsg);

} // namespace
