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

#ifndef PDBCLIENT_CC
#define PDBCLIENT_CC

#include "PDBClient.h"

namespace pdb {

    PDBClient::PDBClient() {}

    PDBClient::PDBClient(int portIn, std::string addressIn,
                         bool usePangaeaIn, bool useQuerySchedulerIn)
        : port(portIn), address(addressIn),
          usePangea(usePangaeaIn), useQueryScheduler(useQuerySchedulerIn) {

      logger = make_shared<PDBLogger>("clientLog");

      catalogClient = std::make_shared<pdb::CatalogClient>(
          portIn, addressIn, make_shared<pdb::PDBLogger>("catalogClientLog"));

      dispatcherClient = DispatcherClient(
          portIn, addressIn, make_shared<pdb::PDBLogger>("dispatcherClientLog"));

      distributedStorageClient = DistributedStorageManagerClient(
          portIn, addressIn,
          make_shared<pdb::PDBLogger>("distributedStorageClientLog"));

      queryClient = pdb::QueryClient(portIn, addressIn,
                                     make_shared<pdb::PDBLogger>("queryClientLog"),
                                     useQuerySchedulerIn);
    }

    PDBClient::~PDBClient() {}

    void PDBClient::registerHandlers(PDBServer &forMe) {}

    /****
     * Methods for invoking DistributedStorageManager-related operations
     */
    void PDBClient::createDatabase(const std::string &databaseName) {

      string errMsg;

      bool result = distributedStorageClient.createDatabase(databaseName, errMsg);
      if (result==false) {
          std::cout << "Not able to create database: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Created database.\n";
      }
    }

    void PDBClient::createSet(const std::string &databaseName,
                              const std::string &setName,
                              const std::string &typeName) {

      string errMsg;

      bool result = distributedStorageClient.createSet(databaseName, setName, typeName,
                                                errMsg, DEFAULT_PAGE_SIZE);

      if (result==false) {
          std::cout << "Not able to create set: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Created set.\n";
      }
    }

    void PDBClient::createSet(const std::string &databaseName,
                              const std::string &setName,
                              const std::string &typeName,
                              size_t pageSize) {

      string errMsg;

      bool result = distributedStorageClient.createSet(databaseName, setName, typeName,
                                                errMsg, pageSize);
      if (result==false) {
          std::cout << "Not able to create set: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Created set.\n";
      }
    }

    void PDBClient::createTempSet(const std::string &databaseName,
                                  const std::string &setName,
                                  const std::string &typeName,
                                  size_t pageSize) {

      string errMsg;

      bool result = distributedStorageClient.createTempSet(databaseName, setName, typeName,
                                                    errMsg, pageSize);
      if (result==false) {
          std::cout << "Not able to create temp set: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Created temp set.\n";
      }
    }

    void PDBClient::removeDatabase(const std::string &databaseName) {

       string errMsg;

      bool result = distributedStorageClient.removeDatabase(databaseName, errMsg);
       if (result==false) {
           std::cout << "Not able to remove database: " + errMsg;
       } else {
           std::cout << "Database has been removed.\n";
       }
    }

    void PDBClient::removeSet(const std::string &databaseName,
                              const std::string &setName) {

      string errMsg;

      bool result = distributedStorageClient.removeSet(databaseName, setName, errMsg);
      if (result==false) {
          std::cout << "Not able to remove set: " + errMsg;
      } else {
          std::cout << "Set has been removed.\n";
      }
    }

    void PDBClient::removeTempSet(const std::string &databaseName,
                                  const std::string &setName,
                                  const std::string &typeName) {

      string errMsg;

      bool result = distributedStorageClient.removeTempSet(databaseName, setName, typeName,
                                                    errMsg);
      if (result==false) {
          std::cout << "Not able to remove Temp set: " + errMsg;
      } else {
          std::cout << "Temp set removed.\n";
      }
    }

    void PDBClient::exportSet(const std::string &databaseName,
                              const std::string &setName,
                              const std::string &outputFilePath,
                              const std::string &format) {

      string errMsg;

      bool result = distributedStorageClient.exportSet(databaseName, setName,
                                                outputFilePath, format, errMsg);
      if (result==false) {
          std::cout << "Not able to export set: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Set has been exported.\n";
      }
    }

    void PDBClient::clearSet(const std::string &databaseName,
                             const std::string &setName,
                             const std::string &typeName) {

      string errMsg;

      bool result = distributedStorageClient.clearSet(databaseName, setName, typeName,
                                               errMsg);
      if (result==false) {
          std::cout << "Not able to clear set: " + errMsg;
      } else {
          std::cout << "Set has been cleared.\n";
      }
    }

    void PDBClient::flushData() {

      string errMsg;

      bool result = distributedStorageClient.flushData(errMsg);
      if (result==false) {
          std::cout << "Not able to flush data: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Data has been flushed.\n";
      }
    }

    std::function<bool(Handle<SimpleRequestResult>)>
    PDBClient::generateResponseHandler(std::string description, std::string &errMsg) {

      return [&](Handle<SimpleRequestResult> result) {
        if (result != nullptr) {
          if (!result->getRes().first) {
            errMsg = description + result->getRes().second;
            logger->error(description + ": " + result->getRes().second);
            return false;
          }
          return true;
        }
        errMsg = "Received nullptr as response";
        logger->error(description + ": received nullptr as response");
        return false;
      };
    }

    /****
     * Methods for invoking Catalog-related operations
     */

    void PDBClient::registerNode(string &localIP, int localPort, string &nodeName,
                                 string &nodeType, int nodeStatus) {

      makeObjectAllocatorBlock(1024 * 1024 * 1, true);

      pdb::Handle<pdb::CatalogNodeMetadata> nodeInfo =
          pdb::makeObject<pdb::CatalogNodeMetadata>(
              String(localIP + ":" + std::to_string(localPort)), String(localIP),
              localPort, String(nodeName), String(nodeType), 1);

      string errMsg;

      bool result = catalogClient->registerNodeMetadata(nodeInfo, errMsg);
      if (result==false) {
          std::cout << "Not able to register node: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Node has been registered.\n";
      }
    }

    void PDBClient::registerType(std::string fileContainingSharedLib) {

      string errMsg;

      bool result = catalogClient->registerType(fileContainingSharedLib, errMsg);
      if (result==false) {
          std::cout << "Not able to register type: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Type has been registered.\n";
      }
    }

    void PDBClient::printCatalogMetadata(
        pdb::Handle<pdb::CatalogPrintMetadata> itemToSearch) {
      string errMsg;
      cout << catalogClient->printCatalogMetadata (
          itemToSearch,
          errMsg) ;
    }

    void PDBClient::listAllRegisteredMetadata() {
      string errMsg;
      cout <<  catalogClient->listAllRegisteredMetadata(errMsg);
    }

    void PDBClient::listRegisteredDatabases() {
      string errMsg;
      cout << catalogClient->listRegisteredDatabases(errMsg);
    }

    void PDBClient::listRegisteredSetsForADatabase(std::string databaseName) {
      string errMsg;
      cout << catalogClient->listRegisteredSetsForADatabase(databaseName, errMsg);
    }

    void PDBClient::listNodesInCluster() {
      string errMsg;
      cout << catalogClient->listNodesInCluster(errMsg);
    }

    void PDBClient::listUserDefinedTypes() {
      string errMsg;
      cout << catalogClient->listUserDefinedTypes(errMsg);
    }

    /****
     * Methods for invoking Dispatcher-related operations
     */

    void PDBClient::registerSet(std::pair<std::string, std::string> setAndDatabase,
                                PartitionPolicy::Policy policy) {

      string errMsg;

      bool result = dispatcherClient.registerSet(setAndDatabase, policy, errMsg);
      if (result==false) {
          std::cout << "Not able to register set: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Set has been registered.\n";
      }
    }

    /****
     * Methods for invoking Query-related operations
     */
    void PDBClient::deleteSet(std::string databaseName, std::string setName) {

      string errMsg;

      bool result = queryClient.deleteSet(databaseName, setName);

      if (result==false) {
          std::cout << "Not able to delete set: " + errMsg;
          exit(-1);
      } else {
          std::cout << "Set has been deleted.\n";
      }
    }
}

#endif
