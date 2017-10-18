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

    PDBClient::PDBClient(int portIn, std :: string addressIn, PDBLoggerPtr myLoggerIn, bool usePangaea) :
            port(portIn), address(addressIn), logger(myLoggerIn),
            usePangea(usePangaea), useQueryScheduler(useQueryScheduler) {

        catalogClient = pdb::CatalogClient(
            portIn,
            addressIn,
            make_shared<pdb::PDBLogger>("catalogClientLog"));

        dispatcherClient = DispatcherClient(
            portIn,
            addressIn,
            make_shared<pdb::PDBLogger>("dispatcherClientLog"));

        distributedStorageClient =  DistributedStorageManagerClient(
            portIn,
            addressIn,
            make_shared<pdb::PDBLogger>("distributedStorageClientLog"));

        queryClient = pdb :: QueryClient(
            portIn,
            addressIn,
            make_shared<pdb::PDBLogger>("queryClientLog"),
            useQueryScheduler);

    }

    PDBClient::~PDBClient(){
    }

    void PDBClient::registerHandlers (PDBServer &forMe) {
    }

    /****
     * Methods for invoking DistributedStorageManager-related operations
     */
    bool PDBClient::createDatabase(const std::string& databaseName, std::string& errMsg) {
        return distributedStorageClient.createDatabase(databaseName, errMsg);
    }

    bool PDBClient::createSet(const std::string& databaseName,
                              const std::string& setName,
                              const std::string& typeName,
                              std::string& errMsg,
                              size_t pageSize) {

        return distributedStorageClient.createSet(databaseName, setName,
                typeName, errMsg, pageSize);
    }

    bool PDBClient::createTempSet(const std::string& databaseName,
                                  const std::string& setName,
                                  const std::string& typeName,
                                  std::string& errMsg,
                                  size_t pageSize) {

        return distributedStorageClient.createTempSet(databaseName, setName,
                typeName, errMsg, pageSize);
    }

    bool PDBClient::removeDatabase(const std::string& databaseName,
                                   std::string & errMsg) {

        return distributedStorageClient.removeDatabase(databaseName,
                                                       errMsg);
    }

    bool PDBClient::removeSet(const std::string& databaseName,
                              const std::string& setName,
                              std::string & errMsg) {

        return distributedStorageClient.removeSet(databaseName, setName,
                errMsg);

    }

    bool PDBClient::removeTempSet(const std::string& databaseName, const std::string& setName,
                                                     const std :: string& typeName, std::string & errMsg) {

        return distributedStorageClient.removeTempSet(databaseName, setName, typeName,
                errMsg);

    }


    bool PDBClient::exportSet (
        const std :: string & databaseName, const std :: string & setName,
        const std :: string & outputFilePath, const std :: string & format,
        std :: string & errMsg) {

            return distributedStorageClient.exportSet(
                databaseName, setName,
                outputFilePath, format, errMsg);
    }


    bool PDBClient::clearSet(const std::string& databaseName, const std::string& setName, const std::string& typeName, std::string & errMsg) {

        return distributedStorageClient.clearSet(
            databaseName, setName,
            typeName, errMsg);

    }


   bool PDBClient::flushData ( std::string & errMsg ) {

       return distributedStorageClient.flushData(errMsg);

   }

    std::function<bool (Handle<SimpleRequestResult>)> PDBClient::generateResponseHandler(
            std::string description, std::string & errMsg) {

        return [&](Handle<SimpleRequestResult> result) {
            if (result != nullptr) {
                if (!result->getRes().first) {
                    errMsg = description + result->getRes ().second;
                    logger->error (description + ": " + result->getRes ().second);
                    return false;
                }
                return true;
            }
            errMsg = "Received nullptr as response";
            logger->error (description + ": received nullptr as response");
            return false;
        };
    }

    /****
     * Methods for invoking Catalog-related operations
     */

    bool PDBClient::registerNode(string &localIP,
                                   int localPort,
                                   string &nodeName,
                                   string &nodeType,
                                   int nodeStatus,
                                   std :: string &errMsg) {

        makeObjectAllocatorBlock (1024 * 1024 * 1, true);

        pdb :: Handle<pdb :: CatalogNodeMetadata> nodeInfo =
                pdb :: makeObject<pdb :: CatalogNodeMetadata>(
                        String(localIP + ":" + std::to_string(localPort)),
                        String(localIP),
                        localPort,
                        String(nodeName),
                        String(nodeType),
                        1);

        return catalogClient.registerNodeMetadata (nodeInfo, errMsg);
    }

    bool PDBClient::registerType (std :: string fileContainingSharedLib, std :: string &errMsg){
        return catalogClient.registerType(fileContainingSharedLib, errMsg);
    }

    bool PDBClient::printCatalogMetadata (pdb :: Handle<pdb :: CatalogPrintMetadata> itemToSearch, std :: string &errMsg) {
        return catalogClient.printCatalogMetadata (itemToSearch, errMsg) ;
    }

    bool PDBClient::listRegisteredDatabases (std :: string &errMsg) {
        ;
    }

    bool PDBClient::listRegisteredSetsForADatabase (std :: string databaseName, std :: string &errMsg) {
        ;
    }

    bool PDBClient::listNodesInCluster (std :: string &errMsg) {
        ;
    }

    /****
     * Methods for invoking Dispatcher-related operations
     */

    bool PDBClient::registerSet(std::pair<std::string, std::string> setAndDatabase, PartitionPolicy::Policy policy, std::string& errMsg) {
        return dispatcherClient.registerSet(setAndDatabase, policy, errMsg);
    }

    template <class DataType>
    bool PDBClient::sendData(std::pair<std::string, std::string> setAndDatabase, Handle<Vector<Handle<DataType>>> dataToSend, std::string& errMsg) {
        return dispatcherClient.sendData(setAndDatabase, dataToSend, errMsg);
    }

}

#endif
