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

#ifndef PDBCLIENT_H
#define PDBCLIENT_H

#include "CatalogClient.h"
#include "DispatcherClient.h"
#include "PDBClient.h"
#include "QueryClient.h"
#include "ServerFunctionality.h"

#include "Handle.h"
#include "PDBVector.h"
#include "PDBObject.h"
#include "PartitionPolicy.h"
#include "CatalogClient.h"


// maybe to be removed
#include "SimpleRequest.h"
#include "DistributedStorageAddDatabase.h"
#include "DistributedStorageAddSet.h"
#include "DistributedStorageAddTempSet.h"
#include "DistributedStorageRemoveDatabase.h"
#include "DistributedStorageRemoveSet.h"
#include "DistributedStorageRemoveTempSet.h"
#include "DistributedStorageExportSet.h"
#include "DistributedStorageClearSet.h"
#include "DistributedStorageCleanup.h"

/**
 * Class for communicating and accessing different Server functionalities.
 */


namespace pdb {

    class PDBClient : public ServerFunctionality {

    public:

        /* Creates PDBClient
         *       portIn: the port number of the Master Catalog
         *    addressIn: the IP address of the Master Catalog
         *    usePangea: the path of the location of the catalog
         *   myLoggerIn: true if this is the Master Catalog Server
         */
        PDBClient(int portIn, std :: string addressIn,
                  PDBLoggerPtr myLoggerIn, bool usePangea,
                  bool useQueryScheduler);

        ~PDBClient();

        void registerHandlers (PDBServer &forMe); // no - op

        /****
         * Methods for invoking DistributedStorageManager-related operations
         */

        // TODO: Allow the user to specify what partitioning policy they want to use for this step in the future
        bool createDatabase(const std::string& databaseName, std::string& errMsg);

        bool createSet(const std::string& databaseName, const std::string& setName, const std::string& typeName,
                       std::string& errMsg, size_t pageSize=DEFAULT_PAGE_SIZE);

        // createTempSet added by Jia (only go through storage)
        bool createTempSet(const std::string& databaseName, const std::string& setName, const std::string& typeName, std::string& errMsg, size_t pageSize=DEFAULT_PAGE_SIZE);

        // templated createSet added by Jia
        template <class DataType>
        bool createSet (const std :: string& databaseName, const std :: string& setName, std :: string &errMsg, size_t pageSize = DEFAULT_PAGE_SIZE);

        // storage cleanup added by Jia
        bool flushData ( std :: string &errMsg );

        bool removeDatabase(const std::string& databaseName, std::string & errMsg);

        bool removeSet(const std::string& databaseName, const std::string& setName, std::string & errMsg);

        // clearSet added by Jia
        bool clearSet(const std::string& databaseName, const std::string& setName, const std::string & typeName, std::string & errMsg);

        // removeTempSet added by Jia (only go through storage)
        bool removeTempSet(const std::string& databaseName, const std::string& setName, const std::string& typeName, std::string & errMsg);

        // export set added by Jia
        // Note that the objects in set must be instances of ExportableObject
        bool exportSet (const std :: string & databaseName, const std :: string & setName, const std :: string & outputFilePath, const std :: string & format, std :: string & errMsg);

        /****
         * Methods for invoking Catalog-related operations
         */

        /* Sends a request to the Catalog Server to register a type with the catalog
         * returns true on success, false on fail */
        bool registerType (std :: string fileContainingSharedLib, std :: string &errMsg);

        /* Sends a request to the Catalog Server to register metadata about a node in the cluster */
        bool registerNode (string &localIP,
                                   int localPort,
                                   string &nodeName,
                                   string &nodeType,
                                   int nodeStatus,
                                   std :: string &errMsg);

        /* Sends a request to the Catalog Server to prints the content of the metadata stored in the catalog */
        bool printCatalogMetadata (pdb :: Handle<pdb :: CatalogPrintMetadata> itemToSearch, std :: string &errMsg);

        bool listRegisteredDatabases (std :: string &errMsg);

        bool listRegisteredSetsForADatabase (std :: string databaseName, std :: string &errMsg);

        bool listNodesInCluster (std :: string &errMsg);

        /****
         * Methods for invoking Dispatcher-related operations
         *
         * @param setAndDatabase
         * @return
         */
        bool registerSet(std::pair<std::string, std::string> setAndDatabase, PartitionPolicy::Policy policy, std::string& errMsg);

        /**
         *
         * @param setAndDatabase
         * @return
         */
        template <class DataType>
        bool sendData(std::pair<std::string, std::string> setAndDatabase, Handle<Vector<Handle<DataType>>> dataToSend, std::string& errMsg);

        template <class DataType>
        bool sendBytes(std::pair<std::string, std::string> setAndDatabase, char * bytes, size_t numBytes, std::string& errMsg);

    private:
        pdb :: CatalogClient catalogClient;
        pdb :: QueryClient queryClient;
        pdb :: DispatcherClient dispatcherClient;
        pdb :: DistributedStorageManagerClient distributedStorageClient;

        std::function<bool (Handle<SimpleRequestResult>)> generateResponseHandler(std::string description, std::string & errMsg);

        int port;
        std :: string address;
        bool usePangea;
        bool useQueryScheduler;
        PDBLoggerPtr logger;

    };

}

#include "PDBClientTemplate.cc"
#include "StorageClientTemplate.cc"
#include "DistributedStorageManagerClientTemplate.cc"
#include "DispatcherClientTemplate.cc"

#endif
