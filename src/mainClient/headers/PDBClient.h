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
#include "DistributedStorageManagerClient.h"
#include "QueryClient.h"
#include "ServerFunctionality.h"

#include "Handle.h"
#include "PDBVector.h"
#include "PDBObject.h"
#include "PartitionPolicy.h"

#include "SimpleRequest.h"

/**
 * This class provides functionality so users can connect and access
 * different Server functionalities of PDB, such as:
 *
 *  Catalog services
 *  Dispatcher services
 *  Distributed Storage services
 *  Query services
 *
 */

namespace pdb {

    class PDBClient : public ServerFunctionality {

    public:

        /* Creates PDBClient
         *
         *            portIn: the port number of the PDB master server
         *         addressIn: the IP address of the PDB master server
         *        myLoggerIn: the logger
         *         usePangea: true if Pangea is used
         * useQueryScheduler: true if Query Scheduler is used
         *
         */
        PDBClient(
                int portIn,
                std :: string addressIn,
                PDBLoggerPtr myLoggerIn,
                bool usePangeaIn,
                bool useQuerySchedulerIn);

        PDBClient();

        ~PDBClient();

        void registerHandlers (PDBServer &forMe); // no - op

        /****
         * Gets a reference to the Catalog Client
         */
        pdb :: CatalogClient getCatalogClient();

        /****
         * Gets a reference to the Dispatcher Client
         */
        pdb :: DispatcherClient getDispatcherClient();

        /****
         * Gets a reference to the Distributed Storage Manager Client
         */
        pdb :: DistributedStorageManagerClient getDistributedStorageClient();

        /****
         * Gets a reference to the Query Client
         */
        pdb :: QueryClient getQueryClient();

        /****
         * Methods for invoking DistributedStorageManager-related operations
         */

        /* Creates a database */
        bool createDatabase(
                const std::string& databaseName,
                std::string& errMsg);

        /* Creates a set with a given type for an existing database, no page_size needed in arg. */
        bool createSet(
                const std::string& databaseName,
                const std::string& setName,
                const std::string& typeName,
                std::string& errMsg);

        /* Creates a set with a given type (using a template) for an existing database, no page_size needed in arg. */
        template <class DataType>
        bool createSet (
                const std :: string& databaseName,
                const std :: string& setName,
                std :: string &errMsg);

        /* Creates a set with a given type for an existing database */
        bool createSet(
                const std::string& databaseName,
                const std::string& setName,
                const std::string& typeName,
                std::string& errMsg,
                size_t pageSize=DEFAULT_PAGE_SIZE);

        /* Creates a temporary set with a given type for an existing database (only goes through storage) */
        bool createTempSet(
                const std::string& databaseName,
                const std::string& setName,
                const std::string& typeName,
                std::string& errMsg,
                size_t pageSize=DEFAULT_PAGE_SIZE);

        /* Flushes data currently in memory into disk. */
        bool flushData ( std :: string &errMsg );

        /* Removes a database from storage. */
        bool removeDatabase(
                const std::string& databaseName,
                std::string & errMsg);

        /* Removes a set for an existing database from storage. */
        bool removeSet(
                const std::string& databaseName,
                const std::string& setName,
                std::string & errMsg);

        /* Removes a set given a type from an existing database. */
        bool clearSet(
                const std::string& databaseName,
                const std::string& setName,
                const std::string & typeName,
                std::string & errMsg);

        /* Removes a temporary set given a type from an existing database (only goes through storage). */
        bool removeTempSet(
                const std::string& databaseName,
                const std::string& setName,
                const std::string& typeName,
                std::string & errMsg);

        /* Exports the content of a set from a database into a file. Note that the objects in
         * set must be instances of ExportableObject
         */
        bool exportSet (
                const std :: string & databaseName,
                const std :: string & setName,
                const std :: string & outputFilePath,
                const std :: string & format,
                std :: string & errMsg);

        /****
         * Methods for invoking Catalog-related operations
         */

        /* Sends a request to the Catalog Server to register a user-defined type defined
         * in a shared library. */
        bool registerType (
                std :: string fileContainingSharedLib,
                std :: string &errMsg);

        /* Sends a request to the Catalog Server to register metadata about a node in the cluster */
        bool registerNode (
                string &localIP,
                int localPort,
                string &nodeName,
                string &nodeType,
                int nodeStatus,
                std :: string &errMsg);

        /* Prints the content of the catalog. */
        bool printCatalogMetadata (
                pdb :: Handle<pdb :: CatalogPrintMetadata> itemToSearch,
                std :: string &errMsg);

        /* Lists the Databases registered in the catalog. */
        bool listRegisteredDatabases (std :: string &errMsg);

        /* Lists the Sets for a given database registered in the catalog. */
        bool listRegisteredSetsForADatabase (
                std :: string databaseName,
                std :: string &errMsg);

        /* Lists the Nodes registered in the catalog. */
        bool listNodesInCluster (std :: string &errMsg);

        /* Lists user-defined types registered in the catalog. */
        bool listUserDefinedTypes (std :: string &errMsg);

        /****
         * Methods for invoking Dispatcher-related operations
         *
         * @param setAndDatabase
         * @return
         */
        bool registerSet(
                std::pair<std::string, std::string> setAndDatabase,
                PartitionPolicy::Policy policy,
                std::string& errMsg);

        /**
         *
         * @param setAndDatabase
         * @return
         */
        template <class DataType>
        bool sendData(
                std::pair<std::string, std::string> setAndDatabase,
                Handle<Vector<Handle<DataType>>> dataToSend,
                std::string& errMsg);

        template <class DataType>
        bool sendBytes(
                std::pair<std::string, std::string> setAndDatabase,
                char * bytes, size_t numBytes,
                std::string& errMsg);

        /****
         * Methods for invoking Query-related operations
         */

        /* Executes some computations */
        template <class... Types>
        bool executeComputations(
                std::string& errMsg,
                Handle<Computation> firstParam,
                Handle<Types>... args);

        /* Deletes a set. */
        bool deleteSet(
                std::string databaseName,
                std::string setName);

        /* Gets a set iterator. */
        template <class Type>
        SetIterator<Type> getSetIterator(
                std::string databaseName,
                std::string setName);

    private:

        pdb :: CatalogClient catalogClient;
        pdb :: DispatcherClient dispatcherClient;
        pdb :: DistributedStorageManagerClient distributedStorageClient;
        pdb :: QueryClient queryClient;

        std::function<bool (Handle<SimpleRequestResult>)> generateResponseHandler(
                std::string description,
                std::string & errMsg);

        int port;
        bool usePangea;
        bool useQueryScheduler;
        std :: string address;
        PDBLoggerPtr logger;

    };

}

#include "StorageClientTemplate.cc"
#include "PDBClientTemplate.cc"
#include "DispatcherClientTemplate.cc"

#endif
