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
#ifndef CATALOG_CLIENT_H
#define CATALOG_CLIENT_H

#include "CatalogCloseSQLiteDBHandler.h"
#include "CatalogDatabaseMetadata.h"
#include "CatalogNodeMetadata.h"
#include "CatalogPrintMetadata.h"
#include "CatalogSetMetadata.h"
#include "CatSharedLibraryByNameRequest.h"
#include "PDBLogger.h"
#include "PDBServer.h"
#include "ServerFunctionality.h"

namespace pdb {

class CatalogClient : public ServerFunctionality {

public:
    /* Destructor */
    ~CatalogClient();

    /* Default Constructor */
    CatalogClient();

    /*
     * Creates a Catalog Client given the port and address of the catalog
     *   for a local catalog the address is typically "localhost"
     *   for a remote catalog, the address is the IP address of the machine
     *   where the catalog resides
     */
    CatalogClient(int port, std::string address, PDBLoggerPtr myLogger);

    /*
     * Creates a Catalog Client given the port and address of the catalog
     *   for a local catalog the address is typically "localhost"
     *   for a remote catalog, the address is the IP address of the machine
     *   where the catalog resides.
     *   If this catalog client points to a catalog in a remote machine, the
     *   pointsToCatalogMasterIn argument must be set to "true"
     *
     */
    CatalogClient(int port,
                  std::string address,
                  PDBLoggerPtr myLogger,
                  bool pointsToCatalogMasterIn);

    /* Registers event handlers associated with this server functionality */
    virtual void registerHandlers(PDBServer& forMe) override;

    /* Uses the name of the object type to find its corresponding typeId, returns -1
     * if not found
     */
    int16_t searchForObjectTypeName(std::string objectTypeName);

    /* Retrieves the content of a Shared Library given it's Type Id */
    bool getSharedLibrary(int16_t identifier, std::string sharedLibraryFileName);

    /* Retrieves the content of a Shared Library along with its registered metadata,
     * given it's typeName. Typically this method is invoked by a remote machine that
     * has no knowledge of the typeID
     */
    bool getSharedLibraryByTypeName(int16_t identifier,
                                    std::string& typeName,
                                    std::string sharedLibraryFileName,
                                    Handle<CatalogUserTypeMetadata>& typeMetadata,
                                    string& sharedLibraryBytes,
                                    std::string& errMsg);

    /* Sends a request to the Catalog Server to register a type with the catalog
     * returns true on success, false on fail */
    bool registerType(std::string fileContainingSharedLib, std::string& errMsg);

    /* Sends a request to the Catalog Server to shut down the server that we are connected to
     * ... returns true on success */
    bool shutDownServer(std::string& errMsg);

    /* Sends a request to the Catalog Server to return the typeName of a user-defined type in the
     * specified database and set; returns "" on err */
    std::string getObjectType(std::string databaseName, std::string setName, std::string& errMsg);

    /* Sends a request to the Catalog Server to Create a new database
     * returns true on success, false on fail
     */
    bool createDatabase(std::string databaseName, std::string& errMsg);

    /* Sends a request to the Catalog Server to register metadata about a database */
    bool registerDatabaseMetadata(std::string databaseName, std::string& errMsg);

    /* Sends a request to the Catalog Server to register metadata about a node in the cluster */
    bool registerNodeMetadata(pdb::Handle<pdb::CatalogNodeMetadata> nodeData, std::string& errMsg);

    /* Templated method for registering a piece of metadata in the catalog */
    template <class Type>
    bool registerGenericMetadata(pdb::Handle<Type> metadataItem, std::string& errMsg);

    /* Templated method for removing a piece of metadata from the catalog */
    template <class Type>
    bool deleteGenericMetadata(pdb::Handle<Type> metadataItem, std::string& errMsg);

    /* Sends a request to the Catalog Server to Creates a new set for a given DataType in a
     * database;
     * returns true on success, false on fail
     */
    template <class DataType>
    bool createSet(std::string databaseName, std::string setName, std::string& errMsg);

    /* same as above, but here we use the type code */
    bool createSet(int16_t identifier,
                   std::string databaseName,
                   std::string setName,
                   std::string& errMsg);

    /* Sends a request to the Catalog Server to delete a database; returns true on success, false on
     * fail
     */
    bool deleteDatabase(std::string databaseName, std::string& errMsg);

    /* Sends a request to the Catalog Server to delete a set returns true on success, false on fail
     */
    bool deleteSet(std::string databaseName, std::string setName, std::string& errMsg);

    /* Sends a request to the Catalog Server to add information about a node to a set for a given
     * database;
     * returns true on success, false on fail
     */
    bool addNodeToSet(std::string nodeIP,
                      std::string databaseName,
                      std::string setName,
                      std::string& errMsg);

    /* Sends a request to the Catalog Server to add information about a node to a database;
     * returns true on success, false on fail
     */
    bool addNodeToDB(std::string nodeIP, std::string databaseName, std::string& errMsg);

    /* Sends a request to the Catalog Server to remove information about a node from a set, this is
     * invoked when storage
     * removes a set for a database in a node in the cluster; returns true on success, false on fail
     */
    bool removeNodeFromSet(std::string nodeIP,
                           std::string databaseName,
                           std::string setName,
                           std::string& errMsg);

    /* Sends a request to the Catalog Server to remove information about a node from a database,
     * this is invoked when storage
     * removes a database in a node in the cluster; returns true on success, false on fail
     */
    bool removeNodeFromDB(std::string nodeIP, std::string databaseName, std::string& errMsg);

    /* Sends a request to the Catalog Server to print the content of the metadata stored in the
     * catalog */
    bool printCatalogMetadata(pdb::Handle<pdb::CatalogPrintMetadata> itemToSearch,
                              std::string& errMsg);

    /* Sends a request to the Catalog Server to print a category of metadata stored in the
     * catalog */
    bool printCatalogMetadata(std::string &categoryToPrint,
                              std::string& errMsg);

    /* Returns true if this Catalog Client points to a remote
     * Master Catalog Server
     */
    bool getPointsToMasterCatalog();

    /* Sets to true if this Catalog Client points to the Master Catalog Server */
    void setPointsToMasterCatalog(bool pointsToMaster);

    /* Sends a request to the Catalog Server to close the SQLite DB Handler */
    bool closeCatalogSQLite(std::string& errMsg);

    /* Lists the Databases registered in the catalog. */
    void listRegisteredDatabases (std :: string &errMsg);

    /* Lists the Sets for a given database registered in the catalog. */
    void listRegisteredSetsForADatabase (std :: string databaseName, std :: string &errMsg);

    /* Lists the Nodes registered in the catalog. */
    void listNodesInCluster (std :: string &errMsg);

    /* Lists the user-defined types registered in the catalog. */
    void listUserDefinedTypes (std :: string &errMsg);

private:
    /* True if this Catalog Client points to the Master Catalog Server */
    bool pointsToCatalogMaster;

    /* The IP address where this Catalog Client is connected to */
    std::string address;

    /* The port where this Catalog Client is connected to */
    int port;

    /* Logger to debug information */
    PDBLoggerPtr myLogger;

    /* To ensure serialized access */
    pthread_mutex_t workingMutex;
};
}

#include "CatalogClientTemplates.cc"

#endif
