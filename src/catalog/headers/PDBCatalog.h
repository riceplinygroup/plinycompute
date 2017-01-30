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
#ifndef PDB_CATALOG_H_
#define PDB_CATALOG_H_

#include <algorithm>
#include <chrono>
#include <ctime>
#include <dirent.h>
#include <dlfcn.h>
#include <fstream>
#include <iostream>
#include <map>
#include <pthread.h>
#include <sqlite3.h>
#include <sstream>
#include <stdio.h>
#include <string>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include <boost/filesystem.hpp>

#include "CatalogNodeMetadata.h"
#include "CatalogPermissionsMetadata.h"
#include "CatalogSetMetadata.h"
#include "CatalogUserTypeMetadata.h"
#include "CatalogDatabaseMetadata.h"
#include "CatalogStandardSetMetadata.h"
#include "CatalogUserTypeMetadata.h"
#include "CatalogStandardDatabaseMetadata.h"
#include "CatalogStandardNodeMetadata.h"
#include "CatalogStandardUserTypeMetadata.h"
#include "Handle.h"
#include "InterfaceFunctions.h"
#include "Object.h"
#include "PDBCommunicator.h"
#include "PDBDebug.h"
#include "PDBLogger.h"
#include "PDBCatalogMsgType.h"

using namespace std;
using namespace pdb;

/**
 * PDBCatalog encapsulates the storage of metadata for an instance of PDB.
 * The underlying persistent storage is an embedded SQLite database with the
 * following tables and their contents:
 *
 *   - data_types: user-defined types
 *   - metrics: user-defined metrics
 *   - pdb_node: metadata about nodes in a cluster
 *   - pdb_database: metadata about databases
 *   - pdb_set: metadata about sets
 *   - pdb_user: metadata about users in a PDB instance (to be implemented)
 *   - pdb_user_permission: access to databases for users of PDB (to be implemented)
 *
 * This class also offers containers for accessing these metadata in memory upon
 * launching an instance of CatalogServer.
 */

    // A function that returns a string with the name of a user-defined object.
    typedef char *GenericFunction();

    class PDBCatalog;
    typedef shared_ptr <PDBCatalog> PDBCatalogPtr;

    class PDBCatalog {
    public:
        /**
         * Constructor
         *
         * @param logger the catalog logger
         * @param location the path to the location of the catalog (is relative to the pdb root
         *        directory
         */
        PDBCatalog(PDBLoggerPtr logger, string location);

        /**
         * Destructor, deletes temporary files.
         *
         */
        ~PDBCatalog();

        /**
         * Opens an instance of the catalog, retrieves metadata from the sqlite instance and
         * invokes a method for loading retrieved metadata into memory.
         *
         */
        void open();

        /**
         * registerUserDefinedObject registers a user-defined type in the Catalog
         * @typeCode is the typeID already assgined
         * @param objectToRegister contains the metadata for this object
         *
         * @param objectBytes are the binary bytes of the Shared Library encapsulated as an string
         * @param typeName is a string obtained from the shared library, which identifies the type (e.g. myCoolType)
         * @param fileName is the path+filename of the Shared Library in the local file system
         *        (e.g. mypath/mylibs/libMyCoolType.so
         * @param tableName is a string identifying the type of the object (e.g. metrics, data_type)
         * @return true on success
         */
        bool registerUserDefinedObject (int16_t typeCode,
                                        pdb :: Handle <CatalogUserTypeMetadata> &objectToRegister,
                                        const string &objectBytes,
                                        const string &typeName,
                                        const string &fileName,
                                        const string &tableName,
                                        string &errorMessage);

        /**
         * addMetadataToCatalog registers a Metadata item into the Catalog, basically saving it
         * as serialized bytes in an Sqlite table, it then adds that metadata item into memory.
         *
         * @param metadataValue encapsulates the object with the metadata
         * @param metadataCategory identifies the metadata category (values are defined in
         *        PDBCatalogMsgType)
         * @param errorMessage error message
         * @return true on success
         */
        template<class CatalogMetadataType, class CatalogStandardMetadataType>
        bool addMetadataToCatalog(Handle<CatalogMetadataType> &metadataValue,
                                  CatalogStandardMetadataType &metadataItem,
                                  int &metadataCategory,
                                  string &errorMessage);

        /**
         * addItemToVector loads a registered Metadata item into memory so it can be accessed by
         * the CatalogServer
         *
         * @param item encapsulates the object with the metadata
         * @param key is a string key used for maps
         * @return true on success
         */
        template<class CatalogMetadataType>
        bool addItemToVector(Handle<CatalogMetadataType> &item, int &key);

        /**
         * updateMetadataInCatalog updates an existing Metadata item in Sqlite along with its
         * content in memory
         *
         * @param metadataValue encapsulates the object with the metadata
         * @param metadataCategory identifies the metadata category (values are defined in
         *        PDBCatalogMsgType)
         * @param errorMessage error message
         * @return true on success
         */
        template<class CatalogMetadataType>
        bool updateMetadataInCatalog(pdb :: Handle<CatalogMetadataType> &metadataValue,
                                     int &metadataCategory,
                                     string &errorMessage);

        /**
         * deleteMetadataInCatalog deletes an existing Metadata item in Sqlite along with its
         * content in memory
         *
         * @param metadataValue encapsulates the object with the metadata
         * @param metadataCategory identifies the metadata category (values are defined in
         *        PDBCatalogMsgType)
         * @param errorMessage error message
         * @return true on success
         */
        //TODO the itme maybe is not needed.
        template<class CatalogMetadataType>
        bool deleteMetadataInCatalog(pdb :: Handle<CatalogMetadataType> metadataValue,
                                                 int &metadataCategory,
                                                 string &errorMessage);


        /**
         * updateItemInVector updates a registered Metadata item in memory so the changes
         * are visible to the CatalogServer
         *
         * @param index is the position in the container
         * @param item is the new metadata content
         * @return true on success
         */
        template<class CatalogMetadataType>
        bool updateItemInVector(int &index, Handle<CatalogMetadataType> &item);

        /**
         * deleteItemInVector deletes a registered Metadata item in memory so the changes
         * are visible to the CatalogServer
         *
         * @param index is the position in the container
         * @param item is the new metadata content
         * @return true on success
         */
        template<class CatalogMetadataType>
        bool deleteItemInVector(int &index, Handle<CatalogMetadataType> &item);

        map<string, CatalogNodeMetadata> getListOfNodesInCluster ();

        /**
         * getSerializedCatalog retrieves the bytes of the entire catalog, this could be used
         * it one wants to ship the catalog from the master node to a different machine
         *
         * @param fileName is the name of the catalago file
         * @param version is the version to retrieve
         * @param returnedBytes containes the bytes encapsulated in a string
         * @param errorMessage the error returned
         *
         * @return true on success
         */
        bool getSerializedCatalog(string fileName,
                string version,
                string &returnedBytes,
                string &errorMessage);

        /**
         * setCatalogVersion sets the version of the catalog, this is typically called when
         * updates are made to the catalog
         *
         * @param version contains the signature of the version
         */
        void setCatalogVersion(string version);

        /**
         * getCatalogVersion gets the version of the catalog as a string
         */
        string getCatalogVersion();

        /**
         * getModifiedMetadata gets the version of the catalog as a string
         */
        void getModifiedMetadata(string dateAsString);

        /**
         * getMetadataFromCatalog retrieves all Metadata from Sqlite for a given category and
         * returns it in a container
         *
         * @param key if blank returns all items in the category, otherwise, only the one
         *        matching the key
         * @param returnedEntries is a Vector of the Objects
         * @param errorMessage error message
         * @param metadataCategory identifies the metadata category (values are defined in
         *        PDBCatalogMsgType)
         * @return true on success
         */
        template<class CatalogMetadataType>
        bool getMetadataFromCatalog(bool onlyModified,
                                    string key,
                                    Handle<pdb::Vector<CatalogMetadataType>> & returnedEntries,
                                    string &errorMessage,
                                    int metadataCategory);

        /**
         * getLastId gets the number of items in a given Metadata category, so the id can be
         * properly set prior to the addition of a new item.
         *
         * @param metadataCategory is an enum to identify the category of metadata (values are
         *        defined in PDBCatalogMsgType)
         * @return the number of items as an int
         */
        int getLastId(int &metadataCategory);

        /**
         * itemName2ItemId maps the id of a Metadata item given its name
         *
         * @param metadataCategory is an enum to identify the category of metadata (values are
         *        defined in PDBCatalogMsgType)
         * @return the value of the item
         */
        string itemName2ItemId(int &metadataCategory, string &key);

        /**
         * keyIsFound checks if a Metadata item is found in its container
         *
         * @param metadataCategory is an enum to identify the category of metadata (values are
         *        defined in PDBCatalogMsgType)
         * @param key to search for
         * @return the value of the item
         */
        bool keyIsFound(int &metadataCategory, string &key, string &value);

        /**
         * overloads the << operator so other classes can print the content of the
         * catalog metadata
         */
        friend std::ostream& operator<<(std::ostream &out, PDBCatalog &catalog);

        /**
         * getLogger gets the logger for the Catalog
         */
        PDBLoggerPtr getLogger();

        // TODO
        //******* to document and review if needed or have to removed
        map <string, CatalogUserTypeMetadata> getUserDefinedTypesList();

        // TODO remove this??
        void unregisterPDBObject (string unregisterMe);

        // Lists all metadata registered in the catalog
        void printsAllCatalogMetadata();

        // Retrieves a dynamic library stored as BLOB in the Catalog
        // returns 1 if success, 0 otherwise.
        bool retrievesDynamicLibrary(string fileName,
                                     string tableName,
                                     Handle<CatalogUserTypeMetadata> &returnedItem,
                                     string &returnedSoLibrary,
                                     string &errorName);

        void testCatalogPrint();

        /**
         * getListOfDatabases returns a Vector of metadata for all Databases registered
         * in the catalog. The information is retrieved from memory rather than disk (SQLite).
         *
         * @param key to search for
         *
         * @return a Vector of items
         */
        void getListOfDatabases(Handle<Vector<CatalogDatabaseMetadata>> &databasesInCatalog,
                                const string &key);

        /**
         * getListOfDatabases returns a Vector of metadata for all Sets registered
         * in the catalog. The information is retrieved from memory rather than disk (SQLite).
         *
         * @param key to search for
         *
         * @return a Vector of items
         */
        void getListOfSets(Handle<Vector<CatalogSetMetadata>> &setsInCatalog, const string &key);

        /**
         * getListOfDatabases returns a Vector of metadata for all Nodes registered
         * in the catalog. The information is retrieved from memory rather than disk (SQLite).
         *
         * @param key to search for
         *
         * @return a Vector of items
         */
        void getListOfNodes(Handle<Vector<CatalogNodeMetadata>> &nodesInCatalog, const string &key);

        /**
         * closeSQLiteHandler closes the SQLite DB handler once is no longer needed
         */
        void closeSQLiteHandler();

    private:

        /**
         * getMapsPDBOjbect2SQLiteTable returns the name of a Sqlite table for a given category of
         * metadata

         * @return true on success
         */
        string getMapsPDBOjbect2SQLiteTable(int typeOfObject);

        /**
         * loadsMetadataIntoMemory loads all Metdata stored in Sqlite and populates the corresponding
         * containers
         */
        void loadsMetadataIntoMemory();


        // Mutex for ensuring proper writing of metadata to catalog
        pthread_mutex_t registerMetadataMutex;

        // This is the initial typeID value to be assigned to types registered via Shared Libraries
        // typeID from 1 to 8191 are reserved for built-in types and assigned automatically at
        // compile time
        int16_t initialTypeID = 8192;

        string catalogVersion;

        PDBLoggerPtr logger;

        /**
         * Handler for the SQLite instance, which stores all catalog metadata
         **/
        sqlite3 *sqliteDBHandler = NULL;

        /**
         * List unique metadata entries
         **/
        vector<CatalogDatabaseMetadata> dbList;
        // list of users in the cluster
        Handle<pdb :: Vector < Handle < CatalogUserTypeMetadata > >> listUsersInCluster;

        // Map of users, given the name of a user as a string,
        // lists all databases belonging to that user.
        multimap < string, CatalogUserTypeMetadata > mapUsersInCluster;

        // map for storing a map of Object name and ID
        map<string, int16_t> mapTypeNameToTypeID;

        // map for storing a map of ID and Object name
        map <int16_t, string> mapTypeIdToTypeName;

        //TODO some of these can be removed???
        // stores information about registered user-defined objects in the catalog by name.
        map<string, CatalogUserTypeMetadata> registeredUserDefinedObjectsByName;

        //TODO new temp containers for metadata,
        map <string, CatalogNodeMetadata> nodesResult;
        Handle<Vector <CatalogNodeMetadata> > nodesValues;

        map <string, CatalogSetMetadata> setsResult;
        Handle<Vector <CatalogSetMetadata> > setValues;

        map <string, CatalogDatabaseMetadata> dbsResult;
        Handle<Vector <CatalogDatabaseMetadata> > dbsValues;

        map <string, CatalogUserTypeMetadata> udfsResult;
        Handle<Vector <CatalogUserTypeMetadata> > udfsValues;
        //end of temp containers

        //Container for all metadata in the catalog
        Handle<Map<String, Handle<Vector<Object> > > > catalogContents;

        // URI string that represents the location of the plinyCatalog.db
        // so it can be use for opening connnections to the catalog.
        string uriPath;

        // this is the root path where the catalog resides
        // it's relative to where PDB runs
        string catalogRootPath;

        // this string contains the full name of the catalog file, to be used
        // by sqlite statements
        string catalogFilename;

        // String that represents the temporary location where the .so files
        // will be stored during runtime
        string tempPath;

        // Creates a statement from a string and executes the query, returning a
        // true if successful, false otherwise.
        bool catalogSqlQuery(string statement);

        // Executes an sql statement in sqlite3 (insert, update or delete)
        bool catalogSqlStep(sqlite3_stmt *stmt, string &errorMsg);

        // Retrieves a Registered Object from the sqlite database
        // returns 1 if success, 0 otherwise.
        bool getRegisteredObject(int16_t typeId,
                                 string fileName,
                                 string typeOfObject,
                                 string typeName,
                                 string &errorMessage);

        // Creates a temporary folder to place the .so files, returns 0 if success, -1
        // otherwise
        int createsTempPath();

        // Sets the URI path
        void setUriPath(string thePath){ uriPath=thePath; }

        // Generates a random string, will be used for creating a random folder to temporarily
        // placed the .so files
        string genRandomString(int len);

        // Deletes all .so files from the temp directory, this is called by the destructor
        void deleteTempSoFiles(string filePath);

        map<int, string> mapsPDBOjbect2SQLiteTable;
        map<int, string>  mapsPDBArrayOjbect2SQLiteTable;

    };


#endif /* FIRSTCATALOG_H_ */
