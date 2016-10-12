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
/*
 * PDBCatalog.cpp
 *
 *  Created on: May 11, 2015
 *      Author: carlos
 */

#include "PDBCatalog.h"

    template<>
    bool PDBCatalog::addItemToVector<CatalogDatabaseMetadata>(Handle<CatalogDatabaseMetadata> &item, int &key){

        dbsValues->push_back(*item);
        dbsResult.insert(make_pair(item->getItemKey().c_str(), *item));
        mapDataBasesInCluster.insert(make_pair(item->getItemName().c_str(),*item));
        return true;
    }

    template<>
    bool PDBCatalog::addItemToVector<CatalogNodeMetadata>(Handle<CatalogNodeMetadata> &item, int &key){

        nodesValues->push_back(*item);
        nodesResult.insert(make_pair(item->getItemKey().c_str(), *item));
        return true;
    }

    template<>
    bool PDBCatalog::addItemToVector<CatalogSetMetadata>(Handle<CatalogSetMetadata> &item, int &key){

        setValues->push_back(*item);
        setsResult.insert(make_pair(item->getItemKey().c_str(), *item));
        mapSetsInCluster.insert(make_pair(item->getItemName().c_str(),*item));
        return true;
    }

    template<>
    bool PDBCatalog::addItemToVector<CatalogUserTypeMetadata>(Handle<CatalogUserTypeMetadata> &item, int &key){

        udfsValues->push_back(*item);
        udfsResult.insert(make_pair(item->getItemName().c_str(), *item));
        mapTypeNameToTypeID.insert(make_pair(item->getItemName().c_str(), atoi(item->getObjectID().c_str())));
        mapTypeIdToTypeName.insert(make_pair(atoi(item->getObjectID().c_str()), item->getItemName().c_str()));
        return true;
    }


    template<>
    bool PDBCatalog::updateItemInVector<CatalogDatabaseMetadata>(int &index, Handle<CatalogDatabaseMetadata> &item){
        (*dbsValues).assign(index, *item);
        // updates also this map
        dbsResult[(*item).getItemKey().c_str()] = (*item);
        return true;
    }

    template<>
    bool PDBCatalog::updateItemInVector<CatalogSetMetadata>(int &index, Handle<CatalogSetMetadata> &item){
        (*setValues).assign(index, *item);
        return true;
    }

    template<>
    bool PDBCatalog::updateItemInVector<CatalogNodeMetadata>(int &index, Handle<CatalogNodeMetadata> &item){
        (*nodesValues).assign(index, *item);
        return true;
    }


    template<>
    bool PDBCatalog::updateItemInVector<CatalogUserTypeMetadata>(int &index, Handle<CatalogUserTypeMetadata> &item){
        (*udfsValues).assign(index, *item);
        return true;
    }

    PDBCatalog::PDBCatalog(PDBLoggerPtr logger, string location){
        pthread_mutex_init(&(registerMetadataMutex), NULL);
        this->logger = logger;

        //sets the paths for the location of the catalog files
        catalogRootPath = location + "/";
        catalogFilename = catalogRootPath+ "plinyCatalog.db";
        setUriPath("file:" + catalogFilename);
        tempPath = catalogRootPath + "tmp_so_files";

        // creates temp folder for extracting so_files (only if folder doesn't exist)
        bool folder = boost::filesystem::create_directories(tempPath);
        if (folder==true) this->logger->writeLn("Libraries temporary folder: " + tempPath + " created.");
        else this->logger->writeLn("Error creating libraries temporary folder: " + tempPath + ".");

        // TODO remove unused containers!!
        // allocates memory for catalog metadata
        listNodesInCluster = makeObject< Vector<Handle <CatalogNodeMetadata>>>();
        listSetsInCluster = makeObject< Vector<Handle<CatalogSetMetadata>>>();
        listDataBasesInCluster = makeObject< Vector<Handle<CatalogDatabaseMetadata>>>();

        listUsersInCluster = makeObject< Vector<Handle<CatalogUserTypeMetadata>>>();
        listUserDefinedTypes = makeObject< Vector<Handle<CatalogUserTypeMetadata>>>();

        nodesValues = makeObject<Vector<CatalogNodeMetadata>>();
        setValues = makeObject<Vector<CatalogSetMetadata>>();;
        dbsValues = makeObject<Vector<CatalogDatabaseMetadata>>();
        udfsValues = makeObject<Vector<CatalogUserTypeMetadata>>();



        // Populates a map to convert strings from PDBObject names to SQLite table names
        // in order to create query strings
        mapsPDBOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBNode,
                                                   "pdb_node"));
        mapsPDBOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBDatabase,
                                                   "pdb_database"));
        mapsPDBOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBSet,
                                                   "pdb_set"));
        mapsPDBOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBUser,
                                                   "pdb_user"));
        mapsPDBOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBPermissions,
                                                   "pdb_user_permission"));
        mapsPDBOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBRegisteredObject,
                                                   "data_types"));

        // Populates a map to convert strings from Vector<PDBOjbect> names to SQLite table names
        // in order to create query strings
        mapsPDBArrayOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBNode,
                                                        "pdb_node"));
        mapsPDBArrayOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBDatabase,
                                                        "pdb_database"));
        mapsPDBArrayOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBSet,
                                                        "pdb_set"));
        mapsPDBArrayOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBUser,
                                                        "pdb_user"));
        mapsPDBArrayOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBPermissions,
                                                        "pdb_user_permission"));
        mapsPDBArrayOjbect2SQLiteTable.insert(make_pair(PDBCatalogMsgType::CatalogPDBRegisteredObject,
                                                        "data_types"));

    }

    PDBCatalog::~PDBCatalog(){
        int deletedFiles = boost::filesystem::remove_all(tempPath);
        this->logger->writeLn(to_string(deletedFiles) + " files have been deleted in temporary folder: " + tempPath);
        pthread_mutex_destroy(&(registerMetadataMutex));
    }

    PDBLoggerPtr PDBCatalog::getLogger(){
        return this->logger;
    }


    void PDBCatalog::setCatalogVersion(string version){
        catalogVersion = version;
    }

    string PDBCatalog::getCatalogVersion(){
        return catalogVersion;
    }

    bool PDBCatalog::getSerializedCatalog(string fileName,
            string version,
            string &returnedBytes,
            string &errorMessage){

        errorMessage = "";

        string fullName = catalogRootPath + "plinyCatalog.db";

        fstream file(fullName.c_str(), ios::in | ios::binary);
        if (!file) {
            errorMessage = "The file " + fullName + " was not found\n";
        }

        file.seekp(0, fstream::end);
        streampos numBytes = file.tellp();
        file.seekp(0, fstream::beg);

        char *buffer = new char[numBytes];
        file.read(buffer, numBytes);
        returnedBytes = string(buffer, numBytes);
        return true;
    }

    void PDBCatalog::open(){

        // Creates a location folder for storing the sqlite file containing metadata for this PDB instance.
        // If location exists, only opens it.
        mkdir(catalogRootPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

        sqlite3 *sqliteDBHandler = NULL;

        int ret = 0;
        // If database doesn't exist creates database along with tables, otherwise,
        // opens database without creating a database/tables.
        if((ret = sqlite3_open_v2(uriPath.c_str(), &sqliteDBHandler,
                                  SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI | SQLITE_OPEN_FULLMUTEX,
                                  NULL)) == SQLITE_OK) {

            // Tables are created with primary key on the .so files to avoid
            // duplicating entries.

            catalogSqlQuery("CREATE TABLE IF NOT EXISTS data_types (itemID TEXT, itemInfo BLOB, soBytes BLOB, timeStamp INTEGER);",
                            sqliteDBHandler);
            catalogSqlQuery("CREATE TABLE IF NOT EXISTS metrics (itemID TEXT, itemInfo BLOB, soBytes BLOB, timeStamp INTEGER);",
                            sqliteDBHandler);
            catalogSqlQuery("CREATE TABLE IF NOT EXISTS pdb_node (itemID TEXT PRIMARY KEY, itemInfo BLOB, timeStamp INTEGER);",
                             sqliteDBHandler);
            catalogSqlQuery("CREATE TABLE IF NOT EXISTS pdb_database (itemID TEXT PRIMARY KEY, itemInfo BLOB, timeStamp INTEGER);",
                            sqliteDBHandler);
            catalogSqlQuery("CREATE TABLE IF NOT EXISTS pdb_set (itemID TEXT PRIMARY KEY, itemInfo BLOB, timeStamp INTEGER);",
                            sqliteDBHandler);
            catalogSqlQuery("CREATE TABLE IF NOT EXISTS pdb_user (itemID TEXT PRIMARY KEY, itemInfo BLOB, timeStamp INTEGER);",
                            sqliteDBHandler);
            catalogSqlQuery("CREATE TABLE IF NOT EXISTS pdb_user_permission (itemID TEXT PRIMARY KEY, itemInfo BLOB, timeStamp INTEGER);",
                            sqliteDBHandler);

            // Loads into memory all metadata so the CatalogServer can access them
            loadsMetadataIntoMemory();

            this->logger->writeLn("Database catalog successfully open.");
            sqlite3_close_v2(sqliteDBHandler);

        }else{
            //cout << "EXISTS!!! " << endl;
        }
    }

    void PDBCatalog::loadsMetadataIntoMemory(){
        string errorMessage;

        //TODO remove unused containers
        // an empty string lists all entries in a given category
        string emptyString("");

        // retrieves metadata from the sqlite DB and populates containers
        if (getMetadataFromCatalog(false, emptyString,listNodesInCluster, nodesValues,
                                   nodesResult, errorMessage,
                                   PDBCatalogMsgType::CatalogPDBNode) == false) cout << errorMessage << endl;

        if (getMetadataFromCatalog(false, emptyString, listSetsInCluster, setValues,
                                   setsResult, errorMessage,
                                   PDBCatalogMsgType::CatalogPDBSet) == false) cout << errorMessage << endl;

        if (getMetadataFromCatalog(false, emptyString, listDataBasesInCluster, dbsValues,
                                   dbsResult, errorMessage,
                                   PDBCatalogMsgType::CatalogPDBDatabase) == false) cout << errorMessage << endl;

        if (getMetadataFromCatalog(false, emptyString, listUserDefinedTypes, udfsValues,
                                   udfsResult, errorMessage,
                                   PDBCatalogMsgType::CatalogPDBRegisteredObject) == true) {
            // TODO document this
            // populates certain metadata
            for (int i=0; i < (*udfsValues).size(); i++){
                cout << "RETRIEVING TYPE " << (*udfsValues)[i].getItemName().c_str() << " | " << (*udfsValues)[i].getObjectID().c_str() << endl;
                udfsResult.insert(make_pair((*udfsValues)[i].getItemName().c_str(), (*udfsValues)[i]));
            }

        } else {
            cout << errorMessage << endl;
        }

        cout << "---------------------------------------" << endl;
        cout << "PDB Metadata Registered in the Catalog: " << endl;

        printsAllCatalogMetadata();

        cout << "\nAll Metadata properly retrieved and loaded into memory!" << endl;
        cout << "--------------------------------------" << endl;

    }

    void PDBCatalog::getModifiedMetadata(string dateAsString){
        string errorMessage;

        //TODO remove unused containers

        Handle<pdb :: Vector < Handle< CatalogNodeMetadata > > > _listNodesInCluster = makeObject< Vector<Handle <CatalogNodeMetadata>>>();

        // list of sets in cluster
        Handle<pdb :: Vector < Handle < CatalogSetMetadata > >> _listSetsInCluster = makeObject< Vector<Handle<CatalogSetMetadata>>>();

        // list of databases in the cluster
        Handle<pdb :: Vector < Handle < CatalogDatabaseMetadata > > > _listDataBasesInCluster = makeObject< Vector<Handle<CatalogDatabaseMetadata>>>();

        vector<CatalogDatabaseMetadata> dbList;
        // list of users in the cluster
        Handle<pdb :: Vector < Handle < CatalogUserTypeMetadata > >> _listUsersInCluster = makeObject< Vector<Handle<CatalogUserTypeMetadata>>>();

        // list user-defined objects in the cluster
        Handle<pdb :: Vector < Handle < CatalogUserTypeMetadata > > > _listUserDefinedTypes = makeObject< Vector<Handle<CatalogUserTypeMetadata>>>();

        map <string, CatalogNodeMetadata> _nodesResult;
        Handle<Vector <CatalogNodeMetadata> > _nodesValues = makeObject<Vector<CatalogNodeMetadata>>();
        map <string, CatalogSetMetadata> _setsResult;
        Handle<Vector <CatalogSetMetadata> > _setValues = makeObject<Vector<CatalogSetMetadata>>();;
        map <string, CatalogDatabaseMetadata> _dbsResult;
        Handle<Vector <CatalogDatabaseMetadata> > _dbsValues = makeObject<Vector<CatalogDatabaseMetadata>>();
        map <string, CatalogUserTypeMetadata> _udfsResult;
        Handle<Vector <CatalogUserTypeMetadata> > _udfsValues = makeObject<Vector<CatalogUserTypeMetadata>>();;

        // retrieves metadata from the sqlite DB and populates containers
        if (getMetadataFromCatalog(true, dateAsString,_listNodesInCluster, _nodesValues,
                                   _nodesResult, errorMessage,
                                   PDBCatalogMsgType::CatalogPDBNode) == false) cout << errorMessage << endl;

        if (getMetadataFromCatalog(true, dateAsString, _listSetsInCluster, _setValues,
                                   _setsResult, errorMessage,
                                   PDBCatalogMsgType::CatalogPDBSet) == false) cout << errorMessage << endl;

        if (getMetadataFromCatalog(true, dateAsString, _listDataBasesInCluster, _dbsValues,
                                   _dbsResult, errorMessage,
                                   PDBCatalogMsgType::CatalogPDBDatabase) == false) cout << errorMessage << endl;

        if (getMetadataFromCatalog(true, dateAsString, _listUserDefinedTypes, _udfsValues,
                                   _udfsResult, errorMessage,
                                   PDBCatalogMsgType::CatalogPDBRegisteredObject) == true) {
            // TODO document this
            // populates certain metadata
            for (int i=0; i < (*_udfsValues).size(); i++){
                cout << "RETRIEVING TYPE " << (*_udfsValues)[i].getItemName().c_str() << " | " << (*_udfsValues)[i].getObjectID().c_str() << endl;
                udfsResult.insert(make_pair((*_udfsValues)[i].getItemName().c_str(), (*_udfsValues)[i]));
            }

        } else {
            cout << errorMessage << endl;
        }

        cout << "---------------------------------------" << endl;
        cout << "PDB Metadata Registered in the Catalog: " << endl;

        cout << "\nNumber of nodes registered in the cluster: " +
                std::to_string((int)(*_nodesValues).size()) << endl;
        cout << "--------------------------------------------" << endl;

        for (int i=0; i< (*_nodesValues).size(); i++){
            cout << (*_nodesValues)[i].printShort() << endl;
        }

        cout << "\nNumber of databases registered: " +
                std::to_string((int)(*_dbsValues).size()) << endl;
        cout << "--------------------------------------------" << endl;

        for (int i=0; i< (*_dbsValues).size(); i++){
            cout << (*_dbsValues)[i].printShort() << endl;
        }
        cout << "\nNumber of sets registered: " +
                std::to_string((int)(*_setValues).size()) << endl;
        cout << "--------------------------------------------" << endl;

        for (int i=0; i< (*_setValues).size(); i++){
            cout << (*_setValues)[i].printShort() << endl;
        }
        cout << "\nNumber of user-defined types registered: " +
                std::to_string((int)(*_udfsValues).size()) << endl;
        cout << "--------------------------------------------" << endl;

        for (int i=0; i< (*_udfsValues).size(); i++){
            cout << (*_udfsValues)[i].printShort() << endl;
        }
        cout << "\nAll Metadata properly retrieved and loaded into memory!" << endl;
        cout << "--------------------------------------" << endl;

    }


    void PDBCatalog::printsAllCatalogMetadata(){

        cout << "\nNumber of nodes registered in the cluster: " +
                std::to_string((int)(*nodesValues).size()) << endl;
        cout << "--------------------------------------------" << endl;

        for (int i=0; i< (*nodesValues).size(); i++){
            cout << (*nodesValues)[i].printShort() << endl;
        }

        cout << "\nNumber of databases registered: " +
                std::to_string((int)(*dbsValues).size()) << endl;
        cout << "--------------------------------------------" << endl;

        for (int i=0; i< (*dbsValues).size(); i++){
            cout << (*dbsValues)[i].printShort() << endl;
        }
        cout << "\nNumber of sets registered: " +
                std::to_string((int)(*setValues).size()) << endl;
        cout << "--------------------------------------------" << endl;

        for (int i=0; i< (*setValues).size(); i++){
            cout << (*setValues)[i].printShort() << endl;
        }
        cout << "\nNumber of user-defined types registered: " +
                std::to_string((int)(*udfsValues).size()) << endl;
        cout << "--------------------------------------------" << endl;

        for (int i=0; i< (*udfsValues).size(); i++){
            cout << (*udfsValues)[i].printShort() << endl;
        }
    }

    // TODO review and clean this
    bool PDBCatalog::loadRegisteredObject(string typeName, string typeOfObject, string fileName, string &errorMessage){
        bool success = false;
        int metadataCategory = (int)PDBCatalogMsgType::CatalogPDBRegisteredObject;
        string id = itemName2ItemId(metadataCategory, typeName);
        string soFileBytes;
        string returnedBytes;

        success = retrievesDynamicLibrary(id,
                                          typeOfObject,
                                          returnedBytes,
                                          soFileBytes,
                                          errorMessage);

        if (success==true){
            success = getRegisteredObject((int16_t)std::atoi(id.c_str()), typeName, typeOfObject, fileName, errorMessage);
            cout << "return 2" << endl;
        }else{
            errorMessage = "error retrieving " + typeName + "\n";
            cout << errorMessage << endl;
        }
        return success;
    }

    // Executes a sqlite3 query on the catalog database given by a query string.
    bool PDBCatalog::catalogSqlQuery(string queryString, sqlite3 *sqliteDBHandler){

        sqlite3_stmt *statement = NULL;

        if(sqlite3_prepare_v2(sqliteDBHandler, queryString.c_str(), -1, &statement, NULL) == SQLITE_OK){
            int result = 0;
            result = sqlite3_step(statement);
            sqlite3_finalize(statement);
            return true;
        }else{
            string error = sqlite3_errmsg(sqliteDBHandler);
            if(error != "not an error"){
                this->logger->writeLn((string)queryString + " " + error);
                return true;
            }else{
                return false;
            }
        }
    }

    // Executes a sqlite3 insert query on the catalog database given by a query string.
    bool PDBCatalog::catalogSqlStep(sqlite3 *sqliteDBHandler, sqlite3_stmt *stmt, string &errorMsg){

        int rc=0;
        if((rc = sqlite3_step(stmt)) == SQLITE_DONE){
            return true;
        }else{
            errorMsg = sqlite3_errmsg(sqliteDBHandler);
            if(errorMsg != "not an error"){
                this->logger->writeLn("Problem running sqlite statement: " + errorMsg);
                return true;
            }else{
                return false;
            }
        }
    }

    //TODO keep only the vector
    template<class CatalogMetadataType>
    bool PDBCatalog::getMetadataFromCatalog(bool onlyModified,
                                            string key,
                                            Handle<pdb::Vector<Handle<CatalogMetadataType>>> &returnedValues,
                                            Handle<pdb::Vector<CatalogMetadataType>> &returnedItems,
                                            map<string, CatalogMetadataType> & itemList,
                                            string &errorMessage,
                                            int metadataCategory){

        sqlite3 * sqliteDBHandlerInternal = NULL;

        pdb :: String emptyString("");

        if ((sqlite3_open_v2(catalogFilename.c_str(), &sqliteDBHandlerInternal,
                             SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_FULLMUTEX,
                             NULL)) != SQLITE_OK)
        {
            this->logger->writeLn("Error opening the database! " + (string)sqlite3_errmsg(sqliteDBHandlerInternal));
        }

        pthread_mutex_lock(&(registerMetadataMutex));

        sqlite3_stmt *statement = NULL;

//        string queryString = "SELECT itemID, itemInfo, timeStamp, strftime('%Y-%m-%d %H:%M:%f', cast(timeStamp as text), 'unixepoch') from " + mapsPDBArrayOjbect2SQLiteTable[metadataCategory];
        string queryString = "SELECT itemID, itemInfo, timeStamp from " + mapsPDBArrayOjbect2SQLiteTable[metadataCategory];
        // if empty string then retrieves all items in the table, otherwise only the item with the
        // given key
//        if (onlyModified == true) queryString.append(" where timeStamp > strftime('%s', '").append(key).append("', 'localtime')");
        if (onlyModified == true) queryString.append(" where timeStamp > ").append(key).append("");
        else if (key!="") queryString.append(" where itemID = '").append(key).append("'");

        cout << queryString << endl;
        if(sqlite3_prepare_v2(sqliteDBHandlerInternal, queryString.c_str(), -1,
                              &statement, NULL) == SQLITE_OK)
        {

            int res = 0;
            while ( 1 ) {
                res = sqlite3_step(statement);

                if ( res == SQLITE_ROW ){

                    // retrieve the serialized record
                    int numBytes = sqlite3_column_bytes(statement, 1);
                    Record <CatalogMetadataType> *recordBytes = (Record  <CatalogMetadataType> *) malloc (numBytes);

                    memcpy(recordBytes, sqlite3_column_blob(statement, 1), numBytes);
                    cout << "retrieving " << key << " with timeStamp= " << sqlite3_column_int(statement, 2) << endl;

                    // get the object
                    Handle<CatalogMetadataType> returnedObject = recordBytes->getRootObject ();

                    string itemId = returnedObject->getItemKey().c_str();

                    //TODO keep only one container Handle<Vector<PDBCatalog>, the other two don't work
                    // maybe try with the new pdb::Map too!
                    returnedValues->push_back(returnedObject);
                    itemList.insert(make_pair(itemId,*returnedObject));
                    returnedItems->push_back(*returnedObject);

                }
                else if ( res == SQLITE_DONE ){
                    break;
                }
            }
            sqlite3_finalize(statement);
            pthread_mutex_unlock(&(registerMetadataMutex));

            return true;
        }else{

            string error = sqlite3_errmsg(sqliteDBHandlerInternal);

            if(error != "not an error"){
                this->logger->writeLn((string)queryString + " " + error);
            }

            sqlite3_finalize(statement);
            sqlite3_close_v2(sqliteDBHandlerInternal);

            pthread_mutex_unlock(&(registerMetadataMutex));
            return false;
        }

    }

    bool PDBCatalog::registerUserDefinedObject (pdb :: Handle <CatalogUserTypeMetadata> &objectToRegister,
                                                const string &objectBytes,
                                                const string &typeName,
                                                const string &fileName,
                                                const string &tableName,
                                                string &errorMessage) {

        bool isSuccess = false;



        pthread_mutex_lock(&(registerMetadataMutex));
        cout << "objectBytes " << objectBytes.size() << endl;
        cout << "typeName " << typeName << endl;
        cout << "fileName " << fileName << endl;
        cout << "tableName " << tableName << endl;

        bool success = false;
        errorMessage = "";

        sqlite3 *sqliteDBHandler = NULL;
        sqlite3_stmt *stmt = NULL;
        uint8_t *serializedBytes = NULL;

        int rc = sqlite3_open_v2(uriPath.c_str(), &sqliteDBHandler,
                                 SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_FULLMUTEX,
                                 NULL);

        if (rc != SQLITE_OK) {

            errorMessage =  "Error opening the Database. " + (string)sqlite3_errmsg(sqliteDBHandler) + "\n";
            success = false;

        } else {

            string queryString("");
            queryString = "INSERT INTO " + tableName + " (itemID, itemInfo, soBytes, timeStamp) VALUES(?, ?, ?, strftime('%s', 'now', 'localtime'))";

            cout << "queryString " << queryString << endl;

            rc = sqlite3_prepare_v2(sqliteDBHandler, queryString.c_str(), -1, &stmt, NULL);
            if (rc != SQLITE_OK) {
                errorMessage =  "Prepared statement failed. " + (string)sqlite3_errmsg(sqliteDBHandler) + "\n";
                success = false;
            } else {

                // Gets the number of registered objects in the catalog
                int16_t numberOfRegisteredObjects = udfsResult.size();
                int totalRegisteredTypes = (int)numberOfRegisteredObjects;
//                cout << "Number of registered objects= " << numberOfRegisteredObjects << endl;

                int newItemId = (int)(initialTypeID + numberOfRegisteredObjects);
                string newObjectId = std::to_string(newItemId);
                String newObjectIndex = String(std::to_string(totalRegisteredTypes));

                cout << "   Object Id -----> " << newObjectId << endl;
                String idToRegister = String(newObjectId);
                String tableToRegister = String(tableName);
                String typeToRegister = String(typeName);

                // Sets pbject ID prior to serialization
                objectToRegister->setItemId(newObjectIndex);
                objectToRegister->setObjectId(idToRegister);
                objectToRegister->setItemKey(typeToRegister);
                objectToRegister->setItemName(typeToRegister);

                // gets the raw bytes of the object
                Record <CatalogUserTypeMetadata> *metadataBytes = getRecord <CatalogUserTypeMetadata>(objectToRegister);

                //TODO I might be able to directly save the bytes into sqlite without the memcpy
                serializedBytes = (uint8_t*) malloc(metadataBytes->numBytes ());
                memcpy(serializedBytes, metadataBytes, metadataBytes->numBytes());
                size_t soBytesSize = objectBytes.length();

                rc = sqlite3_bind_text(stmt, 1, typeToRegister.c_str(), -1, SQLITE_STATIC);

//                cout << "Received item key " << objectToRegister->getItemKey() << endl;
//                cout << "Received item id " << objectToRegister->getItemId() << endl;
//                cout << "Received item name " << objectToRegister->getItemName() << endl;
//                cout << "Received item type " << objectToRegister->getObjectType() << endl;
//                cout << "Received item file " << objectToRegister->getFileName() << endl;
//                cout << "Received item # bytes " << objectToRegister->getLibraryBytes().length() << endl;

                rc = sqlite3_bind_blob(stmt, 2, serializedBytes, metadataBytes->numBytes(), SQLITE_STATIC);
                rc = sqlite3_bind_blob(stmt, 3, objectBytes.c_str(), soBytesSize, SQLITE_STATIC);


                addItemToVector(objectToRegister, totalRegisteredTypes);
                // Inserts newly added object into maps, if they are new adds them, otherwise returns error message
                pair<map<string, Object>::iterator, bool> result;
                pair<map<int16_t, Object>::iterator, bool> result2;

                if (rc != SQLITE_OK) {
                    errorMessage = "Bind operation failed. " + (string)sqlite3_errmsg(sqliteDBHandler) + "\n";
                    success = false;
                } else {
                    rc = sqlite3_step(stmt);
                    if (rc != SQLITE_DONE){
                        if (sqlite3_errcode(sqliteDBHandler)==SQLITE_CONSTRAINT){
                           success = false;
                           errorMessage = fileName + " is already registered in the catalog!" + "\n";
                        }else
                            errorMessage = "Query execution failed. " + (string)sqlite3_errmsg(sqliteDBHandler) + "\n";
                    }else{
                        udfsResult.insert(make_pair(typeName, *objectToRegister));
                        listUserDefinedTypes->push_back(objectToRegister);

                        success = true;
                        this->logger->writeLn("Dynamic library successfully stored in catalog!");
                    }
                }
                sqlite3_finalize(stmt);
                sqlite3_close_v2(sqliteDBHandler);
                delete serializedBytes;
            }
        }
        pthread_mutex_unlock(&(registerMetadataMutex));
        return isSuccess;
    }

    bool PDBCatalog::getRegisteredObject(int16_t typeId, string typeName, string typeOfObject, string sharedLibraryFile, string &errorMessage){
        const char* dlsym_error = "";
        void *so_handle = NULL;
        bool success = false;

        so_handle = dlopen(sharedLibraryFile.c_str(), RTLD_LOCAL | RTLD_LAZY );

        if (!so_handle) {
            errorMessage = "Cannot load Stored Data Type library: " + sharedLibraryFile + " error " + (string)dlsym_error + '\n';
        }else{
            dlerror();

            string getInstance = typeName + "_getInstance";
            string getName = "getObjectTypeName";

            //TODO remove these lines, it's here just for testing purposes
            if (typeName.compare(0, 6, "TemLib") == 0 ){
                getInstance = "ReallyCoolClass_getInstance";
                getName = "ReallyCoolClass_getName";
            }else{
//                pdb::detail::getVTableMapPtr()->getVTablePtr(typeId, typeName);
            }

            GenericFunction * genericFunctionCall = (GenericFunction*) dlsym(so_handle, getName.c_str());
            if ((dlsym_error = dlerror())) {
                errorMessage = "Error, can't find create function " + (string)dlsym_error + "\n";
                cout << "   Error with Dynamic loaded library: " + errorMessage << "." << endl;
                dlclose(so_handle);
            }else{
                string objectTypeName = genericFunctionCall();

                cout << "setting vtpr for id " << typeId << endl;
                cout << "setting vtpr for typeName " << typeName << endl;

                this->logger->writeLn("Object->getName() called -> " + objectTypeName + " symbols successfully loaded to memory!!!");
                cout << "   Dynamic loaded library method called -> " + objectTypeName << ", all is Ok!" << endl;
                success = true;
            }
        }

        dlclose(so_handle);
        return success;
    }

    map <string, CatalogUserTypeMetadata> PDBCatalog::getUserDefinedTypesList(){
        return udfsResult;
    }

    void PDBCatalog::unregisterPDBObject (string unregisterMe) {}

    // Retrieves an .so library file stored as BLOB from the catalog
    // and writes it into a temporary folder/file so it can be loaded using
    // dlopen.
    bool PDBCatalog::retrievesDynamicLibrary(string itemId,
                                        string tableName,
                                        string &objectBytes,
                                        string &returnedSoLibrary,
                                        string &errorMessage){

        pthread_mutex_lock(&(registerMetadataMutex));

        sqlite3 *sqliteDBHandler = NULL;
        sqlite3_blob *pBlob = NULL;
        sqlite3_stmt *pStmt = NULL;

        errorMessage = "";

        if(sqlite3_open_v2(catalogFilename.c_str(), &sqliteDBHandler, SQLITE_OPEN_READWRITE  | SQLITE_OPEN_URI | SQLITE_OPEN_FULLMUTEX  , NULL) != SQLITE_OK){
            errorMessage = "Error opening catalog database: " + (string)sqlite3_errmsg(sqliteDBHandler) + "\n";
            return false;

        }

        // First we get the name of the file given the full path of the .so library
//        string onlyFileName = fileName.substr(fileName.find_last_of("/\\")+1);

        string query = "SELECT itemID, itemInfo, soBytes FROM " + tableName + " where itemID = ?;";
        cout << "query: " << query << " " << itemId << endl;
        if (sqlite3_prepare_v2(sqliteDBHandler, query.c_str(), -1, &pStmt, NULL) != SQLITE_OK){
            errorMessage = "Error query not well formed: " + (string)sqlite3_errmsg(sqliteDBHandler) + "\n";

            sqlite3_reset(pStmt);
            sqlite3_close_v2(sqliteDBHandler);
            return false;
        }

        sqlite3_bind_text(pStmt, 1, itemId.c_str(), -1, SQLITE_STATIC);

        if( sqlite3_step(pStmt) !=SQLITE_ROW ){
            errorMessage = "Error item not found in database: \n";

            sqlite3_reset(pStmt);
            sqlite3_close_v2(sqliteDBHandler);
            return false;
        }

        // retrieves metadata stored as serialized pdb :: Object
        int numBytes = sqlite3_column_bytes(pStmt, 1);

        char *buffer = new char[numBytes];
        memcpy(buffer, sqlite3_column_blob(pStmt, 1), numBytes);
        objectBytes = string(buffer, numBytes);
        delete [] buffer;

        // retrieves the bytes for the .so library
        numBytes = sqlite3_column_bytes(pStmt, 2);

        buffer = new char[numBytes];
        memcpy(buffer, sqlite3_column_blob(pStmt, 2), numBytes);
        returnedSoLibrary = string(buffer, numBytes);
        delete [] buffer;

        sqlite3_reset(pStmt);
        sqlite3_blob_close(pBlob);


        sqlite3_close_v2(sqliteDBHandler);

         //FIX ME This line throws an error
//                 this->logger->writeLn("Library " + fileName + " successfully retrieved. Size in bytes " + to_string(numBytes));

//        this->logger->writeLn("Dynamic library successfully retrieved from the catalog!");

        pthread_mutex_unlock(&(registerMetadataMutex));

        return true;
    }

    string PDBCatalog::genRandomString(int len) {
       srand(time(0));
       string str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
       int pos;
       while(str.size() != len) {
        pos = ((rand() % (str.size() - 1)));
        str.erase (pos, 1);
       }
       return str;
    }

    int PDBCatalog::createsTempPath(){
        tempPath = genRandomString(18);
        return mkdir(tempPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }

    void PDBCatalog::deleteTempSoFiles(string filePath){
        struct dirent *next_file = NULL;
        DIR *theFolder = NULL;

        theFolder = opendir(filePath.c_str());
        if (theFolder!=NULL){
            while ( (next_file = readdir(theFolder)) )
            {
                if (strcmp(next_file->d_name, ".")==0 || strcmp(next_file->d_name, "..")==0) { continue; }
                string fullName = filePath + "/" + next_file->d_name;
                remove(fullName.c_str());
            }
        }
    }

    unsigned PDBCatalog::mapsObjectNameToObjectId(string objectName){
        auto search = mapTypeNameToTypeID.find(objectName);
        if(search != mapTypeNameToTypeID.end()) {
            unsigned objectIndex = (unsigned) search->second;
            return objectIndex;
        }else{
            return -1;
        }
    }

    template<class CatalogMetadataType>
    bool PDBCatalog::addMetadataToCatalog(pdb :: Handle<CatalogMetadataType> metadataValue,
                                          int &metadataCategory,
                                          string &errorMessage){

        pthread_mutex_lock(&(registerMetadataMutex));

        bool isSuccess = false;
        sqlite3_stmt *stmt = NULL;
        sqlite3 *sqliteDBHandlerInternal = NULL;

        string sqlStatement = "INSERT INTO " + mapsPDBOjbect2SQLiteTable[metadataCategory] + " (itemID, itemInfo, timeStamp) VALUES (?, ?, strftime('%s', 'now', 'localtime'))";

        // gets the size of the container for a given type of metadata and
        // uses it to assign the index of a metadata item in its container
        int newId = getLastId(metadataCategory);
        pdb :: String newKeyValue = String(std::to_string(newId));
        string metadataKey = metadataValue->getItemKey().c_str();
        metadataValue->setItemId(newKeyValue);

        auto metadataBytes = getRecord <CatalogMetadataType>(metadataValue);
        size_t numberOfBytes = metadataBytes->numBytes();

        cout << sqlStatement << " with key= " << metadataKey << endl;

        // Opens connection to db
        if((sqlite3_open_v2(uriPath.c_str(), &sqliteDBHandlerInternal,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_FULLMUTEX,
                            NULL)) != SQLITE_OK) {

            errorMessage = "Error opening database: " + (string)sqlite3_errmsg(sqliteDBHandlerInternal);
            this->logger->writeLn(errorMessage);
            isSuccess = false;

        }

        // Prepares statement
        if ((sqlite3_prepare_v2(sqliteDBHandlerInternal, sqlStatement.c_str(), -1,
                                &stmt, NULL)) != SQLITE_OK) {

            errorMessage = "Prepared statement failed. " + (string)sqlite3_errmsg(sqliteDBHandlerInternal);
            this->logger->writeLn(errorMessage);
            isSuccess = false;

        }

        // Binds key for this piece of metadata
        if ((sqlite3_bind_text(stmt, 1, metadataKey.c_str(), -1, SQLITE_STATIC)) != SQLITE_OK) {
            errorMessage = "Bind operation failed. " + (string)sqlite3_errmsg(sqliteDBHandlerInternal) + "\n";
            isSuccess = false;
        }

        // Binds value for this piece of metadata (as a pdb serialized set of bytes)
        if ((sqlite3_bind_blob(stmt, 2, metadataBytes, numberOfBytes, SQLITE_STATIC)) != SQLITE_OK) {
            errorMessage = "Bind operation failed. " + (string)sqlite3_errmsg(sqliteDBHandlerInternal) + "\n";
            isSuccess = false;
        }

        // Runs the insert statement
        if (catalogSqlStep(sqliteDBHandlerInternal, stmt, errorMessage)) {
            // Metadata item inserted in sqlite then add to pdb :: Vector  in memory
            addItemToVector(metadataValue, metadataCategory);
            isSuccess = true;

        } else {
            errorMessage = "Cannot add new item to Catalog";
            this->logger->writeLn(errorMessage);
        }

        sqlite3_finalize(stmt);
        sqlite3_close_v2(sqliteDBHandlerInternal);

        pthread_mutex_unlock(&(registerMetadataMutex));

        if (isSuccess == true) {
            cout << "Item stored in SQLite and loaded into Catalog memory:" << endl;
            cout << "  Item Id ------> " << metadataValue->getItemId().c_str() << endl;
            cout << "  Item Key -----> " << metadataValue->getItemKey().c_str() << endl;
            cout << "  Item Name ----> " << metadataValue->getItemName().c_str() << endl;
        }
        this->logger->writeLn(errorMessage);

//        cout << "Adding " << (*metadataValue).printShort() << endl;

        return isSuccess;

    }

    template<class CatalogMetadataType>
    bool PDBCatalog::updateMetadataInCatalog(pdb :: Handle<CatalogMetadataType> metadataValue,
                                             int &metadataCategory,
                                             string &errorMessage){

        pthread_mutex_lock(&(registerMetadataMutex));

        //TODO remove this couts, used for debugging only
        cout << "Updating metadata" << endl;
        cout << "   Item Index----> " << metadataValue->getItemId() << endl;
        cout << "   Item Key -----> " << metadataValue->getItemKey().c_str() << endl;
        cout << "   Item Value----> " << metadataValue->getItemName().c_str() << endl;
        //        cout << *metadataValue << endl;

        // gets the key and index for this item in order to update the sqlite table and
        // update the container in memory
        String metadataKey = metadataValue->getItemKey();
        int metadataIndex = std::atoi(metadataValue->getItemId().c_str());

        bool isSuccess = false;
        sqlite3_stmt *stmt = NULL;
        string sqlStatement = "UPDATE " + mapsPDBOjbect2SQLiteTable[metadataCategory] + " set itemInfo =  ?, timeStamp = strftime('%s', 'now', 'localtime') where itemId = ?";
//        string sqlStatement = "INSERT OR REPLACE INTO " + mapsPDBOjbect2SQLiteTable[catalogType] + " (itemInfo, itemId) values ( ?, ? )";

//        cout << sqlStatement << " id: " << metadataKey.c_str() << endl;

        auto metadataBytes = getRecord <CatalogMetadataType>(metadataValue);

        size_t numberOfBytes = metadataBytes->numBytes();

        sqlite3 *sqliteDBHandlerInternal = NULL;

        // Opens connection to db
        if((sqlite3_open_v2(uriPath.c_str(), &sqliteDBHandlerInternal, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_FULLMUTEX  , NULL)) != SQLITE_OK){
            errorMessage = "Error opening database: " + (string)sqlite3_errmsg(sqliteDBHandlerInternal);
            this->logger->writeLn(errorMessage);
            isSuccess = false;
        }

        // Prepares statement
        if ((sqlite3_prepare_v2(sqliteDBHandlerInternal,sqlStatement.c_str(),-1, &stmt, NULL)) != SQLITE_OK) {
            errorMessage = "Prepared statement failed. " + (string)sqlite3_errmsg(sqliteDBHandlerInternal);
            this->logger->writeLn(errorMessage);
            isSuccess = false;
        }

        // Binds value for this piece of metadata (as a pdb serialized set of bytes)
        if ((sqlite3_bind_blob(stmt, 1, metadataBytes, numberOfBytes, SQLITE_STATIC)) != SQLITE_OK){

            errorMessage = "Bind operation failed. " + (string)sqlite3_errmsg(sqliteDBHandlerInternal) + "\n";
            isSuccess = false;
        }

        // Binds key for this piece of metadata
        if ((sqlite3_bind_text(stmt, 2, metadataKey.c_str(), -1, SQLITE_STATIC)) != SQLITE_OK){
            errorMessage = "Bind operation failed. " + (string)sqlite3_errmsg(sqliteDBHandlerInternal) + "\n";
            isSuccess = false;
        }

        this->logger->writeLn(errorMessage);

        // Runs the update statement
        if (catalogSqlStep(sqliteDBHandlerInternal, stmt, errorMessage)){
            // if sqlite update goes well, updates container
            updateItemInVector(metadataIndex, metadataValue);
            isSuccess = true;
        }else{
            errorMessage = "Cannot update item in Catalog";
            this->logger->writeLn(errorMessage);
            cout << "updateMetadataToCatalog-> " << errorMessage << endl;
        }
        this->logger->writeLn(errorMessage);

        sqlite3_finalize(stmt);
        sqlite3_close_v2(sqliteDBHandlerInternal);

//        cout << "Updating " << (*metadataValue).printShort() << endl;

        pthread_mutex_unlock(&(registerMetadataMutex));
        return isSuccess;


    }

    template<class CatalogMetadataType>
    bool PDBCatalog::deleteMetadataInCatalog(pdb :: Handle<CatalogMetadataType> metadataValue,
                                             int &metadataCategory,
                                             string &errorMessage){

        pthread_mutex_lock(&(registerMetadataMutex));

        //TODO remove this couts, used for debugging only
        cout << "Deleting metadata" << endl;
        cout << "   Item Index----> " << metadataValue->getItemId() << endl;
        cout << "   Item Key -----> " << metadataValue->getItemKey().c_str() << endl;
        cout << "   Item Value----> " << metadataValue->getItemName().c_str() << endl;
        //        cout << *metadataValue << endl;

        // gets the key and index for this item in order to update the sqlite table and
        // update the container in memory
        String metadataKey = metadataValue->getItemKey();
        int metadataIndex = std::atoi(metadataValue->getItemId().c_str());

        bool isSuccess = false;
        sqlite3_stmt *stmt = NULL;
        string sqlStatement = "DELETE from " + mapsPDBOjbect2SQLiteTable[metadataCategory] + " where itemId = ?";

        cout << sqlStatement << " id: " << metadataKey.c_str() << endl;

        sqlite3 *sqliteDBHandlerInternal = NULL;

        // Opens connection to db
        if((sqlite3_open_v2(uriPath.c_str(), &sqliteDBHandlerInternal, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_FULLMUTEX  , NULL)) != SQLITE_OK){
            errorMessage = "Error opening database: " + (string)sqlite3_errmsg(sqliteDBHandlerInternal);
            this->logger->writeLn(errorMessage);
            isSuccess = false;
        }
        // Prepares statement
        if ((sqlite3_prepare_v2(sqliteDBHandlerInternal,sqlStatement.c_str(),-1, &stmt, NULL)) != SQLITE_OK) {
            errorMessage = "Prepared statement failed. " + (string)sqlite3_errmsg(sqliteDBHandlerInternal);
            this->logger->writeLn(errorMessage);
            isSuccess = false;
        }

        // Binds key for this piece of metadata
        if ((sqlite3_bind_text(stmt, 1, metadataKey.c_str(), -1, SQLITE_STATIC)) != SQLITE_OK){
            errorMessage = "Bind operation failed. " + (string)sqlite3_errmsg(sqliteDBHandlerInternal) + "\n";
            isSuccess = false;
        }

        this->logger->writeLn(errorMessage);

        // Runs the update statement
        if (catalogSqlStep(sqliteDBHandlerInternal, stmt, errorMessage)){
            // if sqlite update goes well, updates container
//            updateItemInVector(metadataIndex, metadataValue);
            isSuccess = true;

        }else{
            errorMessage = "Cannot delete item in Catalog";
            this->logger->writeLn(errorMessage);
        }
        this->logger->writeLn(errorMessage);

        sqlite3_finalize(stmt);
        sqlite3_close_v2(sqliteDBHandlerInternal);

//        cout << "Updating " << (*metadataValue).printShort() << endl;

        pthread_mutex_unlock(&(registerMetadataMutex));
        return isSuccess;


    }

    map<string, vector<CatalogNodeMetadata> > PDBCatalog::getNodesForADatabase (string const databaseName){
        map<string, vector<CatalogNodeMetadata> > listOfNodes;
        return listOfNodes;
    }

    string PDBCatalog::getMapsPDBOjbect2SQLiteTable(int typeOfObject){
        return mapsPDBOjbect2SQLiteTable[typeOfObject];
    }

    int PDBCatalog::getLastId(int &metadataCategory){
        int lastId = 0;
        switch (metadataCategory) {

            case  PDBCatalogMsgType::CatalogPDBNode :
            {
                lastId = nodesValues->size();
                break;
            }

            case  PDBCatalogMsgType::CatalogPDBDatabase :
            {
                lastId = dbsValues->size();
                break;
            }

            case PDBCatalogMsgType::CatalogPDBSet :
            {
                lastId = setValues->size();
                break;
            }

//            default :
//            {
//                lastId = -1;
//                break;
//            }
        }
        return lastId;
    }

     std::ostream& operator<<(std::ostream &out, PDBCatalog &catalog) {
        out << "--------------------------" << endl;
        out << "PDB Metadata Registered in the Catalog: " << endl;

        out << "\n   Number of cluster nodes registered: " + std::to_string((int)catalog.listNodesInCluster->size()) << endl;
        for (int i = 0 ; i < catalog.listNodesInCluster->size(); i ++){
            out << "      Id: " << (*catalog.listNodesInCluster)[i]->getItemId().c_str()
                 <<  " | Node name: " << (*catalog.listNodesInCluster)[i]->getItemName().c_str()
                 << " | Node Address: " << (*catalog.listNodesInCluster)[i]->getItemKey().c_str()
                 << ":" <<  (*catalog.listNodesInCluster)[i]->getNodePort()
                 << endl;
        }

        out << "\n   Number of databases registered: " + std::to_string((int)catalog.listDataBasesInCluster->size()) << endl;
        for (int i = 0 ; i < catalog.listDataBasesInCluster->size(); i ++){
            out << "      Id: " << (*catalog.listDataBasesInCluster)[i]->getItemId().c_str()
                 << " | Database: " << (*catalog.listDataBasesInCluster)[i]->getItemName().c_str()
                 << endl;
        }

        out << "\n   Number of sets registered: " + std::to_string((int)catalog.listSetsInCluster->size()) << endl;
        for (int i = 0 ; i < catalog.listSetsInCluster->size(); i ++){
            out << "      Id: " << (*catalog.listSetsInCluster)[i]->getItemId().c_str()
                 <<  " | Key: " << (*catalog.listSetsInCluster)[i]->getItemKey().c_str()
                 << " | Database: " << (*catalog.listSetsInCluster)[i]->getDBName().c_str()
                 << " | Set: " << (*catalog.listSetsInCluster)[i]->getItemName().c_str()
                 << endl;
        }

        out << "\n   Number of users registered: " + std::to_string((int)catalog.listUsersInCluster->size()) << endl;
        for (int i = 0 ; i < catalog.listUsersInCluster->size(); i ++){
            out << (*catalog.listUsersInCluster)[i]->getItemName() << endl;
        }

        out << "\n   Number of user-defined types registered: " + std::to_string((int)catalog.listUserDefinedTypes->size()) << endl;
        for (int i = 0 ; i < catalog.listUserDefinedTypes->size(); i ++){
            out << "      Id: " << (*catalog.listUserDefinedTypes)[i]->getItemId().c_str()
                 << " | Type Name: " << (*catalog.listUserDefinedTypes)[i]->getItemName().c_str()
                 << endl;
        }
        out << "--------------------------" << endl;
        return out;
    }

    string PDBCatalog::itemName2ItemId(int &metadataCategory, string &key) {
        switch (metadataCategory) {
            case  PDBCatalogMsgType::CatalogPDBNode :
            {
                return nodesResult[key].getItemKey().c_str();
                break;
            }

            case  PDBCatalogMsgType::CatalogPDBDatabase :
            {
                return dbsResult[key].getItemKey().c_str();
                break;
            }

            case PDBCatalogMsgType::CatalogPDBSet :
            {
                return setsResult[key].getItemKey().c_str();
                break;
            }

            case PDBCatalogMsgType::CatalogPDBRegisteredObject :
            {
                // User-defined types and metrics are stored in a different type of map
                return udfsResult[key].getObjectID().c_str();
                break;
            }

            default :
            {
                return "Unknown request!";
                break;
            }
        }
    }

    bool PDBCatalog::keyIsFound(int &metadataCategory, string &key, string &value){
        value = "";

        switch (metadataCategory) {
            case  PDBCatalogMsgType::CatalogPDBNode :
            {
                auto p = nodesResult.find(key);
                if(p != nodesResult.end()){
                    value = p->second.getItemKey().c_str();
                    return true;
                }
                break;
            }

            case  PDBCatalogMsgType::CatalogPDBDatabase :
            {
                auto p = dbsResult.find(key);
                if(p != dbsResult.end()){
                    value = p->second.getItemKey().c_str();
                    return true;
                }
                break;
            }

            case PDBCatalogMsgType::CatalogPDBSet :
            {
                auto p = setsResult.find(key);
                if(p != setsResult.end()){
                    value = p->second.getItemKey().c_str();
                    return true;
                }
                break;
            }

            case PDBCatalogMsgType::CatalogPDBRegisteredObject :
            {
                auto p = udfsResult.find(key);
                if(p != udfsResult.end()){
                    value = p->second.getItemKey().c_str();
                    return true;
                }
                break;
            }

            default :
            {
                return false;
                break;
            }
        }

        return false;
    }

    //TODO remove these implicit instantiations and add and include at the bottom of the h file
    // Add implicit instantiation of all possible types the method
    // will ever use.
    template bool PDBCatalog::addMetadataToCatalog<CatalogNodeMetadata>(pdb :: Handle<CatalogNodeMetadata> metadataValue,
                                                   int &catalogType,
                                                   string &errorMessage);

    template bool PDBCatalog::addMetadataToCatalog<CatalogSetMetadata>(pdb :: Handle<CatalogSetMetadata> metadataValue,
                                                   int &catalogType,
                                                   string &errorMessage);

    template bool PDBCatalog::addMetadataToCatalog<CatalogDatabaseMetadata>(pdb :: Handle<CatalogDatabaseMetadata> metadataValue,
                                                   int &catalogType,
                                                   string &errorMessage);

    // Add implicit instantiation of all possible types the method
    // will ever use
    template bool PDBCatalog::updateMetadataInCatalog<CatalogNodeMetadata>(pdb :: Handle<CatalogNodeMetadata> metadataValue,
                                                 int &catalogType,
                                                 string &errorMessage);

    template bool PDBCatalog::updateMetadataInCatalog<CatalogSetMetadata>(pdb :: Handle<CatalogSetMetadata> metadataValue,
                                                 int &catalogType,
                                                 string &errorMessage);

    template bool PDBCatalog::updateMetadataInCatalog<CatalogDatabaseMetadata>(pdb :: Handle<CatalogDatabaseMetadata> metadataValue,
                                                 int &catalogType,
                                                 string &errorMessage);

    template bool PDBCatalog::updateMetadataInCatalog<CatalogUserTypeMetadata>(pdb :: Handle<CatalogUserTypeMetadata> metadataValue,
                                                 int &catalogType,
                                                 string &errorMessage);

    // Add implicit instantiation of all possible metadata
    template bool PDBCatalog::deleteMetadataInCatalog<CatalogNodeMetadata>(pdb :: Handle<CatalogNodeMetadata> metadataValue,
                                                 int &catalogType,
                                                 string &errorMessage);

    template bool PDBCatalog::deleteMetadataInCatalog<CatalogSetMetadata>(pdb :: Handle<CatalogSetMetadata> metadataValue,
                                                 int &catalogType,
                                                 string &errorMessage);

    template bool PDBCatalog::deleteMetadataInCatalog<CatalogDatabaseMetadata>(pdb :: Handle<CatalogDatabaseMetadata> metadataValue,
                                                 int &catalogType,
                                                 string &errorMessage);

    template bool PDBCatalog::deleteMetadataInCatalog<CatalogUserTypeMetadata>(pdb :: Handle<CatalogUserTypeMetadata> metadataValue,
                                                 int &catalogType,
                                                 string &errorMessage);


    template bool PDBCatalog::addItemToVector(pdb :: Handle< pdb :: CatalogNodeMetadata > &item, int &key);
    template bool PDBCatalog::addItemToVector(pdb :: Handle< pdb :: CatalogSetMetadata > &item, int &key);
    template bool PDBCatalog::addItemToVector(pdb :: Handle< pdb :: CatalogDatabaseMetadata > &item, int &key);
    template bool PDBCatalog::addItemToVector(pdb :: Handle< pdb :: CatalogUserTypeMetadata > &item, int &key);

    template bool PDBCatalog::updateItemInVector(int &index, pdb :: Handle< pdb :: CatalogNodeMetadata > &item);
    template bool PDBCatalog::updateItemInVector(int &index, pdb :: Handle< pdb :: CatalogSetMetadata > &item);
    template bool PDBCatalog::updateItemInVector(int &index, pdb :: Handle< pdb :: CatalogDatabaseMetadata > &item);
    template bool PDBCatalog::updateItemInVector(int &index, pdb :: Handle< pdb :: CatalogUserTypeMetadata > &item);
