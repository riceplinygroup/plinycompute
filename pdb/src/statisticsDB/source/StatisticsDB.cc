#ifndef STATISTICS_DB_CC
#define STATISTICS_DB_CC


#include "StatisticsDB.h"

namespace pdb {


StatisticsDB::StatisticsDB (ConfigurationPtr conf) {

    this->conf = conf;
    this->pathToDBFile = "file:" + this->conf->getStatisticsDB() + "/dbFile";

    //check whether pathToDBFile exists
    //if it exists, we do not need create directories and tables
    //otherwise, we need create directories and tables
    if (createDir()){
        createTables();
    } else {
        openDB();
    }

    //initialize ids
    dataId = getLatestId("DATA") + 1;
    transformationId = getLatestTransformationId() + 1;

}


StatisticsDB::~StatisticsDB () {

    closeDB();

} 


bool StatisticsDB::openDB () {

    std::cout << "to open DB " << this->pathToDBFile << std::endl;
    if (sqlite3_open_v2(this->pathToDBFile.c_str(), &statisticsDBHandler,
                    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL) == SQLITE_OK) {

        sqlite3_exec(statisticsDBHandler, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
        std::cout << "self learning database open successfully" << std::endl;
        return true;
    } else {
        std::cout << "failure in opening self learning database" << std::endl;
        return false;
    }
    return false;

}

bool StatisticsDB::closeDB () {

    //TODO
    return false;
}




bool StatisticsDB::createTables () {

    if (openDB() == false) {
        return false;
    }

    bool ret = execDB ("CREATE TABLE IF NOT EXISTS DATA (ID BIGINT PRIMARY KEY, "
            "DATABASE_NAME VARCHAR(128), SET_NAME VARCHAR(128),"
            "CREATED_JOBID VARCHAR(128), IS_REMOVED BOOLEAN,"
            "SET_TYPE VARCHAR(32), CLASS_NAME VARCHAR(128),"
            "TYPE_ID INT, SIZE BIGINT, PAGE_SIZE BIGINT,"
            "MODIFICATION_TIME BIGINT) WITHOUT ROWID;");
    
    if (ret == true) {

         ret = execDB ("CREATE TABLE IF NOT EXISTS DATA_TRANSFORMATION (ID BIGINT PRIMARY KEY, "
             "INPUT_DATA_ID BIGINT, OUTPUT_DATA_ID BIGINT, "
             "NUM_PARTITIONS INT, NUM_NODES INT, "
             "TRANSFORMATION_TYPE VARCHAR(32), TCAP TEXT, "
             "COMPUTATIONS BLOB, "
             "FOREIGN KEY (INPUT_DATA_ID) REFERENCES DATA(ID),"
             "FOREIGN KEY (OUTPUT_DATA_ID) REFERENCES DATA(ID)) WITHOUT ROWID;");

    }

    return ret;

}

bool StatisticsDB::execDB (std::string cmdString) {

    std::cout << "command: " << cmdString << std::endl;
    sqlite3_stmt * cmdStatement;
    if (sqlite3_prepare_v2 (statisticsDBHandler, cmdString.c_str(), -1, &cmdStatement,
                     NULL) == SQLITE_OK) {
        sqlite3_step(cmdStatement);
        sqlite3_finalize(cmdStatement);
        return true;
    } else {
        std::string error = sqlite3_errmsg(statisticsDBHandler);
        std::cout << error << std::endl;
        sqlite3_finalize(cmdStatement);
        return false;
    }

}

bool StatisticsDB::createDir () {

    if (mkdir(this->conf->getStatisticsDB().c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH)!= -1){
        return true;
    } else {
        return false;
    }

}

bool StatisticsDB::createData (std::string databaseName,
                     std::string setName,
                     std::string created_jobId,
                     std::string setType,
                     std::string className,
                     int typeId,
                     size_t pageSize,
                     long & id) {

     std::cout << "to create data..." << std::endl;
     id = dataId;
     dataId ++;
     string cmdString = "INSERT INTO DATA "
                " (ID, DATABASE_NAME, SET_NAME, CREATED_JOBID, IS_REMOVED, SET_TYPE, "
                "CLASS_NAME, TYPE_ID, SIZE, PAGE_SIZE, MODIFICATION_TIME) "
                "VALUES(" + std::to_string(id) + ","
                          + quoteStr(databaseName) + ","
                          + quoteStr(setName) + ","
                          + quoteStr(created_jobId) + ","
                          + "0,"
                          + quoteStr(setType) + ","
                          + quoteStr(className) + ","
                          + std::to_string(typeId) + ","
                          + "0,"
                          + std::to_string(pageSize) + ","
                          + "strftime('%s', 'now', 'localtime'));";
      std::cout << "CreateData: " << cmdString << std::endl;
      return execDB(cmdString);


}

bool StatisticsDB::updateDataForSize (long id,
                            size_t size) {

      std::string cmdString = "UPDATE DATA set SIZE = " + std::to_string(size) +
                              ", MODIFICATION_TIME = strftime('%s', 'now', 'localtime') where ID = " +
                              std::to_string(id);
      std::cout << "UpdateDataForSize: " << cmdString << std::endl;
      return execDB(cmdString);

}

bool StatisticsDB::updateDataForRemoval (long id) {

      std::string cmdString = std::string("UPDATE DATA set IS_REMOVED = 1") +
                              std::string(", MODIFICATION_TIME = strftime('%s', 'now', 'localtime') where ID = ") +
                              std::to_string(id);
      std::cout << "UpdateDataForRemoval: " << cmdString << std::endl;
      return execDB(cmdString);

}

bool StatisticsDB::createDataTransformation (long input_data_id,
                                   long output_data_id,
                                   int num_partitions,
                                   int num_nodes,
                                   std::string transformationType,
                                   std::string tcap,
                                   Handle<Vector<Handle<Computation>>> computations,
                                   long& id) {

     std::cout << "to create data transformation..." << std::endl;
     id = transformationId;
     sqlite3_stmt * statement;
     transformationId ++;
     replaceStr(tcap, "'", "''");
     string cmdString = "INSERT INTO DATA_TRANSFORMATION "
                " (ID, INPUT_DATA_ID, OUTPUT_DATA_ID, NUM_PARTITIONS, NUM_NODES, TRANSFORMATION_TYPE, TCAP, COMPUTATIONS) "
                "VALUES(" + std::to_string(id) + ","
                          + std::to_string(input_data_id) + ","
                          + std::to_string(output_data_id) + ","
                          + std::to_string(num_partitions) + ","
                          + std::to_string(num_nodes) + ","
                          + quoteStr(transformationType) + ","
                          + quoteStr(tcap)
                          + ", ?);";
      std::cout << "CreateDataTransformation: " << cmdString << std::endl;
      Handle<Vector<Handle<Computation>>> myComputations = 
                deepCopyToCurrentAllocationBlock<Vector<Handle<Computation>>>(computations);
      if (sqlite3_prepare_v2(statisticsDBHandler, cmdString.c_str(), -1, &statement, NULL) == SQLITE_OK) {
                //to get the bytes
                Record<Vector<Handle<Computation>>> * record =
                     getRecord<Vector<Handle<Computation>>>(myComputations);
                sqlite3_bind_blob(statement, 1, record, record->numBytes(), SQLITE_STATIC);
                sqlite3_step(statement);
                sqlite3_finalize(statement);

                return true;          
      } else {
                std::cout << (std::string)(sqlite3_errmsg(statisticsDBHandler)) << std::endl;
                sqlite3_finalize(statement);
                return false;
      }

}

std::vector<std::shared_ptr<TransformedSet>> 
StatisticsDB::getTransformedSets(std::pair<std::string, std::string> databaseAndSetName) {

    std::vector<std::shared_ptr<TransformedSet>>  ret;
    
    return ret;

}


long StatisticsDB::getLatestId (std::string tableName) {

     long id;
     sqlite3_stmt * statement;
     std::string queryString = "SELECT MAX(ID) from " + quoteStr(tableName);
     if (sqlite3_prepare_v2(statisticsDBHandler, queryString.c_str(), -1, &statement, NULL) == SQLITE_OK) {
         int res = sqlite3_step(statement);
         if (res == SQLITE_ROW) {
            id = sqlite3_column_int(statement, 0);
         } else {
            id = -1;
         }
     } else {
         std::cout << (std::string)(sqlite3_errmsg(statisticsDBHandler)) << std::endl;
         id = -1;
     }
     sqlite3_finalize(statement);
     return id;

}


long StatisticsDB::getLatestDataId (std::pair<std::string, std::string> databaseAndSetName) {
     long id;
     sqlite3_stmt * statement;
     std::string queryString = "SELECT MAX(ID) from DATA where DATABASE_NAME=" + quoteStr(databaseAndSetName.first) 
                                  + " AND SET_NAME=" + quoteStr(databaseAndSetName.second);
     std::cout << "Get Latest Data Id: " << queryString << std::endl;
     if (sqlite3_prepare_v2(statisticsDBHandler, queryString.c_str(), -1, &statement, NULL) == SQLITE_OK) {
         int res = sqlite3_step(statement);
         if (res == SQLITE_ROW) {
            id = sqlite3_column_int(statement, 0);
         } else {
            id = -1;
         }
     } else {
         std::cout << (std::string)(sqlite3_errmsg(statisticsDBHandler)) << std::endl;
         id = -1;
     }
     sqlite3_finalize(statement);
     return id; 
}


long StatisticsDB::getLatestTransformationId () {
     return getLatestId("DATA_TRANSFORMATION");
}


std::string StatisticsDB :: quoteStr(std::string& s) {

    return std::string("'") + s + std::string("'");

}

void StatisticsDB :: replaceStr (std::string& str,
                                       const std::string& oldStr,
                                       const std::string& newStr)
{
    std::string::size_type pos = 0u;
    while((pos = str.find(oldStr, pos)) != std::string::npos){
       str.replace(pos, oldStr.length(), newStr);
       pos += newStr.length();
    }
}


}


#endif 
