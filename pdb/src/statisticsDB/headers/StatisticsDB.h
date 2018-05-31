#ifndef STATISTICS_DB_H
#define STATISTICS_DB_H


#include "TransformedSet.h"
#include "Configuration.h"
#include "Handle.h"
#include "PDBVector.h"
#include "Computation.h"
#include <vector>
#include <memory>
#include <sqlite3.h>

/* this class encapsulates a SQLite database to manage statistics
collected along with query execution */

namespace pdb {

class StatisticsDB {

public:

    /* constructor */
    StatisticsDB (ConfigurationPtr conf);

    /* destructor */
    ~StatisticsDB ();

    /*
     * open the database
     * @return, whether the operation is successful or not
     */
    bool openDB ();


    /*
     * close the database
     * @return, whether the operation is successful or not
     */
    bool closeDB ();


    /*
     * create the tables
     * @return, whether the creation is successful or not
     */
    bool createTables ();

    /*
     * execute the query
     * @param cmdString, the command;
     * @return: whether the query execution is successful or not
     */
    bool execDB (std::string cmdString);

    /*
     * create the directory
     * @return: whether the directory creation is successful or not
     */
    bool createDir ();

    /*
     * to add an entry to the data table
     * @param databaseName, the name of the database
     * @param setName, the name of the set
     * @param created_jobId, the job id that creates the set
     * @param setType, the type of the set
     * @param className, the type of the object class stored in the set
     * @param typeId, the registered id of the type
     * @param pageSize, the page size of the set (in megabytes)
     * @param id, the primary key of the entry
     * @return: whether the data entry creation is successful or not
     */
    bool createData (std::string databaseName,
                     std::string setName,
                     std::string created_jobId,
                     std::string setType,
                     std::string className,
                     int typeId,
                     size_t pageSize,
                     long & id);


    /*
     * to update the SIZE field and MODIFICATION_TIME field in the DATA table
     * @param id, the primary key of the entry to modify
     * @param size, the current size of the set
     * @return: whether the update is successful or not
     */
    bool updateDataForSize (long id, 
                            size_t size);

    /*
     * to update the IS_REMOVED field and MODIFICATION_TIME field in the DATA
     * @param id, the primary key of the entry to modify
     * @return: whether the update is successful or not
     */
    bool updateDataForRemoval (long id);


    /*
     * to add an entry to the data_transformation table
     * @param input_data_id, the id of the input set
     * @param output_data_id, the id of the output set
     * @param num_partitions, the number of partitions
     * @param num_nodes, the number of nodes
     * @param transformation_type, the type of transformation, e.g. Partition
     * @param tcap, the tcap string
     * @param computations, the computations associated with tcap string
     * @param id, the primary key of the entry
     * @return: whether the data is successful or not 
     */
    bool createDataTransformation (long input_data_id,
                                   long output_data_id,
                                   int num_partitions,
                                   int num_nodes,
                                   std::string transformationType,
                                   std::string tcap,
                                   Handle<Vector<Handle<Computation>>> computations,
                                   long& id);

    
     /*
      * given input set's database name and set name, 
      * @param databaseAndSetName, the identifier of the input set
      * @return: the list of sets that are transformed from the input set
      * return a list of sets that are transformed from the input set
      */
     std::vector<std::shared_ptr<TransformedSet>> 
        getTransformedSets (std::pair<std::string, std::string> databaseAndSetName);


     /* to get latest id for a specified table
      * @param tableName, the name of the table
      * @return, the last used id (primary key) for the given table
      */      
     long getLatestId (std::string tableName);


     /* to get latest data id for the specified data table
      * @return, the last used id for the data table
      */
     long getLatestDataId (std::pair<std::string, std::string> databaseAndSetName);


     /* to get latest transformation id for the data_transformation table
      * @return, the last used id for the transformation table
      */
     long getLatestTransformationId ();

protected:

    /* to quote a string
     * @param s, the string to be quoted
     * @return: the quoted string
     */
    std::string quoteStr(std::string& s);

    /* to find and replace a string
     * @param str, the string for replacement
     * @param oldStr, the string to be replaced in str
     * @param newStr, the string to replace oldStr in str
     */
    void replaceStr(std::string& str,
               const std::string& oldStr,
               const std::string& newStr);




private:

     //sqlite database file
     std::string pathToDBFile = "dbFile";

     //handler for the sqlite database instance
     sqlite3 * statisticsDBHandler = nullptr;

     //configuration
     ConfigurationPtr conf = nullptr;

     //data id
     long dataId = 0;

     //transformation id
     long transformationId = 0;

};

}

#endif
