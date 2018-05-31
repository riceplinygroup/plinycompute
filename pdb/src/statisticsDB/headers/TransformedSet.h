#ifndef TRANSFORMED_SET_H
#define TRANSFORMED_SET_H

#include <cstring>
#include <string>

namespace pdb {


//this class encapsulates the information for a transformed set

class TransformedSet {

public:

    /* constructor
     * @param databaseName, the name of database
     * @param setName, the name of set
     * @param transformationType, the type for the transformation
     * @param tcap, the tcap for the transformation
     * @param numPartitions, the number of partitions
     */
    TransformedSet (std::string databaseName,
                    std::string setName,
                    std::string transformationType,
                    std::string tcap,
                    int numPartitions);

    /* destructor */
    ~TransformedSet ();

    /*
     * return: databaseName
     */
    std::string getDatabaseName();

    /*
     * return: setName
     */
    std::string getSetName();

    /*
     * return: transformationType
     */
    std::string getTransformationType();


    /*
     * return: tcap
     */
    std::string getTCAP();


    /*
     * return: numPartitions
     */
    int getNumPartitions();

private:

   //database name
   std::string databaseName;

   //set name
   std::string setName;

   //transformation type
   std::string transformationType;

   //transformation tcap
   std::string tcap;

   //number of partitions;
   int numPartitions;


};





}



#endif
