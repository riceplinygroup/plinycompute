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
#ifndef PDB_STATISTICS_H
#define PDB_STATISTICS_H

//by Jia, May 2017

//A class to encapsulate statistics

#include <unordered_map>
#include <memory>


namespace pdb {

class Statistics;
typedef std :: shared_ptr<Statistics> StatisticsPtr;


struct DataStatistics {

    std :: string databaseName;
    std :: string setName;
    int numPages = 0;
    size_t pageSize = 0;
    int numTuples = 0;
    size_t avgTupleSize = 0;

};


class Statistics {

private:

    std :: unordered_map<std :: string, DataStatistics> dataStatistics;
    std :: unordered_map<std :: string, double> atomicComputationSelectivity;
    std :: unordered_map<std :: string, double> lambdaSelectivity;
    

public:

    //constructor    
    Statistics () {}
    
    //destructor
    ~Statistics () {}

    //remove set
    void removeSet (std :: string databaseName, std :: string setName) {
        std :: string key = databaseName+ ":" + setName;
        if (dataStatistics.count(key) > 0) {
            dataStatistics.erase(key);
        }
    }


    //to return number of pages of a set
    int getNumPages (std :: string databaseName, std :: string setName) {
        std :: string key = databaseName+":"+setName;
        if (dataStatistics.count(key) == 0) {
            return -1;
        } else {
            return dataStatistics[key].numPages;
        }
    }

    //to set number of pages of a set
    void setNumPages (std :: string databaseName, std :: string setName, int numPages) {
        std :: string key = databaseName + ":" + setName;
        dataStatistics[key].numPages = numPages;
    }

    //to return page size of a set
    size_t getPageSize (std :: string databaseName, std :: string setName) {
        std :: string key = databaseName+":"+setName;
        if (dataStatistics.count(key) == 0) {
            return -1;
        } else {
            return dataStatistics[key].pageSize;
        }
    }

    //to set page size of a set
    void setPageSize (std :: string databaseName, std :: string setName, size_t pageSize) {
        std :: string key = databaseName + ":" + setName;
        dataStatistics[key].pageSize = pageSize;
    }

    //to return number of tuples of a set
    int getNumTuples (std :: string databaseName, std :: string setName) {
        std :: string key = databaseName+":"+setName;
        if (dataStatistics.count(key) == 0) {
            return -1;
        } else {
            return dataStatistics[key].numTuples;
        }
    }

    //to set number of tuples of a set
    void setNumTuples (std :: string databaseName, std :: string setName, int numTuples) {
        std :: string key = databaseName + ":" + setName;
        dataStatistics[key].numTuples = numTuples;
    }

    //to return average tuple size of a set
    size_t getAvgTupleSize (std :: string databaseName, std :: string setName) {
        std :: string key = databaseName+":"+setName;
        if (dataStatistics.count(key) == 0) {
            return -1;
        } else {
            return dataStatistics[key].avgTupleSize;
        }
    }

    //to set average tuple size of a set
    void setAvgTupleSize (std :: string databaseName, std :: string setName, size_t avgTupleSize) {
        std :: string key = databaseName + ":" + setName;
        dataStatistics[key].avgTupleSize = avgTupleSize;
    }


    //to return selectivity of an atomic computation
    double getAtomicComputationSelectivity (std :: string atomicComputationType) {
       if (atomicComputationSelectivity.count(atomicComputationType) == 0) {
           return 0;
       } else {
           return atomicComputationSelectivity[atomicComputationType];
       }       
    }

    //to set selectivity for an atomic computation
    void setAtomicComputationSelectivity (std :: string atomicComputationType, double selectivity) {
       atomicComputationSelectivity[atomicComputationType] = selectivity;
    }

    //to return selectivity of a lambda
    double getLambdaSelectivity (std :: string lambdaType) {
       if (lambdaSelectivity.count(lambdaType) == 0) {
           return 0;
       } else {
           return lambdaSelectivity[lambdaType];
       }
    }

    //to set selectivity for a lambda
    void setLambdaSelectivity (std :: string lambdaType, double selectivity) {
        lambdaSelectivity[lambdaType] = selectivity;
    }

};

}

#endif
