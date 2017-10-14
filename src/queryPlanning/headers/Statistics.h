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

// by Jia, May 2017

// A class to encapsulate statistics

#include <unordered_map>
#include <memory>
#include <pthread.h>


namespace pdb {

class Statistics;
typedef std::shared_ptr<Statistics> StatisticsPtr;


struct DataStatistics {

    std::string databaseName;
    std::string setName;
    int numPages = 0;
    size_t pageSize = 0;
    size_t numBytes = 0;
    int numTuples = 0;
    size_t avgTupleSize = 0;
};


class Statistics {

private:
    std::unordered_map<std::string, DataStatistics> dataStatistics;
    std::unordered_map<std::string, double> atomicComputationSelectivity;
    std::unordered_map<std::string, double> lambdaSelectivity;
    pthread_mutex_t mutex;

public:
    // constructor
    Statistics() {
        pthread_mutex_init(&mutex, nullptr);
    }

    // destructor
    ~Statistics() {
        pthread_mutex_destroy(&mutex);
    }

    // remove set
    void removeSet(std::string databaseName, std::string setName) {
        std::string key = databaseName + ":" + setName;
        if (dataStatistics.count(key) > 0) {
            dataStatistics.erase(key);
        }
    }


    // to return number of pages of a set
    int getNumPages(std::string databaseName, std::string setName) {
        std::string key = databaseName + ":" + setName;
        if (dataStatistics.count(key) == 0) {
            return 0;
        } else {
            return dataStatistics[key].numPages;
        }
    }

    // to set number of pages of a set
    void setNumPages(std::string databaseName, std::string setName, int numPages) {
        std::string key = databaseName + ":" + setName;
        pthread_mutex_lock(&mutex);
        dataStatistics[key].numPages = numPages;
        pthread_mutex_unlock(&mutex);
    }

    // to return page size of a set
    size_t getPageSize(std::string databaseName, std::string setName) {
        std::string key = databaseName + ":" + setName;
        if (dataStatistics.count(key) == 0) {
            return -1;
        } else {
            return dataStatistics[key].pageSize;
        }
    }

    // to set page size of a set
    void setPageSize(std::string databaseName, std::string setName, size_t pageSize) {
        std::string key = databaseName + ":" + setName;
        pthread_mutex_lock(&mutex);
        dataStatistics[key].pageSize = pageSize;
        pthread_mutex_unlock(&mutex);
    }

    // to return numBytes of a set
    size_t getNumBytes(std::string databaseName, std::string setName) {
        std::string key = databaseName + ":" + setName;
        if (dataStatistics.count(key) == 0) {
            return 0;
        } else {
            return dataStatistics[key].numBytes;
        }
    }

    // to set numBytes of a set
    void setNumBytes(std::string databaseName, std::string setName, size_t numBytes) {
        std::string key = databaseName + ":" + setName;
        pthread_mutex_lock(&mutex);
        dataStatistics[key].numBytes = numBytes;
        pthread_mutex_unlock(&mutex);
    }


    // to return number of tuples of a set
    int getNumTuples(std::string databaseName, std::string setName) {
        std::string key = databaseName + ":" + setName;
        if (dataStatistics.count(key) == 0) {
            return -1;
        } else {
            return dataStatistics[key].numTuples;
        }
    }

    // to set number of tuples of a set
    void setNumTuples(std::string databaseName, std::string setName, int numTuples) {
        std::string key = databaseName + ":" + setName;
        pthread_mutex_lock(&mutex);
        dataStatistics[key].numTuples = numTuples;
        pthread_mutex_unlock(&mutex);
    }

    // to return average tuple size of a set
    size_t getAvgTupleSize(std::string databaseName, std::string setName) {
        std::string key = databaseName + ":" + setName;
        if (dataStatistics.count(key) == 0) {
            return -1;
        } else {
            return dataStatistics[key].avgTupleSize;
        }
    }

    // to set average tuple size of a set
    void setAvgTupleSize(std::string databaseName, std::string setName, size_t avgTupleSize) {
        std::string key = databaseName + ":" + setName;
        pthread_mutex_lock(&mutex);
        dataStatistics[key].avgTupleSize = avgTupleSize;
        pthread_mutex_unlock(&mutex);
    }


    // to return selectivity of an atomic computation
    double getAtomicComputationSelectivity(std::string atomicComputationType) {
        if (atomicComputationSelectivity.count(atomicComputationType) == 0) {
            return 0;
        } else {
            return atomicComputationSelectivity[atomicComputationType];
        }
    }

    // to set selectivity for an atomic computation
    void setAtomicComputationSelectivity(std::string atomicComputationType, double selectivity) {
        pthread_mutex_lock(&mutex);
        atomicComputationSelectivity[atomicComputationType] = selectivity;
        pthread_mutex_unlock(&mutex);
    }

    // to return selectivity of a lambda
    double getLambdaSelectivity(std::string lambdaType) {
        if (lambdaSelectivity.count(lambdaType) == 0) {
            return 0;
        } else {
            return lambdaSelectivity[lambdaType];
        }
    }

    // to set selectivity for a lambda
    void setLambdaSelectivity(std::string lambdaType, double selectivity) {
        pthread_mutex_lock(&mutex);
        lambdaSelectivity[lambdaType] = selectivity;
        pthread_mutex_unlock(&mutex);
    }
};
}

#endif
