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

#ifndef QUERY_CLIENT
#define QUERY_CLIENT

#include "Set.h"
#include "SetIterator.h"
#include "Handle.h"
#include "PDBLogger.h"
#include "PDBVector.h"
#include "CatalogClient.h"
#include "DeleteSet.h"
#include "ExecuteQuery.h"
#include "RegisterReplica.h"
#include "TupleSetExecuteQuery.h"
#include "ExecuteComputation.h"
#include "QueryGraphAnalyzer.h"
#include "Computation.h"
namespace pdb {

class QueryClient {

public:
    QueryClient() {}

    // connect to the database
    QueryClient(int portIn,
                std::string addressIn,
                PDBLoggerPtr myLoggerIn,
                bool useScheduler = false)
        : myHelper(portIn, addressIn, myLoggerIn) {
        port = portIn;
        address = addressIn;
        myLogger = myLoggerIn;
        runUs = makeObject<Vector<Handle<QueryBase>>>();
        queryGraph = makeObject<Vector<Handle<Computation>>>();
        this->useScheduler = useScheduler;
    }

    ~QueryClient() {
        runUs = nullptr;
        queryGraph = nullptr;
    }


    int getPort () {
       return this->port;
    }

    std::string getAddress() {
       return this->address;
    }

    PDBLoggerPtr getLogger() {
       return this->myLogger;
    }


    // access a set in the database
    template <class Type>
    Handle<Set<Type>> getSet(std::string databaseName, std::string setName) {

// verify that the database and set work
#ifdef DEBUG_SET_TYPE
        std::string errMsg;

        std::string typeName = myHelper.getObjectType(databaseName, setName, errMsg);

        if (typeName == "") {
            std::cout << "I was not able to obtain the type for database set " << setName << "\n";
            myLogger->error("query client: not able to verify type: " + errMsg);
            Handle<Set<Type>> returnVal = makeObject<Set<Type>>(false);
            return returnVal;
        }

        if (typeName != getTypeName<Type>()) {
            std::cout << "Wrong type for database set " << setName << "\n";
            Handle<Set<Type>> returnVal = makeObject<Set<Type>>(false);
            return returnVal;
        }
#endif
        Handle<Set<Type>> returnVal = makeObject<Set<Type>>(databaseName, setName);
        return returnVal;
    }

    // get an iterator for a set in the database
    template <class Type>
    SetIterator<Type> getSetIterator(std::string databaseName, std::string setName) {

// verify that the database and set work
#ifdef DEBUG_SET_TYPE
        std::string errMsg;
        std::string typeName = myHelper.getObjectType(databaseName, setName, errMsg);

        if (typeName == "") {
            myLogger->error("query client: not able to verify type: " + errMsg);
            SetIterator<Type> returnVal;
            return returnVal;
        }
#endif
        // commented by Jia, below type check can not work with complex types such as
        // Vector<Handle<Foo>>
        /*
if (typeName != getTypeName <Type> ()) {
    std :: cout << "Wrong type for database set " << setName << "\n";
    SetIterator <Type> returnVal;
    return returnVal;
}
        */
        SetIterator<Type> returnVal(myLogger, port, address, databaseName, setName);
        return returnVal;
    }

    bool deleteSet(std::string databaseName, std::string setName) {
        // this is for query testing stuff
        return simpleRequest<DeleteSet, SimpleRequestResult, bool, String, String>(
            myLogger,
            port,
            address,
            false,
            124 * 1024,
            [&](Handle<SimpleRequestResult> result) {
                std::string errMsg;
                if (result != nullptr) {

                    // make sure we got the correct number of results
                    if (!result->getRes().first) {
                        errMsg = "Could not remove set: " + result->getRes().second;
                        myLogger->error("QueryErr: " + errMsg);
                        return false;
                    }

                    return true;
                }
                errMsg = "Error getting type name: got nothing back from catalog";
                return false;
            },
            databaseName,
            setName);
    }


    //to set query graph
    void setQueryGraph (Handle<Computation> querySink) {
        this->queryGraph->push_back(querySink);
        std::cout << "query graph size = " << this->queryGraph->size() << std::endl;
    }


    
    //to return TCAP string
    std::string getTCAP (std::vector<Handle<Computation>> & computations) {

        QueryGraphAnalyzer queryAnalyzer(this->queryGraph);
        std::string tcapString = queryAnalyzer.parseTCAPString();
        queryAnalyzer.parseComputations(computations);
        return tcapString; 
    }

    //to register a replica with statisticsDB
    bool registerReplica(std::pair<std::string, std::string> inputDatabaseAndSet,
                         std::pair<std::string, std::string> outputDatabaseAndSet,
                         int numPartitions,
                         int numNodes,
                         std::string type,
                         std::string tcap,
                         std::vector<Handle<Computation>> computations) {
         std::string errMsg;
         std::cout << "to register Replica at query cient: " << computations.size() << " computations" << std::endl; 
         return simpleRequest<RegisterReplica, SimpleRequestResult, bool>(
            myLogger,
            port,
            address,
            false,
            4 * 1024 * 1024,
            [&](Handle<SimpleRequestResult> result) {
                    if (result != nullptr) {
                        if (!result->getRes().first) {
                            errMsg = "Error in query: " + result->getRes().second;
                            myLogger->error("Error querying data: " + result->getRes().second);
                            return false;
                        }
                        return true;
                    }
                    errMsg = "Error getting type name: got nothing back from server";
                    return false;
            },
            inputDatabaseAndSet,
            outputDatabaseAndSet,
            numPartitions,
            numNodes,
            type,
            tcap,
            computations);

    }



    //to execute computations
    template <class... Types>
    bool executeComputations(std::string& errMsg,
                             Handle<Computation> firstParam,
                             Handle<Types>... args) {
        queryGraph->push_back(firstParam);
        return executeComputations(errMsg, args...);
    }

    bool executeComputations(std::string& errMsg) {

        // this is the request
        const UseTemporaryAllocationBlock myBlock{256 * 1024 * 1024};
        std::vector<Handle<Computation>> computations;
        std::string tcapString = getTCAP(computations);
        return executeComputations (errMsg,
                                    tcapString,
                                    computations);
    }

    bool executeComputations(std::string& errMsg, 
                             std::string tcapString,
                             std::vector<Handle<Computation>> computations) {

        Handle<Vector<Handle<Computation>>> computationsToSend =
            makeObject<Vector<Handle<Computation>>>();
        for (size_t i = 0; i < computations.size(); i++) {
            computationsToSend->push_back(computations[i]);
        }
        Handle<ExecuteComputation> executeComputation = makeObject<ExecuteComputation>(tcapString);

        // this call asks the database to execute the query, and then it inserts the result set name
        // within each of the results, as well as the database connection information

        // this is for query scheduling stuff
        if (useScheduler == true) {
            return simpleDoubleRequest<ExecuteComputation,
                                       Vector<Handle<Computation>>,
                                       SimpleRequestResult,
                                       bool>(
                myLogger,
                port,
                address,
                false,
                124 * 1024,
                [&](Handle<SimpleRequestResult> result) {
                    if (result != nullptr) {
                        if (!result->getRes().first) {
                            errMsg = "Error in query: " + result->getRes().second;
                            myLogger->error("Error querying data: " + result->getRes().second);
                            return false;
                        }
                        this->queryGraph = makeObject<Vector<Handle<Computation>>>();
                        return true;
                    }
                    errMsg = "Error getting type name: got nothing back from server";
                    this->queryGraph = makeObject<Vector<Handle<Computation>>>();
                    return false;


                },
                executeComputation,
                computationsToSend);


        } else {
            errMsg =
                "This query must be sent to QuerySchedulerServer, but it seems "
                "QuerySchedulerServer is not supported";
            this->queryGraph = makeObject<Vector<Handle<Computation>>>();
            return false;
        }
        this->queryGraph = makeObject<Vector<Handle<Computation>>>();
    }

    void setUseScheduler(bool useScheduler) {
        this->useScheduler = useScheduler;
    }

private:
    // how we connect to the catalog
    CatalogClient myHelper;

    // deprecated
    // this is the query graph we'll execute
    Handle<Vector<Handle<QueryBase>>> runUs;

    // JiaNote: the Computation-based query graph to execute
    Handle<Vector<Handle<Computation>>> queryGraph;


    // connection info
    int port;
    std::string address;

    // for logging
    PDBLoggerPtr myLogger;

    // JiaNote: whether to run in distributed mode
    bool useScheduler;
};
}

#endif
