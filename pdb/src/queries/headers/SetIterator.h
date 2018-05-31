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

#ifndef SET_ITER_H
#define SET_ITER_H

#include "OutputIterator.h"
#include "SetScan.h"
#include <snappy.h>

namespace pdb {

template <class OutType>
class SetIterator {

public:
    // constructor; this should only be used by the query client
    SetIterator(PDBLoggerPtr loggerIn,
                int portIn,
                std::string& serverNameIn,
                std::string& dbNameIn,
                std::string& setNameIn) {
        myLogger = loggerIn;
        port = portIn;
        serverName = serverNameIn;
        dbName = dbNameIn;
        setName = setNameIn;
        wasError = false;
    }

    SetIterator() {
        wasError = true;
    }

    ~SetIterator() {}

    // this basically sets up a connection to the server, and returns it
    OutputIterator<OutType> begin() {

        // if there was an error, just get outta here
        if (wasError) {
            std::cout << "You are trying to create an iterator when there was an error.\n";
            return OutputIterator<OutType>();
        }

        // establish a connection
        std::string errMsg;
        PDBCommunicatorPtr temp = std::make_shared<PDBCommunicator>();
        if (temp->connectToInternetServer(myLogger, port, serverName, errMsg)) {
            myLogger->error(errMsg);
            myLogger->error("output iterator: not able to connect to server.\n");
            return OutputIterator<OutType>();
        }

        // build the request
        const UseTemporaryAllocationBlock tempBlock{1024};
        Handle<SetScan> request = makeObject<SetScan>(dbName, setName);
        if (!temp->sendObject(request, errMsg)) {
            myLogger->error(errMsg);
            myLogger->error("output iterator: not able to send request to server.\n");
            return OutputIterator<OutType>();
        }
        PDB_COUT << "sent SetScan object to manager" << std::endl;
        return OutputIterator<OutType>(temp);
    }

    OutputIterator<OutType> end() {
        return OutputIterator<OutType>();
    }

private:
    // these are used so that the output knows how to connect to the server for iteration
    int port;
    std::string serverName;
    PDBLoggerPtr myLogger;

    // records the place where the input comes from
    std::string dbName;
    std::string setName;

    // true if there is an error
    bool wasError;

    // allows creation of these objects
    friend class QueryClient;
};
}

#endif
