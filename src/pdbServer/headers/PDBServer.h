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
 * File:   PDBServer.h
 * Author: Chris
 *
 * Created on September 25, 2015, 5:04 PM
 */

#ifndef PDBSERVER_H
#define	PDBSERVER_H

#include <memory>
namespace pdb {

// create a smart pointer for PDBServer objects
class PDBServer;
typedef std :: shared_ptr <PDBServer> PDBServerPtr;

}

#include "PDBAlarm.h"
#include "PDBCommWork.h"
#include "PDBLogger.h"
#include "PDBWork.h"
#include "PDBCommunicator.h"
#include <string>
#include <map>

// This class encapsulates a multi-threaded sever in PDB.  The way it works is that one simply registers 
// an event handler (encapsulated inside of a PDBWorkPtr); whenever there is a new connection that comes
// over the given port (or file in the case of a local socket) a PWBWorker is asked to handle the 
// connection using the appropriate port using a cloned version of the specified PDBWork object.
// 

namespace pdb {

class ServerFunctionality;

class PDBServer {
public:

    // server set up for comminication over the internet, with the specified number of threads 
    // (PDBWorker objects) to handle connections to the server.  
    PDBServer(int portNumberIn, int numConnections, PDBLoggerPtr myLogger);

    // server set up for communication using the local file system, with the specified number of threads
    // to handle connections to the server.  Log to the specified logger.
    PDBServer(string fileName, int numConnections, PDBLoggerPtr myLogger);

    // a server has many possible functionalities... storage, catalog client, query planning, etc.
    // to create and add a functionality, call this.  The Functionality class must derive from the
    // ServerFunctionality class, which means that it must implement the pure virtual function
    // RegisterHandlers (PDBServer &) that registers any special handlers that the class needs in order to
    // perform its required tasks
    template <class Functionality, class... Args> 
    void addFunctionality (Args&&... args);

    // gets access to a particular functionality... this might be called (for example) 
    template <class Functionality>
    Functionality &getFunctionality ();

    // asks the server to handle a particular request coming over the wire with the particular work type
    void registerHandler(int16_t typeID, PDBCommWorkPtr handledBy);

    // like registerHandler but repeat the work in a time interval
    // TODO: to be implemented later.
    //  void registerTimedHandler (uint32_t intervalInMilliseconds, PDBWorkPtr handledBy);

    // starts the server---this creates all of the threads and lets the server start taking requests; this
    // call will never return.  Note that if runMeAtStart is not null, then runMeAtStart is executed 
    // before the server starts handling requests
    void startServer(PDBWorkPtr runMeAtStart);

    // asks the server to signal all of the threads activily handling connections that a certain event
    // has occured; this effectively just has us call PDBWorker.signal (signalWithMe) for all of the 
    // workers that are currently handling requests.  Any that indicate that they have died as a result
    // of the signal are forgotten (allowed to go out of scope) and then replaced with a new PDBWorker 
    // object
    void signal(PDBAlarm signalWithMe);

    // tell the server to start listening for people who want to connect
    void listen();

    // asks us to handle one request that is coming over the given PDBCommunicator; return true if this
    // is not the last request over this PDBCommunicator object; buzzMeWhenDone is sent to the worker that
    // is spawned to handle the request
    bool handleOneRequest(PDBBuzzerPtr buzzMeWhenDone, PDBCommunicatorPtr myCommunicator);

    void stop(); //added by Jia

    // Someone added this, but it is BAD!!  This should not be exposed
    // Jia: I understand it is bad, however we need to create threads in a handler, and I feel you do
    //      not want multiple worker queue in one process. So I temprarily enabled this...
    PDBWorkerQueuePtr getWorkerQueue();

    //gets access to logger
    PDBLoggerPtr getLogger(); //added by Jia

private:

    // used to ask the most recently-added functionality to register its handlers
    void registerHandlersFromLastFunctionality ();

    // when we get a message over the input socket, we'll handle it using the registered handler
    map <int16_t, PDBCommWorkPtr> handlers;

    // this is where all of our workers to handle the server requests live
    PDBWorkerQueuePtr myWorkers;

    // handles a request using the given PDBCommunicator to obtain the data
    void handleRequest(PDBCommunicatorPtr myCommunicator);

    // true when the server is done
    bool allDone;

    // the port number for an internet server
    int portNumber;

    // the number of connections to allow simultanously (a new thread is created for each of these)
    int numConnections;

    // the file to use for a UNIX file-based connection
    string unixFile;

    // where to log to
    PDBLoggerPtr myLogger;

    // true if this is an internet server
    bool isInternet;

    // used to run the server
    pthread_t listenerThread;

    // this is the socket we are listening to
    int sockFD;

    // this maps the name of a functionality class to a position
    std :: map <std :: string, int> allFunctionalityNames;

    // this gives us each of the functionalities that the server can perform
    std :: vector <shared_ptr <ServerFunctionality>> allFunctionalities;
};

}

#include "ServerTemplates.cc"

#endif	/* PDBSERVER_H */

