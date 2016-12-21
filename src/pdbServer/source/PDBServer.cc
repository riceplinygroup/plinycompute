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

#ifndef PDB_SERVER_CC
#define PDB_SERVER_CC

#include "BuiltInObjectTypeIDs.h"
#include "Handle.h"
#include "PDBAlarm.h"
#include <iostream>
#include <netinet/in.h>
#include "PDBServer.h"
#include "PDBWorker.h"
#include "ServerWork.h"
#include <signal.h>
#include <sys/socket.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h> 
#include <sys/un.h>
#include <unistd.h>
#include <signal.h>
#include "PDBCommunicator.h"
#include "CloseConnection.h"
#include "ShutDown.h"
#include "ServerFunctionality.h"
#include "UseTemporaryAllocationBlock.h"
#include "SimpleRequestResult.h"
#include <memory>

namespace pdb {

// Constructor for a server passing the port and hostname as args
PDBServer::PDBServer(int portNumberIn, int numConnectionsIn, PDBLoggerPtr myLoggerIn) {

    // remember the communication data
    portNumber = portNumberIn;
    numConnections = numConnectionsIn;
    myLogger = myLoggerIn;
    isInternet = true;
    allDone = false;
    struct sigaction sa;
    memset (&sa, '\0', sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, 0);
    // create the workers
    myWorkers = make_shared <PDBWorkerQueue> (myLogger, numConnections);
}

PDBServer::PDBServer (string unixFileIn, int numConnectionsIn, PDBLoggerPtr myLoggerIn) {

    // remember the communication data
    unixFile = unixFileIn;
    numConnections = numConnectionsIn;
    myLogger = myLoggerIn;
    isInternet = false;
    allDone = false;
    struct sigaction sa;
    memset (&sa, '\0', sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, 0);
    // create the workers
    myWorkers = make_shared <PDBWorkerQueue> (myLogger, numConnections);
}

void PDBServer::registerHandler(int16_t requestID, PDBCommWorkPtr handledBy) {
    handlers[requestID] = handledBy;
}

// this is the entry point for the listener to the port 

void *callListen(void *serverInstance) {
    PDBServer *temp = static_cast<PDBServer *> (serverInstance);
    temp->listen();
    return nullptr;
}

void PDBServer::listen() {

    string errMsg;

    // two cases: first, we are connecting to the internet
    if (isInternet) {

        // wait for an internet socket
        sockFD = socket(AF_INET, SOCK_STREAM, 0);

        // added by Jia to avoid TimeWait state for old sockets
        int optval = 1;
        if (setsockopt(sockFD, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
            myLogger->error("PDBServer: couldn't setsockopt");
            myLogger->error(strerror(errno));
            std :: cout << "PDBServer: couldn't setsockopt:" << strerror(errno) << std :: endl;
            close(sockFD);
            exit(0);
        }


        if (sockFD < 0) {
            myLogger->error("PDBServer: could not get FD to internet socket");
            myLogger->error(strerror(errno));
            close(sockFD);
            exit(0);
        }

        // bind the socket FD
        struct sockaddr_in serv_addr;
        bzero((char *) &serv_addr, sizeof (serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(portNumber);
        int retVal = ::bind(sockFD, (struct sockaddr *) &serv_addr, sizeof (serv_addr));
        if (retVal < 0) {
            myLogger->error("PDBServer: could not bind to local socket");
            myLogger->error(strerror(errno));
            close(sockFD);
            exit(0);
        }

        myLogger->trace("PDBServer: about to listen to the Internet for a connection");

        // set the backlog on the socket
        if (::listen(sockFD, 100) != 0) {
            myLogger->error("PDBServer: listen error");
            myLogger->error(strerror(errno));
            close(sockFD);
            exit(0);
        }

        myLogger->trace("PDBServer: ready to go!");

        // wait for someone to try to connect
        while (!allDone) {
            PDBCommunicatorPtr myCommunicator = make_shared <PDBCommunicator> ();
            if (myCommunicator->pointToInternet(myLogger, sockFD, errMsg)) {
                myLogger->error("PDBServer: could not point to an internet socket: " + errMsg);
                continue;
            }
            myLogger->info(std::string("accepted the connection with sockFD=")+std::to_string(myCommunicator->getSocketFD()));
            std :: cout << "||||||||||||||||||||||||||||||||||" << std :: endl;
            std :: cout << "accepted the connection with sockFD=" << myCommunicator->getSocketFD() << std :: endl;
            handleRequest(myCommunicator);
        }

        // second, we are connecting to a local UNIX socket
    } else {

        myLogger->trace("PDBServer: getting socket to file");
        sockFD = socket(PF_UNIX, SOCK_STREAM, 0);

        if (sockFD < 0) {
            myLogger->error("PDBServer: could not get FD to local socket");
            myLogger->error(strerror(errno));
            exit(0);
        }

        // bind the socket FD
        struct sockaddr_un serv_addr;
        bzero((char *) &serv_addr, sizeof (serv_addr));
        serv_addr.sun_family = AF_UNIX;
        snprintf(serv_addr.sun_path, sizeof (serv_addr.sun_path), "%s", unixFile.c_str());
        
        if (::bind(sockFD, (struct sockaddr *) &serv_addr, sizeof (struct sockaddr_un))) {
            myLogger->error("PDBServer: could not bind to local socket");
            myLogger->error(strerror(errno));
            //if pathToBackEndServer exists, delete it.
            if( unlink(unixFile.c_str()) == 0) {
                cout << "Removed outdated "<<unixFile.c_str()<<".\n";
            } 
            if (::bind(sockFD, (struct sockaddr *) &serv_addr, sizeof (struct sockaddr_un))) {
                myLogger->error("PDBServer: still could not bind to local socket after removing unixFile");
                myLogger->error(strerror(errno));
                exit(0);
            }
        }
        
        myLogger->debug("PDBServer: socket has name");
        myLogger->debug(serv_addr.sun_path);

        myLogger->trace("PDBServer: about to listen to the file for a connection");

        // set the backlog on the socket
        if (::listen(sockFD, 100) != 0) {
            myLogger->error("PDBServer: listen error");
            myLogger->error(strerror(errno));
            exit(0);
        }

        myLogger->trace("PDBServer: ready to go!");

        // wait for someone to try to connect
        while (!allDone) {
            PDBCommunicatorPtr myCommunicator;
	    myCommunicator = make_shared <PDBCommunicator> ();
            if (myCommunicator->pointToFile(myLogger, sockFD, errMsg)) {
                myLogger->error("PDBServer: could not point to an local UNIX socket: " + errMsg);
                continue;
            }
            std :: cout << "||||||||||||||||||||||||||||||||||" << std :: endl;
            std :: cout << "accepted the connection with sockFD=" << myCommunicator->getSocketFD() << std :: endl;
            handleRequest(myCommunicator);
        }
    }
    // let the main thread know we are done
    allDone = true;
}

//gets access to worker queue
PDBWorkerQueuePtr PDBServer::getWorkerQueue() {
        return this->myWorkers;
}

//gets access to logger
PDBLoggerPtr PDBServer::getLogger() {
        return this->myLogger;
}

void PDBServer::handleRequest(PDBCommunicatorPtr myCommunicator) {

    ServerWorkPtr tempWork{make_shared <ServerWork> (*this)};
    tempWork->setGuts(myCommunicator);
    PDBWorkerPtr tempWorker = myWorkers->getWorker();
    tempWorker->execute(tempWork, tempWork->getLinkedBuzzer());
}

// returns true while we need to keep going... false when this connection is done
bool PDBServer::handleOneRequest(PDBBuzzerPtr callerBuzzer, PDBCommunicatorPtr myCommunicator) {

    // figure out what type of message the client is sending us
    int16_t requestID = myCommunicator->getObjectTypeID ();
    string info;
    bool success;

    // if there was a request to close the connection, just get outta here
    if (requestID == CloseConnection_TYPEID) {
	UseTemporaryAllocationBlock tempBlock {2048};
        Handle <CloseConnection> closeMsg = myCommunicator->getNextObject <CloseConnection> (success, info);
        if (!success) {
            myLogger->error("PDBServer: close connection request, but was an error: " + info);
        } else {
            myLogger->trace("PDBServer: close connection request");
        }
        return false;
    }

    if (requestID == NoMsg_TYPEID) {
        string err, info;
        myLogger->trace("PDBServer: the other side closed the connection");
        return false;
    }

    // if we are asked to shut down...
    if (requestID == ShutDown_TYPEID) {
	UseTemporaryAllocationBlock tempBlock {2048};
        Handle <ShutDown> closeMsg = myCommunicator->getNextObject <ShutDown> (success, info);
        if (!success) {
            myLogger->error("PDBServer: close connection request, but was an error: " + info);
        } else {
            myLogger->trace("PDBServer: close connection request");
        }
        std :: cout << "Cleanup server functionalities" << std :: endl;
        // for each functionality, invoke its clean() method
        for (int i = 0; i < allFunctionalities.size(); i++) {
            allFunctionalities.at(i)->cleanup();
        }


	// ack the result
	std :: string errMsg;
	Handle <SimpleRequestResult> result = makeObject <SimpleRequestResult> (true, "successful shutdown of server");
	if (!myCommunicator->sendObject (result, errMsg)) {
            myLogger->error("PDBServer: close connection request, but count not send response: " + errMsg);
	}

        // kill the FD and let everyone know we are done
        allDone = true;
        //close(sockFD); //we can't simply close socket like this, because there are still incoming messages in accepted connections
                         //use reuse address option instead 
        return false;

    } 

    // and get a worker plus the appropriate work to service it
    if (handlers.count(requestID) == 0) {

        // there is not one, so send back an appropriate message
        myLogger->error("PDBServer: could not find an appropriate handler");
	return false;

    // in this case, got a handler
    } else {

        /*// get the handler
        myLogger->writeLn("PDBServer: found an appropriate handler");
        myLogger->writeLn("PDBServer: getting a worker...");

        //should comment out following lines to recover Chris' old code;
        PDBCommWorkPtr tempWork = handlers[requestID]->clone();
        myLogger->writeLn("PDBServer: setting guts");
	tempWork->setGuts (myCommunicator);
        tempWork->execute(callerBuzzer);*/

        //End code replacement for testing

        //Chris' old code: (Observed problem: sometimes, buzzer never get buzzed.)
        // get a worker to run the handler (this blocks if no workers available)
        PDBWorkerPtr tempWorker = myWorkers->getWorker();
        myLogger->trace("PDBServer: got a worker, start to do something...");
        myLogger->trace("PDBServer: requestID "+ std :: to_string (requestID));

        PDBCommWorkPtr tempWork = handlers[requestID]->clone();

        myLogger->trace("PDBServer: setting guts");
        tempWork->setGuts(myCommunicator);
        tempWorker->execute(tempWork, callerBuzzer);
        callerBuzzer->wait();
        myLogger->trace("PDBServer: handler has completed its work");
	return true;

    }

}

void PDBServer::signal(PDBAlarm signalWithMe) {
    myWorkers->notifyAllWorkers(signalWithMe);
}

void PDBServer::startServer(PDBWorkPtr runMeAtStart) {

    // ignore broken pipe signals
    ::signal(SIGPIPE, SIG_IGN);

    // if there was some work to execute to start things up, then do it
    if (runMeAtStart != nullptr) {
        PDBBuzzerPtr buzzMeWhenDone = runMeAtStart->getLinkedBuzzer();
        PDBWorkerPtr tempWorker = myWorkers->getWorker();
        tempWorker->execute(runMeAtStart, buzzMeWhenDone);
        buzzMeWhenDone->wait();
    }

    // listen to the socket
    int return_code = pthread_create(&listenerThread, nullptr, callListen, this);
    if (return_code) {
    	myLogger->error("ERROR; return code from pthread_create () is " + to_string(return_code) );
        exit(-1);
    }

    // and now just sleep 
    while (!allDone) {
        sleep(1);
    }
}

void PDBServer :: registerHandlersFromLastFunctionality () {
	allFunctionalities[allFunctionalities.size () - 1]->recordServer (*this);
        allFunctionalities[allFunctionalities.size () - 1]->registerHandlers (*this);
}

void PDBServer::stop() {
    allDone = true;
}

}

#endif

