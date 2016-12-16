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
 * File:   PDBCommunicator.h
 * Author: Chris
 * 
 * Created on September 26, 2015, 9:01 AM
 */

#ifndef PDBCOMMUNICATOR_H
#define	PDBCOMMUNICATOR_H

#include <memory>

#include "Handle.h"
#include "PDBLogger.h"
#include <stdlib.h>
#include <cstring>

// This class the encoding/decoding of IPC sockets messages in PDB 
namespace pdb {

// create a smart pointer for PDBCommunicator objects
class PDBCommunicator;
typedef std :: shared_ptr <PDBCommunicator> PDBCommunicatorPtr;

class PDBCommunicator {

public:

    // here are the ways that you can set up a PDBCommunicator...
    // the first two assume that the PDBCommunicator is on the server side:

    // #1, you can connect to a Unix domain socket that is pointed at a file (this returns a true
    // if there is an error, and in this case it sets errMsg to describe the error)
    bool pointToFile(PDBLoggerPtr logToMeIn, int socketFDIn, std :: string& errMsg);

    // #2, you can connect to an internet socket... 
    bool pointToInternet(PDBLoggerPtr logToMeIn, int socketFDIn, std :: string& errMs);

    // the next two assume we are on the client side:

    // #3 connect to a server on the internet...
    bool connectToInternetServer(PDBLoggerPtr logToMeIn, int portNumber, std :: string serverAddress,
            std :: string &errMsg);

    // #4, connect to a local server via a UNIX domain socket
    bool connectToLocalServer(PDBLoggerPtr logToMeIn, std :: string fName, std :: string& errMsg);

    // see the size of an object that someone is sending us; blocks until the next object shows up 
    size_t getSizeOfNextObject ();

    // gets the next object over the communicator... reads the object to the specified location
    template <class ObjType>
    Handle <ObjType> getNextObject (void *readToHere, bool &success, std :: string &errMsg);

    // gets the next object over the communicator... reads the object to a temporary allocator
    // with just enough RAM to store the object
    template <class ObjType>
    Handle <ObjType> getNextObject (bool &success, std :: string &errMsg);
    
    // sends an object over the communication channel... return false on error
    template <class ObjType>
    bool sendObject (Handle <ObjType> &sendMe, std :: string &errMsg);

    // sends a bunch of binary data over a channel
    bool sendBytes (void *data, size_t size, std :: string &errMsg);

    // receives a bunch of binary data over a channel
    bool receiveBytes (void *data, std :: string &errMsg);

    // note that the file descriptor corresponding to the socket is always closed by the destructor!
    virtual ~PDBCommunicator();

    // default constructor
    PDBCommunicator();

    // gets the type of the next object (calls getTypeID () on it)
    int16_t getObjectTypeID ();

    void setNeedsToDisconnect(bool option);

    int getSocketFD();

    bool isSocketClosed();

    bool isLongConnection();

    void setLongConnection(bool longConnection);

    bool reconnect( std::string & errMsg );

private:

    // write from start to end to the output socket
    bool doTheWrite(char *start, char *end);
    
    //read the message data from socket
    bool doTheRead(char *dataIn);

    // the size of the next message; keep these two declarations together so they can be read into at once!
    size_t msgSize;

    // this points to the location that we read/write from
    int socketFD;

    // for logging
    PDBLoggerPtr logToMe;

    // remember whether we have read the size of the next message or not
    bool readCurMsgSize;

    // record the type of the next object
    int16_t nextTypeID;

    // This is for automatic connection tear-down.
    // Moved this logic from Chris' message-based communication framework to here.
    bool needToSendDisconnectMsg;

    //JiaNote: to add logic to support long connection
    bool socketClosed;

    bool longConnection;


    //JiaNote: to add logic to support reconnection
    int portNumber;

    std :: string serverAddress;

    std :: string fileName;

    bool isInternet;

};

}

#include "CommunicatorTemplates.cc"

#endif	/* PDBCOMMUNICATOR_H */

