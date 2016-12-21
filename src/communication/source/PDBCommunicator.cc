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


#ifndef PDB_COMMUN_C
#define PDB_COMMUN_C

#include "BuiltInObjectTypeIDs.h"
#include "Handle.h"
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include "Object.h"
#include "PDBVector.h"
#include "CloseConnection.h"
#include "UseTemporaryAllocationBlock.h"
#include "InterfaceFunctions.h"
#include "PDBCommunicator.h"
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <unistd.h>


#define MAX_RETRIES 5


namespace pdb {

PDBCommunicator::PDBCommunicator() {
    readCurMsgSize = false;
    socketFD = -1;
    nextTypeID = NoMsg_TYPEID;
    socketClosed = true;
    //Jia: moved this logic from Chris' message-based communication framework to here
    needToSendDisconnectMsg = false;
    longConnection = false;
}

bool PDBCommunicator::pointToInternet(PDBLoggerPtr logToMeIn, int socketFDIn, std :: string& errMsg) {

    // first, connect to the backend
    logToMe = logToMeIn;

    struct sockaddr_in cli_addr;
    socklen_t clilen = sizeof (cli_addr);
    bzero((char *) &cli_addr, sizeof (cli_addr));
    logToMe->info("PDBCommunicator: about to wait for request from Internet");
    socketFD = accept(socketFDIn, (struct sockaddr *) &cli_addr, &clilen);
    if (socketFD < 0) {
        logToMe->error("PDBCommunicator: could not get FD to internet socket");
        logToMe->error(strerror(errno));
        errMsg = "Could not get socket ";
        errMsg += strerror(errno);
        close(socketFD);
        socketFD = -1;
        return true;
    }
    socketClosed = false;
    logToMe->info("PDBCommunicator: got request from Internet");
    return false;
}

bool PDBCommunicator::connectToInternetServer(PDBLoggerPtr logToMeIn, int portNumber, std :: string serverAddress,
        std :: string &errMsg) {

    logToMe = logToMeIn;
    //std :: cout << "################################" << std :: endl;
    //std :: cout << "To connect to Internet server..." << std :: endl;
    //std :: cout << "portNumber=" << portNumber << std :: endl;
    //std :: cout << "serverAddress=" << serverAddress << std :: endl;
    // set up the socket
    //struct sockaddr_in serv_addr;
    //struct hostent *server;
    /*
    socketFD = socket(AF_INET, SOCK_STREAM, 0);
    std :: cout << "socketFD=" << socketFD << std :: endl;
    if (socketFD < 0) {
        logToMe->error("PDBCommunicator: could not get FD to internet socket");
        logToMe->error(strerror(errno));
        errMsg = "Could not get socket to backend ";
        errMsg += strerror(errno);
        return true;
    }

    logToMe->trace("PDBCommunicator: Got internet socket");
    logToMe->trace("PDBCommunicator: About to check the database for the host name");
    */
    /* CHRIS NOTE: turns out that gethostbyname () is depricated, and should be replaced */
    //std :: cout << "Address cstring=" << serverAddress.c_str() << std :: endl;
    //server = gethostbyname(serverAddress.c_str());
    //std :: cout << "h_name=" << server->h_name << std :: endl;
    //std :: cout << "h_addr_list[0]=" << server->h_addr_list[0] << std :: endl;
    //std :: cout << "h_addr=" << server->h_addr << std :: endl;
    /*if (server == nullptr) {
        logToMe->error("PDBCommunicator: could not get host by name");
        logToMe->error(strerror(errno));
        errMsg = "Could not get host by name ";
        errMsg += strerror(errno);
        return true;
    }*/

    logToMe->trace("PDBCommunicator: About to connect to the remote host");

    /*
    bzero((char *) &serv_addr, sizeof (serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr, (char *) &serv_addr.sin_addr.s_addr, server->h_length);
    std :: cout << "copied to address=" << (char *) inet_ntoa(serv_addr.sin_addr) << std :: endl;
    serv_addr.sin_port = htons(portNumber);
    if (::connect(socketFD, (struct sockaddr *) &serv_addr, sizeof (serv_addr)) < 0) {
        logToMe->error("PDBCommunicator: could not get host by name");
        logToMe->error(strerror(errno));
        errMsg = "Could not connect to server ";
        errMsg += strerror(errno);
        return true;
    }
    */

    // Jia: gethostbyname() has multi-threading issue, to replace it with getaddrinfo()

    struct addrinfo hints;
    struct addrinfo * result, * rp;
    char port[10];
    sprintf(port, "%d", portNumber);

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0;
    hints.ai_protocol = 0;

    int s = getaddrinfo(serverAddress.c_str(), port, &hints, &result);
    if (s != 0) {
        logToMe->error("PDBCommunicator: could not get addr info");
        logToMe->error(strerror(errno));
        errMsg = "Could not get addr info ";
        errMsg += strerror(errno);
        std :: cout << errMsg << std :: endl;
        socketClosed = true;
        return true;
    }

    bool connected = false;
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        int count = 0;
        while (count <= MAX_RETRIES) {
            logToMe->trace("PDBCommunicator: creating socket....");
            socketFD = socket (rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (socketFD == -1) {
                continue;
            }
            if (::connect(socketFD, rp->ai_addr, rp->ai_addrlen) != -1) {
                connected = true;
                break;
            }
            count ++;
            std :: cout << "Connection error, to retry..." << std :: endl;
            sleep (1);
            close(socketFD);
            socketFD = -1;
       }
       if (connected == true) {
           break;
       }
       
    }

    if (rp == NULL) {
        logToMe->error("PDBCommunicator: could not connect to server: address info is null");
        logToMe->error(strerror(errno));
        errMsg = "Could not connect to server: address info is null with ip="+serverAddress+", and port="+port;
        errMsg += strerror(errno);
        std :: cout << errMsg << std :: endl;
        socketClosed = true;
        return true;

    }

    freeaddrinfo(result);
    // Jia: moved automatic tear-down logic from Chris' message-based communication to here
    // note that we need to close this up when we are done
    needToSendDisconnectMsg = true;
    isInternet = true;
    this->portNumber = portNumber;
    this->serverAddress = serverAddress; 
    socketClosed = false;
    logToMe->trace("PDBCommunicator: Successfully connected to the remote host");
    logToMe->trace("PDBCommunicator: Socket FD is " + std :: to_string (socketFD));
/*    std :: cout << "##########################" << std :: endl;
    std :: cout << "Connected to server with port =" << portNumber <<", address =" << serverAddress << ", socket=" << socketFD << std :: endl;
    std :: cout << "==========================" << std :: endl;
*/
    return false;
}

void PDBCommunicator::setNeedsToDisconnect(bool option){
    needToSendDisconnectMsg = option;
}

bool PDBCommunicator::connectToLocalServer(PDBLoggerPtr logToMeIn, std :: string fName, std :: string& errMsg) {

    logToMe = logToMeIn;
    struct sockaddr_un server;
    //TODO: add retry logic here
    socketFD = socket(AF_UNIX, SOCK_STREAM, 0);
    if (socketFD < 0) {
        logToMe->error("PDBCommunicator: could not get FD to local server socket");
        logToMe->error(strerror(errno));
        errMsg = "Could not get FD to local server socket ";
        errMsg += strerror(errno);
        close(socketFD);
        socketFD = -1;
        socketClosed = true;
        return true;
    }

    //std :: cout << "In here!!\n";

    server.sun_family = AF_UNIX;
    strcpy(server.sun_path, fName.c_str());
    if (::connect(socketFD, (struct sockaddr *) &server, sizeof (struct sockaddr_un)) < 0) {
        logToMe->error("PDBCommunicator: could not connect to local server socket");
        logToMe->error(strerror(errno));
        errMsg = "Could not connect to local server socket ";
        errMsg += strerror(errno);
        close(socketFD);
        socketFD = -1;
        socketClosed = true;
        return true;
    }

    // Jia: moved automatic tear-down logic from Chris' message-based communication to here
    // note that we need to close this up when we are done
    needToSendDisconnectMsg = true;
    isInternet = false;
    fileName = fName; 
    //std :: cout << "Connected!!\n";
    socketClosed = false;
    return false;
}

bool PDBCommunicator::pointToFile(PDBLoggerPtr logToMeIn, int socketFDIn, std :: string& errMsg) {

    // connect to the backend
    logToMe = logToMeIn;

    logToMe->trace("PDBCommunicator: about to wait for request from same machine");
    socketFD = accept(socketFDIn, 0, 0);
    if (socketFD < 0) {
        logToMe->error("PDBCommunicator: could not get FD to local socket");
        logToMe->error(strerror(errno));
        errMsg = "Could not get socket ";
        errMsg += strerror(errno);
        close(socketFD);
        socketFD = -1;
        socketClosed = true;
        return true;
    }

    logToMe->trace("PDBCommunicator: got request from same machine");
    socketClosed = false;
    return false;
}

PDBCommunicator::~PDBCommunicator() {

    // Jia: moved below logic from Chris' message-based communication to here.
    // tell the server that we are disconnecting (note that needToSendDisconnectMsg is
    // set to true only if we are a client and we want to close a connection to the server
#ifdef __APPLE__    
    if (needToSendDisconnectMsg && socketFD > 0) {
        const UseTemporaryAllocationBlock tempBlock{1024};
        Handle <CloseConnection> temp = makeObject <CloseConnection> ();
        logToMe->trace("PDBCommunicator: closing connection to the server");
	std :: string errMsg;
	if (!sendObject (temp, errMsg)) {
	    logToMe->trace("PDBCommunicator: could not send close connection message");
	}

    }

    if (socketFD >= 0) {
        close(socketFD);
        socketClosed = true;
        socketFD = -1;
    }
#else
    
  
    if (needToSendDisconnectMsg && socketFD >= 0) {
        close(socketFD);
        socketFD = -1;
    } else if (!needToSendDisconnectMsg && socketFD >= 0) {
        shutdown(socketFD, SHUT_WR);
        //below logic doesn't work!
        /*
        char c;
        ssize_t res = recv(socketFD, &c, 1, MSG_PEEK);
        if (res == 0) {
            std :: cout << "server socket closed" << std :: endl;
        } else {
            std :: cout << "there is some error in the socket" << std :: endl;
        }
        */
        close(socketFD);
        socketFD = -1;
    }
    socketClosed =true;
#endif

}

int PDBCommunicator::getSocketFD() {
   return socketFD;
}

int16_t PDBCommunicator::getObjectTypeID () {

	// if we do not know the next type ID, then get it
        if (!readCurMsgSize) {
            getSizeOfNextObject ();
        }
	return nextTypeID;
}

size_t PDBCommunicator::getSizeOfNextObject () {

    // if we have previously gotten the size, just return it
    if (readCurMsgSize) {
        logToMe->debug("getSizeOfNextObject: we've done this before");
	return msgSize;
    }

    // make sure we got enough bytes... if we did not, then error out
    // JIANOTE: we may not receive all the bytes at once, so we need a loop
    int receivedBytes = 0;
    int receivedTotal = 0;
    int bytesToReceive = (int)(sizeof(int16_t)); 
    int retries = 0;
    while (receivedTotal < (int)(sizeof(int16_t))) {
        if ((receivedBytes = read(socketFD, (char *) ((char *)(&nextTypeID)+receivedTotal*sizeof(char)), bytesToReceive)) < 0) {
            std :: string errMsg = std::string("PDBCommunicator: could not read next message type") + strerror(errno);
            logToMe->error(errMsg);
            std :: cout << errMsg << std :: endl;
            nextTypeID = NoMsg_TYPEID;
            msgSize = 0;
            close(socketFD);
            socketFD = -1;
            if (longConnection) {
                std :: cout << "############################################" << std :: endl;
                std :: cout << "WARNING: LONG CONNECTION CLOSED DUE TO READ ERROR" << std :: endl;
                std :: cout << "############################################" << std :: endl;
            } else {
                std :: cout << "############################################" << std :: endl;
                std :: cout << "WARNING: CONNECTION CLOSED DUE TO READ ERROR" << std :: endl;
                std :: cout << "############################################" << std :: endl;
            }
            socketClosed = true;
            return 0;
         } else if (receivedBytes == 0) {
            logToMe->info("PDBCommunicator: the other side closed the socket when we try to read the type");
            nextTypeID = NoMsg_TYPEID;
            std :: cout << "PDBCommunicator: the other side closed the socket when we try to get next type" << std :: endl;

            //if (retries < MAX_RETRIES) {
            if (retries < 0) {
                retries ++;
                logToMe->info("PDBCommunicator: Retry to see whether network can recover");
                std :: cout << "PDBCommunicator: Retry to see whether network can recover" << std :: endl;
                continue;
            } else {
                 if (longConnection) {
                     std :: cout << "############################################" << std :: endl;
                     std :: cout << "WARNING: LONG CONNECTION CLOSED DUE TO READ ERROR AFTER RETRY" << std :: endl;
                     std :: cout << "############################################" << std :: endl;
                 }
                 close(socketFD);
                 socketFD = -1;
                 socketClosed = true;
                 msgSize = 0;
                 return 0;
            }

         } else {
            logToMe->info(std::string("PDBCommunicator: receivedBytes for reading type is ")+std::to_string(receivedBytes));
            receivedTotal = receivedTotal + receivedBytes;
            bytesToReceive = sizeof(int16_t) - receivedTotal;
         }
    }
    //now we get enough bytes
    logToMe->trace("PDBCommunicator: typeID of next object is " + std :: to_string (nextTypeID));
    logToMe->trace("PDBCommunicator: getting the size of the next object:");

    // make sure we got enough bytes... if we did not, then error out
    receivedBytes = 0;
    receivedTotal = 0;
    bytesToReceive = (int)(sizeof(size_t));
    retries = 0;
    while (receivedTotal < (int)(sizeof(size_t))) {
        if ((receivedBytes = read(socketFD, (char *) ((char *)(&msgSize)+receivedTotal*sizeof(char)), bytesToReceive)) <  0) {
            std :: string errMsg = "PDBCommunicator: could not read next message size:" + std :: to_string(receivedTotal) + strerror(errno);
            logToMe->error (errMsg);
            std :: cout << errMsg << std :: endl;
            close(socketFD);
            socketFD = -1;
            if (longConnection) {
                std :: cout << "############################################" << std :: endl;
                std :: cout << "WARNING: LONG CONNECTION CLOSED DUE TO READ ERROR" << std :: endl;
                std :: cout << "############################################" << std :: endl;
            } else {
                std :: cout << "############################################"
 << std :: endl;
                std :: cout << "WARNING: CONNECTION CLOSED DUE TO READ ERROR"
 << std :: endl;
                std :: cout << "############################################"
 << std :: endl;
            }
            socketClosed = true;
            msgSize = 0;
            return 0;
        } else if (receivedBytes == 0) { 
            logToMe->info("PDBCommunicator: the other side closed the socket when we try to get next size");
            nextTypeID = NoMsg_TYPEID;
            std :: cout << "PDBCommunicator: the other side closed the socket when we try to get next size" << std :: endl;
            //if (retries < MAX_RETRIES) {
            if (retries < 0) {
                 retries ++;
                 std :: cout << "PDBCommunicator: Retry to see whether network can recover" << std :: endl;
                 logToMe->info("PDBCommunicator: Retry to see whether network can recover");
                 continue;
             } else {
                 if (longConnection) {
                     std :: cout << "############################################" << std :: endl;
                     std :: cout << "WARNING: LONG CONNECTION CLOSED DUE TO READ ERROR AFTER RETRIE" << std :: endl;
                     std :: cout << "############################################" << std :: endl;
                 } 
                 close(socketFD);
                 socketFD = -1;
                 socketClosed = true;
                 msgSize = 0;
                 return 0;
             }

        }
        else {
            logToMe->info(std::string("PDBCommunicator: receivedBytes for reading size is ")+std::to_string(receivedBytes));
            receivedTotal = receivedTotal + receivedBytes;
            bytesToReceive = sizeof(size_t) - receivedTotal;
        }
    }
    // OK, we did get enough bytes
    logToMe->trace("PDBCommunicator: size of next object is " + std :: to_string (msgSize));
    readCurMsgSize = true;
    return msgSize;
}

bool PDBCommunicator::doTheWrite(char *start, char *end) {

    int retries = 0;
    // and do the write
    while (end != start) {

        // write some bytes
        ssize_t numBytes = write(socketFD, start, end - start);
        // make sure they went through
        if (numBytes < 0) {
            logToMe->error("PDBCommunicator: error in socket write");
	    logToMe->trace("PDBCommunicator: tried to write " + std :: to_string (end - start) + " bytes.\n");
    	    logToMe->trace("PDBCommunicator: Socket FD is " + std :: to_string (socketFD));
            logToMe->error(strerror(errno));
            //if (retries < MAX_RETRIES) {
            if (retries < 0) {
                retries ++;
                std :: cout << "PDBCommunicator: Retry to see whether network can recover" << std :: endl;
                logToMe->info("PDBCommunicator: Retry to see whether network can recover");
                continue;
            } else {
                std :: cout << "############################################" << std :: endl;
                std :: cout << "WARNING: CONNECTION CLOSED DUE TO WRITE ERROR AFTER RETRY" << std :: endl;
                std :: cout << "############################################" << std :: endl;
                close(socketFD);
                socketFD = -1;
                socketClosed = true;
                return true;
            }
        } else {
	    logToMe->trace("PDBCommunicator: wrote " + std :: to_string (numBytes) + " and are " + std :: to_string (end - start - numBytes) + " to go!");
            start += numBytes;
	}
    }
    return false;
}

bool PDBCommunicator :: doTheRead(char *dataIn) {

    if (!readCurMsgSize) {
        getSizeOfNextObject ();
    }
    readCurMsgSize = false;

    // now, read the rest of the bytes
    char *start = dataIn;
    char *cur = start;
    
    int retries = 0;
    while (cur - start < (long) msgSize) {

        ssize_t numBytes = read(socketFD, cur, msgSize - (cur - start));
        this->logToMe->trace("PDBCommunicator: received bytes: " + std :: to_string (numBytes));

        if (numBytes < 0) {
            logToMe->error("PDBCommunicator: error reading socket when trying to accept text message");
            logToMe->error(strerror(errno));
            close(socketFD);
            socketFD = -1;
            if (longConnection) {
                std :: cout << "############################################" << std :: endl;
                std :: cout << "WARNING: LONG CONNECTION CLOSED DUE TO READ ERROR" << std :: endl;
                std :: cout << "############################################" << std :: endl;
            } else {
                std :: cout << "############################################" << std :: endl;
                std :: cout << "WARNING: CONNECTION CLOSED DUE TO READ ERROR" << std :: endl;
                std :: cout << "############################################" << std :: endl;
            } 
            socketClosed = true;
            return true;
        }
        else if (numBytes == 0) {
            logToMe->info("PDBCommunicator: the other side closed the socket when we do the read");
            std :: cout << "PDBCommunicator: the other side closed the socket when we doTheRead" << std :: endl;
            //if (retries < MAX_RETRIES) {
            if (retries < 0) {
                retries ++;
                logToMe->info("PDBCommunicator: Retry to see whether network can recover");
                std :: cout << "PDBCommunicator: Retry to see whether network can recover" << std :: endl;
                continue;
            } else {
                if (longConnection) {
                    std :: cout << "############################################" << std :: endl;
                    std :: cout << "WARNING: LONG CONNECTION IS CLOSED DUE TO READ ERROR AFTER RETRY" << std :: endl;
                    std :: cout << "############################################" << std :: endl;
                }
                close(socketFD);
                socketFD = -1;
                socketClosed = true;
                return true;
            }
        } else {
            cur += numBytes;
        }
        this->logToMe->trace("PDBCommunicator: " + std :: to_string (msgSize - (cur - start)) + " bytes to go!");
    }
    return false;
}

//JiaNote: add following functions to enable a stable long connection:

bool PDBCommunicator::isSocketClosed() {
    return socketClosed;
}

bool PDBCommunicator::isLongConnection() {
    return longConnection;
}

void PDBCommunicator::setLongConnection(bool longConnection) {
    this->longConnection = longConnection;
}

bool PDBCommunicator::reconnect(std :: string& errMsg) {

    if (needToSendDisconnectMsg == true) {
      //I can reconnect because I'm a client
        std :: cout << "To reconnect..." << std :: endl;

        if (socketFD >= 0) {
            close(socketFD);
            socketFD = -1;
            socketClosed = true;
        }

        if (isInternet == true) {

            return connectToInternetServer(logToMe, portNumber, serverAddress, errMsg);         

        } else {

            return connectToLocalServer(logToMe, fileName, errMsg);
     
        }

     } else {   
        errMsg = "Can't reconnect because I'm a server";
        logToMe->error(errMsg);
        return true;
     }
}

}

#endif
