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


#ifndef PDB_COMMUN_TEMPLATES_C
#define PDB_COMMUN_TEMPLATES_C

#include "BuiltInObjectTypeIDs.h"
#include "Handle.h"
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include "Object.h"
#include "PDBVector.h"
#include "CloseConnection.h"
#include "InterfaceFunctions.h"
#include "PDBCommunicator.h"
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <unistd.h>

namespace pdb {

template <class ObjType>
bool PDBCommunicator :: sendObject (Handle <ObjType> &sendMe, std :: string &errMsg) {

	// first, write the record type
	int16_t recType = getTypeID <ObjType> ();
	if (recType < 0) {
		std :: cout << "BAD!  Trying to send a handle to a non-Object type.\n";
		exit (1);
	}
        //std :: cout << "recType = " << recType << std :: endl;
	if (doTheWrite (((char *) &recType), ((char *) &recType) + sizeof (int16_t))) {
		errMsg = "PDBCommunicator: not able to send the object type";
            	logToMe->error(errMsg);
            	logToMe->error(strerror(errno));
		return false;
	}
        //std :: cout << "written the type\n";
	// next, write the object
	auto *record = getRecord (sendMe);

	if (record == nullptr) {
		int *a = 0;
		*a = 12;
		std :: cout << "BAD!  Trying to get a record for an object not created by this thread's allocator.\n";
		exit (1);
	}
        //std :: cout << "record size ="<<record->numBytes()<<"\n";
	if (doTheWrite ((char *) record, ((char *) record) + record->numBytes ())) {
		errMsg = "PDBCommunicator: not able to send the object size";
            	logToMe->error(errMsg);
            	logToMe->error(strerror(errno));
		return false;
	}
	return true;
}

inline bool PDBCommunicator :: receiveBytes (void *data, std :: string &errMsg) {

	// if we have previously gotten the size, just return it
	if (!readCurMsgSize) {
		getSizeOfNextObject ();
	}

	// the first few bytes of a record always record the size
	char *mem = (char *) data;

	if (doTheRead (mem)) {
		errMsg = "Could not read the next object coming over the wire";
		readCurMsgSize = false;
		return false;
	}

	return true;
}

inline bool PDBCommunicator :: sendBytes (void *data, size_t sizeOfBytes, std :: string &errMsg) {

	int16_t recType = NoMsg_TYPEID;
	if (doTheWrite (((char *) &recType), ((char *) &recType) + sizeof (int16_t))) {
		errMsg = "PDBCommunicator: not able to send the object type";
            	logToMe->error(errMsg);
            	logToMe->error(strerror(errno));
		return false;
	}

	// now write the size 
	if (doTheWrite (((char *) &sizeOfBytes), ((char *) &sizeOfBytes) + sizeof (size_t))) {
		errMsg = "PDBCommunicator: not able to send the object size";
            	logToMe->error(errMsg);
            	logToMe->error(strerror(errno));
		return false;
	}

	// now we put the actual bytes
	if (doTheWrite (((char *) data), ((char *) data) + sizeOfBytes)) {
		errMsg = "PDBCommunicator: not able to send the bytes";
            	logToMe->error(errMsg);
            	logToMe->error(strerror(errno));
		return false;
	}
	
	return true;
}


template <class ObjType>
Handle <ObjType> PDBCommunicator :: getNextObject (void *readToHere, bool &success, std :: string &errMsg) {

	// if we have previously gotten the size, just return it
	if (!readCurMsgSize) {
		getSizeOfNextObject ();
	}

	// the first few bytes of a record always record the size
	char *mem = (char *) readToHere;
	*((size_t *) mem) = msgSize;

	// now we read the rest
	mem += sizeof (size_t);
	msgSize -= sizeof (size_t);

	if (doTheRead (mem)) {
		errMsg = "Could not read the next object coming over the wire";
		success = false;
		readCurMsgSize = false;
		return nullptr;
	}

	// create an object and get outta here
	success = true;
        logToMe->trace ("PDBCommunicator: read the object with no problem.");
        logToMe->trace ("PDBCommunicator: root offset is " + std :: to_string (((Record <ObjType> *) readToHere)->rootObjectOffset ()));
	readCurMsgSize = false;
	return ((Record <ObjType> *) readToHere)->getRootObject ();
}

template <class ObjType>
Handle <ObjType> PDBCommunicator :: getNextObject (bool &success, std :: string &errMsg) {

    // if we have previously gotten the size, just return it
    if (!readCurMsgSize) {
	getSizeOfNextObject ();
    }

    // read in the object
    void *mem = malloc (msgSize);
    Handle <ObjType> temp = getNextObject <ObjType> (mem, success, errMsg);

    // if we were successful, then copy it to the current allocation block
    if (success) {
        logToMe->trace ("PDBCommunicator: about to do the deep copy.");
	temp = getHandle (*temp);
        logToMe->trace ("PDBCommunicator: completed the deep copy.");
	free (mem);
	return temp;
    } else {
	return nullptr;
    }
}

}

#endif
