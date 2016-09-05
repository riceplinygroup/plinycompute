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
	std :: cout << "Getting record type.\n";
	int16_t recType = getTypeID <ObjType> ();
	if (recType < 0) {
		std :: cout << "BAD!  Trying to send a handle to a non-Object type.\n";
		exit (1);
	}
	

	std :: cout << "Sending record type.\n";
	if (doTheWrite (((char *) &recType), ((char *) &recType) + sizeof (int16_t))) {
		errMsg = "PDBCommunicator: not able to send the object type";
            	logToMe->error(errMsg);
            	logToMe->error(strerror(errno));
		return false;
	}

	// next, write the object
	auto *record = getRecord (sendMe);

	if (record == nullptr) {
		int *a = 0;
		*a = 12;
		std :: cout << "BAD!  Trying to get a record for an object not created by this thread's allocator.\n";
		exit (1);
	}

	std :: cout << "Sending record.\n";
	if (doTheWrite ((char *) record, ((char *) record) + record->numBytes ())) {
		errMsg = "PDBCommunicator: not able to send the object size";
            	logToMe->error(errMsg);
            	logToMe->error(strerror(errno));
		return false;
	}
	std :: cout << "Done sending record.\n";
	return true;
}

template <class ObjType>
bool PDBCommunicator :: sendBytes (Handle <ObjType> &sendMe, void *data, size_t sizeOfBytes, std :: string &errMsg) {

	// The layout of what we write to the socket is:
	//
	// | Record holding sendMe | RefCountedObjectPreamble | Bytes of Array <char> up to data[0] | Bytes of data |
	// 
	// This requires that we update the offset of sendMe->dataToSend.myArray so that it is pointing to
	// the first byte of RefCountedObjectPreamble.
	
	auto *record = getRecord (sendMe);
	size_t recSize = record->numBytes ();
	if (record == nullptr) {
		std :: cout << "BAD!  Trying to get a record for an object not created by this thread's allocator.\n";
		exit (1);
	}

	size_t offsetOfMyArray = ((char *) &(sendMe->dataToSend.myArray) - ((char *) record));

	// remember the old offset
	int64_t oldOffset = sendMe->dataToSend.myArray.getOffset ();

	// and set the new one
	sendMe->dataToSend.myArray.setOffset (record->numBytes () - offsetOfMyArray);

	// compute the size of what we are sending
	Array <char> myArray (sizeOfBytes);
	size_t sizeToSend = record->numBytes () + REF_COUNT_PREAMBLE_SIZE + (((char *) &(myArray.data[0])) - ((char *) &myArray)) + sizeOfBytes;

	// set the size in the record
	*((size_t *) record) = sizeToSend;
			
	// first, write the record type
	int16_t recType = getTypeID <ObjType> ();
	if (recType < 0) {
		std :: cout << "BAD!  Trying to send a handle to a non-Object type.\n";
		exit (1);
	}
	
	if (doTheWrite (((char *) &recType), ((char *) &recType) + sizeof (int16_t))) {
		errMsg = "PDBCommunicator: not able to send the object type";
            	logToMe->error(errMsg);
            	logToMe->error(strerror(errno));
		return false;
	}

	// now write the record
	if (doTheWrite (((char *) record), ((char *) record) + recSize)) {
		errMsg = "PDBCommunicator: not able to send the object size";
            	logToMe->error(errMsg);
            	logToMe->error(strerror(errno));
		return false;
	}

	// and write the preamble
	char scratch[REF_COUNT_PREAMBLE_SIZE];
	if (doTheWrite (((char *) scratch), ((char *) scratch) + REF_COUNT_PREAMBLE_SIZE)) {
		errMsg = "PDBCommunicator: not able to send the object";
            	logToMe->error(errMsg);
            	logToMe->error(strerror(errno));
		return false;
	}
	
	// now write the array object
	myArray.setUsed (sizeOfBytes);
	if (doTheWrite (((char *) &myArray), ((char *) &(myArray.data[0])))) {
		errMsg = "PDBCommunicator: not able to send the array header";
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
	
	// and restore the offset
	sendMe->dataToSend.myArray.setOffset (oldOffset);
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
