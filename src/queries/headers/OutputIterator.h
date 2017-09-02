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

#ifndef OUTPUT_ITER_H
#define OUTPUT_ITER_H

#include "PDBDebug.h"
#include "Query.h"
#include "Handle.h"
#include "DoneWithResult.h"
#include "PDBCommunicator.h"
#include "PDBString.h"
#include "KeepGoing.h"
#include <string>
#include <memory>
#include <snappy.h>
#include "UseTemporaryAllocationBlock.h"

namespace pdb {

template <class OutType>
class OutputIterator {

public:

	bool operator != (const OutputIterator &me) const {
		if (connection != nullptr || me.connection != nullptr)
			return true;	
		return false;
	}

	Handle <OutType> &operator * () const {
		    return ((*data)[pos]);
	} 

        int getSize()  {
                return size;
        }
        
        int getPos() {
                return pos;
        }
	
	void operator ++ () {
		if (pos == size - 1) {

			// for allocations
			const UseTemporaryAllocationBlock tempBlock {1024};

			// for errors
			std :: string errMsg;

			// free the last page
			Handle <KeepGoing> temp;
			if (page != nullptr) {

				// if we don't have this line, we'll still be pointing into the freed page
				data = nullptr;
				free (page);
				page = nullptr;
				temp = makeObject <KeepGoing> ();
				if (!connection->sendObject (temp, errMsg)) {
					std :: cout << "Problem sending request: " << errMsg << "\n";
					connection = nullptr;
					return;
				}
			}

			// get the next page
			size_t objSize = connection->getSizeOfNextObject ();
                        PDB_COUT << "to receive " << objSize << " bytes" << std :: endl;
			// if the file is done, then we're good 
			if (connection->getObjectTypeID () == DoneWithResult_TYPEID) {
				connection = nullptr;
				return;	
			}
#ifdef ENABLE_COMPRESSION
                        char * readToHere = new char[objSize];
#else
                        void *readToHere = malloc (objSize);
#endif
			// we've got some more data
			page = (Record <Vector <Handle <OutType>>> *) malloc (DEFAULT_PAGE_SIZE);
			if (!connection->receiveBytes (readToHere, errMsg)) {
				std :: cout << "Problem getting data: " << errMsg << "\n";
				connection = nullptr;
				return;
			}
                        PDB_COUT << "received " << objSize << " bytes" << std :: endl;
#ifdef ENABLE_COMPRESSION
                         snappy::RawUncompress(readToHere, objSize, (char *)(page));
#else
                         memcpy (page, readToHere, objSize);
#endif

#ifdef ENABLE_COMPRESSION
                        delete[] readToHere;
#else
                        free (readToHere);
#endif
			// gets the vector that we are going to iterate over
			data = page->getRootObject ();	
                        PDB_COUT << "to obtain size of vector" << std :: endl;
			size = data->size ();
                        PDB_COUT << "got a page with size="<< size  << std :: endl;			
                        // added by Jia to fix a segfault when size=0
                        if (size > 0) {
			     pos = 0;
                        } else {
                             connection = nullptr;
                             return;
                       }

		} else {
			pos++;
		}
		
	}

	OutputIterator (PDBCommunicatorPtr connectionIn) {
		connection = connectionIn;
		data = nullptr;
		page = nullptr;		

		// get the ball rolling!!
		this->operator++ ();
	}

	OutputIterator () {
		connection = nullptr;
		data = nullptr;
		page = nullptr;
	}

	~OutputIterator () {
			
		// make sure we don't leave a page sitting around
		data = nullptr;
		if (page != nullptr) 
			free (page);

		// nothing to do if we don't have a connection
		if (connection == nullptr) 
			return;

		// for allocations
		const UseTemporaryAllocationBlock tempBlock {1024};

		// tell the server we are done
		Handle <DoneWithResult> temp = makeObject <DoneWithResult> ();
		std :: string errMsg;
		if (!connection->sendObject (temp, errMsg)) {
			std :: cout << "Problem sending done message: " << errMsg << "\n";
			connection = nullptr;
			return;
		}
                PDB_COUT << "send done message" << std :: endl;
	}

private:

	int size = 0;
	int pos = -1;
	Handle <Vector <Handle <OutType>>> data;
	Record <Vector <Handle <OutType>>> *page;
	PDBCommunicatorPtr connection;
};

}

#endif
