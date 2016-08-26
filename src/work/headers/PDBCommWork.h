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
 * File:   PDBCommWork.h
 * Author: Chris
 *
 * Created on September 25, 2015, 5:13 PM
 */

#ifndef PDBCOMMWORK_H
#define	PDBCOMMWORK_H

#include <memory>

// "Comm" here stands for communication; so this object encapsulates some work that
// requires communicating via a socket (ether over the internet, or locally, via a
// UNIX socket

namespace pdb {

class PDBCommWork;
typedef std :: shared_ptr <PDBCommWork> PDBCommWorkPtr;

}
// create a smart pointer for PDBCommWork objects
#include <pthread.h>
#include <string>
#include "PDBCommunicator.h"
#include "PDBWork.h"

namespace pdb {

class PDBCommWork : public PDBWork {
public:

    // gets a new worker of this type and returns it
    virtual PDBCommWorkPtr clone() = 0;

    // accesses the communicator buried in this guy
    PDBCommunicatorPtr getCommunicator();

    // sets the logger and the commuicator
    void setGuts(PDBCommunicatorPtr toMe);

    // inherited from PDBWork
    // ********************************
    // virtual void execute (PDBBuzzerPtr callerBuzzer);
    // void execute (PDBWorker useMe, PDBBuzzerPtr callerBuzzer);
    // PDBWorkerPtr getWorker ();
    // PDBBuzzerPtr getLinkedBuzzer ();

private:

    // this is responsible for talking over the network
    PDBCommunicatorPtr myCommunicator;


};

}

#endif	/* PDBCOMMWORK_H */

