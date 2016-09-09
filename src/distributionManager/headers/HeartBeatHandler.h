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

#ifndef HEART_BEAT_HANDLER_H
#define HEART_BEAT_HANDLER_H

#include "PDBCommWork.h"
#include "PDBDistributionManager.h"

// A smart pointer for HeartBeatHandler

namespace pdb {
class HeartBeatHandler;
typedef shared_ptr<HeartBeatHandler> HeartBeatHandlerPtr;

class HeartBeatHandler : public PDBCommWork {

public:

	HeartBeatHandler(string socketFileName, PDBDistributionManagerPtr distribution);

	~HeartBeatHandler();

	void execute (PDBBuzzerPtr callerBuzzer);

	PDBCommWorkPtr clone ();

private:

	// the name of the socket file
	string fName;

	// pointer to the distribution Manager
	PDBDistributionManagerPtr distribution;

};
}
#endif
