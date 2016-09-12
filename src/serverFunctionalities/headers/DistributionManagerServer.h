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
#ifndef DISTRIBUTION_MANAGER_SERVER_H
#define DISTRIBUTION_MANAGER_SERVER_H

#include "ServerFunctionality.h"
#include "PDBServer.h"
#include "PDBDistributionManager.h"

namespace pdb {

class DistributionManagerServer: public ServerFunctionality {

public:

	// these give us the port and the address of the catalog
	DistributionManagerServer();

	~DistributionManagerServer();

	// from the ServerFunctionality interface
	void registerHandlers(PDBServer &forMe) override;

	// This method adds or update a node.
	// If the node is seen for the first time it adds it to memory with the current time and returns 0
	// If the node already exists it updates the timestamp and returns 1.
	int addOrUpdateNodes(PDBLoggerPtr myLoggerIn, string& nodeID);

	PDBDistributionManagerPtr getDistributionManager();


private:
	PDBDistributionManagerPtr distributionManager;

};

}

#endif
