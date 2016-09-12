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
 * HeartBeatSendWork.h
 *
 *  Created on: Mar 18, 2016
 *      Author: kia
 */

#ifndef DISTRIBUTION_MANAGER_HEARTBEATSENDWORK_H
#define DISTRIBUTION_MANAGER_HEARTBEATSENDWORK_H
#include "PDBCommWork.h"
#include "PDBDistributionManager.h"
#include "NodeInfo.h"

// A smart pointer for HeartBeatHandler

namespace pdb {
class HeartBeatSendWork;
typedef shared_ptr<HeartBeatSendWork> HeartBeatSendWorkPtr;

class HeartBeatSendWork: public PDBCommWork {

public:

	HeartBeatSendWork(string masterNodeHostName, int masterNodePort) {
		this->masterNodeHostName=masterNodeHostName;
		this->masterNodePort=masterNodePort;
	}

	HeartBeatSendWork() {
	}


	~HeartBeatSendWork() {
	}

	void execute(PDBBuzzerPtr callerBuzzer);

	PDBCommWorkPtr clone();

	pdb::Handle<NodeInfo>& getNodeInfo() {
		return m_nodeInfo;
	}

	void setNodeInfo(pdb::Handle<NodeInfo>& nodeInfo) {
		m_nodeInfo = nodeInfo;
	}

private:
	pdb::Handle<NodeInfo> m_nodeInfo;

	string masterNodeHostName;
	int masterNodePort;

};

}

#endif /* SRC_DISTRIBUTIONMANAGER_HEADERS_HEARTBEATSENDWORK_H_ */
