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
#ifndef OBJECTQUERYMODEL_NODEPARTITIONDATA_CC
#define OBJECTQUERYMODEL_NODEPARTITIONDATA_CC

#include "NodePartitionData.h"

namespace pdb {

    NodePartitionData::NodePartitionData(Handle<NodeDispatcherData> nodeData, std::pair<std::string, std::string> setAndDatabaseName) {
        NodePartitionData(nodeData->getNodeId(), nodeData->getPort(), nodeData->getAddress(), setAndDatabaseName);
    }

    NodePartitionData::NodePartitionData(NodeID nodeId, int port, std::string address, std::pair<std::string, std::string> setAndDatabaseName)
            : nodeId(nodeId), port(port), address(address), setName(setAndDatabaseName.first), databaseName(setAndDatabaseName.second) {
        this->totalBytesSent = 0;
    }

    NodeID NodePartitionData::getNodeId() const {
        return this->nodeId;
    }
    int NodePartitionData::getPort() const {
        return this->port;
    }
    std::string NodePartitionData::getAddress() const {
        return this->address;
    }
    std::string NodePartitionData::getSetName() const {
        return this->setName;
    }
    std::string NodePartitionData::getDatabaseName() const {
        return this->databaseName;
    }
    size_t NodePartitionData::getTotalBytesSent() const {
        return this->totalBytesSent;
    }
}

#endif