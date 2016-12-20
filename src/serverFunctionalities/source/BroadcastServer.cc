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
#ifndef OBJECTQUERYMODEL_BROADCASTSERVER_CC
#define OBJECTQUERYMODEL_BROADCASTSERVER_CC

#include "BroadcastServer.h"
namespace pdb {

BroadcastServer::BroadcastServer(PDBLoggerPtr logger, ConfigurationPtr conf) {
   this->conf = conf;
   pthread_mutex_init(&connection_mutex, nullptr);
}



BroadcastServer::BroadcastServer(PDBLoggerPtr logger) {
    this->logger = logger;
    this->conf = nullptr;
    pthread_mutex_init(&connection_mutex, nullptr);
}

BroadcastServer::~BroadcastServer() {
    pthread_mutex_destroy(&connection_mutex);
}

void BroadcastServer::registerHandlers(PDBServer &forMe) {
    // no-op
}

}

#endif

