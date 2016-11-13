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

#ifndef FRONTEND_QUERY_TEST_SERVER_H
#define FRONTEND_QUERY_TEST_SERVER_H

#include "ServerFunctionality.h"
#include "QueryBase.h"
#include "PDBServer.h"

namespace pdb {

class FrontendQueryTestServer : public ServerFunctionality {

public:

	FrontendQueryTestServer ();

        FrontendQueryTestServer (bool isStandalone, bool createOutputSet);

	void registerHandlers (PDBServer &forMe) override;

	// destructor
	~FrontendQueryTestServer ();

private:
	void computeQuery (std :: string setOutputName, std :: string setPrefix, int &whichNode, Handle <QueryBase> &computeMe, 
		std :: vector <std :: string> &tempSetsCreated);

	// this actually computes a selection query
	void doSelection (std :: string setOutputName, Handle <QueryBase> &computeMe);

	int tempSetName;

        bool isStandalone;

        bool createOutputSet;

};

}

#endif
