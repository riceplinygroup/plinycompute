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

#ifndef QUERY_OUTPUT_H
#define QUERY_OUTPUT_H

#include "Query.h"
#include "OutputIterator.h"
#include "TypeName.h"
#include "SetScan.h"

// PRELOAD %QueryOutput <Nothing>%

namespace pdb {

template <class OutType>
class QueryOutput : public QueryBase {

public:

	ENABLE_DEEP_COPY

	QueryOutput (std :: string &dbName, std :: string &setName) {
		this->setSetName (setName);
		this->setdbName (dbName);
		myType = getTypeName <OutType> ();
	}

	QueryOutput () {
		myType = getTypeName <OutType> ();
	}

	QueryOutput (std :: string dbName, std :: string setName, Handle <QueryBase> input) {
		this->setSetName (setName);
		this->setDBName (dbName);
		myType = getTypeName <OutType> ();
		this->setInput (input);
	}

	~QueryOutput () {}

	virtual std :: string getIthInputType (int i) override {
		if (i != 0) {
			return "Bad index";
		}
		return myType;
	}

	virtual std :: string getOutputType () override {
		return "";
	}

	virtual int getNumInputs () override {
		return 1;
	}

	virtual std :: string getQueryType () override {
		return "localoutput";
	}

	void execute (QueryAlgo&) override {}

private:

	// records the output type
	String myType;

};

}

#endif
