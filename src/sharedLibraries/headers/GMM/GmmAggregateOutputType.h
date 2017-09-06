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
#ifndef GMM_AGGREGATE_OUTPUT_H
#define GMM_AGGREGATE_OUTPUT_H

#include "Object.h"
#include "Handle.h"
#include "GmmNewComp.h"


// By Tania, August 2017

using namespace pdb;


class GmmAggregateOutputType : public Object {

private:
    int key = 1;
    GmmNewComp value;

public:

    ENABLE_DEEP_COPY

    GmmAggregateOutputType () {}

	int &getKey(){
		return key;
	}

	GmmNewComp &getValue(){
		return value;
	}

	void print(){
		std::cout<<"GmmAggregateOutputType: index: " << key << " value: " <<"."<< std::endl;
	}

    ~GmmAggregateOutputType () {
    }

};

#endif
