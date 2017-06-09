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

#ifndef WRITE_K_MEANS_SET_H
#define WRITE_K_MEANS_SET_H

//by Shangyu, June 2017

#include "WriteUserSet.h"
#include "KMeansAggregateOutputType.h"

using namespace pdb;
class WriteKMeansSet : public WriteUserSet <KMeansAggregateOutputType> {

public:

	ENABLE_DEEP_COPY

        WriteKMeansSet () {}

        //below constructor is not required, but if we do not call setOutput() here, we must call setOutput() later to set the output set
        WriteKMeansSet (std :: string dbName, std :: string setName) {
            this->setOutput(dbName, setName);
        }

};


#endif
