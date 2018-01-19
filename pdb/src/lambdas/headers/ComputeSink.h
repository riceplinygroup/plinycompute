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

#ifndef COMPUTE_SINK_H
#define COMPUTE_SINK_H

#include "Object.h"
#include "TupleSet.h"

namespace pdb {

class ComputeSink;
typedef std::shared_ptr<ComputeSink> ComputeSinkPtr;

// this class encapsulates a destination for a set of TupleSet objects.  It may represent
// a hash table that a bunch of objects are being written to, or it may represent secondary
// storage where we are writing the output of a query
class ComputeSink {

public:
    // this creates and returns a new output containter to write to
    virtual Handle<Object> createNewOutputContainer() = 0;

    // this writes the tuple set into the output container
    virtual void writeOut(TupleSetPtr writeMe, Handle<Object>& writeToMe) = 0;

    virtual ~ComputeSink() {}
};
}

#endif
