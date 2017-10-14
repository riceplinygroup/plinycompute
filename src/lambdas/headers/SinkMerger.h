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

#ifndef SINK_MERGER_H
#define SINK_MERGER_H

// by Jia, May 2017


#include "Object.h"
#include "TupleSet.h"


namespace pdb {

class SinkMerger;
typedef std::shared_ptr<SinkMerger> SinkMergerPtr;

// this class encapsulates merger of multiple destinations of a set of TupleSet objects.  It may
// represent
// merging of two hash tables that a bunch of objects are being written to
class SinkMerger {

public:
    // this creates and returns a new output containter to write to
    virtual Handle<Object> createNewOutputContainer() = 0;

    // this writes the tuple set into the output container
    virtual void writeOut(Handle<Object> mergeMe, Handle<Object>& mergeToMe) = 0;

    // this writes the tuple set of multiple maps to the output container
    virtual void writeVectorOut(Handle<Object> mergeMe, Handle<Object>& mergeToMe) = 0;

    virtual ~SinkMerger() {}
};
}

#endif
