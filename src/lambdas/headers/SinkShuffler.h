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

#ifndef SINK_SHUFFLER_H
#define SINK_SHUFFLER_H

// by Jia, May 2017


#include "Object.h"
#include "TupleSet.h"
#include "DataTypes.h"


namespace pdb {

class SinkShuffler;
typedef std::shared_ptr<SinkShuffler> SinkShufflerPtr;

// this class encapsulates a shuffler to pick up join maps in the input pages that belongs to the
// shuffler's destination node and create them again in a set of output pages. So the set of output
// pages are belonging to the same destination node.
class SinkShuffler {

public:
    // this sets the id for the destination node
    virtual void setNodeId(int nodeId) = 0;

    // this gets id of the destination node
    virtual int getNodeId() = 0;

    // this creates and returns a new output containter to write to
    virtual Handle<Object> createNewOutputContainer() = 0;

    // this writes the tuple set into the output container
    virtual bool writeOut(Handle<Object> shuffleMe, Handle<Object>& shuffleToMe) = 0;

    virtual ~SinkShuffler() {}
};
}

#endif
