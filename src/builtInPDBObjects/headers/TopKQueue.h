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
#ifndef TOP_K_QUEUE_H
#define TOP_K_QUEUE_H

#include "PDBVector.h"

// PRELOAD %TopKQueue <Nothing>%

namespace pdb {

// this class is used to access items from the TopKQueue class
template <class Score, class ValueType>
class ScoreValuePair;

// template class that is used by top-K aggregation... a top-k aggregation query
// returns one of these queues, containing the items with the top-K largest scores

// note that the < operation must be defined on the Score class...
template <class Score, class ValueType = Nothing>
class TopKQueue : public Object {

private:
    // the key
    int myKey = 1;

    // true if tempScore and tempValue are empty and should be ignored
    bool empty = true;

    // stores type info for the score and the value
    PDBTemplateBase scoreTypeInfo;
    PDBTemplateBase valueTypeInfo;

    // list of scores and values; implements a priority queue
    Handle<Vector<Score>> allScores;
    Handle<Vector<ValueType>> allValues;

    // the total size of this object
    unsigned mySize;

    // the offset (in bytes) from the start of the object until tempValue
    unsigned valueOffset;

    // the number of items we can store
    unsigned k = 1;

    // in the case where there is just one item, these store them
    Score tempScore;
    ValueType tempValue;

public:
    // no arg constructor
    TopKQueue();

    // create a queue and give a max size
    TopKQueue(unsigned k);

    // create a queue that stores the best k items; initialize by storing initScore and initValue
    TopKQueue(unsigned k, Score initScore, ValueType initValue);

    // normally these would be defined by the ENABLE_DEEP_COPY macro, but because
    // this class is special, we need to manually override these methods
    void setUpAndCopyFrom(void* target, void* source) const;
    void deleteObject(void* deleteMe);
    size_t getSize(void* forMe);

private:
    // swap the entries at positions i and j in the queue
    void swap(int i, int j);

public:
    // insert an item into the queue
    void insert(Score& score, ValueType& value);

    // always returns 1
    int& getKey();

    // always returns this
    TopKQueue<Score, ValueType>& getValue();

    // merges the two queues, returning this one
    TopKQueue<Score, ValueType>& operator+(TopKQueue<Score, ValueType>& addMeIn);

    // access the i^th item in the queue
    ScoreValuePair<Score, ValueType> operator[](unsigned i);

    // get the numer of items in the queue
    unsigned size();
};

template <class Score, class ValueType>
class ScoreValuePair {

    Score& myScore;
    ValueType& myValue;

public:
    ScoreValuePair(ScoreValuePair& fromMe) : myScore(fromMe.myScore), myValue(fromMe.myValue) {}

    ScoreValuePair(Score& myScore, ValueType& myValue) : myScore(myScore), myValue(myValue) {}

    Score& getScore() {
        return myScore;
    }

    ValueType& getValue() {
        return myValue;
    }
};
}

#include "TopKQueue.cc"

#endif
