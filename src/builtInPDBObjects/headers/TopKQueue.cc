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
#ifndef TOP_K_QUEUE_CC
#define TOP_K_QUEUE_CC

#include "TopKQueue.h"

namespace pdb {

template <class Score, class ValueType>
TopKQueue<Score, ValueType>::TopKQueue(unsigned kIn) {

    // set up the type info
    scoreTypeInfo.setup<Score>();
    valueTypeInfo.setup<ValueType>();

    // set up the size info
    mySize = sizeof(TopKQueue<Score, ValueType>);
    valueOffset = ((char*)&tempValue) - ((char*)this);

    // and set up k
    k = kIn;
}

template <class Score, class ValueType>
TopKQueue<Score, ValueType>::TopKQueue() {

    // set up the type info
    scoreTypeInfo.setup<Score>();
    valueTypeInfo.setup<ValueType>();

    // set up the size info
    mySize = sizeof(TopKQueue<Score, ValueType>);
    valueOffset = ((char*)&tempValue) - ((char*)this);
}

template <class Score, class ValueType>
TopKQueue<Score, ValueType>::TopKQueue(unsigned kIn, Score initScore, ValueType initValue) {

    // set up the type info
    scoreTypeInfo.setup<Score>();
    valueTypeInfo.setup<ValueType>();

    // set up the size info
    mySize = sizeof(TopKQueue<Score, ValueType>);
    valueOffset = ((char*)&tempValue) - ((char*)this);

    // we are not empty
    empty = false;

    // remember the initial score and value
    tempScore = initScore;
    tempValue = initValue;

    // and remember the limit
    k = kIn;
}

template <class Score, class ValueType>
void TopKQueue<Score, ValueType>::setUpAndCopyFrom(void* target, void* source) const {

    // get pointers to the object to copy
    TopKQueue<Score, ValueType>* from = (TopKQueue<Score, ValueType>*)source;
    TopKQueue<Score, ValueType>* to = (TopKQueue<Score, ValueType>*)target;

    // set up the nullptrs for the handles in the destination
    to->allScores.setOffset(-1);
    to->allValues.setOffset(-1);

    // copy the key
    to->myKey = from->myKey;

    // set up the type info
    to->scoreTypeInfo = from->scoreTypeInfo;
    to->valueTypeInfo = from->valueTypeInfo;

    // and remember the meta-data
    to->empty = from->empty;
    to->mySize = from->mySize;
    to->valueOffset = from->valueOffset;
    to->k = from->k;

    // zero out the score and copy him over
    if (!to->scoreTypeInfo.descendsFromObject()) {
        memmove((void*)&(to->tempScore),
                (void*)&(from->tempScore),
                to->scoreTypeInfo.getSizeOfConstituentObject(nullptr));
    } else {
        to->scoreTypeInfo.setUpAndCopyFromConstituentObject(&(to->tempScore), &(from->tempScore));
    }

    // zero out the score and copy him over
    if (!to->valueTypeInfo.descendsFromObject()) {
        memmove(from->valueOffset + (char*)to,
                from->valueOffset + (char*)from,
                to->valueTypeInfo.getSizeOfConstituentObject(nullptr));
    } else {
        to->valueTypeInfo.setUpAndCopyFromConstituentObject(from->valueOffset + (char*)to,
                                                            from->valueOffset + (char*)from);
    }

    // copy over the arrays of scores and values
    to->allScores = from->allScores;
    to->allValues = from->allValues;
}

template <class Score, class ValueType>
void TopKQueue<Score, ValueType>::deleteObject(void* deleteMe) {
    TopKQueue<Score, ValueType>* foo = (TopKQueue<Score, ValueType>*)deleteMe;
    if (foo->scoreTypeInfo.descendsFromObject())
        foo->scoreTypeInfo.deleteConstituentObject(&(foo->tempScore));
    if (foo->valueTypeInfo.descendsFromObject())
        foo->valueTypeInfo.deleteConstituentObject(foo->valueOffset + (char*)foo);
    foo->allScores = nullptr;
    foo->allValues = nullptr;
}

template <class Score, class ValueType>
size_t TopKQueue<Score, ValueType>::getSize(void* forMe) {
    TopKQueue<Score, ValueType>& target = *((TopKQueue<Score, ValueType>*)forMe);
    return target.mySize;
}

template <class Score, class ValueType>
void TopKQueue<Score, ValueType>::swap(int i, int j) {

    Score* scores = allScores->c_ptr();
    ValueType* values = allValues->c_ptr();
    tempScore = scores[i];
    tempValue = values[i];
    scores[i] = scores[j];
    values[i] = values[j];
    scores[j] = tempScore;
    values[j] = tempValue;
}

template <class Score, class ValueType>
void TopKQueue<Score, ValueType>::insert(Score& score, ValueType& value) {
    // std :: cout << "incoming score=" << score << std :: endl;
    // for an empty heap, just insert
    if (empty) {
        tempScore = score;
        tempValue = value;
        empty = false;
        return;
    } else if (allScores == nullptr) {
        allScores = makeObject<Vector<Score>>();
        allValues = makeObject<Vector<ValueType>>();
        allScores->push_back(tempScore);
        allValues->push_back(tempValue);
    }

    // don't do anything if the new guy is not good enough
    if (score < (*allScores)[0] && allScores->size() >= k)
        return;

    // insert the new guy at the bottom level of the heap

    allScores->push_back(score);
    allValues->push_back(value);

    // now, swap until we have a heap
    Score* scores = allScores->c_ptr();
    ValueType* values = allValues->c_ptr();
    // std :: cout << "score=" << score << " inserted" << std :: endl;
    int current = allScores->size() - 1;
    while (current > 0 && scores[current] < scores[(current - 1) / 2]) {
        // JiaNote: below is the original code
        // swap (current, current / 2);
        // JiaNote: below is the fixed code
        swap(current, (current - 1) / 2);
        current = (current - 1) / 2;
    }

    // if we can fit everyone, then just exit
    if (allScores->size() <= k)
        return;

    // we cannot fit everyone, so we need to remove the worst one
    // put the last one at the first position
    /*for (int i = 0; i < allScores->size(); i++) {
            std :: cout << "score[" << i << "]=" << scores[i] << std :: endl;
    }*/
    // std :: cout << "score=" << scores [0] << " removed" << std :: endl;
    scores[0] = scores[allScores->size() - 1];
    values[0] = values[allScores->size() - 1];
    allScores->pop_back();
    allValues->pop_back();

    // now, swap until we again have a heap
    current = 0;
    int limit = allScores->size();
    while (true) {

        if (current * 2 + 2 < limit) {

            // figure out the smaller child
            int smallPos;
            if (scores[current * 2 + 1] < scores[current * 2 + 2]) {
                smallPos = current * 2 + 1;
            } else {
                smallPos = current * 2 + 2;
            }

            // see if we are larger than the smaller child
            if (scores[smallPos] < scores[current]) {
                swap(smallPos, current);
                current = smallPos;
            } else {
                break;
            }

        } else if (current * 2 + 1 < limit) {
            int smallPos = current * 2 + 1;

            // see if we are larger than the smaller child
            if (scores[smallPos] < scores[current]) {
                swap(smallPos, current);
                current = smallPos;
            } else {
                break;
            }
        } else {
            break;
        }
    }
}

template <class Score, class ValueType>
int& TopKQueue<Score, ValueType>::getKey() {
    return myKey;
}

template <class Score, class ValueType>
TopKQueue<Score, ValueType>& TopKQueue<Score, ValueType>::getValue() {
    return *this;
}

template <class Score, class ValueType>
TopKQueue<Score, ValueType>& TopKQueue<Score, ValueType>::operator+(
    TopKQueue<Score, ValueType>& addMeIn) {

    // if the guy we are adding is empty, do nothing
    if (addMeIn.empty) {
        return *this;
    }

    // in the case where the other guy is not a list, just add his singleton
    if (addMeIn.allScores == nullptr) {
        insert(addMeIn.tempScore, addMeIn.tempValue);
        return *this;
    }

    // in this case, just add in the other guy
    int len = addMeIn.allScores->size();
    Score* scores = addMeIn.allScores->c_ptr();
    ValueType* values = addMeIn.allValues->c_ptr();
    for (int i = 0; i < len; i++) {
        try {
            insert(scores[i], values[i]);
        } catch (NotEnoughSpace& n) {
            std::cout << "Not enough space when trying to insert the " << i << "-th score"
                      << std::endl;
            throw n;
        }
    }

    return *this;
}

template <class Score, class ValueType>
unsigned TopKQueue<Score, ValueType>::size() {
    return allValues->size();
}

template <class Score, class ValueType>
ScoreValuePair<Score, ValueType> TopKQueue<Score, ValueType>::operator[](unsigned i) {
    if (empty) {
        std::cout << "You tried to extract a value from an empty queue!\n";
        exit(1);
    }

    if (allValues == nullptr && i == 0) {
        ScoreValuePair<Score, ValueType> temp(tempScore, tempValue);
        return temp;
    }

    if (allValues == nullptr) {
        std::cout << "You tried to extract a value from past the end of the queue!\n";
        exit(1);
    }

    if (i < allValues->size()) {
        ScoreValuePair<Score, ValueType> temp((*allScores)[i], (*allValues)[i]);
        return temp;
    }

    std::cout << "You tried to extract a value from past the end of the queue!\n";
    exit(1);
}
}

#endif
