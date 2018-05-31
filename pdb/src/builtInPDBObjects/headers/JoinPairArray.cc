
#ifndef JOIN_PAIR_ARRAY_CC
#define JOIN_PAIR_ARRAY_CC

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <type_traits>
#include <cstring>

#include "Handle.h"
#include "Object.h"
#include "InterfaceFunctions.h"

namespace pdb {

// a special code that tells us when a hash slot is unused
#define JM_UNUSED 493295393

// the maximum fill factor before we double
#define JM_FILL_FACTOR .667

// access keys, hashes, and data in the underlying array
#define JM_GET_HASH(data, i) (*((size_t*)(((char*)data) + (i * objSize))))
#define JM_GET_HASH_PTR(data, i) ((size_t*)(((char*)data) + (i * objSize)))
#define JM_GET_NEXT_PTR(data, i) ((uint32_t*)(((char*)data) + sizeof(size_t) + (i * objSize)))
#define JM_GET_VALUE_PTR(data, i) \
    ((void*)(((char*)data) + sizeof(size_t) + sizeof(uint32_t) + (i * objSize)))
#define JM_GET_NEXT(data, i) (*((uint32_t*)(((char*)data) + sizeof(size_t) + (i * objSize))))
#define JM_GET_VALUE(data, i, type) \
    (*((type*)(((char*)data) + sizeof(size_t) + sizeof(uint32_t) + (i * objSize))))

// Note: we need to write all operations in constructors, destructors, and assignment operators
// WITHOUT using
// the underlying type in any way (including assignment, initialization, destruction, size).
//

template <class ValueType>
void JoinPairArray<ValueType>::setUpAndCopyFrom(void* target, void* source) const {

    new (target) JoinPairArray<ValueType>();
    JoinPairArray<ValueType>& fromMe = *((JoinPairArray<ValueType>*)source);
    JoinPairArray<ValueType>& toMe = *((JoinPairArray<ValueType>*)target);

    // copy the number of slots
    toMe.numSlots = fromMe.numSlots;
    toMe.usedSlots = fromMe.usedSlots;

    // copy the type info
    toMe.valueTypeInfo = fromMe.valueTypeInfo;

    // copy over the size info
    toMe.objSize = fromMe.objSize;
    toMe.maxSlots = fromMe.maxSlots;

    // copy over the overflow area
    toMe.overflows = fromMe.overflows;

    // now we need to copy the array
    // if our types are fully primitive, just do a memmove
    if (!toMe.valueTypeInfo.descendsFromObject()) {
        memmove((void*)toMe.data, (void*)fromMe.data, ((size_t)toMe.objSize) * (toMe.numSlots));
        return;
    }

    // one of them is not primitive...

    // these are needed to make the JM_GET_HASH and other macros work correctly... they refer
    // to variables objSize and valueOffset... this.objSize and this.valueOffset are possibly
    // undefined here.  By having local variables that shadow these, we get around potential
    // problems
    uint32_t objSize = toMe.objSize;

    // loop through and do the deep copy
    for (int i = 0; i < toMe.numSlots; i++) {

        // copy over the hash for this guy
        JM_GET_HASH(toMe.data, i) = JM_GET_HASH(fromMe.data, i);

        // don't copy over an unused slot
        if (JM_GET_HASH(fromMe.data, i) == JM_UNUSED)
            continue;

        JM_GET_NEXT(toMe.data, i) = JM_GET_NEXT(fromMe.data, i);

        // and now same thing on the value
        if (!toMe.valueTypeInfo.descendsFromObject()) {
            memmove(JM_GET_VALUE_PTR(toMe.data, i), JM_GET_VALUE_PTR(fromMe.data, i), objSize);
        } else {

            
            toMe.valueTypeInfo.setUpAndCopyFromConstituentObject(JM_GET_VALUE_PTR(toMe.data, i),
                                                                 JM_GET_VALUE_PTR(fromMe.data, i));
        }
    }
}

// just use setUpAndCopyFrom to do a deep copy... this assumes that we have enough space!!
template <class ValueType>
JoinPairArray<ValueType>::JoinPairArray(const JoinPairArray& toMe) {
    setUpAndCopyFrom(this, &toMe);
}

template <class ValueType>
int JoinPairArray<ValueType>::count(const size_t& me) {

    // hash this dude
    size_t hashVal = me == JM_UNUSED ? 858931273 : me;

    // figure out which slot he goes in
    size_t slot = hashVal % (numSlots - 1);

    // in the worst case, we can loop through the entire hash table looking.  :-(
    for (size_t slotsChecked = 0; slotsChecked < numSlots; slotsChecked++) {

        // if we found an empty slot, then this guy was not here
        if (JM_GET_HASH(data, slot) == JM_UNUSED) {
            return 0;
        } else if (JM_GET_HASH(data, slot) == hashVal) {

            // potential match!!
            if (JM_GET_NEXT(data, slot) != UINT32_MAX) {
                return 1 + overflows[JM_GET_NEXT(data, slot)].size();
            } else {
                return 1;
            }
        }

        // if we made it here, then it means that we found a non-empty slot, but no
        // match... so we simply loop to the next iteration... if slot == (numSlots-1), it
        // means we've made it to the end of the hash table... go to the beginning
        if (slot == numSlots - 1)
            slot = 0;

        // otherwise, just go to the next slot
        else
            slot++;
    }

    // we should never reach here
    std::cout << "Warning: Ran off the end of the hash table!!\n";
    exit(1);
}

template <class ValueType>
void JoinPairArray<ValueType>::setUnused(const size_t& me) {

    // hash this dude
    size_t hashVal = me == JM_UNUSED ? 858931273 : me;

    // figure out which slot he goes in
    size_t slot = hashVal % (numSlots - 1);
    // in the worst case, we can loop through the entire hash table looking.  :-(
    for (size_t slotsChecked = 0; slotsChecked < numSlots; slotsChecked++) {

        // if we found an empty slot, then this guy was not here
        if (JM_GET_HASH(data, slot) == JM_UNUSED) {
            return;

            // found a non-empty slot; check for a match
        } else if (JM_GET_HASH(data, slot) == hashVal) {

            if (JM_GET_NEXT(data, slot) != UINT32_MAX &&
                overflows[JM_GET_NEXT(data, slot)].size() >= 1) {
                overflows[JM_GET_NEXT(data, slot)].pop_back();
                return;
            }

            // destruct those guys
            ((ValueType*)(JM_GET_VALUE_PTR(data, slot)))->~ValueType();

            JM_GET_HASH(data, slot) = JM_UNUSED;
            JM_GET_NEXT(data, slot) = UINT32_MAX;

            return;
        }

        // if we made it here, then it means that we found a non-empty slot, but no
        // match... so we simply loop to the next iteration... if slot == numSlots - 1, it
        // means we've made it to the end of the hash table... go to the beginning
        if (slot == numSlots - 1)
            slot = 0;

        // otherwise, just go to the next slot
        else
            slot++;
    }

    // we should never reach here
    std::cout << "Fatal Error: Ran off the end of the hash table!!\n";
    exit(1);
}

template <class ValueType>
JoinRecordList<ValueType> JoinPairArray<ValueType>::lookup(const size_t& me) {

    size_t hashVal = me == JM_UNUSED ? 858931273 : me;

    // figure out which slot he goes in
    size_t slot = hashVal % (numSlots - 1);

    // in the worst case, we can loop through the entire hash table looking.
    for (size_t slotsChecked = 0; slotsChecked < numSlots; slotsChecked++) {

        // if we got here, it means that this guy was not here
        if (JM_GET_HASH(data, slot) == JM_UNUSED || JM_GET_HASH(data, slot) == hashVal) {
            JoinRecordList<ValueType> returnVal(slot, *this);
            return returnVal;
        }

        // if we made it here, then it means that we found a non-empty slot, but no
        // match... so we simply loop to the next iteration... if slot == numSlots - 1, it
        // means we've made it to the end of the hash table... go to the beginning
        if (slot == numSlots - 1)
            slot = 0;

        // otherwise, just go to the next slot
        else
            slot++;
    }

    // we should never reach here
    std::cout << "Fatal Error: Ran off the end of the hash table!!\n";
    exit(1);
}


template <class ValueType>
ValueType& JoinPairArray<ValueType>::push(const size_t& me) {

    // basically, if he is not there, we add him and return a reference to a newly-constructed
    // ValueType... if he is there, we simply return a reference to a newly-constructed ValueType

    // hash this dude
    size_t hashVal = me == JM_UNUSED ? 858931273 : me;

    // figure out which slot he goes in
    size_t slot = hashVal % (numSlots - 1);

    // in the worst case, we can loop through the entire hash table looking.  :-(
    for (size_t slotsChecked = 0; slotsChecked < numSlots; slotsChecked++) {

        // if we found an empty slot, then this guy was not here
        if (JM_GET_HASH(data, slot) == JM_UNUSED) {

            // construct the key and the value
            new (JM_GET_VALUE_PTR(data, slot)) ValueType();

            // add the key
            JM_GET_HASH(data, slot) = hashVal;
            JM_GET_NEXT(data, slot) = UINT32_MAX;

            // increment the number of used slots
            usedSlots++;

            return JM_GET_VALUE(data, slot, ValueType);

            // found a non-empty slot; check for a match
        } else if (JM_GET_HASH(data, slot) == hashVal) {

            // match!!
            if (JM_GET_NEXT(data, slot) == UINT32_MAX) {

                // add the new list of overflows
                Vector<ValueType> temp;
                overflows.push_back(temp);
                JM_GET_NEXT(data, slot) = overflows.size()-1;
            }

            // and add our new guy
            overflows[JM_GET_NEXT(data, slot)].push_back();
            return overflows[JM_GET_NEXT(data, slot)]
                            [overflows[JM_GET_NEXT(data, slot)].size() - 1];
        }

        // if we made it here, then it means that we found a non-empty slot, but no
        // match... so we simply loop to the next iteration... if slot == numSlots - 1, it
        // means we've made it to the end of the hash table... go to the beginning
        if (slot == numSlots - 1)
            slot = 0;

        // otherwise, just go to the next slot
        else
            slot++;
    }

    // we should never reach here
    std::cout << "Fatal Error: Ran off the end of the hash table!!\n";
    exit(1);
}

template <class ValueType>
JoinPairArray<ValueType>::JoinPairArray(uint32_t numSlotsIn) : JoinPairArray() {

    // verify that we are a power of two
    bool gotIt = false;
    uint32_t val = 1;
    for (unsigned int pow = 0; pow <= 31; pow++) {
        if (val >= numSlotsIn) {
            gotIt = true;
            break;
        }
        val *= 2;
    }

    // if we are not a power of two, exit
    if (!gotIt) {
        std::cout << "Fatal Error: Bad: could not get the correct size for the array\n";
        exit(1);
    }

    // remember the size
    numSlots = numSlotsIn;
    maxSlots = numSlotsIn * JM_FILL_FACTOR;

    // set everyone to unused
    for (int i = 0; i < numSlots; i++) {
        JM_GET_HASH(data, i) = JM_UNUSED;
        JM_GET_NEXT(data, i) = UINT32_MAX;
    }
}

template <class ValueType>
bool JoinPairArray<ValueType>::isOverFull() {
    return usedSlots >= maxSlots;
}

template <class ValueType>
JoinPairArray<ValueType>::JoinPairArray() {

    // remember the types for this guy
    valueTypeInfo.setup<ValueType>();

    // used to let the compiler to tell us how to pack items in our array of slots
    JoinMapRecordClass<ValueType> temp;
    objSize = temp.getObjSize();

    // zero slots in the array
    numSlots = 0;

    // no used slots
    usedSlots = 0;

    // the max number of used slots is zero
    maxSlots = 0;
}

// Note: because this can be called by Object.deleteObject (), it must be written so as to not use
// TypeContained
template <class ValueType>
JoinPairArray<ValueType>::~JoinPairArray() {

    // do no work if the guys we store do not come from pdb :: Object
    if (!valueTypeInfo.descendsFromObject())
        return;

    // now, delete each of the objects in there, if we have got an object type
    for (uint32_t i = 0; i < numSlots; i++) {
        if (JM_GET_HASH(data, i) != JM_UNUSED) {
            valueTypeInfo.deleteConstituentObject(JM_GET_VALUE_PTR(data, i));
        }
    }
}

template <class ValueType>
Handle<JoinPairArray<ValueType>> JoinPairArray<ValueType>::doubleArray() {
    PDB_COUT << "bytes available in current allocator block: "
             << getAllocator().getBytesAvailableInCurrentAllocatorBlock() << std::endl;
    std::string out = getAllocator().printInactiveBlocks();
    PDB_COUT << "inactive blocks: " << out << std::endl;
    PDB_COUT << "usedSlots = " << usedSlots << ", maxSlots = " << maxSlots << std::endl;
    uint32_t howMany = numSlots * 2;
    PDB_COUT << "doubleArray to " << howMany << std::endl;
    // allocate the new Array
    Handle<JoinPairArray<ValueType>> tempArray =
        makeObjectWithExtraStorage<JoinPairArray<ValueType>>(objSize * howMany, howMany);

    // first, set everything to unused
    // now, re-hash everything
    JoinPairArray<ValueType>& newOne = *tempArray;

    for (uint32_t i = 0; i < numSlots; i++) {

        if (JM_GET_HASH(data, i) != JM_UNUSED) {

            // copy the dude over
            ValueType* temp = &(newOne.push(JM_GET_HASH(data, i)));
            *temp = JM_GET_VALUE(data, i, ValueType);

            char* whereNextIs = ((char*)temp) - sizeof(uint32_t);
            *((uint32_t*)whereNextIs) = JM_GET_NEXT(data, i);
        }
    }

    newOne.overflows = overflows;

    // and return this guy
    return tempArray;
}

template <class ValueType>
uint32_t JoinPairArray<ValueType>::numUsedSlots() {
    return usedSlots;
}

template <class ValueType>
void JoinPairArray<ValueType>::deleteObject(void* deleteMe) {
    deleter(deleteMe, this);
}

template <class ValueType>
size_t JoinPairArray<ValueType>::getSize(void* forMe) {
    JoinPairArray<ValueType>& target = *((JoinPairArray<ValueType>*)forMe);
    return sizeof(JoinPairArray<Nothing>) + target.objSize * target.numSlots;
}

template <class ValueType>
JoinRecordList<ValueType>::JoinRecordList(uint32_t whichOne, JoinPairArray<ValueType>& parent)
    : whichOne(whichOne), parent(parent) {}

template <class ValueType>
size_t JoinRecordList<ValueType>::getHash() {
    uint32_t objSize = parent.objSize;
    return JM_GET_HASH(parent.data, whichOne);
}

template <class ValueType>
size_t JoinRecordList<ValueType>::size() {

    // in the case where this guy is not in the list, we return a zero
    uint32_t objSize = parent.objSize;
    if (JM_GET_HASH(parent.data, whichOne) == JM_UNUSED)
        return 0;

    if (JM_GET_NEXT(parent.data, whichOne) != UINT32_MAX) {
        if (JM_GET_NEXT(parent.data, whichOne) < parent.overflows.size()) {
            return parent.overflows[JM_GET_NEXT(parent.data, whichOne)].size() + 1;
        } else {
            std::cout << "not invalid slot, return 0" << std::endl;
            return 0;
        }
    } else {
        return 1;
    }
    // otherwise, return the correct slot
    return JM_GET_NEXT(parent.data, whichOne) == UINT32_MAX
        ? 1
        : parent.overflows[JM_GET_NEXT(parent.data, whichOne)].size() + 1;
}

template <class ValueType>
ValueType& JoinRecordList<ValueType>::operator[](const size_t i) {
    uint32_t objSize = parent.objSize;
    return i == 0 ? JM_GET_VALUE(parent.data, whichOne, ValueType)
                  : parent.overflows[JM_GET_NEXT(parent.data, whichOne)][i - 1];
}

template <class ValueType>
JoinMapIterator<ValueType>::JoinMapIterator(Handle<JoinPairArray<ValueType>> iterateMeIn, bool)
    : iterateMe(&(*iterateMeIn)) {
    slot = 0;
    done = false;
    uint32_t objSize = iterateMe->objSize;
    while (slot != iterateMe->numSlots && JM_GET_HASH(iterateMe->data, slot) == JM_UNUSED)
        slot++;

    if (slot == iterateMe->numSlots)
        done = true;

}

template <class ValueType>
JoinMapIterator<ValueType>::JoinMapIterator(Handle<JoinPairArray<ValueType>> iterateMeIn)
    : iterateMe(&(*iterateMeIn)) {
    done = true;
}

template <class ValueType>
JoinMapIterator<ValueType>::JoinMapIterator() {
    iterateMe = nullptr;
    done = true;
}


template <class ValueType>
void JoinMapIterator<ValueType>::operator++() {
    if (!done)
        slot++;

    int objSize = iterateMe->objSize;
    while (slot != iterateMe->numSlots && JM_GET_HASH(iterateMe->data, slot) == JM_UNUSED)
        slot++;

    if (slot == iterateMe->numSlots)
        done = true;
}

template <class ValueType>
JoinRecordList<ValueType>* JoinMapIterator<ValueType>::operator*() {

    JoinRecordList<ValueType>* returnVal = new JoinRecordList<ValueType>(slot, *iterateMe);
    return returnVal;
}

template <class ValueType>
bool JoinMapIterator<ValueType>::operator!=(const JoinMapIterator<ValueType>& me) const {
    if (!done || !me.done)
        return true;
    return false;
}
}

#endif
