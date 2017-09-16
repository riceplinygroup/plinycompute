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

#ifndef PAIR_ARRAY_CC
#define PAIR_ARRAY_CC

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

// this little class is used to ask the compiler to build the layout of the records used to
// store (hash, KeyType, ValueType) triples
template <class KeyType, class ValueType>
struct MapRecordClass {
	size_t hash;
	KeyType key;
	ValueType value;

	size_t getObjSize () {
		return sizeof (MapRecordClass <KeyType, ValueType>);
	}
	
	size_t getValueOffset () {
		return ((char *) &value) - ((char *) this);
	}
};

// a special code that tells us when a hash slot is unused
#define UNUSED 493295393

unsigned int newHash (unsigned int x);

// added by Shangyu
inline size_t specialHash (unsigned u) {
     return newHash (u);
}

inline size_t specialHash (int u) {
     return newHash ((unsigned) u);
}

// this uses SFINAE to call std::hash () on KeyType if applicable; otherwise, it calls KeyType.hash ()
template <class KeyType>
class Hasher {

        // as a fallback, we will pick this one
        template<typename U>
        static auto hash_impl(U const& u, ...)
               -> size_t {
                 return std::hash<U>().operator()(u);
        }

        // overloading rules will select this one first...  ...unless it's not valid
        template <typename U>
        static auto hash_impl(U const& u, int)
                -> decltype(u.hash()) {
                 return u.hash ();
        }

public:
        static auto hash(const KeyType &k) -> decltype(hash_impl(k, 0)) {
		decltype(hash_impl(k, 0)) temp = hash_impl(k, 0);

		// we can't allow the hash function to return UNUSED
		if (temp == UNUSED) 
			temp = 858931273;

		return temp;
        }

        // allows us to specify a specialized hash function by defining specialHash (U const u)
        template <typename U>
        static auto hash_impl(U const& u, int)
                -> decltype(specialHash (u)) {
                 return specialHash (u);
        }	
};

// the maximum fill factor before we double
#define FILL_FACTOR .667

// access keys, hashes, and data in the underlying array
#define GET_HASH(data, i) (*((size_t *) (((char *) data) + (i * objSize))))
#define GET_HASH_PTR(data, i) ((size_t *) (((char *) data) + (i * objSize)))
#define GET_KEY_PTR(data, i) ((void *) (((char *) data) + sizeof (size_t) + (i * objSize )))
#define GET_VALUE_PTR(data, i) ((void *) (((char *) data) + valueOffset + (i * objSize)))
#define GET_KEY(data, i, type) (*((type *) (((char *) data) + sizeof (size_t) + (i * objSize ))))
#define GET_VALUE(data, i, type) (*((type *) (((char *) data) + valueOffset + (i * objSize))))

// Note: we need to write all operations in constructors, destructors, and assignment operators WITHOUT using
// the underlying type in any way (including assignment, initialization, destruction, size).  
//
template <class KeyType, class ValueType>
void PairArray <KeyType, ValueType> :: setDisableDestructor (bool disableOrNot) {

    this->disableDestructor = disableOrNot;

}

template <class KeyType, class ValueType>
bool PairArray <KeyType, ValueType> :: isDestructorDisabled () {

    return this->disableDestructor;

}


template <class KeyType, class ValueType>
void PairArray <KeyType, ValueType> :: setUpAndCopyFrom (void *target, void *source) const {

	new (target) PairArray <KeyType, ValueType> ();
	PairArray <KeyType, ValueType> &fromMe = *((PairArray <KeyType, ValueType> *) source);
	PairArray <KeyType, ValueType> &toMe = *((PairArray <KeyType, ValueType> *) target);

	// copy the number of slots 
	toMe.numSlots = fromMe.numSlots;
	toMe.usedSlots = fromMe.usedSlots;

        toMe.setDisableDestructor(false);

	// copy the type info
	toMe.keyTypeInfo = fromMe.keyTypeInfo;
	toMe.valueTypeInfo = fromMe.valueTypeInfo;

        if (toMe.keyTypeInfo.getTypeCode() == 0) {
            std :: cout << "PairArray: setUpAndCopyFrom: keyTypeInfo = 0 " << std :: endl;
            
        }

        if (toMe.valueTypeInfo.getTypeCode() == 0) {
            std :: cout << "PairArray: setUpAndCopyFrom: valueTypeInfo = 0" << std :: endl;
        }

	// copy over the size info
	toMe.objSize = fromMe.objSize;
	toMe.valueOffset = fromMe.valueOffset;
	toMe.maxSlots = fromMe.maxSlots;

	// now we need to copy the array
	// if our types are fully primitive, just do a memmove
	if (!toMe.keyTypeInfo.descendsFromObject () && !toMe.valueTypeInfo.descendsFromObject ()) {
		memmove ((void *) toMe.data, (void *) fromMe.data, ((size_t) toMe.objSize) * (toMe.numSlots));
		return;
	}

	// one of them is not primitive...

	// compute the key and value sizes
	uint32_t keySize = (toMe.valueOffset - sizeof (size_t));
	uint32_t valueSize = (toMe.objSize - toMe.valueOffset);
 
	// these are needed to make the GET_HASH and other macros work correctly... they refer
	// to variables objSize and valueOffset... this.objSize and this.valueOffset are possibly
	// undefined here.  By having local variables that shadow these, we get around potential problems
	uint32_t objSize = toMe.objSize;
	uint32_t valueOffset = toMe.valueOffset;

	// loop through and do the deep copy
	for (int i = 0; i < toMe.numSlots; i++) {

		// copy over the hash for this guy
		GET_HASH (toMe.data, i) = GET_HASH (fromMe.data, i);

		// don't copy over an unused slot
		if (GET_HASH (fromMe.data, i) == UNUSED) 
			continue;

		// deal with the key... use memmove on a non-object type
		if (!toMe.keyTypeInfo.descendsFromObject ()) {
			memmove (GET_KEY_PTR (toMe.data, i), GET_KEY_PTR (fromMe.data, i), keySize);
		} else {
                        try {
			    toMe.keyTypeInfo.setUpAndCopyFromConstituentObject (GET_KEY_PTR (toMe.data, i), GET_KEY_PTR (fromMe.data, i));
                        }
                        catch (NotEnoughSpace &n) {
                            //JiaNote: if data type is a handle, it may trigger NotEnoughSpace exception, so handle this here.
                            for (int j = i; j < toMe.numSlots; j++) {
                                GET_HASH(toMe.data, j) = UNUSED;
                            }
                            toMe.setDisableDestructor(true);
                            throw n;
                        }
		}

		// and now same thing on the value
		if (!toMe.valueTypeInfo.descendsFromObject ()) {
			memmove (GET_VALUE_PTR (toMe.data, i), GET_VALUE_PTR (fromMe.data, i), valueSize);
		} else {
                        try {
			    toMe.valueTypeInfo.setUpAndCopyFromConstituentObject (GET_VALUE_PTR (toMe.data, i), GET_VALUE_PTR (fromMe.data, i));
                        }
                        catch (NotEnoughSpace &n) {
                            //JiaNote: if data type is a handle, it may trigger NotEnoughSpace exception, so handle this here.
                            for (int j = i; j < toMe.numSlots; j++) {
                                GET_HASH(toMe.data, j) = UNUSED;
                            }
                            toMe.setDisableDestructor(true);
                            throw n;
                        }
		}
	}
}

// just use setUpAndCopyFrom to do a deep copy... this assumes that we have enough space!!
template <class KeyType, class ValueType>
PairArray <KeyType, ValueType> :: PairArray (const PairArray &toMe) {
	setUpAndCopyFrom (this, &toMe);
}

template <class KeyType, class ValueType>
int PairArray <KeyType, ValueType> :: count (const KeyType &me) {

	// hash this dude
	size_t hashVal = Hasher <KeyType> :: hash (me);
	
	// figure out which slot he goes in
	size_t slot = hashVal % (numSlots - 1);

	// in the worst case, we can loop through the entire hash table looking.  :-(
	for (size_t slotsChecked = 0; slotsChecked < numSlots; slotsChecked++) {

		// if we found an empty slot, then this guy was not here
		if (GET_HASH (data, slot) == UNUSED) {
			return 0;
		} else if (GET_HASH (data, slot) == hashVal) {

			// potential match!!
			if (GET_KEY (data, slot, KeyType) == me) {
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
	std :: cout << "in count(): hashVal = " << hashVal << ", slot = " << slot << ", numSlots ="<< numSlots << ". Warning: Ran off the end of the hash table!!\n";
        return 0;
	//exit (1);
}

template <class KeyType, class ValueType>
void PairArray <KeyType, ValueType> :: setUnused (const KeyType &me) {

        // hash this dude
        size_t hashVal = Hasher <KeyType> :: hash (me);

        // figure out which slot he goes in
        size_t slot = hashVal % (numSlots - 1);

        // in the worst case, we can loop through the entire hash table looking.  :-(
        for (size_t slotsChecked = 0; slotsChecked < numSlots; slotsChecked++) {

                // if we found an empty slot, then this guy was not here
                if (GET_HASH (data, slot) == UNUSED) {
                        std :: cout << "WARNING: setUnused for an empty slot" << std :: endl;
                        return;

                // found a non-empty slot; check for a match
                } else if (GET_HASH (data, slot) == hashVal) {

                        // potential match!!
                        if (GET_KEY (data, slot, KeyType) == me) {

                                // destruct those guys
                                ((KeyType *) (GET_KEY_PTR (data, slot)))->~KeyType ();
                                ((ValueType *) (GET_VALUE_PTR (data, slot)))->~ValueType ();
                                GET_HASH (data, slot) = UNUSED;
                                return;
                        }

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
        std :: cout << "in setUnused(): hashVal = " << hashVal << ", slot = " << slot << ", numSlots ="<< numSlots << ". Warning: Ran off the end of the hash table!!\n";
        //exit (1);
}




template <class KeyType, class ValueType> 
ValueType &PairArray <KeyType, ValueType> :: operator [] (const KeyType &me) {
	
	// basically, if he is not there, we add him and return a reference to a newly-constructed
	// ValueType... if he is there, we simply return a reference to a newly-constructed ValueType

	// hash this dude
	size_t hashVal = Hasher <KeyType> :: hash (me);
	
	// figure out which slot he goes in
	size_t slot = hashVal % (numSlots - 1);

	// in the worst case, we can loop through the entire hash table looking.  :-(
	for (size_t slotsChecked = 0; slotsChecked < numSlots; slotsChecked++) {

		// if we found an empty slot, then this guy was not here
		if (GET_HASH (data, slot) == UNUSED) {
                        try {
			    // construct the key and the value
			    new (GET_KEY_PTR (data, slot)) KeyType ();
			    new (GET_VALUE_PTR (data, slot)) ValueType ();
                        } catch (NotEnoughSpace &n) {
                            std :: cout << "Not enough space when in placement new the key type and value type" << std :: endl;
                            throw n;
                        }
                        
			
			// add the key
                        try {
			    GET_KEY (data, slot, KeyType) = me;
			
                            GET_HASH (data, slot) = hashVal;

			    // increment the number of used slots
			    usedSlots++;

                        } catch (NotEnoughSpace &n) {
                            std :: cout << "Not enough space when inserting new key" << std :: endl;
                            throw n;

                        }

			// and return the value
			return GET_VALUE (data, slot, ValueType);

		// found a non-empty slot; check for a match
		} else if (GET_HASH (data, slot) == hashVal) {

			// potential match!!
			if (GET_KEY (data, slot, KeyType) == me) {

				// match!!
				return GET_VALUE (data, slot, ValueType);		
			}

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
	std :: cout << "Fatal Error: Ran off the end of the hash table!!\n";
	exit (1);
}

template <class KeyType, class ValueType>
PairArray <KeyType, ValueType> :: PairArray (uint32_t numSlotsIn) : PairArray () {
	
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

        setDisableDestructor(false);

	// if we are not a power of two, exit
	if (!gotIt) {
		std :: cout << "Fatal Error: Bad: could not get the correct size  " << numSlotsIn << " for the array\n";
		exit (1);
	}

	// remember the size
	numSlots = numSlotsIn;
	maxSlots = numSlotsIn * FILL_FACTOR;	

	// set everyone to unused
	for (int i = 0; i < numSlots; i++) {
		GET_HASH (data, i) = UNUSED;
	}
}

template <class KeyType, class ValueType>
bool PairArray <KeyType, ValueType> :: isOverFull () {
	return usedSlots >= maxSlots;
}

template <class KeyType, class ValueType>
PairArray <KeyType, ValueType> :: PairArray () {

	// remember the types for this guy
	keyTypeInfo.setup <KeyType> ();
	valueTypeInfo.setup <ValueType> ();

	// used to let the compiler to tell us how to pack items in our array of slots
	MapRecordClass <KeyType, ValueType> temp;
	objSize = temp.getObjSize ();
	valueOffset = temp.getValueOffset ();
	
	// zero slots in the array
	numSlots = 0;

	// no used slots
	usedSlots = 0;

	// the max number of used slots is zero
	maxSlots = 0;

        setDisableDestructor(false);

}

// Note: because this can be called by Object.deleteObject (), it must be written so as to not use TypeContained
template <class KeyType, class ValueType>
PairArray <KeyType, ValueType> :: ~PairArray () {

        if (isDestructorDisabled() == true) {
              return;
        }

        if (keyTypeInfo.getTypeCode() == 0) {
            std :: cout << "PairArray: ~PairArray: keyTypeInfo = 0 " << std :: endl;
         
        }

        if (valueTypeInfo.getTypeCode() == 0) {
            std :: cout << "PairArray: ~PairArray: valueTypeInfo = 0" << std :: endl;
        }
	// do no work if the guys we store do not come from pdb :: Object
	if (!keyTypeInfo.descendsFromObject () && !valueTypeInfo.descendsFromObject ()) 
		return;

	// now, delete each of the objects in there, if we have got an object type
	for (uint32_t i = 0; i < numSlots; i++) {
		if (GET_HASH (data, i) != UNUSED) {
			if (keyTypeInfo.descendsFromObject ())
				keyTypeInfo.deleteConstituentObject (GET_KEY_PTR (data, i));
			if (valueTypeInfo.descendsFromObject ())
				valueTypeInfo.deleteConstituentObject (GET_VALUE_PTR (data, i));
		}
	}
}

template <class KeyType, class ValueType>
Handle <PairArray <KeyType, ValueType>> PairArray <KeyType, ValueType> :: doubleArray () {

	uint32_t howMany = numSlots * 2;

	// allocate the new Array
	Handle <PairArray <KeyType, ValueType>> tempArray = 
		makeObjectWithExtraStorage <PairArray <KeyType, ValueType>> (objSize * howMany, howMany);

	// first, set everything to unused
	// now, re-hash everything 
	PairArray <KeyType, ValueType> &newOne = *tempArray;

	for (uint32_t i = 0; i < numSlots; i++) {

		if (GET_HASH (data, i) != UNUSED) {

			// copy the dude over
			newOne [GET_KEY (data, i, KeyType)] = GET_VALUE (data, i, ValueType);

			// and delete the old one
			GET_KEY (data, i, KeyType).~KeyType ();
		        GET_VALUE (data, i, ValueType).~ValueType ();
			GET_HASH (data, i) = UNUSED;
		}
	}

	// and return this guy
	return tempArray;
}

template <class KeyType, class ValueType>
uint32_t PairArray <KeyType, ValueType> :: numUsedSlots () {   
        return usedSlots;                                
}                                                                

template <class KeyType, class ValueType>
void PairArray <KeyType, ValueType> :: deleteObject (void *deleteMe) {   
        deleter (deleteMe, this);
}                                                                
                                                                 
template <class KeyType, class ValueType>
size_t PairArray <KeyType, ValueType> :: getSize (void *forMe) {                                              
	PairArray <KeyType, ValueType> &target = *((PairArray <KeyType, ValueType> *) forMe);
	return sizeof (PairArray <Nothing>) + target.objSize * target.numSlots;
}

template <class KeyType, class ValueType> 
PDBMapIterator <KeyType, ValueType> :: PDBMapIterator (Handle <PairArray <KeyType, ValueType>> iterateMeIn, bool) : iterateMe (iterateMeIn) {
	slot = 0;
	done = false;
	int objSize = iterateMe->objSize;
	while (slot != iterateMe->numSlots && GET_HASH (iterateMe->data, slot) == UNUSED) 
		slot++;

	if (slot == iterateMe->numSlots)
		done = true;
}

template <class KeyType, class ValueType> 
PDBMapIterator <KeyType, ValueType> :: PDBMapIterator (Handle <PairArray <KeyType, ValueType>> iterateMeIn) : iterateMe (iterateMeIn) {
	done = true;
}

template <class KeyType, class ValueType>
void PDBMapIterator <KeyType, ValueType> :: operator ++ () {
	if (!done)
		slot++;

	int objSize = iterateMe->objSize;
	while (slot != iterateMe->numSlots && GET_HASH (iterateMe->data, slot) == UNUSED) 
		slot++;

	if (slot == iterateMe->numSlots)
		done = true;
}

template <class KeyType, class ValueType>
MapRecordClass <KeyType, ValueType> & PDBMapIterator <KeyType, ValueType> :: operator * () {
	
	int objSize = iterateMe->objSize;
	return *((MapRecordClass <KeyType, ValueType> *) GET_HASH_PTR (iterateMe->data, slot)); 
}

template <class KeyType, class ValueType>
bool PDBMapIterator <KeyType, ValueType> :: operator != (const PDBMapIterator <KeyType, ValueType> &me) const {
	if (!done || !me.done)
		return true;
	return false;
}





}

#endif
