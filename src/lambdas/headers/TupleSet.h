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

#ifndef TUPLE_SET_H
#define TUPLE_SET_H

#include "Handle.h"
#include "PDBVector.h"
#include <functional>

namespace pdb {

// this simple function automatically deferences a pointer type
template <bool B, typename InputType>
auto tryDereference (InputType &arg) ->
        typename std :: enable_if_t <B, decltype (*arg) &> {
        return *arg;
}

// called instead if we can't dereference the pointer type
template <bool B, typename InputType>
auto tryDereference (InputType &&arg) ->
        typename std :: enable_if_t <!B, InputType &> {
        return arg;
}

// this simple function finds the size of a 
template <bool B, typename InputType>
auto getSerializedSize () ->
        typename std :: enable_if_t <B, size_t> {
	return InputType :: getObjectSize ();
}

// called instead if we can't dereference the pointer type
template <bool B, typename InputType>
auto getSerializedSize () ->
        typename std :: enable_if_t <!B, size_t> {
	return sizeof (InputType);
}

// this simple function automatically takes the address of a type
template <bool B, typename InputType>
auto tryToObtainPointer (InputType &arg) ->
        typename std :: enable_if_t <B, InputType*> {
        return &arg;
}

// called instead if we can't dereference the pointer type
template <bool B, typename InputType>
auto tryToObtainPointer (InputType &arg) ->
        typename std :: enable_if_t <!B, InputType &> {
        return arg;
}

class TupleSet;
typedef std :: shared_ptr <TupleSet> TupleSetPtr;

// this structure contains type-specific information that will allow us to properly delete and/or fliter
// a column
struct MaintenanceFuncs {

	// this is a deleter for a particular column, stored as a void*
	std :: function <void (void *)> deleter;

	// this is a filter function for a particular column
	std :: function <void * (void *, std :: vector <bool> &)> filter;

        // this replicates instances of a column to run a join
        std :: function <void * (void *, std :: vector <uint32_t> &)> replicate;


	// this is a function that creates and returns a pdb :: Vector for a column
	std :: function <Handle <Vector <Handle <Object>>> ()> createPDBVector;

	// this function writes out the column to a pdb :: Vector
	std :: function <void (Handle <Vector <Handle <Object>>> &, void *, size_t &)> writeToVector;

	// used to deserialize the column
	std :: function <void (void *, std :: vector <void *> &, size_t)> deSerialize;

	// used to serialize the column
	std :: function <void (void *, std :: vector <void *> &, size_t)> serialize;

	// this is the name of the type that we contain
	std :: string typeContained;

	// tells us if we need to delete
	bool mustDelete;

	// tells us the serialized size of an object in this column
	size_t serializedSize;

	// the last value that we wrote if we are writing out this column
	size_t lastWritten = 0;

	// empty constructor
	MaintenanceFuncs () {}

	// fill all of the fields
	MaintenanceFuncs (std :: function <void (void *)> deleter, std :: function <void * (void *, std :: vector <bool> &)> filter,
                std :: function <void * (void *, std :: vector <uint32_t> &)> replicate,
		std :: function <Handle <Vector <Handle <Object>>> ()> createPDBVector, 
		std :: function <void (Handle <Vector <Handle <Object>>> &, void *, size_t &)> writeToVector, 
		std :: function <void (void *, std :: vector <void *> &, size_t)> serialize, 
		std :: function <void (void *, std :: vector <void *> &, size_t)> deSerialize, bool mustDelete, std :: string typeContained, size_t serializedSize) : 
		deleter (deleter), filter (filter), replicate (replicate), createPDBVector (createPDBVector), writeToVector (writeToVector), deSerialize (deSerialize),
		serialize (serialize), typeContained (typeContained), mustDelete (mustDelete), serializedSize (serializedSize) {}

};

// this is the basic type that it pushed through the system during query processing
class TupleSet {

private:

	// the key of this map is an integer (the identifier of the column) 
	// the value is a pair; the first is a pointer to the column, and the second is a pair of
	// lambdas.  The first is a deleter (which frees the column) and the second is a condenser
	// (that filters rows from the column)
	std :: map <int, std :: pair <void *, MaintenanceFuncs>> columns;

public:

	// get the number of columns in this TupleSet
	int getNumColumns () {
		return columns.size ();
	}

	/* TODO: this will be needed to be able to do joins!!! 
	// deep copies the set of serialized objects from the specified location, do the specified location
	void deepCopyAndDelete (std :: vector <void *> &source, std :: vector <void *> &dest) {
		size_t offset = 0;	
		for (int i = 0; columns.count (i) != 0; i++) {
			// columns[i].second.deepCopyAndDelete (source, dest, offset);
			// offset += columns[i].second.serializedSize;
		}	
	}*/

	// gets a list, in order, of the types of the columns in this tuple set
	// this can be used at a later time to re-constitute the tuple set
	std :: vector <std :: string> getTypeNames () {
		std :: vector <std :: string> output;
		for (int i = 0; columns.count (i) != 0; i++) {
			output.push_back (columns[i].second.typeContained);
		}
		return output;
	}

	/*  TODO: this will be needed to be able to do joins!!!
	// gets the serialized size of one row in this tuple set
	size_t getSerializedSize () {
		return columns[whichColumn].second.serializedSize;
		size_t offset = 0;	
		for (int i = 0; columns.count (i) != 0; i++) {
			offset += columns[i].second.serializedSize;
		}
		return offset;
	} */

	// serialize all of the colums in this TupleSet to the positions pointed to by toHere.  Note that
	// the length of toHere must match the number of tuples in this TupleSet
	void serialize (std :: vector <void *> &toHere) {
		size_t offset = 0;	
		for (int i = 0; columns.count (i) != 0; i++) {
			columns[i].second.serialize (columns[i].first, toHere, offset);
			offset += columns[i].second.serializedSize;
		}
	}

	// deserialize all of the columns in this TupleSet from the positions pointed to by fromHere.  
	void deSerialize (std :: vector <void *> &fromHere) {
		size_t offset = 0;
		for (int i = 0; columns.count (i) != 0; i++) {
			columns[i].second.deSerialize (columns[i].first, fromHere, offset);
			offset += columns[i].second.serializedSize;
		}
	}

	// this takes as input a vector of pointers to 
	// return a specified column
	template <typename ColType> 
	std :: vector <ColType> &getColumn (int whichColumn) {
		if (columns.count (whichColumn) == 0) {
			std :: cout << "This is bad. Tried to get column " << whichColumn << " but could not find it.\n";
		}
		return *((std :: vector <ColType> *) columns[whichColumn].first);
	}




	// writes out a specified column... the boolean argument is true when we want to start from scratch; false
	// if we want to continue the last write
	void writeOutColumn (int whichColumn, Handle <Vector <Handle <Object>>> &writeToMe, bool startFromScratch) {
		if (columns.count (whichColumn) == 0) {
			std :: cout << "This is bad. Tried to write out column " << whichColumn << " but could not find it.\n";
		}
		auto &which = columns[whichColumn];

		// if we we need to start over, then do do
		if (startFromScratch)
			 which.second.lastWritten = 0;

		which.second.writeToVector (writeToMe, which.first, which.second.lastWritten);
	}

	// use the specified column to build pdb :: Vector of the correct type to hold the output
	// Note: this had better be a Vector <Handle <Something>> or we are going to have problems!!
	Handle <Vector <Handle <Object>>> getOutputVector (int whichColToOutput) {
		return columns[whichColToOutput].second.createPDBVector ();	
	}

	// see if we have the specified column
	bool hasColumn (int whichColumn) {
		return columns.count (whichColumn) != 0;
	}

	~TupleSet () {

		// delete all of the columns
		for (auto &a : columns) {
			auto &res = a.second;
			if (res.second.mustDelete)
				res.second.deleter (res.first);
		}
	}

	// filters a column
	void filterColumn (int whichColToFilter, std :: vector <bool> &usingMe) {

		// kill the old one so we don't have a memory leak
		if (hasColumn (whichColToFilter)) {

			// filter the column, getting a new version
			auto &value = columns[whichColToFilter];
			auto res = value.second.filter (value.first, usingMe);	

			// delete the old one, if necessary
			if (value.second.mustDelete) {
				value.second.deleter (value.first);
			}

			// record the new column
			value.first = res;

			// remember that we need to delete it
			value.second.mustDelete = true;
			return;
		}	

		std :: cout << "This is really bad... trying to filter a non-existing column";
	}

        // creates a replication of the column from another tuple set, copying each item a specified
        // number of times and deleting the target, if necessary
        void replicate (TupleSetPtr fromMe, int whichColInFromMe, int whichColToCopyTo, std :: vector <uint32_t> &replications) {

                // kill the old one so we don't have a memory leak
                if (hasColumn (whichColToCopyTo)) {
                        // delete the existing column, if necessary
                        auto &value = columns[whichColToCopyTo];
                        if (value.second.mustDelete) {
                                value.second.deleter (value.first);
                        }
                }

                // create a copy of the maintenance funcs
                auto &value = fromMe->columns[whichColInFromMe];
                MaintenanceFuncs temp = value.second;

                // remember that this is a deep copy... so we need to delete
                temp.mustDelete = true;

                // and go ahead and replicate the column
                void *newCol = temp.replicate (value.first, replications);

                // and go ahead and remember the column
                columns[whichColToCopyTo] = std :: make_pair (newCol, temp);
        }



	// copies a column from another TupleSet, deleting the target, if necessary
	void copyColumn (TupleSetPtr fromMe, int whichColInFromMe, int whichColToCopyTo) {

		// kill the old one so we don't have a memory leak
		if (hasColumn (whichColToCopyTo)) {

			// delete the existing column, if necessary
			auto &value = columns[whichColToCopyTo];
			if (value.second.mustDelete) {
				value.second.deleter (value.first);	
			}
		}	

		// create a copy of the maintenance funcs
		auto &value = fromMe->columns[whichColInFromMe];
		MaintenanceFuncs temp = value.second;

		// remember that this is a shallow copy... no need to delete
		temp.mustDelete = false;

		// and go ahead and remember the column
		columns[whichColToCopyTo] = std :: make_pair (value.first, temp);
	}

	// creates a new column, adding it to the tuple set
	template <typename ColType>
	void addColumn (int where, std :: vector <ColType> *addMe, bool needToDelete) {

		// delete the old one, if needed
		if (columns.count (where) != 0) {
			auto &value = columns[where];
			if (value.second.mustDelete) {
				value.second.deleter (value.first);	
			}
		}

		// now, add the new column... this reqires creating three lambdas to deal with
		// column maintenance.  The first lamba deletes the column, correctly taking into
		// account the type of the column...
		std :: function <void (void *)> deleter;
		deleter = [] (void *deleteMe) {
				std :: vector <ColType> *killMe = (std :: vector <ColType> *) deleteMe;
				delete killMe;
			};

		// and the second lambda filters the column, again correctly taking into account
		// the type of the column
		std :: function <void * (void *, std :: vector <bool> &)> filter;
		filter = [] (void *filter, std :: vector <bool> &whichAreValid) {
				std :: vector <ColType> &filterMe = *((std :: vector <ColType> *) filter);

				// count the number of rows that need to be retained
				int counter = 0;
				for (auto a : whichAreValid)
					if (a)
						counter++;

				// copy the ones that need to be retained over
				std :: vector <ColType> *newVec = new std :: vector <ColType> (counter);
				counter = 0;
				for (int i = 0; i < filterMe.size (); i++) {
					if (whichAreValid[i])
						(*newVec)[counter++] = filterMe[i];
				}
	
				// and return the result
				return (void *) newVec;
			};
                std :: function <void * (void *, std :: vector <uint32_t> &)> replicate;
                replicate = [] (void *replicate, std :: vector <uint32_t> &timesToReplicate) {

                                std :: vector <ColType> &replicateMe = *((std :: vector <ColType> *) replicate);

                                // count the number of rows that need to be retained
                                int counter = 0;
                                for (auto &a : timesToReplicate)
                                        counter += a;

                                // copy the ones that need to be retained over
                                std :: vector <ColType> *newVec = new std :: vector <ColType> (counter);
                                counter = 0;
                                for (int i = 0; i < timesToReplicate.size (); i++) {
                                        for (int j = 0; j < timesToReplicate[i]; j++) {
                                                (*newVec)[counter] = replicateMe[i];
                                                counter++;
                                        }
                                }

                                // and return the result
                                return (void *) newVec;
                        };

	
		// the third lambda is responsible for writing this column to an output vector
		std :: function <void (Handle <Vector <Handle <Object>>> &, void *, size_t &)> writeToVector;
		if (std :: is_base_of <PtrBase, ColType> :: value) 
			writeToVector = [] (Handle <Vector <Handle <Object>>> &writeToMe, void *writeMe, size_t &lastWritten) {
				std :: vector <Ptr <Handle <Object>>> &writeMeOut = *((std :: vector <Ptr <Handle <Object>>> *) writeMe);
				Vector <Handle <Object>> &outputToMe = *writeToMe;
				for (; lastWritten < writeMeOut.size (); lastWritten++) {
					Ptr <Handle <Object>> temp = writeMeOut[lastWritten];
					outputToMe.push_back (*(writeMeOut[lastWritten]));
				}
			};
		else
			writeToVector = [] (Handle <Vector <Handle <Object>>> &writeToMe, void *writeMe, size_t &lastWritten) {
				std :: vector <Handle <Object>> &writeMeOut = *((std :: vector <Handle <Object>> *) writeMe);
				Vector <Handle <Object>> &outputToMe = *writeToMe;
				for (; lastWritten < writeMeOut.size (); lastWritten++) {
					outputToMe.push_back (writeMeOut[lastWritten]);
				}
			};

		// the fourth lambda is responsible for writing this column to an array of positions
		std :: function <void (void *, std :: vector <void *> &, size_t)> serialize;
		serialize = [] (void *serializeMe, std :: vector <void *> &toHere, size_t offset) {

			// get the column who we are serializing
			std :: vector <ColType> &writeMeOut = *((std :: vector <ColType> *) serializeMe);

			// make sure we have the correct number of slots to serilize to
			int numToWrite = writeMeOut.size ();
			if (numToWrite != toHere.size ()) {
				std :: cout << "This is bad.  Why does the number of serialization slots not match the TupleSet size?\n";
				exit (1);
			}

			// serialize everyone
			for (int i = 0; i < numToWrite; i++) {

				// tryDereference will return either (a) the pointed-to object if this is a Ptr <> type, or (b) the object
				ColType *temp = nullptr;
				typename std :: remove_reference <decltype (tryDereference <std :: is_base_of <PtrBase, ColType> :: value> (*temp))> :: type *target = 
					(typename std :: remove_reference <decltype (tryDereference <std :: is_base_of <PtrBase, ColType> :: value> (*temp))> :: type *) 
					(((char *) toHere[i]) + offset);

				// now, copy the object over (will automatically do a deep copy if needed)
				*target = tryDereference <std :: is_base_of <PtrBase, ColType> :: value> (writeMeOut[i]);
			}
		};	
		
		// the fifth lambda is responsible for reading this column from an array of positions
		std :: function <void (void *, std :: vector <void *> &, size_t)> deSerialize;
		deSerialize = [] (void *deSerializeToMe, std :: vector <void *> &fromHere, size_t offset) {

			// get the column who we are deSerializing to
			std :: vector <ColType> &writeToMe = *((std :: vector <ColType> *) deSerializeToMe);

			// make sure we have the correct number of slots 
			int numToWrite = writeToMe.size ();
			if (numToWrite != fromHere.size ()) {
				std :: cout << "This is bad.  Why does the number of serialization slots not match the TupleSet size?\n";
				exit (1);
			}

			// deserialize everyone
			for (int i = 0; i < numToWrite; i++) {

				// tryDereference will return either (a) the pointed-to object if this is a Ptr <> type, or (b) the object... so source
				// is going to be a pointer to an object type
				typename std :: remove_reference <decltype (tryDereference <std :: is_base_of <PtrBase, ColType> :: value> (writeToMe[0]))> :: type *source = 
					(typename std :: remove_reference <decltype (tryDereference <std :: is_base_of <PtrBase, ColType> :: value> (writeToMe[0]))> :: type *) 
					(((char *) fromHere[i]) + offset);

				// tryToObtainPointer is going to (a) return a pointer to *source if this ColType is a Ptr <> type, or else (b) *source itself
				// if ColType is not a Ptr <> type
				writeToMe[i] = tryToObtainPointer <std :: is_base_of <PtrBase, ColType> :: value> (*source);
			}
		};

		// finally, the sixth creates a pdb :: Vector to hold the column
		std :: function <Handle <Vector <Handle <Object>>> ()> createPDBVector;
		createPDBVector = [] () {
				Handle <Vector <Handle <ColType>>> returnVal = makeObject <Vector <Handle <ColType>>> ();
				return unsafeCast <Vector <Handle <Object>>> (returnVal);
			};
	
		MaintenanceFuncs myFuncs (deleter, filter, replicate, createPDBVector, writeToVector, serialize, deSerialize, 
			needToDelete, getTypeName <ColType> (), getSerializedSize <std :: is_base_of <PtrBase, ColType> :: value, ColType> ());
		columns [where] = std :: make_pair ((void *) addMe, myFuncs);
	}
};

}

#endif

