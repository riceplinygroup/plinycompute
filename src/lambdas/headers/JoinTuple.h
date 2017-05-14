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

#ifndef JOIN_TUPLE_H
#define JOIN_TUPLE_H

#include "JoinMap.h"
#include "SinkMerger.h"

namespace pdb {

template <typename T>
void copyFrom (T &out, Handle <T> &in) {
        out = *in;
}

template <typename T>
void copyFrom (T &out, T &in) {
        out = in;
}

template <typename T>
void copyFrom (Handle <T> &out, Handle <T> &in) {
        out = in;
}

template <typename T>
void copyFrom (Handle<T> &out, T &in) {
        *out = in;
}

template <typename T>
void copyTo (T &out, Handle <T> &in) {
	char *location = (char *) &out;
	location -= REF_COUNT_PREAMBLE_SIZE;
	in = (RefCountedObject <T> *) location;
}

template <typename T>
void copyTo (Handle <T> &out, Handle <T> &in) {
        in = out;
}

// this checks to see if the class is abstract
// used like: decltype (IsAbstract <Foo> :: val) a;
// the type of a will be Handle <Foo> if foo is abstract, and Foo otherwise.
//
template <typename T>
struct IsAbstract {
    template <typename U>
    static U test(U x, int);

    template <typename U, typename ...Rest>
    static Handle <U> test(U &x, Rest...);

    static decltype (test<T>(*((T *) 0), 1)) val;
};

// all join tuples decend from this
class JoinTupleBase {

};

// this template is used to hold a tuple made of one row from each of a number of columns in a TupleSet
template <typename HoldMe, typename MeTo>
class JoinTuple : public JoinTupleBase {

public:

	// this stores the base type
        decltype (IsAbstract <HoldMe> :: val) myData;

	// and this is the recursion
        MeTo myOtherData;

	static void *allocate (TupleSet &processMe, int where) {
		std :: cout << "Creating column for type " << getTypeName <Handle <HoldMe>> () << " at position " << where << "\n";
		std :: vector <Handle <HoldMe>> *me = new std :: vector <Handle <HoldMe>>;
		processMe.addColumn (where, me, true);
		return me;	
	}

        //JiaNote: add the below two functions to facilitate JoinMap merging for broadcast join
        void copyDataFrom (Handle<HoldMe> me) {
                pdb :: copyFrom (myData, me);
        }

        void copyDataFrom (HoldMe me) {
                pdb :: copyFrom (myData, me);
        }

	void copyFrom (void *input, int whichPos) {
		// std :: cout << "Packing column for type " << getTypeName <decltype (IsAbstract <HoldMe> :: val)> () << " at position " << whichPos << "\n";
		std :: vector <Handle <HoldMe>> &me = *((std :: vector <Handle <HoldMe>> *) input);
		pdb :: copyFrom (myData, me[whichPos]);	
	}

	void copyTo (void *input, int whichPos) {
		std :: vector <Handle <HoldMe>> &me = *((std :: vector <Handle <HoldMe>> *) input);

		if (whichPos >= me.size ()) {
			Handle <HoldMe> temp;
			pdb :: copyTo (myData, temp);
			me.push_back (temp);
		} else {
			pdb :: copyTo (myData, me[whichPos]);	
		}
	}

	static void truncate (void *input, int i) {
        	std :: vector <Handle <HoldMe>> &valColumn = *((std :: vector <Handle <HoldMe>> *) (input));
        	valColumn.erase (valColumn.begin (), valColumn.begin () + i);
	}

	static void eraseEnd (void *input, int i) {
        	std :: vector <Handle <HoldMe>> &valColumn = *((std :: vector <Handle <HoldMe>> *) (input));
        	valColumn.resize (i);
	}
	
};



/***** CODE TO CREATE A SET OF ATTRIBUTES IN A TUPLE SET *****/

// this adds a new column to processMe of type TypeToCreate.  This is added at position offset + positions[whichPos]
template <typename TypeToCreate>
typename std :: enable_if<sizeof (TypeToCreate :: myOtherData) == 0, void>::type createCols (void **putUsHere, TupleSet &processMe, 
	int offset, int whichPos, std :: vector <int> positions) {
	putUsHere[whichPos] = TypeToCreate :: allocate (processMe, offset + positions[whichPos]);
}

// recursive version of the above
template <typename TypeToCreate>
typename std :: enable_if<sizeof (TypeToCreate :: myOtherData) != 0, void>::type createCols (void **putUsHere,
	TupleSet &processMe, int offset, int whichPos, std :: vector <int> positions) {
	putUsHere[whichPos] = TypeToCreate :: allocate (processMe, offset + positions[whichPos]);
	createCols <decltype (TypeToCreate :: myOtherData)> (putUsHere, processMe, offset, whichPos + 1, positions);
}

//JiaNote: add below two functions to c a join tuple from another join tuple
/**** CODE TO COPY A JOIN TUPLE FROM ANOTHER JOIN TUPLE ****/

//this is the non-recursive version of packData; called if the type does NOT have a field called myOtherData, in which case
// we can just directly copy the data
template <typename TypeToPackData>
typename std :: enable_if<(sizeof (TypeToPackData :: myOtherData) == 0)&& (sizeof (TypeToPackData :: myData) != 0), void>::type packData (TypeToPackData &arg, TypeToPackData data) {
        arg.copyDataFrom (data.myData);
}

//this is the recursive version of packData; called if the type has a field called myOtherData to which we can recursively pack values to.
template <typename TypeToPackData>
typename std :: enable_if<(sizeof (TypeToPackData :: myOtherData) != 0) && (sizeof (TypeToPackData :: myData) != 0), void>::type packData (TypeToPackData &arg, TypeToPackData data) {
        arg.copyDataFrom (data.myData);
        packData (arg.myOtherData, data.myOtherData);
}


/***** CODE TO PACK A JOIN TUPLE FROM A SET OF VALUES SPREAD ACCROSS COLUMNS *****/

// this is the non-recursive version of pack; called if the type does NOT have a field called "myData", in which case
// we can just directly copy the data
template <typename TypeToPack>
typename std :: enable_if<sizeof (TypeToPack :: myOtherData) == 0, void>::type pack (TypeToPack &arg, int whichPosInTupleSet, int whichVec, void **us) {
	arg.copyFrom (us[whichVec], whichPosInTupleSet);
}

// this is the recursive version of pack; called if the type has a field called "myData" to which we can recursively
// pack values to.  Basically, what it does is to accept a pointer to a list of pointers to various std :: vector 
// objects.  We are going to recurse through the list of vectors, and for each vector, we record the entry at 
// the position whichPosInTupleSet
template <typename TypeToPack>
typename std :: enable_if<sizeof (TypeToPack :: myOtherData) != 0, void>::type pack (TypeToPack &arg, int whichPosInTupleSet, int whichVec, void **us) {

	arg.copyFrom (us[whichVec], whichPosInTupleSet);
	pack (arg.myOtherData, whichPosInTupleSet, whichVec + 1, us);
}

/***** CODE TO UNPACK A JOIN TUPLE FROM A SET OF VALUES SPREAD ACCROSS COLUMNS *****/

// this is the non-recursive version of unpack
template <typename TypeToUnPack>
typename std :: enable_if<sizeof (TypeToUnPack :: myOtherData) == 0, void>::type unpack (TypeToUnPack &arg, int whichPosInTupleSet, int whichVec, void **us) {
	arg.copyTo (us[whichVec], whichPosInTupleSet);
}

// this is analagous to pack, except that it unpacks this tuple into an array of vectors
template <typename TypeToUnPack>
typename std :: enable_if<sizeof (TypeToUnPack :: myOtherData) != 0, void>::type unpack (TypeToUnPack &arg, int whichPosInTupleSet, int whichVec, void **us) {

	arg.copyTo (us[whichVec], whichPosInTupleSet);
	unpack (arg.myOtherData, whichPosInTupleSet, whichVec + 1, us);
}

/***** CODE TO ERASE DATA FROM THE END OF A SET OF VECTORS *****/

// this is the non-recursive version of eraseEnd
template <typename TypeToTruncate>
typename std :: enable_if<sizeof (TypeToTruncate :: myOtherData) == 0, void>::type eraseEnd (int i, int whichVec, void **us) {

	TypeToTruncate :: eraseEnd (us[whichVec], i);
}

// recursive version
template <typename TypeToTruncate>
typename std :: enable_if<sizeof (TypeToTruncate :: myOtherData) != 0, void>::type eraseEnd (int i, int whichVec, void **us) {

	TypeToTruncate :: eraseEnd (us[whichVec], i);
	eraseEnd <decltype (TypeToTruncate::myOtherData)> (i, whichVec + 1, us);
}

/***** CODE TO TRUNCATE A SET OF VECTORS *****/

// this is the non-recursive version of truncate
template <typename TypeToTruncate>
typename std :: enable_if<sizeof (TypeToTruncate :: myOtherData) == 0, void>::type truncate (int i, int whichVec, void **us) {

	TypeToTruncate :: truncate (us[whichVec], i);
}

// this function goes through a list of vectors, and truncates each of them so that the first i entries of each vector is removed
template <typename TypeToTruncate>
typename std :: enable_if<sizeof (TypeToTruncate :: myOtherData) != 0, void>::type truncate (int i, int whichVec, void **us) {

	TypeToTruncate :: truncate (us[whichVec], i);
	truncate <decltype (TypeToTruncate::myOtherData)> (i, whichVec + 1, us);
}

// this clsas is used to encapaulte the computation that is responsible for probing a hash table
template <typename RHSType>
class JoinProbe : public ComputeExecutor {

private:

        // this is the output TupleSet that we return
        TupleSetPtr output;

        // the attribute to operate on
        int whichAtt;

        // to setup the output tuple set
        TupleSetSetupMachine myMachine;

	// the hash talbe we are processing
	Handle <JoinMap <RHSType>> inputTable;

	// the list of counts for matches of each of the input tuples
	std :: vector <uint32_t> counts;

	// this is the list of all of the output columns in the output TupleSetPtr
	void **columns;

	// used to create space of attributes in the case that the atts from attsToIncludeInOutput are not the first bunch of atts
	// inside of the output tuple
	int offset;

public:

	~JoinProbe () {
		if (columns != nullptr)
			delete columns;
	}

	// when we probe a hash table, a subset of the atts that we need to put into the output stream are stored in the hash table... the positions
	// of these packed atts are stored in typesStoredInHash, so that they can be extracted.  inputSchema, attsToOperateOn, and attsToIncludeInOutput 
	// are standard for executors: they tell us the details of the input that are streaming in, as well as the identity of the has att, and 
	// the atts that will be streamed to the output, from the input.  needToSwapLHSAndRhs is true if it's the case that theatts stored in the
	// hash table need to come AFTER the atts being streamed through the join
	JoinProbe (void *hashTable, std :: vector <int> &positions, 
		TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput, bool needToSwapLHSAndRhs) :
                myMachine (inputSchema, attsToIncludeInOutput) {

		// extract the hash table we've been given
		Record <JoinMap <RHSType>> *input = (Record <JoinMap <RHSType>> *) hashTable;
		std :: cout << "In join probe\n";
		inputTable = input->getRootObject ();

		// set up the output tuple
		output = std :: make_shared <TupleSet> ();
		columns = new void *[positions.size ()];
		if (needToSwapLHSAndRhs) {
			offset = positions.size ();
			createCols <RHSType> (columns, *output, 0, 0, positions);
			std :: cout << "We do need to add the pipelined data to the back end of the output tuples.\n";
		} else {
			offset = 0;
			createCols <RHSType> (columns, *output, attsToIncludeInOutput.getAtts ().size (), 0, positions);
		}

		// this is the input attribute that we will hash in order to try to find matches
		std :: vector <int> matches = myMachine.match (attsToOperateOn);
		whichAtt = matches[0];

	}

	TupleSetPtr process (TupleSetPtr input) override {

		std :: vector <size_t> inputHash = input->getColumn <size_t> (whichAtt);
		JoinMap <RHSType> &inputTableRef = *inputTable;

		// redo the vector of hash counts if it's not the correct size
		if (counts.size () != inputHash.size ()) {
			counts.resize (inputHash.size ());
		}
		
		// now, run through and attempt to hash	
		int overallCounter = 0;
		for (int i = 0; i < inputHash.size (); i++) {
			
			// deal with all of the matches
			auto a = inputTableRef.lookup (inputHash[i]);
			int numHits = a.size ();
			
			for (int which = 0; which < numHits; which++) {
				unpack (a[which], overallCounter, 0, columns);
				overallCounter++;
			}

			// remember how many matches we had
			counts[i] = numHits;
		}	

		// truncate if we have extra
		eraseEnd <RHSType> (overallCounter, 0, columns);
		
		// and finally, we need to relpicate the input data
		myMachine.replicate (input, output, counts, offset);

		// outta here!
		return output;
	}
};


//JiaNote: this class is used to create a SinkMerger object that merges multiple JoinSinks for broadcast join
template <typename RHSType>
class JoinSinkMerger : public SinkMerger {


public:


        ~JoinSinkMerger () {
        }

        JoinSinkMerger () {
        }

        Handle <Object> createNewOutputContainer () override {

                // we simply create a new map to store the output
                Handle <JoinMap <RHSType>> returnVal = makeObject <JoinMap <RHSType>> ();
                return returnVal;
        }

        void writeOut (Handle<Object> mergeMe, Handle <Object> &mergeToMe) override {

                // get the map we are adding to
                Handle <JoinMap <RHSType>> mergedMap = unsafeCast <JoinMap <RHSType>> (mergeToMe);
                JoinMap <RHSType> &myMap = *mergedMap;
                std :: cout << "Merged map current size: " << myMap.size() << std :: endl;
                Handle <JoinMap <RHSType>> mapToMerge = unsafeCast <JoinMap <RHSType>> (mergeMe);
                JoinMap <RHSType> &theOtherMap = *mapToMerge;
                std :: cout << "The map to merge size: " << theOtherMap.size() << std :: endl;

                for (JoinMapIterator<RHSType> iter = theOtherMap.begin(); iter != theOtherMap.end(); ++iter) {
                    JoinRecordList<RHSType> * myList = *iter;
                    size_t mySize = myList->size();
                    size_t myHash = myList->getHash();
                    if (mySize > 0) {
                        for (size_t i = 0; i < mySize; i++) {
                        
                            RHSType * temp = &(myMap.push(myHash));
                            packData (*temp, ((*myList)[i]));

                        }
                    }
                    delete (myList);
                }
         }


};




// this class is used to create a ComputeSink object that stores special objects that wrap up multiple columns of a tuple
template <typename RHSType> 
class JoinSink : public ComputeSink {

private:

	// tells us which attribute is the key
	int keyAtt;

	// if useTheseAtts[i] = j, it means that the i^th attribute that we need to extract from the input tuple is j
	std :: vector <int> useTheseAtts;

	// if whereEveryoneGoes[i] = j, it means that the i^th entry in useTheseAtts goes in the j^th slot in the holder tuple
	std :: vector <int> whereEveryoneGoes;

	// this is the list of columns that we are processing
	void **columns = nullptr;

public:

	~JoinSink () {
		if (columns != nullptr)
			delete columns;
	}

	JoinSink (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &additionalAtts, std :: vector <int> &whereEveryoneGoes) :
		whereEveryoneGoes (whereEveryoneGoes) {

		// used to manage attributes and set up the output
		TupleSetSetupMachine myMachine (inputSchema);

		// figure out the key att
		std :: vector <int> matches = myMachine.match (attsToOperateOn);
		keyAtt = matches[0];

		// now, figure out the attributes that we need to store in the hash table
		useTheseAtts = myMachine.match (additionalAtts);
	}

	Handle <Object> createNewOutputContainer () override {
		
		// we simply create a new map to store the output
		Handle <JoinMap <RHSType>> returnVal = makeObject <JoinMap <RHSType>> ();
		return returnVal;
	}

	void writeOut (TupleSetPtr input, Handle <Object> &writeToMe) override {

		// get the map we are adding to
		Handle <JoinMap <RHSType>> writeMe = unsafeCast <JoinMap <RHSType>> (writeToMe);
		JoinMap <RHSType> &myMap = *writeMe;

		// get all of the columns
		if (columns == nullptr)
			columns = new void *[whereEveryoneGoes.size ()];

		int counter = 0;
		// before: for (auto &a: whereEveryoneGoes) {
		for (counter = 0; counter < whereEveryoneGoes.size (); counter++) {
			// before: columns[a] = (void *) &(input->getColumn <int> (useTheseAtts[counter]));
			columns[counter] = (void *) &(input->getColumn <int> (useTheseAtts[whereEveryoneGoes[counter]]));
			// before: counter++;
		}

		// this is where the hash attribute is located
		std :: vector <size_t> &keyColumn = input->getColumn <size_t> (keyAtt);

		size_t length = keyColumn.size ();
		for (size_t i = 0; i < length; i++) {

			// try to add the key... this will cause an allocation for a new key/val pair
			if (myMap.count (keyColumn[i]) == 0) {

				try {
					RHSType &temp = myMap.push (keyColumn[i]);
					pack (temp, i, 0, columns);
	
				// if we get an exception, then we could not fit a new key/value pair
				} catch (NotEnoughSpace &n) {
	
					// if we got here, then we ran out of space, and so we need to delete the already-processed
					// data so that we can try again...
					myMap.setUnused (keyColumn[i]);
					truncate <RHSType> (i, 0, columns);
					keyColumn.erase (keyColumn.begin (), keyColumn.begin () + i);
					throw n;	
				}

			// the key is there
			} else {

				// and add the value
				RHSType *temp;
				try {
				
					temp = &(myMap.push (keyColumn[i]));

				// an exception means that we couldn't complete the addition
				} catch (NotEnoughSpace &n) { 

					truncate <RHSType> (i, 0, columns);
					keyColumn.erase (keyColumn.begin (), keyColumn.begin () + i);
					throw n;
				}
	
				// now try to do the copy
				try {

					pack (*temp, i, 0, columns);

				// if the copy didn't work, pop the value off
				} catch (NotEnoughSpace &n) {

					myMap.setUnused (keyColumn[i]);
					truncate <RHSType> (i, 0, columns);
					keyColumn.erase (keyColumn.begin (), keyColumn.begin () + i);
					throw n;
				}	
			}
		}	
	}
};

// all join singletons descend from this
class JoinTupleSingleton {

public:

	virtual ComputeExecutorPtr getProber (void *hashTable, std :: vector <int> &positions,
                TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput, bool needToSwapLHSAndRhs) = 0;

	virtual ComputeSinkPtr getSink (TupleSpec &consumeMe, TupleSpec &attsToOpOn, TupleSpec &projection, std :: vector <int> whereEveryoneGoes) = 0;

        virtual SinkMergerPtr getMerger() = 0;

};

// this is an actual class 
template <typename HoldMe>
class JoinSingleton : public JoinTupleSingleton {

	// the actual data that we hold
        HoldMe myData;

public:

	// gets a hash table prober 
	ComputeExecutorPtr getProber (void *hashTable, std :: vector <int> &positions, 
                TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput, bool needToSwapLHSAndRhs) override {
		return std :: make_shared <JoinProbe <HoldMe>> (hashTable, positions, inputSchema, 
			attsToOperateOn, attsToIncludeInOutput, needToSwapLHSAndRhs);
	}

	// creates a compute sink for this particular type
        ComputeSinkPtr getSink (TupleSpec &consumeMe, TupleSpec &attsToOpOn, TupleSpec &projection, std :: vector <int> whereEveryoneGoes) override {
		return std :: make_shared <JoinSink <HoldMe>> (consumeMe, attsToOpOn, projection, whereEveryoneGoes);
        }

        //create a merger
        SinkMergerPtr getMerger () override {
                return std :: make_shared <JoinSinkMerger <HoldMe>> ();
        }
};

typedef std::shared_ptr <JoinTupleSingleton> JoinTuplePtr;

inline int findType (std :: string &findMe, std :: vector <std :: string> &typeList) {

	for (int i = 0; i < typeList.size (); i++) {
		if (typeList[i] == findMe) {
			typeList[i] = std :: string ("");
			return i;
		}
	}
	return -1;
}

template <typename In1>
typename std :: enable_if<std :: is_base_of <JoinTupleBase, In1> :: value, JoinTuplePtr>::type findCorrectJoinTuple 
	(std :: vector <std :: string> &typeList, std :: vector <int> &whereEveryoneGoes);

template <typename In1, typename ...Rest>
typename std :: enable_if<sizeof ...(Rest) != 0 && !std :: is_base_of <JoinTupleBase, In1> :: value, JoinTuplePtr>::type findCorrectJoinTuple 
	(std :: vector <std :: string> &typeList, std :: vector <int> &whereEveryoneGoes);

template <typename In1, typename In2, typename ...Rest>
typename std :: enable_if<std :: is_base_of <JoinTupleBase, In1> :: value, JoinTuplePtr>::type findCorrectJoinTuple 
	(std :: vector <std :: string> &typeList, std :: vector <int> &whereEveryoneGoes);

template <typename In1>
typename std :: enable_if<!std :: is_base_of <JoinTupleBase, In1> :: value, JoinTuplePtr>::type findCorrectJoinTuple 
	(std :: vector <std :: string> &typeList, std :: vector <int> &whereEveryoneGoes);

template <typename In1>
typename std :: enable_if<!std :: is_base_of <JoinTupleBase, In1> :: value, JoinTuplePtr>::type findCorrectJoinTuple 
	(std :: vector <std :: string> &typeList, std :: vector <int> &whereEveryoneGoes) {

	// we must always have one type...
	JoinTuplePtr returnVal;
	std :: string in1Name = getTypeName <Handle <In1>> ();
        std :: cout << "in1Name=" << in1Name << std :: endl;
	int in1Pos = findType (in1Name, typeList);

	if (in1Pos != -1) {
		whereEveryoneGoes.push_back (in1Pos);
		typeList [in1Pos] = in1Name;
		return std :: make_shared <JoinSingleton <JoinTuple <In1, char[0]>>> ();
	} else {
		std :: cout << "Why did we not find a type?\n";
		exit (1);
	}
}

template <typename In1>
typename std :: enable_if<std :: is_base_of <JoinTupleBase, In1> :: value, JoinTuplePtr>::type findCorrectJoinTuple 
	(std :: vector <std :: string> &typeList, std :: vector <int> &whereEveryoneGoes) {
	return std :: make_shared <JoinSingleton <In1>> ();
}

template <typename In1, typename ...Rest>
typename std :: enable_if<sizeof ...(Rest) != 0 && !std :: is_base_of <JoinTupleBase, In1> :: value, JoinTuplePtr>::type findCorrectJoinTuple 
	(std :: vector <std :: string> &typeList, std :: vector <int> &whereEveryoneGoes) {

	JoinTuplePtr returnVal;
	std :: string in1Name = getTypeName <Handle <In1>> ();
        std :: cout << "in1Name =" << in1Name << std :: endl;
	int in1Pos = findType (in1Name, typeList);

	if (in1Pos != -1) {
		returnVal = findCorrectJoinTuple <JoinTuple <In1, char[0]>, Rest...> (typeList, whereEveryoneGoes);
		whereEveryoneGoes.push_back (in1Pos);
		typeList [in1Pos] = in1Name;
	} else {
		returnVal = findCorrectJoinTuple <Rest...> (typeList, whereEveryoneGoes);
	}

	return returnVal;
}

template <typename In1, typename In2, typename ...Rest>
typename std :: enable_if<std :: is_base_of <JoinTupleBase, In1> :: value, JoinTuplePtr>::type findCorrectJoinTuple 
	(std :: vector <std :: string> &typeList, std :: vector <int> &whereEveryoneGoes) {

	JoinTuplePtr returnVal;
	std :: string in2Name = getTypeName <Handle <In2>> ();
	int in2Pos = findType (in2Name, typeList);

	if (in2Pos != -1) {
		returnVal = findCorrectJoinTuple <JoinTuple <In2, In1>, Rest...> (typeList, whereEveryoneGoes);
		whereEveryoneGoes.push_back (in2Pos);
		typeList [in2Pos] = in2Name;
	} else {
		returnVal = findCorrectJoinTuple <In1, Rest...> (typeList, whereEveryoneGoes);
	}

	return returnVal;
}

}

#endif
