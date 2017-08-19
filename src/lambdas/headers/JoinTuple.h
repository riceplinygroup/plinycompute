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
#include "SinkShuffler.h"

namespace pdb {

template <typename T>
void copyFrom (T &out, Handle <T> &in) {
        //std :: cout << "copy T from Handle" << std :: endl;
        out = *in;
        //std :: cout << "[IN]" << std :: endl; 
        //in->printMeta();
        //std :: cout << "[OUT]" << std :: endl;
        //out.printMeta();
}

template <typename T>
void copyFrom (T &out, T &in) {
        //std :: cout << "copy T from T" << std :: endl;
        //std :: cout << "[IN]" << std :: endl;
        //in.printMeta();
        //std :: cout << "[OUT]" << std :: endl;
        //out.printMeta();
        out = in;
}

template <typename T>
void copyFrom (Handle <T> &out, Handle <T> &in) {
        //std :: cout << "copy Handle from Handle" << std :: endl;
        out = in;
}

template <typename T>
void copyFrom (Handle<T> &out, T &in) {
        //std :: cout << "copy Handle from T" << std :: endl;
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
		//std :: cout << "Creating column for type " << getTypeName <Handle <HoldMe>> () << " at position " << where << "\n";
		std :: vector <Handle <HoldMe>> *me = new std :: vector <Handle <HoldMe>>;
		processMe.addColumn (where, me, true);
		return me;	
	}

        //JiaNote: add the below two functions to facilitate JoinMap merging for broadcast join
        void copyDataFrom (Handle<HoldMe> me) {
                //std :: cout << "copyDataFrom Handle<HoldMe>" << std :: endl;
                pdb :: copyFrom (myData, me);
        }

        void copyDataFrom (HoldMe me) {
                //std :: cout << "copyDataFrom HoldMe" << std :: endl;
                pdb :: copyFrom (myData, me);
        }

	void copyFrom (void *input, int whichPos) {
		//std :: cout << "Packing column for type " << getTypeName <decltype (IsAbstract <HoldMe> :: val)> () << " at position " << whichPos << "\n";
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
		//std :: cout << "In join probe\n";
		inputTable = input->getRootObject ();

		// set up the output tuple
		output = std :: make_shared <TupleSet> ();
		columns = new void *[positions.size ()];
		if (needToSwapLHSAndRhs) {
			offset = positions.size ();
			createCols <RHSType> (columns, *output, 0, 0, positions);
			//std :: cout << "We do need to add the pipelined data to the back end of the output tuples.\n";
		} else {
			offset = 0;
			createCols <RHSType> (columns, *output, attsToIncludeInOutput.getAtts ().size (), 0, positions);
		}

		// this is the input attribute that we will hash in order to try to find matches
		std :: vector <int> matches = myMachine.match (attsToOperateOn);
		whichAtt = matches[0];

	}

        std :: string getType() override {
                return "JoinProbe";
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
                //std :: cout << "Merged map current size: " << myMap.size() << std :: endl;
                Handle <JoinMap <RHSType>> mapToMerge = unsafeCast <JoinMap <RHSType>> (mergeMe);
                JoinMap <RHSType> &theOtherMap = *mapToMerge;
                //std :: cout << "The map to merge size: " << theOtherMap.size() << std :: endl;
                
                for (JoinMapIterator<RHSType> iter = theOtherMap.begin(); iter != theOtherMap.end(); ++iter) {
                    JoinRecordList<RHSType> * myList = *iter;
                    size_t mySize = myList->size();
                    size_t myHash = myList->getHash();
                    //std :: cout << "myHash is " << myHash << " and mySize is " << mySize << std :: endl;
                    if (mySize > 0) {
                        for (size_t i = 0; i < mySize; i++) {
                            try {        
                                RHSType * temp = &(myMap.push(myHash));
                                packData (*temp, ((*myList)[i]));
                                //std :: cout << "merged one with myHash=" << myHash << " i=" << i << " and my size=" << mySize << std :: endl;
                            } catch (NotEnoughSpace &n) {
                                std :: cout << "ERROR: join data is too large to be built in one map, results are truncated!" << std :: endl;
                                delete (myList);
                                return;
                            }
                        }
                    }
                    delete (myList);
                }
         }


};


//JiaNote: this class is used to create a special JoinSource that will generate a stream of TupleSet from a series of JoinMaps

template <typename RHSType>
class PartitionedJoinMapTupleSetIterator : public ComputeSource {

private:

        //my partition id
        size_t myPartitionId;

        // function to call to get another vector to process
        std :: function <void * ()> getAnotherVector;

        // function to call to free the vector
        std :: function <void (void *)> doneWithVector;

        // this is the vector to process
        Handle <Vector<Handle<JoinMap <RHSType>>>> iterateOverMe;

        // the pointer to current page holding the vector, and the last page that we previously processed
        Record <Vector<Handle<JoinMap <RHSType>>>> *myRec, *lastRec;

        // how many objects to put into a chunk
        size_t chunkSize;

        // where we are in the Vector
        size_t pos;

        // the current JoinMap
        Handle<JoinMap<RHSType>> curJoinMap;

        // where we are in the JoinMap
        JoinMapIterator<RHSType>  curJoinMapIter;

        // end iterator
        JoinMapIterator<RHSType> joinMapEndIter;

        // where we are in the Record list
        size_t posInRecordList;

        // and the tuple set we return
        TupleSetPtr output;

        // the hash column in the output TupleSet
        std :: vector<size_t> * hashColumn;

        // this is the list of output columns except the hash column in the output TupleSet
        void ** columns;

        //whether we have processed all pages
        bool isDone;

public:

        // the first param is a callback function that the iterator will call in order to obtain the page holding the next vector to iterate
        // over.  The secomd param is a callback that the iterator will call when the specified page is done being processed and can be
        // freed.  The third param tells us how many objects to put into a tuple set.
        // The fourth param tells us positions of those packed columns.
        PartitionedJoinMapTupleSetIterator (size_t myPartitionId, std :: function <void * ()> getAnotherVector,
                std :: function <void (void *)> doneWithVector, size_t chunkSize, std :: vector<int> positions) :
                getAnotherVector (getAnotherVector), doneWithVector (doneWithVector), chunkSize (chunkSize) {

                //set my partition id
                this->myPartitionId = myPartitionId;
                  
                // create the tuple set that we'll return during iteration
                output = std :: make_shared <TupleSet> ();
                // extract the vector from the input page
                myRec = (Record <Vector <Handle <JoinMap<RHSType>>>> *) getAnotherVector ();

                if (myRec != nullptr) {

                    iterateOverMe = myRec->getRootObject ();
                    PDB_COUT << "Got iterateOverMe" << std :: endl;
                    // create the output vector for hash value and put it into the tuple set
                    hashColumn = new std :: vector <size_t>;
                    output->addColumn (0, hashColumn, true);
                    // create the output vector for objects and put it into the tuple set
                    columns = new void * [positions.size()];
                    createCols<RHSType> (columns, *output, 1, 0, positions);
                    isDone = false;  
                
                } else {

                    iterateOverMe = nullptr;
                    output = nullptr;
                    isDone = true;
                }

                // we are at position zero
                pos = 0;
                curJoinMap = nullptr;
                posInRecordList = 0;
                // and we have no data so far
                lastRec = nullptr;
        }

        void setChunkSize (size_t chunkSize) override {
             this->chunkSize = chunkSize;
        }

        // returns the next tuple set to process, or nullptr if there is not one to process
        TupleSetPtr getNextTupleSet () override {

                //JiaNote: below two lines are necessary to fix a bug that iterateOverMe may be nullptr when first time get to here
                if ((iterateOverMe == nullptr) || (isDone == true)) {
                     return nullptr;
                }


                // if we made it here with lastRec being a valid pointer, then it means
                // that we have gone through an entire cycle, and so all of the data that
                // we will ever reference stored in lastRec has been fluhhed through the
                // pipeline; hence, we can kill it

                if (lastRec != nullptr) {
                        doneWithVector (lastRec);
                        lastRec = nullptr;
                }

                int overallCounter = 0;
                while (true) {
                   while (curJoinMap == nullptr) {
                      std :: cout << "current JoinMap is nullptr, try pos=" << pos << std :: endl;
                      curJoinMap = (*iterateOverMe)[pos];
                      pos ++;
                      if (curJoinMap != nullptr) {
                          if ((curJoinMap->getPartitionId() % curJoinMap->getNumPartitions())!= myPartitionId) {
                              //this is not my map
                              curJoinMap = nullptr;
                          } else {
                              curJoinMapIter = curJoinMap->begin();
                              joinMapEndIter = curJoinMap->end();
                              posInRecordList = 0;
                          } 
                      }
                      if (curJoinMap == nullptr) {
                          if (pos == iterateOverMe->size()) {
                              break;
                          } else {
                              continue;
                          }
                      }
                   }
                   //there are two possibilities, first we find my map, second we come to end of this page
                   if (curJoinMap != nullptr) {
                      while (curJoinMapIter != joinMapEndIter) { 
                          JoinRecordList<RHSType> * myList = *curJoinMapIter;
                          size_t mySize = myList->size();
                          size_t myHash = myList->getHash();
                          posInRecordList = 0;
                          for (size_t i = 0; i < mySize; i++) {
                              unpack ((*myList)[i], overallCounter, 0, columns);
                              (*hashColumn)[overallCounter] = myHash;
                              posInRecordList++;
                              overallCounter++;
                              if (overallCounter == this->chunkSize) {
                                  hashColumn->resize (overallCounter);
                                  eraseEnd <RHSType> (overallCounter, 0, columns);
                                  return output;
                              }
                          }
                      }
                   } else if (pos == iterateOverMe->size()) {
                        // this means that we got to the end of the vector
                        lastRec = myRec;

                        // try to get another vector
                        myRec = (Record <Vector <Handle <JoinMap<RHSType>>>> *) getAnotherVector ();

                        // if we could not, then we are outta here
                        if (myRec == nullptr) {
                                isDone = true;
                                iterateOverMe = nullptr;
                                if (overallCounter > 0) {

                                    hashColumn->resize (overallCounter);
                                    eraseEnd <RHSType> (overallCounter, 0, columns);
                                    return output;
                                    
                                } else {
                                    return nullptr;
                                }
                        }
                        // and reset everything
                        iterateOverMe = myRec->getRootObject ();
                        pos = 0;
                    
                   }                   
                }
                return nullptr;

        }


        ~PartitionedJoinMapTupleSetIterator () {

                // if lastRec is not a nullptr, then it means that we have not yet freed it
                if (lastRec != nullptr)
                        doneWithVector (lastRec);
                lastRec = nullptr;
        }


};

// JiaNote: this class is used to create a Shuffler that picks all JoinMaps that belong to one node, and push back JoinMaps to another vector.
template <typename RHSType>
class JoinSinkShuffler : public SinkShuffler {

private:

        int nodeId;

public:

        ~JoinSinkShuffler () {
        }

        JoinSinkShuffler () {
        }

        void setNodeId (int nodeId) {
                this->nodeId = nodeId;
        }

        int getNodeId () {
                return nodeId;
        }

        Handle <Object> createNewOutputContainer () override {

                // we simply create a new map to store the output
                Handle <Vector<Handle<JoinMap <RHSType>>>> returnVal = makeObject <Vector<Handle<JoinMap <RHSType>>>> ();
                return returnVal;
        }

        void writeOut (Handle<Object> shuffleMe, Handle <Object> &shuffleToMe) override {

                // get the map we are adding to
                Handle <Vector<Handle<JoinMap <RHSType>>>> shuffledMaps= unsafeCast <Vector<Handle<JoinMap <RHSType>>>> (shuffleToMe);
                Vector<Handle<JoinMap <RHSType>>> &myMaps = *shuffledMaps; 
                Handle<JoinMap<RHSType>> thisMap;
                try {
                    thisMap = makeObject<JoinMap<RHSType>> ();
                }
                catch (NotEnoughSpace &n) {
                    std :: cout << "ERROR: can't allocate for new map" << std :: endl;
                    return;
                }
                JoinMap<RHSType> myMap = *thisMap;
                Handle<JoinMap <RHSType>> theOtherMap = unsafeCast <JoinMap <RHSType>> (shuffleMe);
                JoinMap<RHSType> mapToShuffle = *theOtherMap;
                for (JoinMapIterator<RHSType> iter = mapToShuffle.begin(); iter != mapToShuffle.end(); ++iter) {
                    JoinRecordList<RHSType> * myList = *iter;
                    size_t mySize = myList->size();
                    size_t myHash = myList->getHash();
                    if (mySize > 0) {
                        for (size_t i = 0; i < mySize; i++) {
                            try {
                                RHSType * temp = &(myMap.push(myHash));
                                packData (*temp, ((*myList)[i]));
                            } catch (NotEnoughSpace &n) {
                                std :: cout << "ERROR: join data is too large to be built in one map, results are truncated!" << std :: endl;
                                delete (myList);
                                return;
                            }
                        }
                    }
                    delete (myList);
                }
                myMaps.push_back(thisMap);
         }


};

// JiaNote: this class is used to create a special JoinSink that are partitioned into multiple JoinMaps
template <typename RHSType>
class PartitionedJoinSink : public ComputeSink {

private:

        // number of partitions
        int numPartitionsPerNode;

        // number of nodes for shuffling
        int numNodes;

        // tells us which attribute is the key
        int keyAtt;

        // if useTheseAtts[i] = j, it means that the i^th attribute that we need to extract from the input tuple is j
        std :: vector <int> useTheseAtts;

        // if whereEveryoneGoes[i] = j, it means that the i^th entry in useTheseAtts goes in the j^th slot in the holder tuple
        std :: vector <int> whereEveryoneGoes;

        // this is the list of columns that we are processing
        void **columns = nullptr;

public:

        ~PartitionedJoinSink () {
                if (columns != nullptr)
                        delete columns;
        }

        PartitionedJoinSink (int numPartitionsPerNode, int numNodes, TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &additionalAtts, std :: vector <int> &whereEveryoneGoes) :
                whereEveryoneGoes (whereEveryoneGoes) {

                this->numPartitionsPerNode = numPartitionsPerNode;

                this->numNodes = numNodes;

                // used to manage attributes and set up the output
                TupleSetSetupMachine myMachine (inputSchema);

                // figure out the key att
                std :: vector <int> matches = myMachine.match (attsToOperateOn);
                keyAtt = matches[0];

                // now, figure out the attributes that we need to store in the hash table
                useTheseAtts = myMachine.match (additionalAtts);
        }

        Handle <Object> createNewOutputContainer () override {
                std :: cout << "PartitionedJoinSink: to create a Vector of JoinMap instance" << std :: endl;
                // we create a vector of maps to store the output
                Handle <Vector<Handle<Vector<Handle<JoinMap <RHSType>>>>>> returnVal = makeObject <Vector<Handle<Vector<Handle<JoinMap <RHSType>>>>>> (numNodes);
                for (int i = 0; i < numNodes; i++) {
                    Handle<Vector<Handle<JoinMap<RHSType>>>> myVector = makeObject<Vector<Handle<JoinMap<RHSType>>>>(numPartitionsPerNode);
                    for (int j = 0; j < numPartitionsPerNode; j++) {
                        Handle<JoinMap<RHSType>> myMap = makeObject<JoinMap<RHSType>> (2, i*numPartitionsPerNode+j, numPartitionsPerNode);
                        myVector->push_back(myMap);
                    }
                    returnVal->push_back(myVector);
                }
                return returnVal;
        }

        void writeOut (TupleSetPtr input, Handle <Object> &writeToMe) override {
                PDB_COUT << "PartitionedJoinSink: write out tuples in this tuple set" << std :: endl;
                // get the map we are adding to
                Handle <Vector<Handle<Vector<Handle<JoinMap <RHSType>>>>>> writeMe = unsafeCast <Vector<Handle<Vector<Handle<JoinMap <RHSType>>>>>> (writeToMe);
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
                        size_t index = keyColumn[i] % (this->numPartitionsPerNode * this->numNodes);
                        size_t nodeIndex = index / this->numPartitionsPerNode;
                        size_t partitionIndex = index % this->numPartitionsPerNode;
                        JoinMap <RHSType> &myMap = *((*((*writeMe)[nodeIndex]))[partitionIndex]);
                        //std :: cout << "to write value with hash=" << keyColumn[i] << " to JoinMap with nodeIndex=" << nodeIndex << " and partitionIndex=" << partitionIndex << " and size=" << myMap.size() << std ::endl;
                        // try to add the key... this will cause an allocation for a new key/val pair
                        if (myMap.count (keyColumn[i]) == 0) {
                                //std :: cout << "key doesn't exist" << std :: endl;
                                try {
                                        RHSType &temp = myMap.push (keyColumn[i]);
                                        pack (temp, i, 0, columns);

                                // if we get an exception, then we could not fit a new key/value pair
                                } catch (NotEnoughSpace &n) {
                                        std :: cout << "we are running out of space in writing join sink" << std :: endl;     
                                        // if we got here, then we ran out of space, and so we need to delete the already-processed
                                        // data so that we can try again...
                                        myMap.setUnused (keyColumn[i]);
                                        truncate <RHSType> (i, 0, columns);
                                        keyColumn.erase (keyColumn.begin (), keyColumn.begin () + i);
                                        throw n;
                                }

                        // the key is there
                        } else {
                                //std :: cout << "key exists" << std :: endl;
                                // and add the value
                                RHSType *temp;
                                try {

                                        temp = &(myMap.push (keyColumn[i]));

                                // an exception means that we couldn't complete the addition
                                } catch (NotEnoughSpace &n) {

                                        std :: cout << "we are running out of space in writing join sink" << std :: endl;
                                        truncate <RHSType> (i, 0, columns);
                                        keyColumn.erase (keyColumn.begin (), keyColumn.begin () + i);
                                        throw n;
                                }

                                // now try to do the copy
                                try {

                                        pack (*temp, i, 0, columns);

                                // if the copy didn't work, pop the value off
                                } catch (NotEnoughSpace &n) {
                                        std :: cout << "we are running out of space in writing join sink" << std :: endl;
                                        myMap.setUnused (keyColumn[i]);
                                        truncate <RHSType> (i, 0, columns);
                                        keyColumn.erase (keyColumn.begin (), keyColumn.begin () + i);
                                        throw n;
                                }
                        }
                }
                for (int i = 0; i < writeMe->size(); i++) {
                     Handle<Vector<Handle<JoinMap<RHSType>>>> myVec = (*writeMe)[i];
                     for (int j = 0; j < myVec->size(); j++) {
                         Handle<JoinMap<RHSType>> myMap = (*myVec)[j];
                         std :: cout << "myMapSize[" << i << "][" << j << "]=" << myMap->size() << std :: endl;
                     }
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
                PDB_COUT << "JoinSink: to create new JoinMap instance" << std :: endl;		
		// we simply create a new map to store the output
		Handle <JoinMap <RHSType>> returnVal = makeObject <JoinMap <RHSType>> ();
		return returnVal;
	}

	void writeOut (TupleSetPtr input, Handle <Object> &writeToMe) override {
                PDB_COUT << "JoinSink: write out tuples in this tuple set" << std :: endl;
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
                                        //std :: cout << "we are running out of space in writing join sink" << std :: endl;	
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

                                        //std :: cout << "we are running out of space in writing join sink" << std :: endl;
					truncate <RHSType> (i, 0, columns);
					keyColumn.erase (keyColumn.begin (), keyColumn.begin () + i);
					throw n;
				}
	
				// now try to do the copy
				try {

					pack (*temp, i, 0, columns);

				// if the copy didn't work, pop the value off
				} catch (NotEnoughSpace &n) {
                                        //std :: cout << "we are running out of space in writing join sink" << std :: endl;
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

	virtual ComputeSinkPtr getSink (TupleSpec &consumeMe, TupleSpec &attsToOpOn, TupleSpec &projection, std :: vector <int> & whereEveryoneGoes) = 0;

        virtual ComputeSinkPtr getPartitionedSink (int numPartitionsPerNode, int numNodes, TupleSpec &consumeMe, TupleSpec &attsToOpOn, TupleSpec &projection, std :: vector <int> & whereEveryoneGoes) = 0;


        virtual ComputeSourcePtr getPartitionedSource (size_t myPartitionId, std :: function <void * ()> getAnotherVector, std :: function <void (void *)> doneWithVector, size_t chunkSize, std :: vector<int> & whereEveryoneGoes) = 0;

        virtual SinkMergerPtr getMerger() = 0;

        virtual SinkShufflerPtr getShuffler() = 0;

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
        ComputeSinkPtr getSink (TupleSpec &consumeMe, TupleSpec &attsToOpOn, TupleSpec &projection, std :: vector <int> & whereEveryoneGoes) override {
		return std :: make_shared <JoinSink <HoldMe>> (consumeMe, attsToOpOn, projection, whereEveryoneGoes);
        }

        // JiaNote: create a partitioned sink for this particular type
        ComputeSinkPtr getPartitionedSink (int numPartitionsPerNode, int numNodes, TupleSpec &consumeMe, TupleSpec &attsToOpOn, TupleSpec &projection, std :: vector <int> & whereEveryoneGoes) override {
                return std :: make_shared <PartitionedJoinSink <HoldMe>> (numPartitionsPerNode, numNodes, consumeMe, attsToOpOn, projection, whereEveryoneGoes);
        }

        // JiaNote: create a partitioned source for this particular type
        ComputeSourcePtr getPartitionedSource (size_t myPartitionId, std :: function <void * ()> getAnotherVector, std :: function <void (void *)> doneWithVector, size_t chunkSize, std :: vector<int> & whereEveryoneGoes) override {
                return std :: make_shared <PartitionedJoinMapTupleSetIterator<HoldMe>> (myPartitionId, getAnotherVector, doneWithVector, chunkSize, whereEveryoneGoes);
        }


        // JiaNote: create a merger
        SinkMergerPtr getMerger () override {
                return std :: make_shared <JoinSinkMerger <HoldMe>> ();
        }

        // JiaNote: create a shuffler
        SinkShufflerPtr getShuffler () override {
                return std :: make_shared <JoinSinkShuffler <HoldMe>> ();
        }

};

typedef std::shared_ptr <JoinTupleSingleton> JoinTuplePtr;

inline int findType (std :: string &findMe, std :: vector <std :: string> &typeList) {
        //std :: cout << "findMe is " << findMe << std :: endl;
	for (int i = 0; i < typeList.size (); i++) {
                //std :: cout << "typeList[" << i << "]=" << typeList[i] << std :: endl;
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
		//std :: cout << "Why did we not find a type?\n";
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
        std :: cout << "in2Name =" << in2Name << std :: endl;
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
