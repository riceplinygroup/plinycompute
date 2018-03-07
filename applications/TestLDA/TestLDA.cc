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
#ifndef TEST_LDA_CC
#define TEST_LDA_CC

/* 
 * Learning Latent Dirichlet Allocation (LDA) by Gibbs Sampling
 */

#include "PDBDebug.h"
#include "PDBVector.h"
#include "Query.h"
#include "Lambda.h"
#include "PDBClient.h"

#include "Set.h"
#include "DataTypes.h"
#include "DoubleVector.h"
#include "ScanIntSet.h"

#include "sharedLibraries/headers/LDADocument.h"
#include "sharedLibraries/headers/LDADocIDAggregate.h"
#include "sharedLibraries/headers/ScanLDADocumentSet.h"
#include "LDAInitialTopicProbSelection.h"
#include "WriteIntDoubleVectorPairSet.h"
#include "IntDoubleVectorPair.h"
#include "IntIntVectorPair.h"
#include "sharedLibraries/headers/LDADocWordTopicAssignment.h"
#include "ScanIntDoubleVectorPairSet.h"

#include "LDA/LDAInitialWordTopicProbSelection.h"
#include "LDA/ScanTopicsPerWord.h"
#include "LDA/LDADocWordTopicJoin.h"
#include "LDA/LDADocTopicAggregate.h"
#include "LDA/LDADocTopicProbSelection.h"
#include "LDA/LDADocWordTopicAssignmentIdentity.h"
#include "LDA/LDATopicWordAggregate.h"
#include "LDA/WriteLDADocWordTopicAssignment.h"
#include "LDA/LDATopicWordProbMultiSelection.h"
#include "LDA/LDADocAssignmentMultiSelection.h"
#include "LDA/LDAWordTopicAggregate.h"
#include "LDA/WriteTopicsPerWord.h"
#include "LDA/LDATopicAssignmentMultiSelection.h"
#include "LDA/LDATopicWordProb.h"

#include <ctime>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>
#include <cstddef>
#include <iostream>
#include <fstream>
#include <math.h>
#include <random>
#include <sstream>


using namespace pdb;
int main(int argc, char* argv[]) {

    /* Read in the parameters */
    if (argc != 8 && argc != 6) {
        std::cout << "Usage: #masterIp #iterations #words #topics #addData[Y/N] "
                     "#addDataFromFile[Y/N] #docs(If addDataFromFile = N)/#inputFile (If "
                     "addDataFromFile = Y) \n";
        return (-1);
    }

    std::string masterIp;
    masterIp = argv[1];

    int iter = std::stoi(argv[2]);
    std::cout << "The number of iterations: " << iter << std::endl;

    int numWord = std::stoi(argv[3]);
    std::cout << "The dictionary size: " << numWord << std::endl;
    int numTopic = std::stoi(argv[4]);
    std::cout << "The number of topics: " << numTopic << std::endl;

    bool whetherToAddData = true;
    if (strcmp(argv[5], "N") == 0) {
        whetherToAddData = false;
    }

    bool whetherAddFromFile = false;
    int numDoc = 0;
    std::string inFileName = "";
    if (whetherToAddData) {
        if (strcmp(argv[6], "N") == 0) {
            numDoc = std::stoi(argv[7]);
            std::cout << "The number of documents: " << numDoc << std::endl;
        } else {
            whetherAddFromFile = true;
            inFileName = argv[7];
        }
    }

    /* Set up the client */
  PDBClient pdbClient(8108, masterIp, false, true);

    /* Load the libraries */
    string errMsg;
    std::vector<std::string> v = {"libraries/libIntDoubleVectorPair.so",
                                  "libraries/libLDADocument.so",
                                  "libraries/libLDATopicWordProb.so",
                                  "libraries/libScanTopicsPerWord.so",
                                  "libraries/libScanIntDoubleVectorPairSet.so",
                                  "libraries/libLDADocWordTopicAssignmentIdentity.so",
                                  "libraries/libLDADocIDAggregate.so",
                                  "libraries/libWriteLDADocWordTopicAssignment.so",
                                  "libraries/libWriteIntDoubleVectorPairSet.so",
                                  "libraries/libLDADocWordTopicAssignment.so",
                                  "libraries/libScanLDADocumentSet.so",
                                  "libraries/libLDAInitialTopicProbSelection.so",
                                  "libraries/libWriteIntDoubleVectorPairSet.so",
                                  "libraries/libScanIntSet.so",
                                  "libraries/libLDADocAssignmentMultiSelection.so",
                                  "libraries/libLDATopicAssignmentMultiSelection.so",
                                  "libraries/libWriteTopicsPerWord.so",
                                  "libraries/libTopicAssignment.so",
                                  "libraries/libDocAssignment.so",
                                  "libraries/libLDADocWordTopicJoin.so",
                                  "libraries/libLDAInitialWordTopicProbSelection.so",
                                  "libraries/libLDADocTopicAggregate.so",
                                  "libraries/libLDADocTopicProbSelection.so",
                                  "libraries/libLDATopicWordAggregate.so",
                                  "libraries/libLDATopicWordProbMultiSelection.so",
                                  "libraries/libLDAWordTopicAggregate.so"};

    for (auto& a : v) {
        pdbClient.registerType(a);
    }

    if (whetherToAddData == true) {

        /* Create the Database and Sets */
        pdbClient.createDatabase("LDA_db");

        pdbClient.removeSet("LDA_db", "LDA_input_set", errMsg);
        pdbClient.createSet<LDADocument>("LDA_db",
                                         "LDA_input_set");
	    std::cout << "Not able to create set: " + errMsg;
            exit(-1);
        }

        pdbClient.removeSet("LDA_db", "LDA_meta_data_set", errMsg);
        pdbClient.createSet<int>("LDA_db",
                                 "LDA_meta_data_set");

        int blockSize = 8;

	/* Add synthetic data */
        if (!whetherAddFromFile && numDoc > 0) {

            pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
            pdb::Handle<pdb::Vector<pdb::Handle<LDADocument>>> storeMe =
                pdb::makeObject<pdb::Vector<pdb::Handle<LDADocument>>>();

	    /* 
	     * Add the data for documents. 
	     * Format: (docID, wordID, count)
	     */
            for (int docNum = 0; docNum < numDoc; docNum++) {

                int wordsSoFar = 0;
                for (int i = 0; i <= numWord; i++) {

                    try {
                        if ((i == numWord && wordsSoFar == 0) ||
                            (i != numWord && drand48() < 0.015)) {
                            wordsSoFar++;
                            pdb::Handle<LDADocument> myData = pdb::makeObject<LDADocument>();
                            myData->setDoc(docNum);

                            if (i == numWord) {
                                myData->setWord(lrand48() % numWord);
                            } else {
                                myData->setWord(i);
                            }

                            myData->setCount(lrand48() % 5);
                            storeMe->push_back(myData);
                        }

                    } catch (pdb::NotEnoughSpace& n) {
                        pdbClient.sendData<LDADocument>(
                                std::pair<std::string, std::string>("LDA_input_set", "LDA_db"),
                                storeMe);
                        pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                        storeMe = pdb::makeObject<pdb::Vector<pdb::Handle<LDADocument>>>();
                    }
                }
            }

            /* Handle the last few entries */
            if (storeMe->size() > 0) {
                pdbClient.sendData<LDADocument>(
                        std::pair<std::string, std::string>("LDA_input_set", "LDA_db"),
                        storeMe);
                pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
            }

            /* Add the data for the dictionary. One wordID per entry. */
            pdb::Handle<pdb::Vector<pdb::Handle<int>>> storeMeToo =
                pdb::makeObject<pdb::Vector<pdb::Handle<int>>>();
            for (int i = 0; i < numWord; i++) {
                Handle<int> me = makeObject<int>(i);
                storeMeToo->push_back(me);
            }

            pdbClient.sendData<int>(
                    std::pair<std::string, std::string>("LDA_meta_data_set", "LDA_db"),
                    storeMeToo);
                pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);

            pdbClient.flushData();
        }

	/* Add data from the input file */
        else {
            int blockSize = 8;
            std::ifstream inFile(inFileName);
            std::string line;
            int docID, wordID, countNum;
            bool rollback = false;
            bool end = false;

	    /* Read in the documents from the input file */
            while (!end) {
                pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                pdb::Handle<pdb::Vector<pdb::Handle<LDADocument>>> storeMe =
                    pdb::makeObject<pdb::Vector<pdb::Handle<LDADocument>>>();

                try {
                    while (1) {
                        if (!rollback) {
                            if (!(inFile >> docID >> wordID >> countNum)) {
                                end = true;
                                break;
                            }
                        } else
                            rollback = false;
                        pdb::Handle<LDADocument> myData = pdb::makeObject<LDADocument>();
                        myData->setDoc(docID);
                        myData->setWord(wordID);
                        myData->setCount(countNum);
                        storeMe->push_back(myData);
                    }
                    pdbClient.sendData<LDADocument>(
                            std::pair<std::string, std::string>("LDA_input_set", "LDA_db"),
                            storeMe);

                } catch (pdb::NotEnoughSpace& n) {
                    pdbClient.sendData<LDADocument>(
                            std::pair<std::string, std::string>("LDA_input_set", "LDA_db"),
                            storeMe);
                    rollback = false;
                }

            }

            inFile.close();

	    /* Add the data for the dictionary. One wordID per entry. */
            pdb::Handle<pdb::Vector<pdb::Handle<int>>> storeMeToo =
                pdb::makeObject<pdb::Vector<pdb::Handle<int>>>();
            for (int i = 0; i < numWord; i++) {
                Handle<int> me = makeObject<int>(i);
                storeMeToo->push_back(me);
            }

            pdbClient.sendData<int>(
                    std::pair<std::string, std::string>("LDA_meta_data_set", "LDA_db"),
                    storeMeToo);
            pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
            pdbClient.flushData();
    }

    /* Create sets to store the intermediate data that will be used between iterations */
    std::string myNextReaderForTopicsPerWordSetName =
        std::string("TopicsPerWord") + std::to_string(0);
    std::string myNextReaderForTopicsPerDocSetName =
        std::string("TopicsPerDoc") + std::to_string(0);
    std::string myNextWriterForTopicsPerWordSetName =
        std::string("TopicsPerWord") + std::to_string(1);
    std::string myNextWriterForTopicsPerDocSetName =
        std::string("TopicsPerDoc") + std::to_string(1);

    pdbClient.removeSet("LDA_db", myNextReaderForTopicsPerDocSetName, errMsg);
    pdbClient.createSet<IntDoubleVectorPair>("LDA_db",
                                             myNextReaderForTopicsPerDocSetName);

    pdbClient.removeSet("LDA_db", myNextReaderForTopicsPerWordSetName, errMsg);
    pdbClient.createSet<IntDoubleVectorPair>("LDA_db",
                                             myNextReaderForTopicsPerWordSetName);

    pdbClient.removeSet("LDA_db", myNextWriterForTopicsPerDocSetName, errMsg);
    pdbClient.createSet<IntDoubleVectorPair>("LDA_db",
                                             myNextWriterForTopicsPerDocSetName);

    pdbClient.removeSet("LDA_db", myNextWriterForTopicsPerWordSetName, errMsg);
    pdbClient.createSet<IntDoubleVectorPair>("LDA_db",
                                             myNextWriterForTopicsPerWordSetName);

    /* Main LDA Program */

    /* Prior data */
    pdb::makeObjectAllocatorBlock(1024 * 1024 * 1024, true);

    auto total_begin = std::chrono::high_resolution_clock::now();
    pdb::Handle<pdb::Vector<double>> alpha =
        pdb::makeObject<pdb::Vector<double>>(numTopic, numTopic);
    pdb::Handle<pdb::Vector<double>> beta = pdb::makeObject<pdb::Vector<double>>(numWord, numWord);
    alpha->fill(1.0);
    beta->fill(1.0);

    /* Initialization */

    /* Initialize the topic mixture probabilities for each document */
    Handle<Computation> myInitialScanSet =
        makeObject<ScanLDADocumentSet>("LDA_db", "LDA_input_set");
    Handle<Computation> myDocID = makeObject<LDADocIDAggregate>();
    myDocID->setInput(myInitialScanSet);
    Handle<Computation> myDocTopicProb = makeObject<LDAInitialTopicProbSelection>(*alpha);
    myDocTopicProb->setInput(myDocID);

    /* Initialize the (wordID, topic prob vector) */
    Handle<Computation> myMetaScanSet = makeObject<ScanIntSet>("LDA_db", "LDA_meta_data_set");
    Handle<Computation> myWordTopicProb = makeObject<LDAInitialWordTopicProbSelection>(numTopic);
    myWordTopicProb->setInput(myMetaScanSet);

    Handle<Computation> input1 = myDocTopicProb;
    Handle<Computation> input2 = myWordTopicProb;

    /* Main training loops */
    for (int n = 0; n < iter; n++) {

        /* [1] Set up the join that will assign all of the words in the corpus to topics */
        Handle<Computation> myDocWordTopicJoin = makeObject<LDADocWordTopicJoin>(numWord);
        myDocWordTopicJoin->setInput(0, myInitialScanSet);
        myDocWordTopicJoin->setInput(1, input1);
        myDocWordTopicJoin->setInput(2, input2);

        /* Do an identity selection */
        Handle<Computation> myIdentitySelection = makeObject<LDADocWordTopicAssignmentIdentity>();
        myIdentitySelection->setInput(myDocWordTopicJoin);

        /* [2] Set up the sequence of actions that re-compute the topic probabilities for each document */

        /* Get the set of topics assigned for each doc */
        Handle<Computation> myDocWordTopicCount = makeObject<LDADocAssignmentMultiSelection>();
        myDocWordTopicCount->setInput(myIdentitySelection);

        /* Aggregate the topics */
        Handle<Computation> myDocTopicCountAgg = makeObject<LDADocTopicAggregate>();
        myDocTopicCountAgg->setInput(myDocWordTopicCount);

        /* Get the new set of doc-topic probabilities */
        Handle<Computation> myDocTopicProb = makeObject<LDADocTopicProbSelection>(*alpha);
        myDocTopicProb->setInput(myDocTopicCountAgg);

        /* [3] Set up the sequence of actions that re-compute the word probs for each topic */

        /* Get the set of words assigned for each topic in each doc */
        Handle<Computation> myTopicWordCount = makeObject<LDATopicAssignmentMultiSelection>();
        myTopicWordCount->setInput(myIdentitySelection);

        /* Aggregate them */
        Handle<Computation> myTopicWordCountAgg = makeObject<LDATopicWordAggregate>();
        myTopicWordCountAgg->setInput(myTopicWordCount);

        /* Use those aggregations to get per-topic probabilities */
        Handle<Computation> myTopicWordProb =
            makeObject<LDATopicWordProbMultiSelection>(*beta, numTopic);
        myTopicWordProb->setInput(myTopicWordCountAgg);

        /* Get the per-word probabilities */
        Handle<Computation> myWordTopicProb = makeObject<LDAWordTopicAggregate>();
        myWordTopicProb->setInput(myTopicWordProb);

        /* 
	 * [4] Write the intermediate results doc-topic probability and word-topic probability to sets
	 *     Use them in the next iteration
	 */
        std::string myWriterForTopicsPerWordSetName =
            std::string("TopicsPerWord") + std::to_string((n + 1) % 2);
        Handle<Computation> myWriterForTopicsPerWord =
            makeObject<WriteTopicsPerWord>("LDA_db", myWriterForTopicsPerWordSetName);
        myWriterForTopicsPerWord->setInput(myWordTopicProb);

        std::string myWriterForTopicsPerDocSetName =
            std::string("TopicsPerDoc") + std::to_string((n + 1) % 2);
        Handle<Computation> myWriterForTopicsPerDoc =
            makeObject<WriteIntDoubleVectorPairSet>("LDA_db", myWriterForTopicsPerDocSetName);
        myWriterForTopicsPerDoc->setInput(myDocTopicProb);

	/* Excute the computations */
        pdbClient.executeComputations(
                myWriterForTopicsPerWord, myWriterForTopicsPerDoc);

	/* [5] Prepare sets for the next iteration */

        /* Clear the sets that have been read in this iteration by old readers */
        std::string myReaderForTopicsPerWordSetName =
            std::string("TopicsPerWord") + std::to_string(n % 2);
        std::string myReaderForTopicsPerDocSetName =
            std::string("TopicsPerDoc") + std::to_string(n % 2);
        pdbClient.clearSet(
                "LDA_db", myReaderForTopicsPerWordSetName, "pdb::IntDoubleVectorPair");
        pdbClient.clearSet(
                "LDA_db", myReaderForTopicsPerDocSetName, "pdb::IntDoubleVectorPair");

        /* Finally, create the new readers */
        input2 = makeObject<ScanTopicsPerWord>("LDA_db", myWriterForTopicsPerWordSetName);
        input1 = makeObject<ScanIntDoubleVectorPairSet>("LDA_db", myWriterForTopicsPerDocSetName);
        myInitialScanSet = makeObject<ScanLDADocumentSet>("LDA_db", "LDA_input_set");
    }

    auto end = std::chrono::high_resolution_clock::now();

    /* Output the results as topic-word probability */
    std::string myWriterForTopicsPerWordSetName =
        std::string("TopicsPerWord") + std::to_string(iter % 2);
    SetIterator<LDATopicWordProb> initTopicProbResult =
            pdbClient.getSetIterator<LDATopicWordProb>("LDA_db", myWriterForTopicsPerWordSetName);
    std::cout << "LDA results: (Word ID, Topic Probability)" << std::endl;
    for (auto& a : initTopicProbResult) {
    	std :: cout << "Word ID: " << a->getKey () << " Topic Probability: ";
        a->getVector().print();
        std :: cout << std::endl;
    }

    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std::cout << "Can't cleanup so files" << std::endl;
    }

    std::cout << "Time Duration: "
         << std::chrono::duration_cast<std::chrono::duration<float>>(end - total_begin).count()
         << " secs." << std::endl;
}

#endif
