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

#include "LDADocument.h"
#include "LDADocIDAggregate.h"
#include "ScanLDADocumentSet.h"
#include "LDAInitialTopicProbSelection.h"
#include "WriteIntDoubleVectorPairSet.h"
#include "IntDoubleVectorPair.h"
#include "IntIntVectorPair.h"
#include "LDADocWordTopicAssignment.h"
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
    std::ofstream term("/dev/tty", std::ios_base::out);

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
    term << "The number of iterations: " << iter << std::endl;

    int numWord = std::stoi(argv[3]);
    term << "The dictionary size: " << numWord << std::endl;
    int numTopic = std::stoi(argv[4]);
    term << "The number of topics: " << numTopic << std::endl;

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
            term << "The number of documents: " << numDoc << std::endl;
        } else {
            whetherAddFromFile = true;
            inFileName = argv[7];
        }
    }

    /* Set up the client */
    pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

    PDBClient pdbClient(
            8108, masterIp,
            clientLogger,
            false,
            true);

    CatalogClient catalogClient(
            8108,
            masterIp,
            clientLogger);

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
        if (!pdbClient.registerType(a, errMsg)) {
            std::cout << "could not load library: " << errMsg << "\n";
	}
    }

    if (whetherToAddData == true) {
        // Step 1. Create Database and Set

        // now, create a new database
        if (!pdbClient.createDatabase("LDA_db", errMsg)) {
            cout << "Not able to create database: " + errMsg;
            exit(-1);
        }

        // now, create a new set in that database
        pdbClient.removeSet("LDA_db", "LDA_input_set", errMsg);
        if (!pdbClient.createSet<LDADocument>("LDA_db",
                                         "LDA_input_set",
                                         errMsg)) {
            cout << "Not able to create set: " + errMsg;
            exit(-1);
        }

        pdbClient.removeSet("LDA_db", "LDA_meta_data_set", errMsg);
        if (!pdbClient.createSet<int>("LDA_db",
                                 "LDA_meta_data_set",
                                 errMsg)) {
            cout << "Not able to create set: " + errMsg;
            exit(-1);
        }


        // Step 2. Add data
        int blockSize = 8;
        if (!whetherAddFromFile && numDoc > 0) {

            pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
            pdb::Handle<pdb::Vector<pdb::Handle<LDADocument>>> storeMe =
                pdb::makeObject<pdb::Vector<pdb::Handle<LDADocument>>>();

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
                        std::cout << "Sending " << storeMe->size() << " data objects.\n";
                        if (!pdbClient.sendData<LDADocument>(
                                std::pair<std::string, std::string>("LDA_input_set", "LDA_db"),
                                storeMe,
                                errMsg)) {
                            std::cout << "Failed to send data to dispatcher server" << std::endl;
                            return -1;
                        }
                        pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                        storeMe = pdb::makeObject<pdb::Vector<pdb::Handle<LDADocument>>>();
                    }
                }
            }

            // just in case the last few entries did not get sent
            if (storeMe->size() > 0) {
                if (!pdbClient.sendData<LDADocument>(
                        std::pair<std::string, std::string>("LDA_input_set", "LDA_db"),
                        storeMe,
                        errMsg)) {
                    std::cout << "Failed to send data to dispatcher server" << std::endl;
                    return -1;
                }
                pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
            }

            // now we create one entry per word
            pdb::Handle<pdb::Vector<pdb::Handle<int>>> storeMeToo =
                pdb::makeObject<pdb::Vector<pdb::Handle<int>>>();
            for (int i = 0; i < numWord; i++) {
                Handle<int> me = makeObject<int>(i);
                storeMeToo->push_back(me);
            }

            if (!pdbClient.sendData<int>(
                    std::pair<std::string, std::string>("LDA_meta_data_set", "LDA_db"),
                    storeMeToo,
                    errMsg)) {
                std::cout << "Failed to send data to dispatcher server" << std::endl;
                return -1;
                pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
            }

            // to write back all buffered records
            pdbClient.flushData(errMsg);
        }  // If not add from file

        else {
            int blockSize = 8;
            std::ifstream inFile(inFileName);
            std::string line;
            int docID, wordID, countNum;
            bool rollback = false;
            bool end = false;

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
                        //           std::cout << "Count number: " << countNum << endl;
                    }
                    if (!pdbClient.sendData<LDADocument>(
                            std::pair<std::string, std::string>("LDA_input_set", "LDA_db"),
                            storeMe,
                            errMsg)) {
                        std::cout << "Failed to send data to dispatcher server" << std::endl;
                        return -1;
                    }

                } catch (pdb::NotEnoughSpace& n) {
                    if (!pdbClient.sendData<LDADocument>(
                            std::pair<std::string, std::string>("LDA_input_set", "LDA_db"),
                            storeMe,
                            errMsg)) {
                        std::cout << "Failed to send data to dispatcher server" << std::endl;
                        return -1;
                    }
                    rollback = false;
                }

            }

            inFile.close();

            // now we create one entry per word
            pdb::Handle<pdb::Vector<pdb::Handle<int>>> storeMeToo =
                pdb::makeObject<pdb::Vector<pdb::Handle<int>>>();
            for (int i = 0; i < numWord; i++) {
                Handle<int> me = makeObject<int>(i);
                storeMeToo->push_back(me);
            }

            if (!pdbClient.sendData<int>(
                    std::pair<std::string, std::string>("LDA_meta_data_set", "LDA_db"),
                    storeMeToo,
                    errMsg)) {
                std::cout << "Failed to send data to dispatcher server" << std::endl;
                return -1;
                pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
            }
            pdbClient.flushData(errMsg);
        }
    }

    std::string myNextReaderForTopicsPerWordSetName =
        std::string("TopicsPerWord") + std::to_string(0);
    std::string myNextReaderForTopicsPerDocSetName =
        std::string("TopicsPerDoc") + std::to_string(0);
    std::string myNextWriterForTopicsPerWordSetName =
        std::string("TopicsPerWord") + std::to_string(1);
    std::string myNextWriterForTopicsPerDocSetName =
        std::string("TopicsPerDoc") + std::to_string(1);

    pdbClient.removeSet("LDA_db", myNextReaderForTopicsPerDocSetName, errMsg);
    if (!pdbClient.createSet<IntDoubleVectorPair>("LDA_db",
                                             myNextReaderForTopicsPerDocSetName,
                                             errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit(-1);
    }

    pdbClient.removeSet("LDA_db", myNextReaderForTopicsPerWordSetName, errMsg);
    if (!pdbClient.createSet<IntDoubleVectorPair>("LDA_db",
                                             myNextReaderForTopicsPerWordSetName,
                                             errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit(-1);
    }

    pdbClient.removeSet("LDA_db", myNextWriterForTopicsPerDocSetName, errMsg);
    if (!pdbClient.createSet<IntDoubleVectorPair>("LDA_db",
                                             myNextWriterForTopicsPerDocSetName,
                                             errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit(-1);
    }

    pdbClient.removeSet("LDA_db", myNextWriterForTopicsPerWordSetName, errMsg);
    if (!pdbClient.createSet<IntDoubleVectorPair>("LDA_db",
                                             myNextWriterForTopicsPerWordSetName,
                                             errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit(-1);
    }

    // connect to the query clientpdbClient
    // Some meta data
    pdb::makeObjectAllocatorBlock(1024 * 1024 * 1024, true);

    auto total_begin = std::chrono::high_resolution_clock::now();
    pdb::Handle<pdb::Vector<double>> alpha =
        pdb::makeObject<pdb::Vector<double>>(numTopic, numTopic);
    pdb::Handle<pdb::Vector<double>> beta = pdb::makeObject<pdb::Vector<double>>(numWord, numWord);
    alpha->fill(1.0);
    beta->fill(1.0);

    // Initialization for LDA
    Vector<Handle<Computation>> tempOut;

    // Initialize the topic mixture probabilities for each doc
    Handle<Computation> myInitialScanSet =
        makeObject<ScanLDADocumentSet>("LDA_db", "LDA_input_set");
    Handle<Computation> myDocID = makeObject<LDADocIDAggregate>();
    myDocID->setInput(myInitialScanSet);
    Handle<Computation> myDocTopicProb = makeObject<LDAInitialTopicProbSelection>(*alpha);
    myDocTopicProb->setInput(myDocID);

    // Initialize the (wordID, topic prob vector)
    Handle<Computation> myMetaScanSet = makeObject<ScanIntSet>("LDA_db", "LDA_meta_data_set");
    Handle<Computation> myWordTopicProb = makeObject<LDAInitialWordTopicProbSelection>(numTopic);
    myWordTopicProb->setInput(myMetaScanSet);

    Handle<Computation> input1 = myDocTopicProb;
    Handle<Computation> input2 = myWordTopicProb;

    // Start LDA iterations
    //    auto begin = std :: chrono :: high_resolution_clock :: now();
    for (int n = 0; n < iter; n++) {

        auto iter_begin = std::chrono::high_resolution_clock::now();

        term << "*****************************************" << std::endl;
        term << "I am in iteration : " << n << std::endl;
        term << "*****************************************" << std::endl;

        // first we set up the join that will assign all of the words in the corpus to topics
        Handle<Computation> myDocWordTopicJoin = makeObject<LDADocWordTopicJoin>(numWord);
        myDocWordTopicJoin->setInput(0, myInitialScanSet);
        myDocWordTopicJoin->setInput(1, input1);
        myDocWordTopicJoin->setInput(2, input2);

        // do an identity selection, since a join can't have two outputs
        Handle<Computation> myIdentitySelection = makeObject<LDADocWordTopicAssignmentIdentity>();
        myIdentitySelection->setInput(myDocWordTopicJoin);

        // now we set up the sequence of actions that re-compute the topic probabilities for each
        // doc
        // get the set of topics assigned for each doc
        Handle<Computation> myDocWordTopicCount = makeObject<LDADocAssignmentMultiSelection>();
        myDocWordTopicCount->setInput(myIdentitySelection);

        // aggregate them
        Handle<Computation> myDocTopicCountAgg = makeObject<LDADocTopicAggregate>();
        myDocTopicCountAgg->setInput(myDocWordTopicCount);
        // and get the new set of doc probabilities
        Handle<Computation> myDocTopicProb = makeObject<LDADocTopicProbSelection>(*alpha);
        myDocTopicProb->setInput(myDocTopicCountAgg);

        // now we set up the sequence of actions that re-compute the word probs for each topic
        // get the set of words assigned for each topic in each doc
        Handle<Computation> myTopicWordCount = makeObject<LDATopicAssignmentMultiSelection>();
        myTopicWordCount->setInput(myIdentitySelection);

        // agg them
        Handle<Computation> myTopicWordCountAgg = makeObject<LDATopicWordAggregate>();
        myTopicWordCountAgg->setInput(myTopicWordCount);
        // use those aggs to get per-topic probabilities
        Handle<Computation> myTopicWordProb =
            makeObject<LDATopicWordProbMultiSelection>(*beta, numTopic);
        myTopicWordProb->setInput(myTopicWordCountAgg);

        // and see what the per-word probabilities are
        Handle<Computation> myWordTopicProb = makeObject<LDAWordTopicAggregate>();
        myWordTopicProb->setInput(myTopicWordProb);

        // now, we get the writers
        std::string myWriterForTopicsPerWordSetName =
            std::string("TopicsPerWord") + std::to_string((n + 1) % 2);
        Handle<Computation> myWriterForTopicsPerWord =
            makeObject<WriteTopicsPerWord>("LDA_db", myWriterForTopicsPerWordSetName);
        myWriterForTopicsPerWord->setInput(myWordTopicProb);

        // now, we get the writers
        std::string myWriterForTopicsPerDocSetName =
            std::string("TopicsPerDoc") + std::to_string((n + 1) % 2);
        Handle<Computation> myWriterForTopicsPerDoc =
            makeObject<WriteIntDoubleVectorPairSet>("LDA_db", myWriterForTopicsPerDocSetName);
        myWriterForTopicsPerDoc->setInput(myDocTopicProb);

        if (!pdbClient.executeComputations(
                errMsg, myWriterForTopicsPerWord, myWriterForTopicsPerDoc)) {
            std::cout << "Query failed. Message was: " << errMsg << "\n";
            return 1;
        }

        // clear the set that have been read in this iteration by old readers
        auto clear_begin = std::chrono::high_resolution_clock::now();
        std::string myReaderForTopicsPerWordSetName =
            std::string("TopicsPerWord") + std::to_string(n % 2);
        std::string myReaderForTopicsPerDocSetName =
            std::string("TopicsPerDoc") + std::to_string(n % 2);
        if (!pdbClient.clearSet(
                "LDA_db", myReaderForTopicsPerWordSetName, "pdb::IntDoubleVectorPair", errMsg)) {
            cout << "Not able to create set: " + errMsg;
            exit(-1);
        }
        if (!pdbClient.clearSet(
                "LDA_db", myReaderForTopicsPerDocSetName, "pdb::IntDoubleVectorPair", errMsg)) {
            cout << "Not able to create set: " + errMsg;
            exit(-1);
        }
        auto clear_end = std::chrono::high_resolution_clock::now();
        // finally, create the new readers
        input2 = makeObject<ScanTopicsPerWord>("LDA_db", myWriterForTopicsPerWordSetName);
        input1 = makeObject<ScanIntDoubleVectorPairSet>("LDA_db", myWriterForTopicsPerDocSetName);
        myInitialScanSet = makeObject<ScanLDADocumentSet>("LDA_db", "LDA_input_set");
        auto iter_end = std::chrono::high_resolution_clock::now();
        term << "Time Duration for clear-set " << n << ": "
             << std::chrono::duration_cast<std::chrono::duration<float>>(clear_end - clear_begin)
                    .count()
             << " secs." << std::endl;
        if (n == 0) {
            term << "Time Duration for iteration " << n << ": "
                 << std::chrono::duration_cast<std::chrono::duration<float>>(iter_end - total_begin)
                        .count()
                 << " secs." << std::endl;
        } else {
            term << "Time Duration for iteration " << n << ": "
                 << std::chrono::duration_cast<std::chrono::duration<float>>(iter_end - iter_begin)
                        .count()
                 << " secs." << std::endl;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    int totOut = 0;
    std::string myWriterForTopicsPerWordSetName =
        std::string("TopicsPerWord") + std::to_string(iter % 2);
    SetIterator<LDATopicWordProb> initTopicProbResult =
            pdbClient.getSetIterator<LDATopicWordProb>("LDA_db", myWriterForTopicsPerWordSetName);
    for (auto& a : initTopicProbResult) {
        //    		std :: cout << "Word ID: " << a->getKey () << " Topic probabilities: ";
        //		a->getVector().print();
        //  		std :: cout << std::endl;
        totOut++;
    }

    term << "Time Duration: "
         << std::chrono::duration_cast<std::chrono::duration<float>>(end - total_begin).count()
         << " secs." << std::endl;
    term << "The total number of output I have: " << totOut << std::endl;
}

#endif
