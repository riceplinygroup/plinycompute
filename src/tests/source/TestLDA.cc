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


// By Shangyu, June 2017
// LDA using Gibbs Sampling;

#include "PDBDebug.h"
#include "PDBVector.h"
#include "Query.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"

#include "DispatcherClient.h"
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

#include "LDA/LDAInitialWordTopicProbMultiSelection.h"
#include "LDA/LDADocWordTopicJoin.h"
#include "LDA/LDADocTopicAggregate.h"
#include "LDA/LDADocTopicProbSelection.h"
#include "LDA/LDADocWordTopicMultiSelection.h"
#include "LDA/LDATopicWordAggregate.h"
#include "LDA/LDATopicWordProbMultiSelection.h"
#include "LDA/LDAWordTopicAggregate.h"
#include "LDA/WriteLDADocWordTopicAssignmentSet.h"

#include "LDA/LDADocWordTopicCount.h"
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



using namespace pdb;
int main (int argc, char * argv[]) {
    bool printResult = true;
    bool clusterMode = false;
   // freopen("output.txt","w",stdout);
    std::ofstream term("/dev/tty", std::ios_base::out);

//    std::ofstream out("output.txt");
//    std::streambuf *coutbuf = std::cout.rdbuf(); //save old buf
//    std::cout.rdbuf(out.rdbuf()); //redirect std::cout to out.txt!

    const std::string red("\033[0;31m");
    const std::string green("\033[1;32m");
    const std::string yellow("\033[1;33m");
    const std::string blue("\033[1;34m");
    const std::string cyan("\033[0;36m");
    const std::string magenta("\033[0;35m");
    const std::string reset("\033[0m");


    std :: cout << "Usage: #printResult[Y/N] #clusterMode[Y/N] #dataSize[MB] #masterIp #addData[Y/N]" << std :: endl;        
    if (argc > 1) {
        if (strcmp(argv[1],"N") == 0) {
            printResult = false;
            std :: cout << "You successfully disabled printing result." << std::endl;
        }else {
            printResult = true;
            std :: cout << "Will print result." << std :: endl;
        }
    } else {
        std :: cout << "Will print result. If you don't want to print result, you can add N as the first parameter to disable result printing." << std :: endl;
    }

    if (argc > 2) {
        if (strcmp(argv[2],"Y") == 0) {
            clusterMode = true;
            std :: cout << "You successfully set the test to run on cluster." << std :: endl;
        } else {
            clusterMode = false;
        }
    } else {
        std :: cout << "Will run on local node. If you want to run on cluster, you can add any character as the second parameter to run on the cluster configured by $PDB_HOME/conf/serverlist." << std :: endl;
    }

    int numOfMb = 64; //by default we add 64MB data
    if (argc > 3) {
        numOfMb = atoi(argv[3]);
    }
    numOfMb = 64; //Force it to be 64 by now.


    std :: cout << "To add data with size: " << numOfMb << "MB" << std :: endl;

    std :: string masterIp = "localhost";
    if (argc > 4) {
        masterIp = argv[4];
    }
    std :: cout << "Master IP Address is " << masterIp << std :: endl;

    bool whetherToAddData = true;
    if (argc > 5) {
        if (strcmp(argv[5],"N") == 0) {
            whetherToAddData = false;
        }
    }


    term << blue << std :: endl;
    term << "*****************************************" << std :: endl;
    term << "LDA starts : " << std :: endl;
    term << "*****************************************" << std :: endl;
    term << reset << std :: endl;

    
    term << blue << "The LDA paramers are: " << std :: endl;
    term << reset << std :: endl;

    // Basic parameters for LDA
    int iter = 1;
    int numDoc = 10;
    int numWord = 10;
    int numTopic = 4;

    if (argc > 6) {
	iter = std::stoi(argv[6]);
    }
    term << "The number of iterations: " << iter << std :: endl;

    if (argc > 7) {
	numDoc = std::stoi(argv[7]);
    }
    term << "The number of documents: " << numDoc << std :: endl;

    if (argc > 8) {
	numWord = std::stoi(argv[8]);
    }
    term << "The dictionary size: " << numWord << std :: endl;

    if (argc > 9) {
	numTopic = std::stoi(argv[9]);
    }
    term << "The number of topics: " << numTopic << std :: endl;


    std :: cout << std :: endl;


    pdb :: PDBLoggerPtr clientLogger = make_shared<pdb :: PDBLogger>("clientLog");

    pdb :: DistributedStorageManagerClient temp (8108, masterIp, clientLogger);

    pdb :: CatalogClient catalogClient (8108, masterIp, clientLogger);

    string errMsg;

    // Some meta data
    pdb :: makeObjectAllocatorBlock(1 * 1024 * 1024, true);
    pdb::Handle<pdb::Vector<double>> alpha = pdb::makeObject<pdb::Vector<double>> (numTopic, numTopic);
    pdb::Handle<pdb::Vector<double>> beta = pdb::makeObject<pdb::Vector<double>> (numWord, numWord);

    // For the random number generator
    std::random_device rd;
    std::mt19937 randomGen(rd());
	
    alpha->fill(1.0);
    beta->fill(1.0);

    catalogClient.registerType ("libraries/libIntDoubleVectorPair.so", errMsg);
    catalogClient.registerType ("libraries/libLDADocument.so", errMsg);
    catalogClient.registerType ("libraries/libLDADocWordTopicAssignment.so", errMsg);


    if (whetherToAddData == true) {
        //Step 1. Create Database and Set

        // now, create a new database
        if (!temp.createDatabase ("LDA_db", errMsg)) {
            cout << "Not able to create database: " + errMsg;
            exit (-1);
        } else {
            cout << "Created database.\n";
        }

        // now, create a new set in that database
        if (!temp.createSet<LDADocument> ("LDA_db", "LDA_input_set", errMsg)) {
            cout << "Not able to create set: " + errMsg;
            exit (-1);
        } else {
            cout << "Created set.\n";
        }
	
        if (!temp.createSet<int> ("LDA_db", "LDA_meta_data_set", errMsg)) {
            cout << "Not able to create set: " + errMsg;
            exit (-1);
        } else {
            cout << "Created set.\n";
        }
	

        //Step 2. Add data
        DispatcherClient dispatcherClient = DispatcherClient(8108, masterIp, clientLogger);


        if (numOfMb > 0) {
            int numIterations = numOfMb/64;
            std:: cout << "Number of MB: " << numOfMb << " Number of Iterations: " << numIterations << std::endl;
            int remainder = numOfMb - 64*numIterations;
            if (remainder > 0) { 
                numIterations = numIterations + 1; 
            }
            for (int num = 0; num < numIterations; num++) {
                std::cout << "Iterations: "<< num << std::endl;
                int blockSize = 64;
                if ((num == numIterations - 1) && (remainder > 0)){
                    blockSize = remainder;
                }
                pdb :: makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
		pdb::Handle<pdb::Vector<pdb::Handle<LDADocument>>> storeMe = 
			pdb::makeObject<pdb::Vector<pdb::Handle<LDADocument>>> ();
                try {


                    for (int i = 0; i < numDoc; i++) {

			int length = storeMe->size();
			std::uniform_int_distribution<> int_unif(1, 3);
                        for (int j = 0; j < numWord; j++){
			    std::uniform_real_distribution<> real_unif(0, 1);
			    double ifWord = real_unif(randomGen);
			    if (ifWord > 0.3) {
                                pdb :: Handle <LDADocument> myData = pdb::makeObject<LDADocument>();
				myData->setDoc(i);
				myData->setWord(j);
				myData->setCount(int_unif(randomGen));
      			//        myData->push_back(i);
                        //    	myData->push_back(j);
			//	myData->push_back(int_unif(randomGen));

                        	storeMe->push_back (myData);
			    }
                        }
			if (storeMe->size() == length) {
				term << "We do not get any words for this document: " << i << std::endl;
				std::uniform_int_distribution<> int_unif2(0, numWord - 1);
                                pdb :: Handle <LDADocument> myData = pdb::makeObject<LDADocument>();
				myData->setDoc(i);
				myData->setWord(int_unif2(randomGen));
				myData->setCount(int_unif(randomGen));

      			      //  myData->push_back(i);
			      //myData->push_back(int_unif2(randomGen));
				//myData->push_back(int_unif(randomGen));
                        	storeMe->push_back (myData);
			}
                    }
		    
		    term << std :: endl;
		    term << green << "input data: " << reset << std :: endl;
                    for (int i=0; i<storeMe->size();i++){
                        (*storeMe)[i]->print();
                    }

	            if (!dispatcherClient.sendData<LDADocument>(std::pair<std::string, std::string>("LDA_input_set", "LDA_db"), storeMe, errMsg)) {
                        std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                        return -1;
                    }

		    
                } catch (pdb :: NotEnoughSpace &n) {
                    if (!dispatcherClient.sendData<LDADocument>(std::pair<std::string, std::string>("LDA_input_set", "LDA_db"), storeMe, errMsg)) {
                        std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                        return -1;
                    }
                }
                PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std :: endl;
            }

            //to write back all buffered records        
            temp.flushData( errMsg );
        }

    // add meta data
	
        if (numOfMb > 0) {
            int numIterations = numOfMb/64;
            std:: cout << "Number of MB: " << numOfMb << " Number of Iterations: " << numIterations << std::endl;
            int remainder = numOfMb - 64*numIterations;
            if (remainder > 0) { 
                numIterations = numIterations + 1; 
            }
            for (int num = 0; num < numIterations; num++) {
                std::cout << "Iterations: "<< num << std::endl;
                int blockSize = 64;
                if ((num == numIterations - 1) && (remainder > 0)){
                    blockSize = remainder;
                }
                pdb :: makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
		pdb::Handle<pdb::Vector<pdb::Handle<int>>> storeMe = pdb::makeObject<pdb::Vector<pdb::Handle<int>>> ();
                try {
		    Handle <int> myData = makeObject <int> (numWord);
		    storeMe->push_back(myData);
		  //  storeMe->push_back(&numWord);

		    term << std :: endl;
		    term << green << "Dictionary size: " << *((*storeMe)[0]) << reset << std :: endl;
		    term << std :: endl;

                    if (!dispatcherClient.sendData<int>(std::pair<std::string, std::string>("LDA_meta_data_set", "LDA_db"), storeMe, errMsg)) {
                        std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                        return -1;
                    }

        		    
                } catch (pdb :: NotEnoughSpace &n) {
                    if (!dispatcherClient.sendData<int>(std::pair<std::string, std::string>("LDA_meta_data_set", "LDA_db"), storeMe, errMsg)) {
                        std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                        return -1;
                    }
                }
                PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std :: endl;
            }

            //to write back all buffered records        
            temp.flushData( errMsg );
        }
	
    }




    // now, create a new set in that database to store output data
    
    PDB_COUT << "to create a new set to store the (doc, topic probability)" << std :: endl;
    if (!temp.createSet<IntDoubleVectorPair> ("LDA_db", "LDA_doc_topic_prob_test_set", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit (-1);
    } else {
        cout << "Created set LDA_doc_topic_prob_test_set.\n";
    }

    PDB_COUT << "to create a new set to store the (word, topic probability)" << std :: endl;
    if (!temp.createSet<IntDoubleVectorPair> ("LDA_db", "LDA_word_topic_prob_test_set", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit (-1);
    } else {
        cout << "Created set LDA_word_topic_prob_test_set.\n";
    }

    PDB_COUT << "to create a new set to store the sampled topics" << std :: endl;
    if (!temp.createSet<LDADocWordTopicAssignment> ("LDA_db", "LDA_topic_join_test_set", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit (-1);
    } else {
        cout << "Created set LDA_topic_join_test_set.\n";
    }


    /*
    PDB_COUT << "to create a new set for storing output data" << std :: endl;
    if (!temp.createSet<DoubleVector> ("LDA_db", "LDA_output_set", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit (-1);
    } else {
        cout << "Created set LDA_output_set.\n";
    }
	*/


    
    //Step 3. To execute a Query
	// for allocations

	// register this query class
    
    catalogClient.registerType ("libraries/libLDADocIDAggregate.so", errMsg);
    catalogClient.registerType ("libraries/libScanLDADocumentSet.so", errMsg);
    catalogClient.registerType ("libraries/libLDAInitialTopicProbSelection.so", errMsg);
    catalogClient.registerType ("libraries/libWriteIntDoubleVectorPairSet.so", errMsg);
    catalogClient.registerType ("libraries/libScanIntSet.so", errMsg);
    catalogClient.registerType ("libraries/libLDADocWordTopicJoin.so", errMsg);
    catalogClient.registerType ("libraries/libLDAInitialWordTopicProbMultiSelection.so", errMsg);
    catalogClient.registerType ("libraries/libLDADocTopicAggregate.so", errMsg);
    catalogClient.registerType ("libraries/libLDADocTopicProbSelection.so", errMsg);
    catalogClient.registerType ("libraries/libIntIntVectorPair.so", errMsg);
    catalogClient.registerType ("libraries/libLDADocWordTopicMultiSelection.so", errMsg);
    catalogClient.registerType ("libraries/libLDATopicWordAggregate.so", errMsg);
    catalogClient.registerType ("libraries/libLDATopicWordProbMultiSelection.so", errMsg);
    catalogClient.registerType ("libraries/libLDAWordTopicAggregate.so", errMsg);
    catalogClient.registerType ("libraries/libLDADocWordTopicCount.so", errMsg);
    catalogClient.registerType ("libraries/libLDATopicWordProb.so", errMsg);
    catalogClient.registerType ("libraries/libWriteLDADocWordTopicAssignmentSet.so", errMsg);


	// connect to the query client
    QueryClient myClient (8108, "localhost", clientLogger, true);
//    const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};
    
    // Initialization for LDA

    Vector<Handle<Computation>> tempOut;
       
    // Initialize the topic mixture probabilities for each doc
        
    Handle<Computation> myInitialScanSet = makeObject<ScanLDADocumentSet>("LDA_db", "LDA_input_set");
    Handle<Computation> myDocID = makeObject<LDADocIDAggregate>();
    myDocID->setInput(myInitialScanSet);
    Handle<Computation> myDocTopicProb = makeObject<LDAInitialTopicProbSelection>(*alpha);
    myDocTopicProb->setInput(myDocID);
//    Handle <Computation> myWriter = makeObject<WriteIntDoubleVectorPairSet>("LDA_db", "LDA_word_topic_prob_test_set");
//    myWriter->setInput(myDocTopicProb);
    
    

    // Initialize the (wordID, topic prob vector)
    
    Handle<Computation> myMetaScanSet = makeObject<ScanIntSet>("LDA_db", "LDA_meta_data_set");
    Handle<Computation> myWordTopicProb = makeObject<LDAInitialWordTopicProbMultiSelection>(numTopic);
    myWordTopicProb->setInput(myMetaScanSet);
//    Handle <Computation> myWriter = makeObject<WriteIntDoubleVectorPairSet>("LDA_db", "LDA_doc_topic_prob_test_set");
//    myWriter->setInput(myWordTopicProb);

//    tempOut.push_back(myDocTopicProb);
//    tempOut.push_back(myWordTopicProb);
	
    /*
    if (!myClient.executeComputations(errMsg, myWriter)) {
        std :: cout << "Query failed. Message was: " << errMsg << "\n";
        return 1;
    }
    */
    

    // Start LDA iterations
    	
//    for (int n = 0; n < iter; n++) {


                //const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 24};

		term << "*****************************************" << std :: endl;
//		term << "I am in iteration : " << n << std :: endl;
		term << "*****************************************" << std :: endl;
		    
    		
		// Sample for the topics	
		
    		//Handle<Computation> myScanSet = makeObject<ScanLDADocumentSet>("LDA_db", "LDA_input_set");
		Handle <Computation> myDocWordTopicJoin = makeObject <LDADocWordTopicJoin> ();
		myDocWordTopicJoin->setInput(0, myInitialScanSet);
		myDocWordTopicJoin->setInput(1, myDocTopicProb);
//		myDocWordTopicJoin->setInput(1, tempOut[0]);
		myDocWordTopicJoin->setInput(2, myWordTopicProb);
//		myDocWordTopicJoin->setInput(2, tempOut[1]);
		
		/*
		// Sample for the (doc, topic probability)
		
		Handle <Computation> myDocTopicCountAgg= makeObject <LDADocTopicAggregate> (numTopic);
		myDocTopicCountAgg->setInput(myDocWordTopicJoin);
		Handle <Computation> myDocTopicProbThis = makeObject<LDADocTopicProbSelection>(*alpha);
//		myDocTopicProb = makeObject<LDADocTopicProbSelection>(*alpha);
		myDocTopicProbThis->setInput(myDocTopicCountAgg);
//		tempOut[0] = myDocTopicProb;
		*/

		// Sample for the (topic, word probability)
		/*
		Handle <Computation> myDocWordTopicCount = makeObject <LDADocWordTopicMultiSelection> ();
		myDocWordTopicCount->setInput(myDocWordTopicJoin);
		Handle <Computation> myTopicWordAgg = makeObject <LDATopicWordAggregate> (numWord);
		myTopicWordAgg->setInput(myDocWordTopicCount);
		Handle <Computation> myTopicWordProb = makeObject <LDATopicWordProbMultiSelection> (*beta);
		myTopicWordProb->setInput(myTopicWordAgg);

		// Aggregate to get (word, topic probability)
		Handle <Computation> myWordTopicProbThis = makeObject <LDAWordTopicAggregate> (numTopic);		
		myWordTopicProbThis->setInput(myTopicWordProb);
		*/
//	}

	//	Handle <Computation> myWriter = makeObject<WriteIntDoubleVectorPairSet>("LDA_db", "LDA_doc_topic_prob_test_set");
	//	myWriter->setInput(tempOut[0]);
	//	myWriter->setInput(myDocTopicProbThis);
		
		/*	
		Handle <Computation> myWriter1 = makeObject<WriteIntDoubleVectorPairSet>("LDA_db", "LDA_word_topic_prob_test_set");
		myWriter1->setInput(myWordTopicProbThis);
		*/

		Handle <Computation> myWriter = makeObject<WriteLDADocWordTopicAssignmentSet>("LDA_db", "LDA_topic_join_test_set");
		myWriter->setInput(myDocWordTopicJoin);
	
	
		auto begin = std :: chrono :: high_resolution_clock :: now();
		if (!myClient.executeComputations(errMsg, myWriter)) {
			std :: cout << "Query failed. Message was: " << errMsg << "\n";
			return 1;
		}

		    
		std :: cout << "The query is executed successfully!" << std :: endl;

		/*
		SetIterator <IntDoubleVectorPair> initTopicProbResult = 
						myClient.getSetIterator <IntDoubleVectorPair> ("LDA_db", "LDA_doc_topic_prob_test_set");
		for (Handle<IntDoubleVectorPair> a : initTopicProbResult) {
			term << red << "Doc ID: " << a->getInt() << " Topic Probability: ";
			a->getVector().print(); 
			term << std::endl;	
		}

		term << reset << std::endl;	
		*/

		auto end = std::chrono::high_resolution_clock::now();
		term << "Time Duration: " <<
		std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;


		/*
		std :: cout << std :: endl;
		std :: cout << std :: endl;

		temp.clearSet("LDA_db", "LDA_output_set", "pdb::KMeansAggregateOutputType", errMsg);

    }

    std::cout << std::endl;

    //QueryClient myClient (8108, "localhost", clientLogger, true);

	// print the resuts
    if (printResult == true) {
//        std :: cout << "to print result..." << std :: endl;

//	std :: cout << std :: endl;

	*
        SetIterator <DoubleVector> input = myClient.getSetIterator <DoubleVector> ("LDA_db", "LDA_input_set");
        std :: cout << "Query input: "<< std :: endl;
        int countIn = 0;
        for (auto a : input) {
            countIn ++;
            std :: cout << countIn << ":";
            a->print();
            std :: cout << std::endl;
        }
	*

        
        SetIterator <KMeansAggregateOutputType> result = myClient.getSetIterator <KMeansAggregateOutputType> ("LDA_db", "LDA_output_set");


	std :: cout << std :: endl;
	std :: cout << blue << "*****************************************" << reset << std :: endl;
	std :: cout << blue << "K-means resultss : " << reset << std :: endl;
	std :: cout << blue << "*****************************************" << reset << std :: endl;
	std :: cout << std :: endl;

//                std :: cout << "The std model I have is: " << std :: endl;
	for (int i = 0; i < k; i++) {
	     std :: cout << "Cluster index: " << i << std::endl;
	     for (int j = 0; j < dim - 1; j++) {
		 std :: cout << blue << model[i][j] << ", " << reset;
	     }
		 std :: cout << blue << model[i][dim - 1] << reset << std :: endl;
	}

    }
    */


    std :: cout << std :: endl;

    if (clusterMode == false) {
	    // and delete the sets
       // myClient.deleteSet ("LDA_db", "LDA_output_set");
        myClient.deleteSet ("LDA_db", "LDA_doc_topic_prob_test_set");
        myClient.deleteSet ("LDA_db", "LDA_word_topic_prob_test_set");
        myClient.deleteSet ("LDA_db", "LDA_topic_join_test_set");
    } else {
	/*
        if (!temp.removeSet ("LDA_db", "LDA_output_set", errMsg)) {
            cout << "Not able to remove set: " + errMsg;
            exit (-1);
        }
	*/
        if (!temp.removeSet ("LDA_db", "LDA_doc_topic_prob_test_set", errMsg)) {
            cout << "Not able to remove set: " + errMsg;
            exit (-1);
        }
        else if (!temp.removeSet ("LDA_db", "LDA_word_topic_prob_test_set", errMsg)) {
            cout << "Not able to remove set: " + errMsg;
            exit (-1);
        }
        else if (!temp.removeSet ("LDA_db", "LDA_topic_join_test_set", errMsg)) {
            cout << "Not able to remove set: " + errMsg;
            exit (-1);
        }


	else {
            cout << "Removed set.\n";
        }
    }
    int code = system ("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        std :: cout << "Can't cleanup so files" << std :: endl;
    }
//    std::cout << "Time Duration: " <<
//    std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;
}

#endif
