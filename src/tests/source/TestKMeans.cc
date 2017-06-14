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
#ifndef TEST_KMEANS_CC
#define TEST_KMEANS_CC


//by Shangyu, May 2017
//K-means clustering;

#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"

#include "WriteKMeansSet.h"
#include "KMeansAggregate.h"
#include "KMeansDataCountAggregate.h"
#include "ScanDoubleVectorSet.h"
#include "KMeansAggregateOutputType.h"
#include "KMeansCentroid.h"
#include "KMeansDataCountAggregate.h"
#include "KMeansSampleSelection.h"
#include "WriteDoubleVectorSet.h"

#include "DispatcherClient.h"
#include "Set.h"
#include "DataTypes.h"
#include "DoubleVector.h"
#include <ctime>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>
#include <cstddef>
#include <iostream>
#include <math.h>
#include <random>



using namespace pdb;
int main (int argc, char * argv[]) {
    bool printResult = true;
    bool clusterMode = false;
    freopen("output.txt","w",stdout);

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


    std :: cout << std :: endl;
    std :: cout << "*****************************************" << std :: endl;
    std :: cout << "K-means starts : " << std :: endl;
    std :: cout << "*****************************************" << std :: endl;
    std :: cout << std :: endl;

    
    std :: cout << "The K-means paramers are: " << std :: endl;
    std :: cout << std :: endl;

    int iter = 1;
    int k = 3;
    int dim = 3;
    int numData = 10;

    if (argc > 6) {
	iter = std::stoi(argv[6]);
    }
    std :: cout << "The number of iterations: " << iter << std :: endl;

    if (argc > 7) {
	k = std::stoi(argv[7]);
    }
    std :: cout << "The number of clusters: " << k << std :: endl;

    if (argc > 8) {
	numData = std::stoi(argv[8]);
    }
    std :: cout << "The number of data points: " << numData << std :: endl;

    if (argc > 9) {
	dim = std::stoi(argv[9]);
    }
    std :: cout << "The dimension of each data point: " << dim << std :: endl;


    std :: cout << std :: endl;


    pdb :: PDBLoggerPtr clientLogger = make_shared<pdb :: PDBLogger>("clientLog");

    pdb :: DistributedStorageManagerClient temp (8108, masterIp, clientLogger);

    pdb :: CatalogClient catalogClient (8108, masterIp, clientLogger);

    string errMsg;

    // Some meta data
    pdb :: makeObjectAllocatorBlock(1 * 1024 * 1024, true);
    pdb::Handle<pdb::Vector<DoubleVector>> tmpModel = pdb::makeObject<pdb::Vector<DoubleVector>> (k, k);
    int dataCount = 0;
    int myk = 0;
    int kk = 0;
//    srand(time(0));
    // For the random number generator
    std::random_device rd;
    std::mt19937 randomGen(rd());


    if (whetherToAddData == true) {
        //Step 1. Create Database and Set
        // now, register a type for user data
        //TODO: once sharedLibrary is supported, add this line back!!!
        catalogClient.registerType ("libraries/libKMeansAggregateOutputType.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansCentroid.so", errMsg);

        // now, create a new database
        if (!temp.createDatabase ("kmeans_db", errMsg)) {
            cout << "Not able to create database: " + errMsg;
            exit (-1);
        } else {
            cout << "Created database.\n";
        }

        // now, create a new set in that database
        if (!temp.createSet<DoubleVector> ("kmeans_db", "kmeans_input_set", errMsg)) {
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
                pdb::Handle<pdb::Vector<pdb::Handle<DoubleVector>>> storeMe = pdb::makeObject<pdb::Vector<pdb::Handle<DoubleVector>>> ();
                try {
                	// Write 10 data points
                	// Each data point has 3 dimensions
                	//k = 3;
                	double bias = 0;

                    for (int i = 0; i < numData; i++) {
                        pdb :: Handle <DoubleVector> myData = pdb::makeObject<DoubleVector>(dim);
                        for (int j = 0; j < dim; j++){

			    std::uniform_real_distribution<> unif(0, 1);
			    bias = unif(randomGen) * 0.01;
                            //bias = ((double) rand() / (RAND_MAX)) * 0.01;
                            //myData->push_back(tmp);
			    myData->setDouble(j, i%k*3 + bias);
                        }
                        storeMe->push_back (myData);
                    }
		    /*std :: cout << "input data: " << std :: endl;
                    for (int i=0; i<storeMe->size();i++){
                        (*storeMe)[i]->print();
                    }*/
		    
		    std :: cout << "initial model: " << std :: endl;
		    for (int i = 0; i < k; i++) {
			(*tmpModel)[i] = *((*storeMe)[i]);
			(*tmpModel)[i].print();
		    }	    

                    if (!dispatcherClient.sendData<DoubleVector>(std::pair<std::string, std::string>("kmeans_input_set", "kmeans_db"), storeMe, errMsg)) {
                        std :: cout << "Failed to send data to dispatcher server" << std :: endl;
                        return -1;
                    }
                } catch (pdb :: NotEnoughSpace &n) {
                    if (!dispatcherClient.sendData<DoubleVector>(std::pair<std::string, std::string>("kmeans_input_set", "kmeans_db"), storeMe, errMsg)) {
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

    PDB_COUT << "to create a new set to store the data count" << std :: endl;
    if (!temp.createSet<int> ("kmeans_db", "kmeans_data_count_set", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit (-1);
    } else {
        cout << "Created set kmeans_data_count_set.\n";
    }


    PDB_COUT << "to create a new set to store the initial model" << std :: endl;
    if (!temp.createSet<DoubleVector> ("kmeans_db", "kmeans_initial_model_set", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit (-1);
    } else {
        cout << "Created set kmeans_initial_model_set.\n";
    }

    
    PDB_COUT << "to create a new set for storing output data" << std :: endl;
    if (!temp.createSet<KMeansAggregateOutputType> ("kmeans_db", "kmeans_output_set", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit (-1);
    } else {
        cout << "Created set kmeans_output_set.\n";
    }

    //Step 3. To execute a Query
	// for allocations

	// register this query class
    catalogClient.registerType ("libraries/libKMeansAggregate.so", errMsg);
    catalogClient.registerType ("libraries/libKMeansDataCountAggregate.so", errMsg);
    catalogClient.registerType ("libraries/libScanDoubleVectorSet.so", errMsg);
    catalogClient.registerType ("libraries/libWriteKMeansSet.so", errMsg);
    catalogClient.registerType ("libraries/libKMeansDataCountAggregate.so", errMsg);
    catalogClient.registerType ("libraries/libKMeansSampleSelection.so", errMsg);
    catalogClient.registerType ("libraries/libWriteDoubleVectorSet.so", errMsg);
    


	// connect to the query client
    QueryClient myClient (8108, "localhost", clientLogger, true);
    
    // pdb::Handle<pdb::Vector<DoubleVector>> model = pdb::makeObject<pdb::Vector<DoubleVector>> (k, k);

    std :: vector< std :: vector <double> > model(k, vector<double>(dim));
    for (int i = 0; i < k; i++) {
	for (int j = 0; j < dim; j++) {
       //		model[i][j] = (*tmpModel)[i].getDouble(j);
       		model[i][j] = 0;
	}
    }
    /*
    std :: cout << "My intial model: " << std :: endl;
    for (int i = 0; i < k; i++) {
	(*model)[i] = (*tmpModel)[i];
	(*model)[i].print();
    }
    */

    // Initialization for the model in the KMeansAggregate
       
    // Calculate the count of input data points 

    Handle<Computation> myInitialScanSet = makeObject<ScanDoubleVectorSet>("kmeans_db", "kmeans_input_set");
    Handle<Computation> myDataCount = makeObject<KMeansDataCountAggregate>();
    myDataCount->setInput(myInitialScanSet);
//    myDataCount->setAllocatorPolicy(noReuseAllocator);
    myDataCount->setOutput("kmeans_db", "kmeans_data_count_set");

    if (!myClient.executeComputations(errMsg, myDataCount)) {
        std :: cout << "Query failed. Message was: " << errMsg << "\n";
        return 1;
    }
    SetIterator <int> dataCountResult = myClient.getSetIterator <int> ("kmeans_db", "kmeans_input_set");
    for (Handle<int> a : dataCountResult) {
	dataCount = *a;
    }

    std :: cout << std :: endl;
    
    // Randomly sample k data points from the input data through Bernoulli sampling
    // We guarantee the sampled size >= k in 99.99% of the time

    double fraction = 1;
    double delta = 1e-4;
    double gamma = - log(delta) / dataCount;
    fraction = fmin(1, fraction + gamma + sqrt(gamma * gamma + 2 * gamma * fraction));
    std :: cout << "The sample threshold is: " << fraction << std :: endl;
    while(myk < k) {
	
	    Handle<Computation> myInitalScanSet = makeObject<ScanDoubleVectorSet>("kmeans_db", "kmeans_input_set");
    	    Handle<Computation> myDataSample = makeObject<KMeansSampleSelection>(fraction);
    	    myDataSample->setInput(myInitialScanSet);
	    Handle<Computation> myWriteSet = makeObject<WriteDoubleVectorSet>("kmeans_db", "kmeans_initial_model_set");
	    myWriteSet->setInput(myDataSample);

	 
	    if (!myClient.executeComputations(errMsg, myWriteSet)) {
		std :: cout << "Query failed. Message was: " << errMsg << "\n";
		return 1;
	    }
	    SetIterator <DoubleVector> sampleResult = myClient.getSetIterator <DoubleVector> ("kmeans_db", "kmeans_initial_model_set");

	    int sampleCount = 0;
	    for (Handle<DoubleVector> a : sampleResult) {
		sampleCount++;
	    }	    

	    if (sampleCount <= k - myk) {   // Assign the sampled values to the model directly
		for (Handle<DoubleVector> a : sampleResult) {

			// Sample without replacement
			bool notNew = true;
			for (int i = 0; i < myk; i++) {
				for (int j = 0; j < dim; j++)
					notNew = (model[i][j] == a->getDouble(j)) && notNew;
				if (notNew) {
					break;
				}
				else {
					if (i != myk - 1)
						notNew = true;
				}
			}

			if (!notNew) {
				std :: cout << "The sample we got is: " << std :: endl;
				for (int i = 0; i < dim; i++) {
					model[myk][i] = a->getDouble(i);
					myk++;
					std :: cout << model[myk][i] << ", ";
				}
				std :: cout << std :: endl;
			}
            	} 
	    }
	    else {	// Randomly select (k - myk) samples from sampleResult
		std :: cout << "We got " << sampleCount << " samples. We will randomly select " << (k-myk) << " samples from them." << std :: endl;
		std :: set<int> randomID;
		int myPos = 0;
		while (randomID.size() < (k-myk)) {
			std::uniform_int_distribution<> unif(0, sampleCount);
			randomID.insert(unif(randomGen));
		}
		for (Handle<DoubleVector> a : sampleResult) {
			if (randomID.count(myPos)) {

				// Sample without replacement
				bool notNew = true;
				for (int i = 0; i < myk; i++) {
					for (int j = 0; j < dim; j++)
						notNew = (model[i][j] == a->getDouble(j)) && notNew;
					if (notNew) {
						break;
					}
					else {
						if (i != myk - 1)
							notNew = true;
					}
				}

				if (!notNew) {
					std :: cout << "The sample we got is: " << std :: endl;
					for (int i = 0; i < dim; i++) {
						model[myk][i] = a->getDouble(i);
						myk++;
						std :: cout << model[myk][i] << ", ";
					}
					std :: cout << std :: endl;
				}
			}
			myPos++;
                }
	    }

	
	    std :: cout << "After this sampling, we will need another " << (myk >= k? 0 : k-myk) << " samples." << std :: endl;

	    temp.clearSet("kmeans_db", "kmeans_initial_model_set", "pdb::DoubleVector", errMsg);

	    std :: cout << std :: endl;
    }
    
  

    // Start k-means iterations
    // QueryClient myClient (8108, "localhost", clientLogger, true);

    for (int n = 0; n <= iter; n++) {


                const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 24};

		std :: cout << "*****************************************" << std :: endl;
		std :: cout << "I am in iteration : " << n << std :: endl;
		std :: cout << "*****************************************" << std :: endl;


    		pdb::Handle<pdb::Vector<Handle<DoubleVector>>> my_model = pdb::makeObject<pdb::Vector<Handle<DoubleVector>>> ();

		for (int i = 0; i < k; i++) {
			Handle<DoubleVector> tmp = pdb::makeObject<DoubleVector>(dim);
			my_model->push_back(tmp);
			for (int j = 0; j < dim; j++) {
				(*my_model)[i]->setDouble(j, model[i][j]);
			}
		}

                std :: cout << "The std model I have is: " << std :: endl;
                for (int i = 0; i < k; i++) {
                     for (int j = 0; j < dim; j++) {
                         std :: cout << "model[" << i << "][" << j << "]=" << model[i][j] << std :: endl;
                     }
                }

	    	std :: cout << "The model I have is: " << std :: endl;
		for (int i = 0; i < k; i++) {
			(*my_model)[i]->print();
	    	}		    


		
    		Handle<Computation> myScanSet = makeObject<ScanDoubleVectorSet>("kmeans_db", "kmeans_input_set");
		Handle<Computation> myQuery = makeObject<KMeansAggregate>(my_model);
		myQuery->setInput(myScanSet);
                myQuery->setAllocatorPolicy(noReuseAllocator);
                myQuery->setOutput("kmeans_db", "kmeans_output_set");
		//Handle<Computation> myWriteSet = makeObject<WriteKMeansSet>("kmeans_db", "kmeans_output_set");
		//myWriteSet->setAllocatorPolicy(noReuseAllocator);
		//myWriteSet->setInput(myQuery);


		auto begin = std :: chrono :: high_resolution_clock :: now();

		if (!myClient.executeComputations(errMsg, myQuery)) {
			std :: cout << "Query failed. Message was: " << errMsg << "\n";
			return 1;
		}

		std :: cout << "The query is executed successfully!" << std :: endl;

		// update the model
		SetIterator <KMeansAggregateOutputType> result = myClient.getSetIterator <KMeansAggregateOutputType> ("kmeans_db", "kmeans_output_set");
		kk = 0;
		for (Handle<KMeansAggregateOutputType> a : result) {
			if (kk >= k)
					break;
			std :: cout << "The cluster index I got is " << (*a).getKey() << std :: endl;
			std :: cout << "The cluster count sum I got is " << (*a).getValue().getCount() << std :: endl;
			std :: cout << "The cluster mean sum I got is " << std :: endl;
			(*a).getValue().getMean().print();
	//		(*model)[kk] = (*a).getValue().getMean() / (*a).getValue().getCount();
			DoubleVector tmpModel = (*a).getValue().getMean() / (*a).getValue().getCount();
			for (int i = 0; i < dim; i++) {
				model[kk][i] = tmpModel.getDouble(i);
			}
			std :: cout << "I am updating the model in position: " << kk << std :: endl;
			for(int i = 0; i < dim; i++)
  				std::cout << i << ": " << model[kk][i] << ' ';
			std :: cout << std :: endl;
			std :: cout << std :: endl;
	//		(*model)[kk].print();
			kk++;
		}
		if (kk < k) {
			std :: cout << "These clusters do not have data: "  << std :: endl;
			for (int i = kk; i < k; i++) {
				std :: cout << i << ", ";
				for (int j = 0; j < dim; j++) {
				//	(*model)[kk].setDouble(j, 0.00001);
					double bias = ((double) rand() / (RAND_MAX));
					model[kk][j] = 5 + bias;
				}
			}
		}
		std :: cout << std :: endl;
		std :: cout << std :: endl;

		temp.clearSet("kmeans_db", "kmeans_output_set", "pdb::KMeansAggregateOutputType", errMsg);

		auto end = std::chrono::high_resolution_clock::now();
		std::cout << "Time Duration: " <<
		std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;
    }

    std::cout << std::endl;

    //QueryClient myClient (8108, "localhost", clientLogger, true);

	// print the resuts
    if (printResult == true) {
        std :: cout << "to print result..." << std :: endl;

	/*
        SetIterator <DoubleVector> input = myClient.getSetIterator <DoubleVector> ("kmeans_db", "kmeans_input_set");
        std :: cout << "Query input: "<< std :: endl;
        int countIn = 0;
        for (auto a : input) {
            countIn ++;
            std :: cout << countIn << ":";
            a->print();
            std :: cout << std::endl;
        }
	*/

        
        SetIterator <KMeansAggregateOutputType> result = myClient.getSetIterator <KMeansAggregateOutputType> ("kmeans_db", "kmeans_output_set");
        std :: cout << "K-means results: "<< std :: endl;
       // int countOut = 0;
        for (Handle<KMeansAggregateOutputType> a : result) {
       //     countOut ++;
            std :: cout << a->getKey() << ":";
	    DoubleVector tmp = a->getValue().getMean() / a->getValue().getCount();
            tmp.print();
            
            std :: cout << std::endl;
        }
        //std :: cout << "K-means output count:" << countOut << "\n";
       
    }


    if (clusterMode == false) {
	    // and delete the sets
        myClient.deleteSet ("kmeans_db", "kmeans_output_set");
        myClient.deleteSet ("kmeans_db", "kmeans_initial_model_set");
        myClient.deleteSet ("kmeans_db", "kmeans_data_count_set");
    } else {
        if (!temp.removeSet ("kmeans_db", "kmeans_output_set", errMsg)) {
            cout << "Not able to remove set: " + errMsg;
            exit (-1);
        }
        else if (!temp.removeSet ("kmeans_db", "kmeans_initial_model_set", errMsg)) {
            cout << "Not able to remove set: " + errMsg;
            exit (-1);
        }
        else if (!temp.removeSet ("kmeans_db", "kmeans_data_count_set", errMsg)) {
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
