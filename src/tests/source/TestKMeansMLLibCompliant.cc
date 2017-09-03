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
#ifndef TEST_KMEANS_MLLIB_COMPLIANT_CC
#define TEST_KMEANS_MLLIB_COMPLIANT_CC

// JiaNote: this file is based on functions developed by Shangyu, but just a bit refactored to be fully compliant with MLLib
// K-means clustering;

#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"

#include "Sampler.h"

#include "WriteKMeansSet.h"
#include "KMeansAggregate.h"
#include "KMeansDataCountAggregate.h"
#include "ScanDoubleVectorSet.h"
#include "ScanDoubleArraySet.h"
#include "KMeansAggregateOutputType.h"
#include "KMeansCentroid.h"
#include "KMeansDataCountAggregate.h"
#include "KMeansSampleSelection.h"
#include "KMeansNormVectorMap.h"
#include "WriteDoubleVectorSet.h"

#include "DispatcherClient.h"
#include "Set.h"
#include "DataTypes.h"
#include "DoubleVector.h"
#include "SumResult.h"
#include "WriteSumResultSet.h"

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
#include <vector>

#define COUT std::cout
//#define COUT term

using namespace pdb;

#ifndef NUM_KMEANS_DIMENTIONS
    #define NUM_KMEANS_DIMENSIONS 100
#endif


int main (int argc, char * argv[]) {
    bool printResult = true;
    bool clusterMode = false;
//    freopen("output.txt","w",stdout);
    freopen("/dev/tty","w",stdout);

//    std::ofstream out("output.txt");
//    std::streambuf *COUTbuf = COUT.rdbuf(); //save old buf
//    COUT.rdbuf(out.rdbuf()); //redirect std::COUT to out.txt!

//    std::ofstream term("/dev/tty", std::ios_base::out);

    const std::string red("\033[0;31m");
    const std::string green("\033[1;32m");
    const std::string yellow("\033[1;33m");
    const std::string blue("\033[1;34m");
    const std::string cyan("\033[0;36m");
    const std::string magenta("\033[0;35m");
    const std::string reset("\033[0m");


    COUT << "Usage: #printResult[Y/N] #clusterMode[Y/N] #dataSize[MB] #masterIp #addData[Y/N]" << std :: endl;        
    if (argc > 1) {
        if (strcmp(argv[1],"N") == 0) {
            printResult = false;
            COUT << "You successfully disabled printing result." << std::endl;
        }else {
            printResult = true;
            COUT << "Will print result." << std :: endl;
        }
    } else {
        COUT << "Will print result. If you don't want to print result, you can add N as the first parameter to disable result printing." << std :: endl;
    }

    if (argc > 2) {
        if (strcmp(argv[2],"Y") == 0) {
            clusterMode = true;
            COUT << "You successfully set the test to run on cluster." << std :: endl;
        } else {
            clusterMode = false;
        }
    } else {
        COUT << "Will run on local node. If you want to run on cluster, you can add any character as the second parameter to run on the cluster configured by $PDB_HOME/conf/serverlist." << std :: endl;
    }

    int numOfMb = 64; //by default we add 64MB data
    if (argc > 3) {
        numOfMb = atoi(argv[3]);
    }
    numOfMb = 64; //Force it to be 64 by now.


    COUT << "To add data with size: " << numOfMb << "MB" << std :: endl;

    std :: string masterIp = "localhost";
    if (argc > 4) {
        masterIp = argv[4];
    }
    COUT << "Master IP Address is " << masterIp << std :: endl;

    bool whetherToAddData = true;
    if (argc > 5) {
        if (strcmp(argv[5],"N") == 0) {
            whetherToAddData = false;
        }
    }


    COUT << blue << std :: endl;
    COUT << "*****************************************" << std :: endl;
    COUT << "K-means starts : " << std :: endl;
    COUT << "*****************************************" << std :: endl;
    COUT << reset << std :: endl;

    
    COUT << "The K-means paramers are: " << std :: endl;
    COUT << std :: endl;

    int iter = 1;
    int k = 3;
    int dim = 3;
    int numData = 10;
    bool addDataFromFile = true;

    if (argc > 6) {
	iter = std::stoi(argv[6]);
    }
    COUT << "The number of iterations: " << iter << std :: endl;

    if (argc > 7) {
	k = std::stoi(argv[7]);
    }
    COUT << "The number of clusters: " << k << std :: endl;

    /*
    if (argc > 8) {
	numData = std::stoi(argv[8]);
    }
    COUT << "The number of data points: " << numData << std :: endl;
    */

    if (argc > 8) {
	dim = std::stoi(argv[8]);
    }
    COUT << "The dimension of each data point: " << dim << std :: endl;

    std :: string fileName = "/mnt/data_generator_kmeans/gaussian_pdb/kmeans_data";
    if (argc > 9) {
        fileName = argv[9];
    }
    COUT << "Input file: " << fileName << std :: endl;
    COUT << std :: endl;


    pdb :: PDBLoggerPtr clientLogger = make_shared<pdb :: PDBLogger>("clientLog");

    pdb :: DistributedStorageManagerClient temp (8108, masterIp, clientLogger);

    pdb :: CatalogClient catalogClient (8108, masterIp, clientLogger);

    string errMsg;

    // Some meta data
    pdb :: makeObjectAllocatorBlock(1 * 1024 * 1024, true);
//    pdb::Handle<pdb::Vector<DoubleVector>> tmpModel = pdb::makeObject<pdb::Vector<DoubleVector>> (k, k);
    std :: vector<double> avgData(dim, 0);
    int dataCount = 0;
    int myk = 0;
    int kk = 0;
    bool ifFirst = true;
//    srand(time(0));
    // For the random number generator
    std::random_device rd;
    std::mt19937 randomGen(rd());

    if ((numOfMb > 0) && (whetherToAddData == false)) {
        //Step 1. Create Database and Set
        // now, register a type for user data
        //TODO: once sharedLibrary is supported, add this line back!!!
        catalogClient.registerType ("libraries/libKMeansAggregateOutputType.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansCentroid.so", errMsg);
        catalogClient.registerType ("libraries/libWriteSumResultSet.so", errMsg);
        // register this query class
        catalogClient.registerType ("libraries/libKMeansAggregate.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansDataCountAggregate.so", errMsg);
        catalogClient.registerType ("libraries/libScanDoubleVectorSet.so", errMsg);
        catalogClient.registerType ("libraries/libScanDoubleArraySet.so", errMsg);
        catalogClient.registerType ("libraries/libWriteKMeansSet.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansDataCountAggregate.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansSampleSelection.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansNormVectorMap.so", errMsg);
        catalogClient.registerType ("libraries/libWriteDoubleVectorSet.so", errMsg);
        // now, create a new database
        if (!temp.createDatabase ("kmeans_db", errMsg)) {
            COUT << "Not able to create database: " + errMsg;
            exit (-1);
        } else {
            COUT << "Created database.\n";
        }

        // now, create a new set in that database
        if (!temp.createSet<double[]> ("kmeans_db", "kmeans_input_set", errMsg)) {
            COUT << "Not able to create set: " + errMsg;
            exit (-1);
        } else {
            COUT << "Created set.\n";
        }
    }

    if (whetherToAddData == true) {
        //Step 1. Create Database and Set
        // now, register a type for user data
        //TODO: once sharedLibrary is supported, add this line back!!!
        catalogClient.registerType ("libraries/libKMeansAggregateOutputType.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansCentroid.so", errMsg);
	catalogClient.registerType ("libraries/libWriteSumResultSet.so", errMsg);
        // register this query class
        catalogClient.registerType ("libraries/libKMeansAggregate.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansDataCountAggregate.so", errMsg);
        catalogClient.registerType ("libraries/libScanDoubleVectorSet.so", errMsg);
        catalogClient.registerType ("libraries/libScanDoubleArraySet.so", errMsg);
        catalogClient.registerType ("libraries/libWriteKMeansSet.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansDataCountAggregate.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansSampleSelection.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansNormVectorMap.so", errMsg);
        catalogClient.registerType ("libraries/libWriteDoubleVectorSet.so", errMsg);
        // now, create a new database
        if (!temp.createDatabase ("kmeans_db", errMsg)) {
            COUT << "Not able to create database: " + errMsg;
            exit (-1);
        } else {
            COUT << "Created database.\n";
        }

        // now, create a new set in that database
        if (!temp.createSet<double[NUM_KMEANS_DIMENSIONS]> ("kmeans_db", "kmeans_input_set", errMsg)) {
            COUT << "Not able to create set: " + errMsg;
            exit (-1);
        } else {
            COUT << "Created set.\n";
        }


        //Step 2. Add data
        DispatcherClient dispatcherClient = DispatcherClient(8108, masterIp, clientLogger);


        if (numOfMb >= 0) {
		if (addDataFromFile) {
			int blockSize = 256;
			//std :: ifstream inFile("/mnt/data_generator_kmeans/gaussian_pdb/kmeans_data");
                        std ::ifstream inFile(fileName.c_str());
			std :: string line;
			bool rollback = false;
			bool end = false;

			while(!end) {
				pdb :: makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
				pdb::Handle<pdb::Vector<pdb :: Handle<double[NUM_KMEANS_DIMENSIONS]>>> storeMe = pdb::makeObject<pdb::Vector<pdb::Handle<double[NUM_KMEANS_DIMENSIONS]>>> ();
				try {
					
				     while(1){       
						if (!rollback){
						//      std::istringstream iss(line);
							if(!std::getline(inFile, line)){
								end = true;
								break;
							}
							else {
								pdb :: Handle<double [NUM_KMEANS_DIMENSIONS]> myData =
                                                                     pdb :: makeObject<double [NUM_KMEANS_DIMENSIONS]>();
								std::stringstream  lineStream(line);
								double value;
                                                                int index = 0;
                                                                double * myRawData = *myData;
								while(lineStream >> value)
								{
                                                                    myRawData[index] = value;
                                                                    index++;
								}
                                                                storeMe->push_back (myData);
							}
						}
						else    {   
							rollback = false;
						        pdb :: Handle <double[NUM_KMEANS_DIMENSIONS]> myData = pdb::makeObject<double[NUM_KMEANS_DIMENSIONS]>();
                                                        std :: stringstream lineStream(line);
                                                        double value;
                                                        int index = 0;
                                                        double * myRawData = *myData;
                                                        while(lineStream >> value)
                                                        {
                                                              myRawData[index] = value;
                                                              index++;
                                                        }
						        storeMe->push_back (myData);
						}
		                    }
					
				    end = true;

				    if (!dispatcherClient.sendData<double[NUM_KMEANS_DIMENSIONS]>(std::pair<std::string, std::string>("kmeans_input_set", "kmeans_db"), storeMe, errMsg)) {
					COUT << "Failed to send data to dispatcher server" << std :: endl;
					return -1;
				    }
			            std::cout << "Dispatched " << storeMe->size() << " data in the last patch!" << std::endl;
				    temp.flushData( errMsg );
				} catch (pdb :: NotEnoughSpace &n) {
				    if (!dispatcherClient.sendData<double[NUM_KMEANS_DIMENSIONS]>(std::pair<std::string, std::string>("kmeans_input_set", "kmeans_db"), storeMe, errMsg)) {
					COUT << "Failed to send data to dispatcher server" << std :: endl;
					return -1;
				    }
				    std::cout << "Dispatched " << storeMe->size() << " data when allocated block is full!" << std::endl;
				    rollback = true;
				}
				PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std :: endl;

			} // while not end
			inFile.close();

		}
	}
    }
    // now, create a new set in that database to store output data

    PDB_COUT << "to create a new set to store the norm vectors" << std :: endl;
    if (!temp.createSet<DoubleVector> ("kmeans_db", "kmeans_norm_vector_set", errMsg)) {
        COUT << "Not able to create set: " + errMsg;
        exit (-1);
    } else { 
        COUT << "Created set kmeans_norm_vector_set.\n";
    }


    PDB_COUT << "to create a new set to store the data count" << std :: endl;
    if (!temp.createSet<SumResult> ("kmeans_db", "kmeans_data_count_set", errMsg)) {
        COUT << "Not able to create set: " + errMsg;
        exit (-1);
    } else {
        COUT << "Created set kmeans_data_count_set.\n";
    }


    PDB_COUT << "to create a new set to store the initial model" << std :: endl;
    if (!temp.createSet<DoubleVector> ("kmeans_db", "kmeans_initial_model_set", errMsg)) {
        COUT << "Not able to create set: " + errMsg;
        exit (-1);
    } else {
        COUT << "Created set kmeans_initial_model_set.\n";
    }

    
    PDB_COUT << "to create a new set for storing output data" << std :: endl;
    if (!temp.createSet<KMeansAggregateOutputType> ("kmeans_db", "kmeans_output_set", errMsg)) {
        COUT << "Not able to create set: " + errMsg;
        exit (-1);
    } else {
        COUT << "Created set kmeans_output_set.\n";
    }

    //Step 3. To execute a Query
	// for allocations


	// connect to the query client
    QueryClient myClient (8108, "localhost", clientLogger, true);
    
    auto iniBegin = std :: chrono :: high_resolution_clock :: now();

    // Initialization for the model in the KMeansAggregate
       
    // Calculate the count of input data points 
    
    
    Handle<Computation> myInitialScanSet = makeObject<ScanDoubleArraySet>("kmeans_db", "kmeans_input_set");
    Handle<Computation> myNormVectorMap = makeObject<KMeansNormVectorMap>();
    myNormVectorMap->setInput(myInitialScanSet);
    Handle<Computation> myNormVectorWriter = makeObject<WriteDoubleVectorSet> ("kmeans_db", "kmeans_norm_vector_set");
    myNormVectorWriter->setInput(myNormVectorMap);

    if (!myClient.executeComputations(errMsg, myNormVectorWriter)) {
        COUT << "Query failed. Message was: " << errMsg << "\n";
        return 1;
    }
    auto iniNormEnd = std :: chrono :: high_resolution_clock :: now();
    
    Handle<Computation> myScanSet = makeObject<ScanDoubleVectorSet>("kmeans_db", "kmeans_norm_vector_set");
    Handle<Computation> myDataCount = makeObject<KMeansDataCountAggregate>();
    myDataCount->setInput(myScanSet);
    Handle <Computation> myWriter = makeObject<WriteSumResultSet>("kmeans_db", "kmeans_data_count_set");
    myWriter->setInput(myDataCount);

    if (!myClient.executeComputations(errMsg, myWriter)) {
        COUT << "Query failed. Message was: " << errMsg << "\n";
        return 1;
    }
    SetIterator <SumResult> dataCountResult = myClient.getSetIterator <SumResult> ("kmeans_db", "kmeans_data_count_set");
    for (Handle<SumResult> a : dataCountResult) {
	
	dataCount = a->getTotal();
    }
        
//    dataCount = 10;
    COUT << "The number of data points: " << dataCount << std :: endl;
    COUT << std :: endl;
    
    // Randomly sample k data points from the input data through Bernoulli sampling
    // We guarantee the sampled size >= k in 99.99% of the time
    const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};
    srand(time(NULL));
    double fraction = Sampler::computeFractionForSampleSize (k, dataCount, false);
    COUT << "The sample threshold is: " << fraction << std :: endl;
    int initialCount = dataCount;
   
    Vector <Handle<DoubleVector>> mySamples;
    while(mySamples.size() < k) {
            std :: cout << "Needed to sample due to insufficient sample size." << std :: endl;	
	    Handle<Computation> mySampleScanSet = makeObject<ScanDoubleVectorSet>("kmeans_db", "kmeans_norm_vector_set");
    	    Handle<Computation> myDataSample = makeObject<KMeansSampleSelection>(fraction);
    	    myDataSample->setInput(mySampleScanSet);
	    Handle<Computation> myWriteSet = makeObject<WriteDoubleVectorSet>("kmeans_db", "kmeans_initial_model_set");
	    myWriteSet->setInput(myDataSample);
	 
	    if (!myClient.executeComputations(errMsg, myWriteSet)) {
		COUT << "Query failed. Message was: " << errMsg << "\n";
		return 1;
	    }
	    SetIterator <DoubleVector> sampleResult = myClient.getSetIterator <DoubleVector> ("kmeans_db", "kmeans_initial_model_set");
               
	    for (Handle<DoubleVector> a : sampleResult) {
                 //std :: cout << "received 1 sample" << std :: endl;
                 //a->print();
                 //std :: cout << std :: endl;
                 Handle<DoubleVector> myDoubles = makeObject<DoubleVector>(dim);
                 double * rawData = a->getRawData();
                 double * myRawData = myDoubles->getRawData();
                 for (int i = 0; i < dim; i++) {
                     myRawData[i] = rawData[i];
                 }
                 mySamples.push_back(myDoubles);
	    }
            std :: cout << "Now we have " << mySamples.size() << " samples" << std :: endl;
	    temp.clearSet("kmeans_db", "kmeans_initial_model_set", "pdb::DoubleVector", errMsg);
    }
    Sampler::randomizeInPlace(mySamples);
    //take k samples
    mySamples.resize(k);
    //distinct
    Vector <Handle<DoubleVector>> myDistinctSamples;
    for (size_t i = 0; i < k; i++) {
        //std :: cout << "the " << i << "-th element" << std :: endl;
        //(mySamples[i])->print();
        std :: cout << std :: endl;
        int j;
        for (j = i+1; j < k; j++) {
                if (((mySamples[i])->equals(mySamples[j]))) {
                    //std :: cout << "it equals with the " << j << "-th element" << std :: endl;
                    break;
                }
        }
        if ((mySamples.size() > 0) && (j == k)) {
            myDistinctSamples.push_back(mySamples[i]);
        }
    }                        

    k = myDistinctSamples.size(); 
    std :: cout << "There are " << k << "distinct clusters" << std :: endl;
    std :: vector< std :: vector <double> > model(k, vector<double>(dim));
    for (int i = 0; i < k; i++) {
        Handle<DoubleVector> tmp = myDistinctSamples[i];
        COUT << "The " << i << "-th sample is:" << endl;
        double * rawData = tmp->getRawData();
        for (int j = 0; j < dim; j++) {
                model[i][j] = rawData[j];
                COUT << model[i][j] << " ";
        }
        COUT << std :: endl;
     }  
     COUT << std :: endl;
   

    auto iniEnd = std :: chrono :: high_resolution_clock :: now();
  
    
    // Start k-means iterations
    // QueryClient myClient (8108, "localhost", clientLogger, true);

    for (int n = 0; n < iter; n++) {

                auto  iterBegin = std :: chrono :: high_resolution_clock :: now();
                const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 24};

		COUT << "*****************************************" << std :: endl;
		COUT << "I am in iteration : " << n << std :: endl;
		COUT << "*****************************************" << std :: endl;


    		pdb::Handle<pdb::Vector<Handle<DoubleVector>>> my_model = pdb::makeObject<pdb::Vector<Handle<DoubleVector>>> ();

		for (int i = 0; i < k; i++) {
			Handle<DoubleVector> tmp = pdb::makeObject<DoubleVector>(dim);
			my_model->push_back(tmp);
                        //JiaNote: use raw C++ data directly
                        double * rawData = (*my_model)[i]->getRawData();
			for (int j = 0; j < dim; j++) {
				rawData[j] = model[i][j];
			}
		}

    		Handle<Computation> myScanSet = makeObject<ScanDoubleVectorSet>("kmeans_db", "kmeans_norm_vector_set");
                //Handle<Computation> myScanSet = makeObject<ScanDoubleVectorSet>("kmeans_db", "kmeans_input_set");
		Handle<Computation> myQuery = makeObject<KMeansAggregate>(my_model);
		myQuery->setInput(myScanSet);
                //myQuery->setAllocatorPolicy(noReuseAllocator);
                myQuery->setOutput("kmeans_db", "kmeans_output_set");

		auto begin = std :: chrono :: high_resolution_clock :: now();

		if (!myClient.executeComputations(errMsg, myQuery)) {
			COUT << "Query failed. Message was: " << errMsg << "\n";
			return 1;
		}

		auto end = std::chrono::high_resolution_clock::now();
		COUT << "The query is executed successfully!" << std :: endl;

		// update the model
		SetIterator <KMeansAggregateOutputType> result = myClient.getSetIterator <KMeansAggregateOutputType> ("kmeans_db", "kmeans_output_set");
		kk = 0;

		if (ifFirst) {
			for (Handle<KMeansAggregateOutputType> a : result) {
				if (kk >= k)
						break;
                                
				COUT << "The cluster index I got is " << (*a).getKey() << std :: endl;
                                size_t count = (*a).getValue().getCount();
				COUT << "The cluster count sum I got is " << count << std :: endl;
				//COUT << "The cluster mean sum I got is " << std :: endl;
                                //JiaNote: use reference                                
                                DoubleVector & meanVec = (*a).getValue().getMean();
				//meanVec.print();
				DoubleVector tmpModel = meanVec /count;
                                //JiaNote: use rawData
                                double * rawData = tmpModel.getRawData();
				for (int i = 0; i < dim; i++) {
					model[kk][i] = rawData[i];
				}
                                rawData = meanVec.getRawData();
				for (int i = 0; i < dim; i++) {
					avgData[i] += rawData[i];
				}

				COUT << "I am updating the model in position: " << kk << std :: endl;
				for(int i = 0; i < dim; i++)
					COUT << i << ": " << model[kk][i] << ' ';
				COUT << std :: endl;
				COUT << std :: endl;
				kk++;
			}
			for (int i = 0; i < dim; i++) {
				avgData[i] = avgData[i] / dataCount;
			}
			/*
			COUT << "The average of data points is : \n";
			for (int i = 0; i < dim; i++)
				COUT << i << ": " << avgData[i] << ' ';
			COUT << std :: endl;
			COUT << std :: endl;
			*/
			ifFirst = false;
		}
		else {
			for (Handle<KMeansAggregateOutputType> a : result) {
				if (kk >= k)
						break;
				COUT << "The cluster index I got is " << (*a).getKey() << std :: endl;
                                size_t count = (*a).getValue().getCount();
				COUT << "The cluster count sum I got is " << count << std :: endl;
				//COUT << "The cluster mean sum I got is " << std :: endl;
				//(*a).getValue().getMean().print();
		//		(*model)[kk] = (*a).getValue().getMean() / (*a).getValue().getCount();
                                DoubleVector & meanVec = (*a).getValue().getMean();
				DoubleVector tmpModel = meanVec / count;
                                //JiaNote: using raw C++ data
                                double * rawData = tmpModel.getRawData();
				for (int i = 0; i < dim; i++) {
					model[kk][i] = rawData[i];
				}
				/*COUT << "I am updating the model in position: " << kk << std :: endl;
				for(int i = 0; i < dim; i++)
					COUT << i << ": " << model[kk][i] << ' ';
				COUT << std :: endl;
				COUT << std :: endl;
                                */
				kk++;
			}
		}
		if (kk < k) {
			COUT << "These clusters do not have data: "  << std :: endl;
			for (int i = kk; i < k; i++) {
				COUT << i << ", ";
				for (int j = 0; j < dim; j++) {
					double bias = ((double) rand() / (RAND_MAX));
					model[i][j] = avgData[j] + bias;
				}
			}
		}
		COUT << std :: endl;
		COUT << std :: endl;

		temp.clearSet("kmeans_db", "kmeans_output_set", "pdb::KMeansAggregateOutputType", errMsg);
                auto iterEnd = std :: chrono :: high_resolution_clock :: now();
		COUT << "Server-side Time Duration for Iteration-: " << n << " is:" <<
		std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;
                COUT << "Total Time Duration for Iteration-: " << n << " is:" <<
                std::chrono::duration_cast<std::chrono::duration<float>>(iterEnd-iterBegin).count() << " secs." << std::endl;
    }

    auto allEnd = std :: chrono :: high_resolution_clock :: now();

    COUT << std::endl;

    // print the resuts
    if (printResult == true) {
        
        SetIterator <KMeansAggregateOutputType> result = myClient.getSetIterator <KMeansAggregateOutputType> ("kmeans_db", "kmeans_output_set");


	COUT << std :: endl;
	COUT << blue << "*****************************************" << reset << std :: endl;
	COUT << blue << "K-means resultss : " << reset << std :: endl;
	COUT << blue << "*****************************************" << reset << std :: endl;
	COUT << std :: endl;

//                COUT << "The std model I have is: " << std :: endl;
	for (int i = 0; i < k; i++) {
	     COUT << "Cluster index: " << i << std::endl;
	     for (int j = 0; j < dim - 1; j++) {
		 COUT << blue << model[i][j] << ", " << reset;
	     }
		 COUT << blue << model[i][dim - 1] << reset << std :: endl;
	}

    }


    COUT << std :: endl;
    COUT << "Norm Vector Map Time Duration: " <<
                std::chrono::duration_cast<std::chrono::duration<float>>(iniNormEnd-iniBegin).count() << " secs." << std::endl;
    COUT << "Sampling Time Duration: " <<
                std::chrono::duration_cast<std::chrono::duration<float>>(iniEnd-iniNormEnd).count() << " secs." << std::endl;
    COUT << "Total Processing Time Duration: " <<
                std::chrono::duration_cast<std::chrono::duration<float>>(allEnd-iniEnd).count() << " secs." << std::endl;
    if (clusterMode == false) {
	    // and delete the sets
        myClient.deleteSet ("kmeans_db", "kmeans_output_set");
        myClient.deleteSet ("kmeans_db", "kmeans_initial_model_set");
        myClient.deleteSet ("kmeans_db", "kmeans_data_count_set");
        myClient.deleteSet ("kmeans_db", "kmeans_norm_vector_set");

    } else {
        if (!temp.removeSet ("kmeans_db", "kmeans_output_set", errMsg)) {
            COUT << "Not able to remove set: " + errMsg;
            exit (-1);
        }
        else if (!temp.removeSet ("kmeans_db", "kmeans_initial_model_set", errMsg)) {
            COUT << "Not able to remove set: " + errMsg;
            exit (-1);
        }
        else if (!temp.removeSet ("kmeans_db", "kmeans_data_count_set", errMsg)) {
            COUT << "Not able to remove set: " + errMsg;
            exit (-1);
        }
        else if (!temp.removeSet ("kmeans_db", "kmeans_norm_vector_set", errMsg)) {
            COUT << "Not able to remove set: " + errMsg;
            exit (-1);
        }
	else {
            COUT << "Removed set.\n";
        }
    }
    int code = system ("scripts/cleanupSoFiles.sh");
    if (code < 0) {
        COUT << "Can't cleanup so files" << std :: endl;
    }
//    COUT << "Time Duration: " <<
//    std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;
}

#endif
