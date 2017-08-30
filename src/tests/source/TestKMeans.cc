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


// By Shangyu, May 2017
// K-means clustering;

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


    if (whetherToAddData == true) {
        //Step 1. Create Database and Set
        // now, register a type for user data
        //TODO: once sharedLibrary is supported, add this line back!!!
        catalogClient.registerType ("libraries/libKMeansAggregateOutputType.so", errMsg);
        catalogClient.registerType ("libraries/libKMeansCentroid.so", errMsg);
	catalogClient.registerType ("libraries/libWriteSumResultSet.so", errMsg);

        // now, create a new database
        if (!temp.createDatabase ("kmeans_db", errMsg)) {
            COUT << "Not able to create database: " + errMsg;
            exit (-1);
        } else {
            COUT << "Created database.\n";
        }

        // now, create a new set in that database
        if (!temp.createSet<DoubleVector> ("kmeans_db", "kmeans_input_set", errMsg)) {
            COUT << "Not able to create set: " + errMsg;
            exit (-1);
        } else {
            COUT << "Created set.\n";
        }


        //Step 2. Add data
        DispatcherClient dispatcherClient = DispatcherClient(8108, masterIp, clientLogger);


        if (numOfMb > 0) {
		if (addDataFromFile) {
			int blockSize = 64;
			std :: ifstream inFile("/home/ubuntu/kmeans_data");
			std :: string line;
			std::vector<double>   lineData;
			bool rollback = false;
			bool end = false;

			while(!end) {
				pdb :: makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
				pdb::Handle<pdb::Vector<pdb::Handle<DoubleVector>>> storeMe = pdb::makeObject<pdb::Vector<pdb::Handle<DoubleVector>>> ();
				try {
					
					while(1){       
						if (!rollback){
						//      std::istringstream iss(line);
							if(!std::getline(inFile, line)){
								end = true;
								break;
							}
							else {
								lineData.clear();
								std::stringstream  lineStream(line);
								double value;
								while(lineStream >> value)
								{

								    lineData.push_back(value);
								}
							}
						}
						else    
							rollback = false;
						pdb :: Handle <DoubleVector> myData = pdb::makeObject<DoubleVector>(dim);
				//		std::vector<double>::iterator it; 
				//		for(it = lineData.begin() ; it < lineData.end(); it++) {
						for(int i = 0; i < lineData.size(); ++i) {
							myData->setDouble(i, lineData[i]);
						//	myData->data->push_back(*it);

						}
						storeMe->push_back (myData);
						//COUT << "first number: " << myData->getDouble(0) << endl;
					}
					
				    /*COUT << "input data: " << std :: endl;
				    for (int i=0; i<storeMe->size();i++){
					(*storeMe)[i]->print();
				    }*/
				    end = true;

				    if (!dispatcherClient.sendData<DoubleVector>(std::pair<std::string, std::string>("kmeans_input_set", "kmeans_db"), storeMe, errMsg)) {
					COUT << "Failed to send data to dispatcher server" << std :: endl;
					return -1;
				    }
					std::cout << "Dispatched " << storeMe->size() << " data in the last patch!" << std::endl;
					temp.flushData( errMsg );
				} catch (pdb :: NotEnoughSpace &n) {
				    if (!dispatcherClient.sendData<DoubleVector>(std::pair<std::string, std::string>("kmeans_input_set", "kmeans_db"), storeMe, errMsg)) {
					COUT << "Failed to send data to dispatcher server" << std :: endl;
					return -1;
				    }
				    std::cout << "Dispatched " << storeMe->size() << " data when allocated block is full!" << std::endl;
				    rollback = false;
				}
				PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std :: endl;

			} // while not end
		//            }
			inFile.close();

		    //to write back all buffered records        
		  //  temp.flushData( errMsg );
		}
	}
	else {
		int blockSize = 64;
                pdb :: makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                pdb::Handle<pdb::Vector<pdb::Handle<DoubleVector>>> storeMe = pdb::makeObject<pdb::Vector<pdb::Handle<DoubleVector>>> ();
                try {

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
                    /*COUT << "input data: " << std :: endl;
                    for (int i=0; i<storeMe->size();i++){
                        (*storeMe)[i]->print();
                    }*/

			if (!dispatcherClient.sendData<DoubleVector>(std::pair<std::string, std::string>("kmeans_input_set", "kmeans_db"), storeMe, errMsg)) {
                        COUT << "Failed to send data to dispatcher server" << std :: endl;
                        return -1;
                    }
                } catch (pdb :: NotEnoughSpace &n) {
                    if (!dispatcherClient.sendData<DoubleVector>(std::pair<std::string, std::string>("kmeans_input_set", "kmeans_db"), storeMe, errMsg)) {
                        COUT << "Failed to send data to dispatcher server" << std :: endl;
                        return -1;
                    }
                }
                PDB_COUT << blockSize << "MB data sent to dispatcher server~~" << std :: endl;
         //   }

            //to write back all buffered records        
            temp.flushData( errMsg );

	}
    }
    // now, create a new set in that database to store output data

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

    auto iniBegin = std :: chrono :: high_resolution_clock :: now();

    std :: vector< std :: vector <double> > model(k, vector<double>(dim));
    for (int i = 0; i < k; i++) {
	for (int j = 0; j < dim; j++) {
       //		model[i][j] = (*tmpModel)[i].getDouble(j);
       		model[i][j] = 0;
	}
    }

    // Initialization for the model in the KMeansAggregate
       
    // Calculate the count of input data points 
    
    Handle<Computation> myInitialScanSet = makeObject<ScanDoubleVectorSet>("kmeans_db", "kmeans_input_set");
    Handle<Computation> myDataCount = makeObject<KMeansDataCountAggregate>();
    myDataCount->setInput(myInitialScanSet);
//    myDataCount->setAllocatorPolicy(noReuseAllocator);
//    myDataCount->setOutput("kmeans_db", "kmeans_data_count_set");
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

    double fraction = k * 1.0 / dataCount;
    double delta = 1e-4;
    double gamma = - log(delta) / dataCount;
    fraction = fmin(1, fraction + gamma + sqrt(gamma * gamma + 2 * gamma * fraction));
    COUT << "The sample threshold is: " << fraction << std :: endl;
    while(myk < k) {
	
	    Handle<Computation> mySampleScanSet = makeObject<ScanDoubleVectorSet>("kmeans_db", "kmeans_input_set");
    	    Handle<Computation> myDataSample = makeObject<KMeansSampleSelection>(fraction);
    	    myDataSample->setInput(mySampleScanSet);
	    Handle<Computation> myWriteSet = makeObject<WriteDoubleVectorSet>("kmeans_db", "kmeans_initial_model_set");
	    myWriteSet->setInput(myDataSample);

	 
	    if (!myClient.executeComputations(errMsg, myWriteSet)) {
		COUT << "Query failed. Message was: " << errMsg << "\n";
		return 1;
	    }
	    SetIterator <DoubleVector> sampleResult = myClient.getSetIterator <DoubleVector> ("kmeans_db", "kmeans_initial_model_set");

	    int sampleCount = 0;
	    for (Handle<DoubleVector> a : sampleResult) {
		sampleCount++;
	    }	    

	    int previous = myk;
	    if (sampleCount <= k - myk) {   // Assign the sampled values to the model directly
		for (Handle<DoubleVector> a : sampleResult) {

			// Sample without replacement
			bool notNew = true;
                        //JiaNote: use raw C++ data
                        double * rawData = a->getRawData();
			for (int i = 0; i < previous; i++) {
				for (int j = 0; j < dim; j++)
					notNew = (model[i][j] == rawData[j]) && notNew;
				if (notNew) {
					break;
				}
				else {
					if (i != myk - 1)
						notNew = true;
				}
			}

			if ( (!notNew) || (previous == 0) ) {
				COUT << "The sample we got is: " << std :: endl;
				for (int i = 0; i < dim; i++) {
					model[myk][i] = rawData[i];
					COUT << model[myk][i] << ", ";
				}
				myk++;
				COUT << std :: endl;
			}
            	} 
	    }
	    else {	// Randomly select (k - myk) samples from sampleResult
		COUT << "We got " << sampleCount << " samples. We will randomly select " << (k-myk) << " samples from them." << std :: endl;
		std :: set<int> randomID;
		int myPos = 0;
		while (randomID.size() < (k-myk)) {
			std::uniform_int_distribution<> unif(0, sampleCount - 1);
			int tempID = unif(randomGen);
			COUT << "The ID for sampling: " << tempID << std :: endl;
			randomID.insert(tempID);
		}
		for (Handle<DoubleVector> a : sampleResult) {
			if (randomID.count(myPos)) {

				COUT << "The ID we got for sampling: " << myPos << std :: endl;

				// Sample without replacement
				bool notNew = true;
                                //JiaNote: use raw C++ data
                                double * rawData = a->getRawData();
				for (int i = 0; i < previous; i++) {
					for (int j = 0; j < dim; j++)
						notNew = (model[i][j] == rawData[j]) && notNew;
					if (notNew) {
						break;
					}
					else {
						if (i != myk - 1)
							notNew = true;
					}
				}

				if ((!notNew) || (previous == 0)) {
					//COUT << "The sample we got is: " << std :: endl;
					for (int i = 0; i < dim; i++) {
						model[myk][i] = rawData[i];
						//COUT << model[myk][i] << ", ";
					}
					myk++;
					//COUT << std :: endl;
				}
			}
			myPos++;
                }
	    }

	
	    COUT << "After this sampling, we will need another " << (myk >= k? 0 : k-myk) << " samples." << std :: endl;

	    temp.clearSet("kmeans_db", "kmeans_initial_model_set", "pdb::DoubleVector", errMsg);

	    COUT << std :: endl;
    }

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

		/*
                COUT << "The std model I have is: " << std :: endl;
                for (int i = 0; i < k; i++) {
                     for (int j = 0; j < dim; j++) {
                         COUT << "model[" << i << "][" << j << "]=" << model[i][j] << std :: endl;
                     }
                }
		*/
		/*
	    	COUT << "The model I have is: " << std :: endl;
		for (int i = 0; i < k; i++) {
			(*my_model)[i]->print();
	    	}
		*/		    

		
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
				COUT << "The cluster count sum I got is " << (*a).getValue().getCount() << std :: endl;
				COUT << "The cluster mean sum I got is " << std :: endl;
                                //JiaNote: use reference                                
                                DoubleVector & meanVec = (*a).getValue().getMean();
				meanVec.print();
				DoubleVector tmpModel = meanVec / (*a).getValue().getCount();
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
				COUT << "The cluster count sum I got is " << (*a).getValue().getCount() << std :: endl;
				COUT << "The cluster mean sum I got is " << std :: endl;
				(*a).getValue().getMean().print();
		//		(*model)[kk] = (*a).getValue().getMean() / (*a).getValue().getCount();
				DoubleVector tmpModel = (*a).getValue().getMean() / (*a).getValue().getCount();
                                //JiaNote: using raw C++ data
                                double * rawData = tmpModel.getRawData();
				for (int i = 0; i < dim; i++) {
					model[kk][i] = rawData[i];
				}
				COUT << "I am updating the model in position: " << kk << std :: endl;
				for(int i = 0; i < dim; i++)
					COUT << i << ": " << model[kk][i] << ' ';
				COUT << std :: endl;
				COUT << std :: endl;
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
/*
    COUT << "Initialization Time Duration: " <<
                std::chrono::duration_cast<std::chrono::duration<float>>(iniEnd-iniBegin).count() << " secs." << std::endl;
    COUT << "Total Processing Time Duration: " <<
                std::chrono::duration_cast<std::chrono::duration<float>>(allEnd-iniEnd).count() << " secs." << std::endl;
*/
    //QueryClient myClient (8108, "localhost", clientLogger, true);

	// print the resuts
    if (printResult == true) {
//        COUT << "to print result..." << std :: endl;

//	COUT << std :: endl;

	/*
        SetIterator <DoubleVector> input = myClient.getSetIterator <DoubleVector> ("kmeans_db", "kmeans_input_set");
        COUT << "Query input: "<< std :: endl;
        int countIn = 0;
        for (auto a : input) {
            countIn ++;
            COUT << countIn << ":";
            a->print();
            COUT << std::endl;
        }
	*/

        
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
    COUT << "Initialization Time Duration: " <<
                std::chrono::duration_cast<std::chrono::duration<float>>(iniEnd-iniBegin).count() << " secs." << std::endl;
    COUT << "Total Processing Time Duration: " <<
                std::chrono::duration_cast<std::chrono::duration<float>>(allEnd-iniEnd).count() << " secs." << std::endl;
    if (clusterMode == false) {
	    // and delete the sets
        myClient.deleteSet ("kmeans_db", "kmeans_output_set");
        myClient.deleteSet ("kmeans_db", "kmeans_initial_model_set");
        myClient.deleteSet ("kmeans_db", "kmeans_data_count_set");
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
