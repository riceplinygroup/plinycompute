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
#ifndef TEST_GMM21
#define TEST_GMM21


// By Tania, August 2017
// Gaussian Mixture Model (based on EM);

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

#include "GmmModel.h"
//#include "GMM/GmmModel.h"
#include "GMM/GmmAggregate.h"
#include "GMM/GmmAggregateOutputType.h"
#include "GMM/GmmNewComp.h"


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
#include <thread>


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

    //***********************************************************************************
	//**** INPUT PARAMETERS ***************************************************************
	//***********************************************************************************


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
    numOfMb = 10; //Force it to be 64 by now.


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
    COUT << "GMM starts : " << std :: endl;
    COUT << "*****************************************" << std :: endl;
    COUT << reset << std :: endl;

    
    COUT << "The GMM paramers are: " << std :: endl;
    COUT << std :: endl;

    int iter = 1;
    int k = 2;
    int dim = 2;
    int numData = 10;
    double tol = 0.001; //Convergence threshold

    if (argc > 6) {
    	iter = std::stoi(argv[6]);
    }
    COUT << "The number of iterations: " << iter << std :: endl;

    if (argc > 7) {
    	k = std::stoi(argv[7]);
    }
    COUT << "The number of clusters: " << k << std :: endl;

    if (argc > 8) {
    	numData = std::stoi(argv[8]);
    }
    COUT << "The number of data points: " << numData << std :: endl;

    if (argc > 9) {
    	dim = std::stoi(argv[9]);
    }
    COUT << "The dimension of each data point: " << dim << std :: endl;

    if (argc > 10) {
        tol = std::stod(argv[10]);
    }
    COUT << "Convergence threshold: " << tol << std :: endl;


    COUT << std :: endl;


    //***********************************************************************************
   	//**** LOAD DATA ********************************************************************
   	//***********************************************************************************


    pdb :: PDBLoggerPtr clientLogger = make_shared<pdb :: PDBLogger>("clientLog");

    pdb :: DistributedStorageManagerClient temp (8108, masterIp, clientLogger);

    pdb :: CatalogClient catalogClient (8108, masterIp, clientLogger);

    string errMsg;

    // Some meta data
    //pdb :: makeObjectAllocatorBlock(1 * 1024 * 1024, true);
//    pdb::Handle<pdb::Vector<DoubleVector>> tmpModel = pdb::makeObject<pdb::Vector<DoubleVector>> (k, k);
    //std :: vector<double> avgData(dim, 0);
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

        // now, create a new database
        if (!temp.createDatabase ("gmm_db", errMsg)) {
            COUT << "Not able to create database: " + errMsg;
            exit (-1);
        } else {
            COUT << "Created database.\n";
        }

        // now, create a new set in that database
        if (!temp.createSet<DoubleVector> ("gmm_db", "gmm_input_set", errMsg)) {
            COUT << "Not able to create set: " + errMsg;
            exit (-1);
        } else {
            COUT << "Created set.\n";
        }


        //Step 2. Add data
        DispatcherClient dispatcherClient = DispatcherClient(8108, masterIp, clientLogger);


        if (numOfMb > 0) {
            int numIterations = numOfMb/64;
            COUT << "Number of MB: " << numOfMb << " Number of Iterations: " << numIterations << std::endl;
            int remainder = numOfMb - 64*numIterations;
            if (remainder > 0) { 
                numIterations = numIterations + 1; 
            }
            for (int num = 0; num < numIterations; num++) {
                COUT << "Iterations: "<< num << std::endl;
                int blockSize = 1024;
                if ((num == numIterations - 1) && (remainder > 0)){
                    blockSize = remainder;
                }
                pdb :: makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
                pdb::Handle<pdb::Vector<pdb::Handle<DoubleVector>>> storeMe = pdb::makeObject<pdb::Vector<pdb::Handle<DoubleVector>>> ();
                try {

                    double bias = 0;

                    for (int i = 0; i < numData; i++) {
                        pdb :: Handle <DoubleVector> myData = pdb::makeObject<DoubleVector>(dim);
                        for (int j = 0; j < dim; j++){

							std::uniform_real_distribution<> unif(0, 1);
							bias = unif(randomGen) * 0.01;
							//bias = unif(randomGen) * 1.0;
										//bias = ((double) rand() / (RAND_MAX)) * 0.01;
										//myData->push_back(tmp);

							/*if (i%2	 == 0){
								myData->setDouble(j, 0 + bias);
							}
							else
								myData->setDouble(j, 10 + bias);*/

							myData->setDouble(j, i%k*3 + bias);
						}
                        storeMe->push_back (myData);
                    }
					COUT << "input data: " << std :: endl;
					/*for (int i=0; i<storeMe->size();i++) {
						//(*storeMe)[i]->print();
						std::cout << "[" << (*storeMe)[i]->getDouble(0) << ", ";
						for (int j=1;j<dim-1; j++)
							std::cout << (*storeMe)[i]->getDouble(j) << ", ";
						std::cout << (*storeMe)[i]->getDouble(dim-1) << "]," << std::endl;

					}*/


                    if (!dispatcherClient.sendData<DoubleVector>(std::pair<std::string, std::string>("gmm_input_set", "gmm_db"), storeMe, errMsg)) {
                        COUT << "Failed to send data to dispatcher server" << std :: endl;
                        return -1;
                    }
                } catch (pdb :: NotEnoughSpace &n) {
                    if (!dispatcherClient.sendData<DoubleVector>(std::pair<std::string, std::string>("gmm_input_set", "gmm_db"), storeMe, errMsg)) {
                        COUT << "Failed to send data to dispatcher server" << std :: endl;
                        return -1;
                    }
                }
                COUT << blockSize << "MB data sent to dispatcher server~~" << std :: endl;
            }

            //to write back all buffered records        
            temp.flushData( errMsg );
        }

    }

    //***********************************************************************************
	//****REGISTER SO ***************************************************************
	//***********************************************************************************


	// register this query class
	catalogClient.registerType ("libraries/libGmmAggregate.so", errMsg);
	catalogClient.registerType ("libraries/libGmmModel.so", errMsg);
	catalogClient.registerType ("libraries/libGmmNewComp.so", errMsg);
	catalogClient.registerType ("libraries/libGmmAggregateOutputType.so", errMsg);


	//I WILL REUSE SOME OF THEM!
	catalogClient.registerType ("libraries/libKMeansDataCountAggregate.so", errMsg);
	catalogClient.registerType ("libraries/libScanDoubleVectorSet.so", errMsg);
	catalogClient.registerType ("libraries/libWriteSumResultSet.so", errMsg);

	/*catalogClient.registerType ("libraries/libWriteKMeansSet.so", errMsg);
	catalogClient.registerType ("libraries/libKMeansSampleSelection.so", errMsg);
	catalogClient.registerType ("libraries/libWriteDoubleVectorSet.so", errMsg);
	catalogClient.registerType ("libraries/libKMeansAggregateOutputType.so", errMsg);
	catalogClient.registerType ("libraries/libKMeansCentroid.so", errMsg);*/


	//***********************************************************************************
	//****CREATE SETS***************************************************************
	//***********************************************************************************


	// now, create a new set in that database to store output data

	/*PDB_COUT << "to create a new set to store the data count" << std :: endl;
	if (!temp.createSet<SumResult> ("gmm_db", "gmm_data_count_set", errMsg)) {
		COUT << "Not able to create set: " + errMsg;
		exit (-1);
	} else {
		COUT << "Created set gmm_data_count_set.\n";
	}*/

	/*
	PDB_COUT << "to create a new set to store the initial model" << std :: endl;
	if (!temp.createSet<DoubleVector> ("gmm_db", "gmm_initial_model_set", errMsg)) {
		COUT << "Not able to create set: " + errMsg;
		exit (-1);
	} else {
		COUT << "Created set gmm_initial_model_set.\n";
	}*/


	PDB_COUT << "to create a new set for storing output data" << std :: endl;
	if (!temp.createSet<GmmAggregateOutputType> ("gmm_db", "gmm_output_set", errMsg)) {
		COUT << "Not able to create set: " + errMsg;
		exit (-1);
	} else {
		COUT << "Created set gmm_output_set.\n";
	}



    //***********************************************************************************
    //****INITIALIZE MODEL***************************************************************
    //***********************************************************************************


	//https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/clustering/GaussianMixture.scala

	//// Determine initial weights and corresponding Gaussians.
    // If the user supplied an initial GMM, we use those values, otherwise
    // we start with uniform weights, a random mean from the data, and
    // diagonal covariance matrices using component variances
	// derived from the samples


    //pdb :: makeObjectAllocatorBlock(256 * 1024 * 1024, true);
    // connect to the query client
    QueryClient myClient (8108, "localhost", clientLogger, true);


    //const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 1024};

    std:: cout << "Creating model" << std::endl;
	Handle<GmmModel> model = makeObject<GmmModel>(k,dim);


	//TODO: update means with sampled data
	std:: cout << "Updating means and covars" << std::endl;

	std::vector<std::vector<double>> means(k, std::vector<double>(dim, 0.0));
	std::vector<std::vector<double>> covars(k, std::vector<double>(dim*dim,0.0));

	std:: cout << "To for:" << std::endl;


	for (int i=0; i<k; i++){
		for (int j=0; j<dim; j++){
			means[i][j] = i%k*3 + 1.0;
			covars[i][j*dim+j] = 0.5;
		}
	}


	model->updateMeans(means);
	model->updateCovars(covars);




	//***********************************************************************************
	//**** COUNT Number of datapoints **************************************************
	//***********************************************************************************

	/*

	// Initialization for the model in the KMeansAggregate

	// Calculate the count of input data points

	Handle<Computation> myInitialScanSet = makeObject<ScanDoubleVectorSet>("gmm_db", "gmm_input_set");
	Handle<Computation> myDataCount = makeObject<KMeansDataCountAggregate>();
	myDataCount->setInput(myInitialScanSet);
//    myDataCount->setAllocatorPolicy(noReuseAllocator);
//    myDataCount->setOutput("kmeans_db", "kmeans_data_count_set");
	Handle <Computation> myWriter = makeObject<WriteSumResultSet>("gmm_db", "gmm_data_count_set");
	myWriter->setInput(myDataCount);

	if (!myClient.executeComputations(errMsg, myWriter)) {
		COUT << "Query failed. Message was: " << errMsg << "\n";
		return 1;
	}
	SetIterator <SumResult> dataCountResult = myClient.getSetIterator <SumResult> ("gmm_db", "gmm_data_count_set");
	for (Handle<SumResult> a : dataCountResult) {

	dataCount = a->getTotal();
	}

//    dataCount = 10;
	COUT << "The number of data points: " << dataCount << std :: endl;
	COUT << std :: endl; */



	//***********************************************************************************
	//**** MAIN ALGORITHM: ***************************************************************
	//***********************************************************************************
	//- Aggregation performs Expectation - Maximization Steps
	//- The partial sums from the Output are used to update Means, Weights and Covars



    pdb :: makeObjectAllocatorBlock(256 * 1024 * 1024, true);

	//iter = 2;
	for (int currentIter=0; currentIter < iter; currentIter++){

		std::cout << "***MY MODEL AT ITERATION " << currentIter << std::endl;
		model->print();

		const UseTemporaryAllocationBlock tempBlock {256 * 1024 * 1024};

		Handle<Computation> scanInputSet = makeObject<ScanDoubleVectorSet>("gmm_db", "gmm_input_set");
		Handle<Computation> gmmIteration = makeObject<GmmAggregate>(model);
		gmmIteration->setInput(scanInputSet);
	    //gmmIteration->setAllocatorPolicy(noReuseAllocator);
		gmmIteration->setOutput("gmm_db", "gmm_output_set");
		//Handle <Computation> myWriter = makeObject<WriteSumResultSet>("gmm_db", "gmm_data_count_set");
		//myWriter->setInput(myDataCount);

		std::cout << "Ready to start computations" << std::endl;

		auto begin = std :: chrono :: high_resolution_clock :: now();

		if (!myClient.executeComputations(errMsg, gmmIteration)) {
			COUT << "Query failed. Message was: " << errMsg << "\n";
			return 1;
		}

		//Read output and update Means, Weights and Covars in Model
		SetIterator <GmmAggregateOutputType> result = myClient.getSetIterator <GmmAggregateOutputType> ("gmm_db", "gmm_output_set");
		kk = 0;
		std::cout << "ITERATION OUTPUT " << currentIter << std :: endl;

		for (Handle<GmmAggregateOutputType> a : result) {
			GmmNewComp output = (*a).getValue();
			/*std::cout << "SumR"  << std :: endl;
					output.getSumR().print();
			for (int i=0; i<k; i++){
				std::cout << "--Comp " << i << std :: endl;
				std::cout << "    WeightedX " << i << std :: endl;
				output.getWeightedX(i).print();
				std::cout << "    WeightedX2 " << i << std :: endl;
				output.getWeightedX2(i).print();
			}*/
			model->updateModel(output);
			model->print();
		}

		temp.clearSet("gmm_db", "gmm_output_set", "pdb::GmmAggregateOutputType", errMsg);

		auto end = std::chrono::high_resolution_clock::now();
		COUT << "Time Duration: " <<
		std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;

		long x = 2000;
		std::this_thread::sleep_for(std::chrono::milliseconds(x));
	}




// #################################################################################
// # CLEAN UP ######################################################################
// #################################################################################

    COUT << blue << std :: endl;
	COUT << "*****************************************" << std :: endl;
	COUT << "Cleaning sets : " << std :: endl;
	COUT << "*****************************************" << std :: endl;
	COUT << reset << std :: endl;

    if (clusterMode == false) {
	    // and delete the sets
        myClient.deleteSet ("gmm_db", "gmm_output_set");
        myClient.deleteSet ("gmm_db", "gmm_input_set");

        //myClient.deleteSet ("gmm_db", "gmm_initial_model_set");
        //myClient.deleteSet ("gmm_db", "gmm_data_count_set");
    } else {
        if (!temp.removeSet ("gmm_db", "gmm_output_set", errMsg)) {
            COUT << "Not able to remove set: " + errMsg;
            exit (-1);
        }
        else if (!temp.removeSet ("gmm_db", "gmm_input_set", errMsg)) {
			COUT << "Not able to remove set: " + errMsg;
			exit (-1);
		}
        /*else if (!temp.removeSet ("gmm_db", "gmm_initial_model_set", errMsg)) {
            COUT << "Not able to remove set: " + errMsg;
            exit (-1);
        }
        else if (!temp.removeSet ("gmm_db", "gmm_data_count_set", errMsg)) {
            COUT << "Not able to remove set: " + errMsg;
            exit (-1);
        }*/

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
