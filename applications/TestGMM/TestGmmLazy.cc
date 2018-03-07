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
#ifndef TEST_GMM_LAZY
#define TEST_GMM_LAZY

// By Tania, August 2017
// Gaussian Mixture Model (based on EM);

#include "Lambda.h"
#include "PDBClient.h"
#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"

#include "ScanUserSet.h"
#include "WriteUserSet.h"

#include "GmmAggregateLazy.h"
#include "GmmAggregateNewComp.h"
#include "GmmAggregateOutputLazy.h"
#include "GmmDataCountAggregate.h"
#include "GmmModel.h"
#include "GmmSampleSelection.h"
#include "Sampler.h"

#include "DataTypes.h"
#include "DoubleVector.h"
#include "Set.h"

#include <chrono>
#include <cstddef>
#include <ctime>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <math.h>
#include <random>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <time.h>
#include <unistd.h>

#define COUT std::cout

using namespace pdb;
int main(int argc, char *argv[]) {
  bool printResult = true;
  bool clusterMode = false;

  freopen("/dev/tty", "w", stdout);

  const std::string red("\033[0;31m");
  const std::string green("\033[1;32m");
  const std::string yellow("\033[1;33m");
  const std::string blue("\033[1;34m");
  const std::string cyan("\033[0;36m");
  const std::string magenta("\033[0;35m");
  const std::string reset("\033[0m");

  //***********************************************************************************
  //**** INPUT PARAMETERS
  //***************************************************************
  //***********************************************************************************

  COUT << "Usage: #printResult[Y/N] #clusterMode[Y/N] blocksSize[MB] #masterIp "
          "#randomData[Y/N] #addData[Y/N] "
          "#niter, #clusters #nDatapoints #nDimensions "
          "#pathToInputFile(randomData == N)"
       << std::endl;
  if (argc > 1) {
    if (strcmp(argv[1], "N") == 0) {
      printResult = false;
      COUT << "You successfully disabled printing result." << std::endl;
    } else {
      printResult = true;
      COUT << "Will print result." << std::endl;
    }
  } else {
    COUT << "Will print result. If you don't want to print result, you can add "
            "N as the first "
            "parameter to disable result printing."
         << std::endl;
  }

  if (argc > 2) {
    if (strcmp(argv[2], "Y") == 0) {
      clusterMode = true;
      COUT << "You successfully set the test to run on cluster." << std::endl;
    } else {
      clusterMode = false;
    }
  } else {
    COUT << "Will run on local node. If you want to run on cluster, you can "
            "add any character "
            "as the second parameter to run on the cluster configured by "
            "$PDB_HOME/conf/serverlist."
         << std::endl;
  }

  int blocksize = 256; // by default we add 64MB data
  if (argc > 3) {
    blocksize = atoi(argv[3]);
  }

  std::string masterIp = "localhost";
  if (argc > 4) {
    masterIp = argv[4];
  }
  COUT << "Master IP Address is " << masterIp << std::endl;

  bool randomData = true;
  if (argc > 5) {
    if (strcmp(argv[5], "N") == 0) {
      randomData = false;
    }
  }

  bool whetherToAddData = true;
  if (argc > 6) {
    if (strcmp(argv[6], "N") == 0) {
      whetherToAddData = false;
    }
  }

  COUT << blue << std::endl;
  COUT << "*****************************************" << std::endl;
  COUT << "GMM starts : " << std::endl;
  COUT << "*****************************************" << std::endl;
  COUT << reset << std::endl;

  COUT << "The GMM paramers are: " << std::endl;
  COUT << std::endl;

  int iter = 1;
  int k = 2;
  int dim = 2;
  int numData = 10;
  double convergenceTol = 0.001; // Convergence threshold

  if (argc > 7) {
    iter = std::stoi(argv[7]);
  }
  COUT << "The number of iterations: " << iter << std::endl;

  if (argc > 8) {
    k = std::stoi(argv[8]);
  }
  COUT << "The number of clusters: " << k << std::endl;

  if (argc > 9) {
    numData = std::stoi(argv[9]);
  }
  COUT << "The number of data points: " << numData << std::endl;

  if (argc > 10) {
    dim = std::stoi(argv[10]);
  }
  COUT << "The dimension of each data point: " << dim << std::endl;

  std::string fileName = "/mnt/gmm_data.txt";
  if (argc > 11) {
    fileName = argv[11];
  }

  COUT << "Input file: " << fileName << std::endl;
  COUT << std::endl;

  COUT << std::endl;

  //***********************************************************************************
  //**** LOAD DATA
  //********************************************************************
  //***********************************************************************************

  PDBClient pdbClient(8108, masterIp, false, true);

  string errMsg;

  //    srand(time(0));
  // For the random number generator
  std::random_device rd;
  std::mt19937 randomGen(rd());

  //***********************************************************************************
  //****READ INPUT DATA
  //***************************************************************
  //***********************************************************************************

  // Step 1. Create Database and Set
  // now, register a type for user data

  if (whetherToAddData == true) {
    // now, create a new database
    pdbClient.createDatabase("gmm_db");

    pdbClient.createSet<DoubleVector>("gmm_db", "gmm_input_set");
  }

  // Step 2. Add data

  if (whetherToAddData == true) {
    if (randomData == true) {

      int addedData = 0;
      while (addedData < numData) {

        pdb::makeObjectAllocatorBlock(blocksize * 1024 * 1024, true);

        pdb::Handle<pdb::Vector<pdb::Handle<DoubleVector>>> storeMe =
            pdb::makeObject<pdb::Vector<pdb::Handle<DoubleVector>>>();
        try {
          double bias = 0;
          for (int i = addedData; i < numData; i++) {
            pdb::Handle<DoubleVector> myData =
                pdb::makeObject<DoubleVector>(dim);
            for (int j = 0; j < dim; j++) {

              std::uniform_real_distribution<> unif(0, 1);
              bias = unif(randomGen) * 0.01;
              myData->setDouble(j, i % k * 3 + bias);
            }
            storeMe->push_back(myData);

            addedData += 1;
          }

          COUT << "Added " << storeMe->size() << " Total: " << addedData
               << std::endl;

          pdbClient.sendData<DoubleVector>(
                  std::pair<std::string, std::string>("gmm_input_set",
                                                      "gmm_db"),
                  storeMe, errMsg)) {
            COUT << "Failed to send data to dispatcher server" << std::endl;
            return -1;
          }
        } catch (pdb::NotEnoughSpace &n) {
          COUT << "Added " << storeMe->size() << " Total: " << addedData
               << std::endl;

          pdbClient.sendData<DoubleVector>(
                  std::pair<std::string, std::string>("gmm_input_set",
                                                      "gmm_db"),
                  storeMe, errMsg)) {
            COUT << "Failed to send data to dispatcher server" << std::endl;
            return -1;
          }
        }
        COUT << blocksize << "MB data sent to dispatcher server~~" << std::endl;

      } // End while

      // to write back all buffered records
      pdbClient.flushData(errMsg);

    } else { // Load from file

      numData = 0;

      std::ifstream inFile(fileName.c_str());
      std::string line;
      bool rollback = false;
      bool end = false;

      numData = 0;
      while (!end) {
        pdb::makeObjectAllocatorBlock(blocksize * 1024 * 1024, true);

        pdb::Handle<pdb::Vector<pdb::Handle<DoubleVector>>> storeMe =
            pdb::makeObject<pdb::Vector<pdb::Handle<DoubleVector>>>();
        try {

          while (1) {
            if (!rollback) {
              //      std::istringstream iss(line);
              if (!std::getline(inFile, line)) {
                end = true;
                break;
              } else {
                pdb::Handle<DoubleVector> myData =
                    pdb::makeObject<DoubleVector>(dim);
                std::stringstream lineStream(line);
                double value;
                int index = 0;
                while (lineStream >> value) {
                  //(*myData)[index] = value;
                  myData->setDouble(index, value);
                  index++;
                }
                storeMe->push_back(myData);
                // myData->print();
              }
            } else {
              rollback = false;
              pdb::Handle<DoubleVector> myData =
                  pdb::makeObject<DoubleVector>(dim);
              std::stringstream lineStream(line);
              double value;
              int index = 0;
              while (lineStream >> value) {
                //(*myData)[index] = value;
                myData->setDouble(index, value);
                index++;
              }
              storeMe->push_back(myData);
            }
          }

          end = true;

          // send the rest of data at the end, it can happen that the exception
          // never
          // happens.
          pdbClient.sendData<DoubleVector>(
                  std::pair<std::string, std::string>("gmm_input_set",
                                                      "gmm_db"),
                  storeMe, errMsg)) {
            COUT << "Failed to send data to dispatcher server" << std::endl;
            return -1;
          }
          numData += storeMe->size();
          COUT << "Added " << storeMe->size() << " Total: " << numData
               << std::endl;

          pdbClient.flushData(errMsg);
        } catch (pdb::NotEnoughSpace &n) {
          pdbClient.sendData<DoubleVector>(
                  std::pair<std::string, std::string>("gmm_input_set",
                                                      "gmm_db"),
                  storeMe, errMsg)) {
            COUT << "Failed to send data to dispatcher server" << std::endl;
            return -1;
          }

          numData += storeMe->size();
          COUT << "Added " << storeMe->size() << " Total: " << numData
               << std::endl;

          rollback = true;
        }
        PDB_COUT << blocksize << "MB data sent to dispatcher server~~"
                 << std::endl;

      } // while not end
      inFile.close();
    } // End load data!!

  } // End if - whetherToAddData = true

  //***********************************************************************************
  //****REGISTER SO
  //***************************************************************
  //***********************************************************************************

  // register this query class
  pdbClient.registerType("libraries/libGmmAggregateLazy.so", errMsg);
  pdbClient.registerType("libraries/libGmmModel.so", errMsg);
  pdbClient.registerType("libraries/libGmmAggregateOutputLazy.so", errMsg);
  pdbClient.registerType("libraries/libGmmAggregateDatapoint.so", errMsg);
  pdbClient.registerType("libraries/libGmmAggregateNewComp.so", errMsg);
  pdbClient.registerType("libraries/libGmmSampleSelection.so", errMsg);

  //***********************************************************************************
  //****CREATE
  //SETS***************************************************************
  //***********************************************************************************

  PDB_COUT << "to create a new set to store the initial model" << std::endl;

  pdbClient.createSet<DoubleVector>("gmm_db", "gmm_initial_model_set",
                                         errMsg)) {
    COUT << "Not able to create set: " + errMsg;
    exit(-1);
  } else {
    COUT << "Created set gmm_initial_model_set.\n";
  }

  PDB_COUT << "to create a new set for storing output data" << std::endl;

  pdbClient.createSet<GmmAggregateOutputLazy>(
          "gmm_db", "gmm_output_set",
          errMsg)) { //, size_t(32) * size_t(1024) * size_t(1024))) {
    COUT << "Not able to create set: " + errMsg;
    exit(-1);
  } else {
    COUT << "Created set gmm_output_set.\n";
  }

  //***********************************************************************************
  //****SELECT INITIALIZATION
  //DATA*****************************************************
  //***********************************************************************************

  auto iniBegin = std::chrono::high_resolution_clock::now();

  COUT << "Starting the sampling ..." << std::endl;

  // We use 5 samples to initialize each GMM component
  // Done after MLlib implementation
  int nSamples = 5 * k;

  // Randomly sample k data points from the input data through Bernoulli
  // sampling
  // We guarantee the sampled size >= k in 99.99% of the time
  // const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};

  srand(time(NULL));
  const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 128};

  double fraction =
      Sampler::computeFractionForSampleSize(nSamples, numData, false);
  COUT << "The sample threshold is: " << fraction << std::endl;
  int initialCount = numData;

  std::vector<Handle<DoubleVector>> mySamples;
  while (mySamples.size() < nSamples) {
    std::cout << "Needed to sample due to insufficient sample size."
              << std::endl;
    Handle<Computation> mySampleScanSet =
        makeObject<ScanUserSet<DoubleVector>>("gmm_db", "gmm_input_set");

    Handle<Computation> myDataSample = makeObject<GmmSampleSelection>(fraction);
    myDataSample->setInput(mySampleScanSet);
    Handle<Computation> myWriteSet = makeObject<WriteUserSet<DoubleVector>>(
        "gmm_db", "gmm_initial_model_set");
    myWriteSet->setInput(myDataSample);

    std::cout << "Let's execute the Sampling!" << std::endl;

    pdbClient.executeComputations(errMsg, myWriteSet)) {
      COUT << "Query failed. Message was: " << errMsg << "\n";
      return 1;
    }

    std::cout << "Sampling done!: " << std::endl;
    SetIterator<DoubleVector> sampleResult =
        pdbClient.getSetIterator<DoubleVector>("gmm_db",
                                               "gmm_initial_model_set");

    for (Handle<DoubleVector> a : sampleResult) {
      std::cout << "Scanning result from sampling:" << std::endl;

      Handle<DoubleVector> myDoubles = makeObject<DoubleVector>(dim);

      double *rawData = a->getRawData();
      double *myRawData = myDoubles->getRawData();

      for (int i = 0; i < dim; i++) {
        myRawData[i] = rawData[i];
      }
      mySamples.push_back(myDoubles);
    }
    std::cout << "Now we have " << mySamples.size() << " samples" << std::endl;
    pdbClient.clearSet("gmm_db", "gmm_initial_model_set", "pdb::DoubleVector",
                       errMsg);
  }

  //***********************************************************************************
  //****INITIALIZE
  //MODEL***************************************************************
  //***********************************************************************************

  // https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/clustering/GaussianMixture.scala

  // Determine initial weights and corresponding Gaussians.
  // If the user supplied an initial GMM, we use those values, otherwise
  // we start with uniform weights, a random mean from the data, and
  // diagonal covariance matrices using component variances
  // derived from the samples

  pdb::makeObjectAllocatorBlock(256 * 1024 * 1024, true);

  std::cout << "Creating model" << std::endl;
  Handle<GmmModel> model = makeObject<GmmModel>(k, dim);

  std::cout << "Updating means and covars" << std::endl;

  std::vector<std::vector<double>> means(k, std::vector<double>(dim, 0.0));
  std::vector<std::vector<double>> covars(k,
                                          std::vector<double>(dim * dim, 0.0));

  int nsamples = 5;

  // Init mean
  for (int i = 0; i < k; i++) {
    for (int j = 0; j < nsamples; j++) {
      for (int l = 0; l < dim; l++) {
        means[i][l] += mySamples[i * nsamples + j]->getDouble(l);
      }
    }
    for (int j = 0; j < dim; j++) {
      means[i][j] /= nsamples;
    }
  }

  // Init covar
  for (int i = 0; i < k; i++) {
    for (int j = 0; j < nsamples; j++) {
      for (int l = 0; l < dim; l++) {
        double t = (mySamples[i * nsamples + j]->getDouble(l) - means[i][l]);
        t *= t;
        covars[i][l * dim + l] += t;
      }
    }
    for (int j = 0; j < dim; j++) {
      covars[i][j * dim + j] /= nsamples;
    }
  }

  model->updateMeans(means);
  model->updateCovars(covars);
  model->calcInvCovars();

  auto iniEnd = std::chrono::high_resolution_clock::now();

  //***********************************************************************************
  //**** MAIN ALGORITHM:
  //***************************************************************
  //***********************************************************************************
  //- Aggregation performs Expectation - Maximization Steps
  //- The partial sums from the Output are used to update Means, Weights and
  //Covars

  // Recording iteration times
  std::vector<float> iterTimes(iter);
  std::vector<float> iterProcessingOnlyTimes(iter);

  bool converged = false;
  double previousLogLikelihood;
  double currentLogLikelihood;

  for (int currentIter = 0; currentIter < iter; currentIter++) {

    auto iterBegin = std::chrono::high_resolution_clock::now();

    std::cout << "ITERATION " << (currentIter + 1) << std::endl;

    const UseTemporaryAllocationBlock tempBlock{256 * 1024 * 1024};

    Handle<GmmModel> currentModel = model;

    if (printResult) {
      currentModel->print();
    }

    Handle<Computation> scanInputSet =
        makeObject<ScanUserSet<DoubleVector>>("gmm_db", "gmm_input_set");
    Handle<Computation> gmmIteration =
        makeObject<GmmAggregateLazy>(currentModel);
    gmmIteration->setInput(scanInputSet);
    gmmIteration->setOutput("gmm_db", "gmm_output_set");

    std::cout << "Ready to start computations" << std::endl;

    auto begin = std::chrono::high_resolution_clock::now();

    pdbClient.executeComputations(errMsg, gmmIteration)) {
      COUT << "Query failed. Message was: " << errMsg << "\n";
      return 1;
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Query finished!	" << std::endl;

    // Read output and update Means, Weights and Covars in Model
    SetIterator<GmmAggregateOutputLazy> result =
        pdbClient.getSetIterator<GmmAggregateOutputLazy>("gmm_db",
                                                         "gmm_output_set");

    std::cout << "ITERATION OUTPUT " << currentIter << " , FROM " << iter
              << " ITERATIONS" << std::endl;

    for (Handle<GmmAggregateOutputLazy> a : result) {

      std::cout << "Entering loop to process result" << std::endl;
      previousLogLikelihood = currentLogLikelihood;
      currentLogLikelihood = currentModel->updateModel((*a).getNewComp());

      std::cout << " previousLogLikelihood: " << previousLogLikelihood
                << " currentLogLikelihood  " << currentLogLikelihood
                << std::endl;
    }

    model = currentModel;

    pdbClient.clearSet("gmm_db", "gmm_output_set",
                       "pdb::GmmAggregateOutputLazy", errMsg);

    COUT << std::endl;
    COUT << std::endl;

    auto iterEnd = std::chrono::high_resolution_clock::now();
    COUT << "Server-side Time Duration for Iteration-: " << currentIter
         << " is:"
         << std::chrono::duration_cast<std::chrono::duration<float>>(end -
                                                                     begin)
                .count()
         << " secs." << std::endl;
    COUT << "Total Time Duration for Iteration-: " << currentIter << " is:"
         << std::chrono::duration_cast<std::chrono::duration<float>>(iterEnd -
                                                                     iterBegin)
                .count()
         << " secs." << std::endl;

    iterProcessingOnlyTimes[currentIter] =
        std::chrono::duration_cast<std::chrono::duration<float>>(end - begin)
            .count();
    iterTimes[currentIter] =
        std::chrono::duration_cast<std::chrono::duration<float>>(iterEnd -
                                                                 iterBegin)
            .count();

    // Check for convergence.
    if (currentIter > 0 and
        abs(currentLogLikelihood - previousLogLikelihood) < convergenceTol) {
      converged = true;
      COUT << "***** CONVERGED AT ITERATION " << currentIter << std::endl;
      break;
    }
  }

  auto allEnd = std::chrono::high_resolution_clock::now();

  // Print result
  if (printResult) {
    model->print();
  }

  COUT << std::endl;

  COUT << "Sampling Time Duration: "
       << std::chrono::duration_cast<std::chrono::duration<float>>(iniEnd -
                                                                   iniBegin)
              .count()
       << " secs." << std::endl;

  COUT << "Total Processing Time Duration: "
       << std::chrono::duration_cast<std::chrono::duration<float>>(allEnd -
                                                                   iniEnd)
              .count()
       << " secs." << std::endl;

  COUT << "Times per iteration:";
  for (int i = 0; i < iter; i++) {
    COUT << " " << iterTimes[i];
  }
  COUT << std::endl;

  COUT << "Mean times per iteration (except iter 0):";

  double totalIter = 0;

  for (int i = 1; i < iter; i++) {
    totalIter += iterTimes[i];
  }
  totalIter /= (iter - 1);
  COUT << " " << totalIter;

  /////
  COUT << std::endl;
  COUT << "Times per iteration (processing only, no model update):";
  for (int i = 0; i < iter; i++) {
    COUT << " " << iterProcessingOnlyTimes[i];
  }
  COUT << std::endl;

  COUT << "Mean times per iteration ((processing only, no model update, except "
          "iter 0):";

  totalIter = 0;

  for (int i = 1; i < iter; i++) {
    totalIter += iterProcessingOnlyTimes[i];
  }
  totalIter /= (iter - 1);
  COUT << " " << totalIter;

  COUT << std::endl;
  COUT << std::endl;

  // #################################################################################
  // # CLEAN UP
  // ######################################################################
  // #################################################################################

  COUT << blue << std::endl;
  COUT << "*****************************************" << std::endl;
  COUT << "Cleaning sets : " << std::endl;
  COUT << "*****************************************" << std::endl;
  COUT << reset << std::endl;

  if (clusterMode == false) {
    // and delete the sets
    pdbClient.deleteSet("gmm_db", "gmm_output_set");
    pdbClient.deleteSet("gmm_db", "gmm_initial_model_set");

  } else {
    pdbClient.removeSet("gmm_db", "gmm_output_set", errMsg)) {
      COUT << "Not able to remove set: " + errMsg;
      exit(-1);
    }
    pdbClient.removeSet("gmm_db", "gmm_initial_model_set", errMsg)) {
      COUT << "Not able to remove set: " + errMsg;
      exit(-1);
    } else {
      COUT << "Removed set.\n";
    }
  }
  int code = system("scripts/cleanupSoFiles.sh");
  if (code < 0) {
    COUT << "Can't cleanup so files" << std::endl;
  }
}

#endif
