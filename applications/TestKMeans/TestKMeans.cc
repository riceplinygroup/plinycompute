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

/* K-means clustering */

#include "Lambda.h"
#include "PDBClient.h"
#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"

#include "Sampler.h"

#include "KMeansAggregate.h"
#include "KMeansAggregateOutputType.h"
#include "KMeansCentroid.h"
#include "KMeansDataCountAggregate.h"
#include "KMeansNormVectorMap.h"
#include "KMeansSampleSelection.h"
#include "ScanDoubleArraySet.h"
#include "ScanKMeansDoubleVectorSet.h"
#include "WriteKMeansDoubleVectorSet.h"
#include "WriteKMeansSet.h"

#include "DataTypes.h"
#include "KMeansDoubleVector.h"
#include "Set.h"
#include "SumResult.h"
#include "WriteSumResultSet.h"

#include <chrono>
#include <cstddef>
#include <ctime>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <math.h>
#include <random>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <vector>

using namespace pdb;

/* 
 * Use macros for performance consideration 
 * NUM_KMEANS_DIMENSIONS denotes the dimentionality of the input data
 * This value should be consistent with the one in KMeansNormVectorMap.h
 * and ScanDoubleArraySet.h 
 */
#ifndef NUM_KMEANS_DIMENSIONS
#define NUM_KMEANS_DIMENSIONS 1000
#endif

int main(int argc, char *argv[]) {

  bool printResult = true;
  bool clusterMode = false;

  const std::string blue("\033[1;34m");
  const std::string reset("\033[0m");

  /* Read in the parameters */
  if (argc < 9) {
    std::cout << "Insufficient parameters! Will use default values for missing "
                 "parameters."
              << std::endl;
    std::cout
        << "Usage: #printResult[Y/N] #clusterMode[Y/N] #masterIp #addData[Y/N] "
           "#numOfIteration #numOfCluster #convergeThreshold #dataFile"
        << std::endl;
  }
  if (argc > 1) {
    if (strcmp(argv[1], "N") == 0) {
      printResult = false;
    }
  }

  if (argc > 2) {
    if (strcmp(argv[2], "Y") == 0) {
      clusterMode = true;
    }
  }

  std::string masterIp = "localhost";
  if (argc > 3) {
    masterIp = argv[3];
  }

  bool whetherToAddData = true;
  if (argc > 4) {
    if (strcmp(argv[4], "N") == 0) {
      whetherToAddData = false;
    }
  }

  std::cout << "The K-means paramers: " << std::endl;
  std::cout << std::endl;

  int iter = 1;
  int k = 3;
  double threshold = 0.00001;

  if (argc > 5) {
    iter = std::stoi(argv[5]);
  }
  std::cout << "The number of iterations: " << iter << std::endl;

  if (argc > 6) {
    k = std::stoi(argv[6]);
  }
  std::cout << "The number of clusters: " << k << std::endl;

  std::cout << "The dimension of each data point: " << NUM_KMEANS_DIMENSIONS
            << std::endl;

  if (argc > 7) {
    threshold = std::stof(argv[7]);
  }
  std::cout << "The converging threshould: " << threshold << std::endl;

  std::string fileName = "/mnt/kmeans_data";
  if (argc > 8) {
    fileName = argv[8];
  }
  std::cout << "Input data file: " << fileName << std::endl;
  std::cout << std::endl;

  /* Setup the client */
  PDBClient pdbClient(8108, masterIp, false, true);

  string errMsg;

  /* Register types and classes */
  pdbClient.registerType("libraries/libKMeansAggregateOutputType.so");
  pdbClient.registerType("libraries/libKMeansCentroid.so");
  pdbClient.registerType("libraries/libWriteSumResultSet.so");
  pdbClient.registerType("libraries/libKMeansAggregate.so");
  pdbClient.registerType("libraries/libKMeansDataCountAggregate.so");
  pdbClient.registerType("libraries/libScanKMeansDoubleVectorSet.so");
  pdbClient.registerType("libraries/libScanDoubleArraySet.so");
  pdbClient.registerType("libraries/libWriteKMeansSet.so");
  pdbClient.registerType("libraries/libKMeansDataCountAggregate.so");
  pdbClient.registerType("libraries/libKMeansSampleSelection.so");
  pdbClient.registerType("libraries/libKMeansNormVectorMap.so");
  pdbClient.registerType("libraries/libWriteKMeansDoubleVectorSet.so");

  /* Meta data */
  pdb::makeObjectAllocatorBlock(1 * 1024 * 1024, true);
  int dataCount = 0;
  int kk = 0;

  /* Data does not need to be loaded */
  if (whetherToAddData == false) {

    pdbClient.createDatabase("kmeans_db");

    pdbClient.createSet<double[]>("kmeans_db", "kmeans_input_set");
  }

  /* Add data from the input file */
  else {

    /* Create a new database */
    pdbClient.createDatabase("kmeans_db");

    /* Create a new set */
    pdbClient.createSet<double[NUM_KMEANS_DIMENSIONS]>(
            "kmeans_db", "kmeans_input_set");

    /* Start adding data */

    int blockSize = 256;
    std::ifstream inFile(fileName.c_str());
    std::string line;
    bool rollback = false;
    bool end = false;

    while (!end) {
      pdb::makeObjectAllocatorBlock(blockSize * 1024 * 1024, true);
      pdb::Handle<pdb::Vector<pdb::Handle<double[NUM_KMEANS_DIMENSIONS]>>>
          storeMe = pdb::makeObject<
              pdb::Vector<pdb::Handle<double[NUM_KMEANS_DIMENSIONS]>>>(2177672);
      try {

        while (1) {
          if (!rollback) {
            if (!std::getline(inFile, line)) {
              end = true;
              break;
            } else {
              pdb::Handle<double[NUM_KMEANS_DIMENSIONS]> myData =
                  pdb::makeObject<double[NUM_KMEANS_DIMENSIONS]>();
              std::stringstream lineStream(line);
              double value;
              int index = 0;
              while (lineStream >> value) {
                (*myData)[index] = value;
                index++;
              }
              storeMe->push_back(myData);
            }
          } else {
            rollback = false;
            pdb::Handle<double[NUM_KMEANS_DIMENSIONS]> myData =
                pdb::makeObject<double[NUM_KMEANS_DIMENSIONS]>();
            std::stringstream lineStream(line);
            double value;
            int index = 0;
            while (lineStream >> value) {
              (*myData)[index] = value;
              index++;
            }
            storeMe->push_back(myData);
          }
        }

        end = true;

        /* Send the data to the database */
        pdbClient.sendData<double[NUM_KMEANS_DIMENSIONS]>(
                std::pair<std::string, std::string>("kmeans_input_set",
                                                    "kmeans_db"),
                storeMe);
        pdbClient.flushData();
      } catch (pdb::NotEnoughSpace &n) {
        pdbClient.sendData<double[NUM_KMEANS_DIMENSIONS]>(
                std::pair<std::string, std::string>("kmeans_input_set",
                                                    "kmeans_db"),
                storeMe);
        rollback = true;
      }
    }
    inFile.close();
  }

  /* Create a new set to store the normalized inpu data */
  pdbClient.createSet<KMeansDoubleVector>(
          "kmeans_db", "kmeans_norm_vector_set");

  /* Create a new set to store the data count */
  pdbClient.createSet<SumResult>("kmeans_db", "kmeans_data_count_set");

  /* Create a new set to store the initial model */
  pdbClient.createSet<KMeansDoubleVector>(
          "kmeans_db", "kmeans_initial_model_set");

  /* Create a new set to store output data */
  pdbClient.createSet<KMeansAggregateOutputType>(
          "kmeans_db", "kmeans_output_set");

  /* Main program */

  /* Compute the 2-norm for the input data */
  Handle<Computation> myInitialScanSet =
      makeObject<ScanDoubleArraySet>("kmeans_db", "kmeans_input_set");
  Handle<Computation> myNormVectorMap = makeObject<KMeansNormVectorMap>();
  myNormVectorMap->setInput(myInitialScanSet);
  Handle<Computation> myNormVectorWriter =
      makeObject<WriteKMeansDoubleVectorSet>("kmeans_db",
                                             "kmeans_norm_vector_set");
  myNormVectorWriter->setInput(myNormVectorMap);

  pdbClient.executeComputations(myNormVectorWriter);

  /* Compute the number of data */
  Handle<Computation> myScanSet = makeObject<ScanKMeansDoubleVectorSet>(
      "kmeans_db", "kmeans_norm_vector_set");
  Handle<Computation> myDataCount = makeObject<KMeansDataCountAggregate>();
  myDataCount->setInput(myScanSet);
  Handle<Computation> myWriter =
      makeObject<WriteSumResultSet>("kmeans_db", "kmeans_data_count_set");
  myWriter->setInput(myDataCount);

  pdbClient.executeComputations(myWriter);
  SetIterator<SumResult> dataCountResult =
      pdbClient.getSetIterator<SumResult>("kmeans_db", "kmeans_data_count_set");
  for (Handle<SumResult> a : dataCountResult) {

    dataCount = a->getTotal();
  }

  /*
   * Initialize the model
   * Randomly sample k data points from the input data through Bernoulli
   * sampling Guarantee the sampled size >= k in 99.99% of the time
   */

  const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 128};
  srand(time(NULL));
  double fraction = Sampler::computeFractionForSampleSize(k, dataCount, false);
  int initialCount = dataCount;

  Vector<Handle<KMeansDoubleVector>> mySamples;
  while (mySamples.size() < k) {
    Handle<Computation> mySampleScanSet = makeObject<ScanKMeansDoubleVectorSet>(
        "kmeans_db", "kmeans_norm_vector_set");
    Handle<Computation> myDataSample =
        makeObject<KMeansSampleSelection>(fraction);
    myDataSample->setInput(mySampleScanSet);
    Handle<Computation> myWriteSet = makeObject<WriteKMeansDoubleVectorSet>(
        "kmeans_db", "kmeans_initial_model_set");
    myWriteSet->setInput(myDataSample);

    pdbClient.executeComputations(myWriteSet);
    SetIterator<KMeansDoubleVector> sampleResult =
        pdbClient.getSetIterator<KMeansDoubleVector>(
            "kmeans_db", "kmeans_initial_model_set");

    for (Handle<KMeansDoubleVector> a : sampleResult) {
      Handle<KMeansDoubleVector> myDoubles = makeObject<KMeansDoubleVector>();
      double *rawData = a->getRawData();
      double *myRawData = myDoubles->getRawData();
      for (int i = 0; i < NUM_KMEANS_DIMENSIONS; i++) {
        myRawData[i] = rawData[i];
      }
      mySamples.push_back(myDoubles);
    }
    pdbClient.clearSet("kmeans_db", "kmeans_initial_model_set",
                       "pdb::KMeansDoubleVector");
  }
  Sampler::randomizeInPlace(mySamples);

  /* Take k samples */
  mySamples.resize(k);
  /* Ensure sampled data points distinct */
  Vector<Handle<KMeansDoubleVector>> myDistinctSamples;
  for (size_t i = 0; i < k; i++) {
    int j;
    for (j = i + 1; j < k; j++) {
      if (((mySamples[i])->equals(mySamples[j]))) {
        break;
      }
    }
    if ((mySamples.size() > 0) && (j == k)) {
      myDistinctSamples.push_back(mySamples[i]);
    }
  }

  k = myDistinctSamples.size();
  std::vector<std::vector<double>> model(k,
                                         vector<double>(NUM_KMEANS_DIMENSIONS));
  for (int i = 0; i < k; i++) {
    Handle<KMeansDoubleVector> tmp = myDistinctSamples[i];
    double *rawData = tmp->getRawData();
    for (int j = 0; j < NUM_KMEANS_DIMENSIONS; j++) {
      model[i][j] = rawData[j];
    }
  }

  auto iniEnd = std::chrono::high_resolution_clock::now();

  /* K-means training loop */
  for (int n = 0; n < iter; n++) {

    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 24};

    /* Read in the model */
    pdb::Handle<pdb::Vector<Handle<KMeansDoubleVector>>> my_model =
        pdb::makeObject<pdb::Vector<Handle<KMeansDoubleVector>>>();

    for (int i = 0; i < k; i++) {
      Handle<KMeansDoubleVector> tmp = pdb::makeObject<KMeansDoubleVector>();
      double *rawData = tmp->getRawData();
      double norm = 0;
      for (int j = 0; j < NUM_KMEANS_DIMENSIONS; j++) {
        rawData[j] = model[i][j];
        norm += rawData[j] * rawData[j];
      }
      tmp->norm = norm;
      my_model->push_back(tmp);
    }

    /* Aggregation for each cluster */
    Handle<Computation> myScanSet = makeObject<ScanKMeansDoubleVectorSet>(
        "kmeans_db", "kmeans_norm_vector_set");
    Handle<Computation> myQuery = pdb::makeObject<KMeansAggregate>(my_model);
    myQuery->setInput(myScanSet);
    myQuery->setOutput("kmeans_db", "kmeans_output_set");

    pdbClient.executeComputations(errMsg, myQuery)) {
      std::cout << "Query failed. Message was: " << errMsg << "\n";
      return 1;
    }

    /* Update the model */
    SetIterator<KMeansAggregateOutputType> result =
        pdbClient.getSetIterator<KMeansAggregateOutputType>(
            "kmeans_db", "kmeans_output_set");
    kk = 0;
    bool converge = true;
    for (Handle<KMeansAggregateOutputType> a : result) {
      if (kk >= k)
        break;
      size_t count = (*a).getValue().getCount();
      if (count == 0) {
        kk++;
        continue;
      }
      KMeansDoubleVector &meanVec = (*a).getValue().getMean();
      KMeansDoubleVector tmpModel = meanVec / count;
      if (converge &&
          (tmpModel.getFastSquaredDistance((*(*my_model)[kk])) > threshold)) {
        converge = false;
      }
      double *rawData = tmpModel.getRawData();
      for (int i = 0; i < NUM_KMEANS_DIMENSIONS; i++) {
        model[kk][i] = rawData[i];
      }
      kk++;
    }

    pdbClient.clearSet("kmeans_db", "kmeans_output_set",
                       "pdb::KMeansAggregateOutputType");
    if (converge == true) {
      std::cout << "Converged" << std::endl;
      break;
    }
  }

  auto allEnd = std::chrono::high_resolution_clock::now();

  /* Print out the resuts */
  if (printResult == true) {

    SetIterator<KMeansAggregateOutputType> result =
        pdbClient.getSetIterator<KMeansAggregateOutputType>(
            "kmeans_db", "kmeans_output_set");

    std::cout << std::endl;
    std::cout << blue << "*****************************************" << reset
              << std::endl;
    std::cout << blue << "The model I learned: " << reset << std::endl;
    std::cout << blue << "*****************************************" << reset
              << std::endl;
    std::cout << std::endl;

    for (int i = 0; i < k; i++) {
      std::cout << "Cluster index: " << i << std::endl;
      for (int j = 0; j < NUM_KMEANS_DIMENSIONS - 1; j++) {
        std::cout << model[i][j] << ", ";
      }
      std::cout << model[i][NUM_KMEANS_DIMENSIONS - 1] << std::endl;
    }
  }

  std::cout << "Running Time: "
            << std::chrono::duration_cast<std::chrono::duration<float>>(allEnd -
                                                                        iniEnd)
                   .count()
            << " secs." << std::endl;

  /* Remove the sets */
  if (clusterMode == false) {
    pdbClient.deleteSet("kmeans_db", "kmeans_output_set");
    pdbClient.deleteSet("kmeans_db", "kmeans_initial_model_set");
    pdbClient.deleteSet("kmeans_db", "kmeans_data_count_set");
    pdbClient.deleteSet("kmeans_db", "kmeans_norm_vector_set");

  } else {
    pdbClient.removeSet("kmeans_db", "kmeans_output_set");
    pdbClient.removeSet("kmeans_db", "kmeans_initial_model_set");
    pdbClient.removeSet("kmeans_db", "kmeans_data_count_set");
    pdbClient.removeSet("kmeans_db", "kmeans_norm_vector_set");
  }
  int code = system("scripts/cleanupSoFiles.sh");
  if (code < 0) {
    std::cout << "Can't cleanup so files" << std::endl;
  }
}

#endif
