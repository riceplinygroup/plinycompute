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
#ifndef TEST_GMM_LOAD_DATA
#define TEST_GMM_LOAD_DATA

// By Tania, September 2017
// Gaussian Mixture Model (based on EM) - Load DATA!!

#include "Lambda.h"
#include "PDBClient.h"
#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"

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

#define BLOCKSIZE DEFAULT_NET_PAGE_SIZE

#define COUT std::cout
//#define COUT term

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
  // numOfMb = 10; //Force it to be 64 by now.

  COUT << "To add data with size: " << blocksize << "MB" << std::endl;

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

  PDBClient pdbClient(
            8108,
            masterIp,
            false,
            true);

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
  // TODO: once sharedLibrary is supported, add this line back!!!

  if (whetherToAddData == true) {
    // now, create a new database
    if (!pdbClient.createDatabase("gmm_db", errMsg)) {
      COUT << "Not able to create database: " + errMsg;
      exit(-1);
    } else {
      COUT << "Created database.\n";
    }

    // now, create a new set in that database
    if (!pdbClient.createSet<DoubleVector>("gmm_db", "gmm_input_set", errMsg)) {
      COUT << "Not able to create set: " + errMsg;
      exit(-1);
    } else {
      COUT << "Created set.\n";
    }
  }

  // Step 2. Add data

  auto begin = std::chrono::high_resolution_clock::now();

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

          if (!pdbClient.sendData<DoubleVector>(
                  std::pair<std::string, std::string>("gmm_input_set",
                                                      "gmm_db"),
                  storeMe, errMsg)) {
            COUT << "Failed to send data to dispatcher server" << std::endl;
            return -1;
          }
        } catch (pdb::NotEnoughSpace &n) {
          COUT << "Added " << storeMe->size() << " Total: " << addedData
               << std::endl;
          if (!pdbClient.sendData<DoubleVector>(
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
          if (!pdbClient.sendData<DoubleVector>(
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
          if (!pdbClient.sendData<DoubleVector>(
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

  auto end = std::chrono::high_resolution_clock::now();

  COUT << "Time loading data: "
       << std::chrono::duration_cast<std::chrono::duration<float>>(end - begin)
              .count()
       << " secs." << std::endl;
}

#endif
