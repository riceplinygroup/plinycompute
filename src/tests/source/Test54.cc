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

#ifndef TEST_54_CC
#define TEST_54_CC

#include "StorageClient.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"
#include <stdlib.h>
#include <time.h>
// this won't be visible to the v-table map, since it is not in the biult in types directory

#ifndef NUM_DIMENSIONS
    #define NUM_DIMENSIONS 100
#endif

int main (int argc, char * argv[]) {

        int numOfMb = 128;

	std:: cout << "Make sure to run bin/test603 in a different window to provide a catalog/storage server.\n";
        std :: cout << "You can provide one parameter as the size of data to add (in MB)"<< std :: endl;

        if (argc >1) {
            numOfMb = atoi(argv[1]);
        }

        std :: cout << "to add data with size: " << numOfMb << "MB" << std :: endl;

        bool clusterMode = false;
        if (argc > 2) {
             clusterMode = true;
             std :: cout << "We are running in cluster mode" << std :: endl;
        }
        else {
             std :: cout << "We are not running in cluster mode, if you want to run in cluster mode, please provide any character as second parameter" << std :: endl;
        }

	// register the shared employee class
	pdb :: StorageClient temp (8108, "localhost", make_shared <pdb :: PDBLogger> ("clientLog"), true);

	string errMsg;
        
        //to register selection type
        if (clusterMode == true) {
            temp.registerType ("libraries/libPartialResult.so", errMsg);
            temp.registerType ("libraries/libKMeansQuery.so", errMsg);
        }


	// now, create a new database
	if (!temp.createDatabase ("kmeans_db", errMsg)) {
		cout << "Not able to create database: " + errMsg;
                return -1;
	} else {
		cout << "Created database.\n";
	}

	// now, create a new set in that database
	if (!temp.createSet <double [NUM_DIMENSIONS]> ("kmeans_db", "kmeans_set", errMsg)) {
		cout << "Not able to create set: " + errMsg;
                return -1;
	} else {
		cout << "Created set.\n";
	}

        int total = 0;
        srand((unsigned int)(time(NULL)));
        if (numOfMb > 0) {
           int numIterations = numOfMb/64;
           int remainder = numOfMb - 64 * numIterations; 
           if (remainder > 0) {  numIterations = numIterations + 1; }
	   for (int num = 0; num < numIterations; ++num) {
		 // now, create a bunch of data
                 int blockSize = 64;
                 if ((num == numIterations - 1) && (remainder > 0)) {
                     blockSize = remainder;
                 }
	         pdb :: makeObjectAllocatorBlock (1024 * 1024 * blockSize, true);
		 pdb :: Handle <pdb :: Vector <pdb :: Handle <double [NUM_DIMENSIONS]>>> storeMe = pdb :: makeObject <pdb :: Vector <pdb :: Handle <double [NUM_DIMENSIONS]>>> ();
		 int i,j;
		 try {
			for (i = 0; true; i++) {
					pdb :: Handle <double [NUM_DIMENSIONS]> myData = pdb :: makeObject <double [NUM_DIMENSIONS]>();
                                        for (j=0; j<NUM_DIMENSIONS; j++) {
                                           (*myData)[j]=rand()/double(RAND_MAX);
                                        }	
					storeMe->push_back (myData);
                                        total++;
			}
		
		} catch (pdb :: NotEnoughSpace &n) {
		
			// we got here, so go ahead and store the vector
			if (!temp.storeData <double [NUM_DIMENSIONS]> (storeMe, "kmeans_db", "kmeans_set", errMsg)) {
				cout << "Not able to store data: " + errMsg;
					return -1;
			}	
			std :: cout << i << std::endl;
			std :: cout << "stored the data!!\n";
		}
	  }
      }

      // and shut down the server
      temp.flushData(errMsg);
/*        	
	if (!temp.shutDownServer (errMsg))
		std :: cout << "Shut down not clean: " << errMsg << "\n";
*/	
      std :: cout << "count=" << total << std :: endl;
      return 0;
}

#endif

