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
#include <iostream>

#include "CatalogServer.h"
#include "PDBClient.h"
#include "ScanStringIntPairSet.h"

int main(int argc, char *argv[]) {

  PDBLoggerPtr clientLogger = make_shared<PDBLogger>("clientLog");
  std::string managerIp = "localhost";
  int port = 8108;

  if (argc > 1) {
    managerIp = argv[1];
  }
  if (argc > 2) {
    port = atoi(argv[2]);
  }
  std::cout << "Manager IP Address is " << managerIp << ":" << port << std::endl;

    PDBClient pdbClient(
            port,
            managerIp);

  string errMsg;

  pdbClient.registerType("libraries/libDoubleVectorAggregation.so");
  pdbClient.listUserDefinedTypes();
  pdbClient.listNodesInCluster();
  pdbClient.listRegisteredDatabases();

  if (!pdbClient.createDatabase("catalog_test_db")) {
    std::cout << "Not able to create database: " + pdbClient.getErrorMessage();
    exit(-1);
  }

  pdbClient.listRegisteredDatabases();
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db");

  if (!pdbClient.createSet<int>("catalog_test_db", "catalog_test_db_set1")) {
    std::cout << "Not able to create set: " + errMsg;
    exit(-1);
  }

  pdbClient.listRegisteredSetsForADatabase("catalog_test_db");

  if (!pdbClient.createSet<StringIntPair>("catalog_test_db",
                                          "catalog_test_db_set2")) {
    std::cout << "Not able to create set: " + errMsg;
    exit(-1);
  }

  pdbClient.listRegisteredSetsForADatabase("catalog_test_db");

  if (!pdbClient.createSet<String>("catalog_test_db", "catalog_test_db_set3")) {
    std::cout << "Not able to create set: " + errMsg;
    exit(-1);
  }

  pdbClient.listRegisteredDatabases();

  if (!pdbClient.createDatabase("catalog_test_db2")) {
    std::cout << "Not able to create database: " + errMsg;
    exit(-1);
  }

  pdbClient.listRegisteredDatabases();
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2");

  if (!pdbClient.createSet<int>("catalog_test_db2", "catalog_test_db2_set1")) {
    std::cout << "Not able to create set: " + errMsg;
    exit(-1);
  }

  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2");

  if (!pdbClient.createSet<StringIntPair>("catalog_test_db2",
                                          "catalog_test_db2_set2")) {
    std::cout << "Not able to create set: " + errMsg;
    exit(-1);
  }

  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2");

  if (!pdbClient.createSet<String>("catalog_test_db2", "catalog_test_db2_set3")) {
    std::cout << "Not able to create set: " + errMsg;
    exit(-1);
  }

  pdbClient.removeSet("catalog_test_db", "catalog_test_db_set1");
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db");

  pdbClient.removeSet("catalog_test_db", "catalog_test_db_set2");
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db");

  pdbClient.removeSet("catalog_test_db", "catalog_test_db_set3");
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db");

  pdbClient.removeSet("catalog_test_db2", "catalog_test_db2_set1");
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2");

  pdbClient.removeSet("catalog_test_db2", "catalog_test_db2_set2");
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2");

  pdbClient.removeSet("catalog_test_db2", "catalog_test_db2_set3");
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2");

  pdbClient.removeDatabase("catalog_test_db");
  pdbClient.listRegisteredDatabases();

  pdbClient.removeDatabase("catalog_test_db2");
  pdbClient.listRegisteredDatabases();

  pdbClient.listUserDefinedTypes();

  pdbClient.registerType("libraries/libSimpleJoin.so");
  pdbClient.registerType("libraries/libScanIntSet.so");
  pdbClient.listUserDefinedTypes();

  pdbClient.registerType("libraries/libScanStringIntPairSet.so");
  pdbClient.registerType("libraries/libScanStringSet.so");

  pdbClient.listUserDefinedTypes();
  pdbClient.registerType("libraries/libWriteStringSet.so");
  pdbClient.listUserDefinedTypes();
}
