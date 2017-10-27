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
#include "ScanStringIntPairSet.h"
#include "PDBClient.h"
#include "CatalogServer.h"

int main(int argc, char *argv[]) {

  PDBLoggerPtr clientLogger = make_shared<PDBLogger>("clientLog");
  std::string masterIp = "localhost";
  int port =8108;

  if (argc > 1) {
      masterIp = argv[1];
  }
  if (argc > 2) {
      port = atoi(argv[2]);
  }
  std::cout << "Master IP Address is " << masterIp << ":" << port << std::endl;

  PDBClient pdbClient(
          port,
          masterIp,
          clientLogger,
          false,
          true);

  CatalogClient catalogClient(
          port,
          masterIp,
          clientLogger);

  string errMsg;

  pdbClient.listNodesInCluster(errMsg);
  pdbClient.listRegisteredDatabases(errMsg);

  if (!pdbClient.createDatabase("catalog_test_db", errMsg)) {
      cout << "Not able to create database: " + errMsg;
      exit(-1);
  }

  pdbClient.listRegisteredDatabases(errMsg);
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db", errMsg);

  if (!pdbClient.createSet<int>("catalog_test_db", "catalog_test_db_set1", errMsg)) {
      cout << "Not able to create set: " + errMsg;
      exit(-1);
  }

  pdbClient.listRegisteredSetsForADatabase("catalog_test_db", errMsg);

  if (!pdbClient.createSet<StringIntPair>("catalog_test_db", "catalog_test_db_set2", errMsg)) {
      cout << "Not able to create set: " + errMsg;
      exit(-1);
  }

  pdbClient.listRegisteredSetsForADatabase("catalog_test_db", errMsg);

  if (!pdbClient.createSet<String>("catalog_test_db", "catalog_test_db_set3", errMsg)) {
      cout << "Not able to create set: " + errMsg;
      exit(-1);
  }

  pdbClient.listRegisteredDatabases(errMsg);

  if (!pdbClient.createDatabase("catalog_test_db2", errMsg)) {
      cout << "Not able to create database: " + errMsg;
      exit(-1);
  }

  pdbClient.listRegisteredDatabases(errMsg);
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2", errMsg);

  if (!pdbClient.createSet<int>("catalog_test_db2", "catalog_test_db2_set1", errMsg)) {
      cout << "Not able to create set: " + errMsg;
      exit(-1);
  }

  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2", errMsg);

  if (!pdbClient.createSet<StringIntPair>("catalog_test_db2", "catalog_test_db2_set2", errMsg)) {
      cout << "Not able to create set: " + errMsg;
      exit(-1);
  }

  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2", errMsg);

  if (!pdbClient.createSet<String>("catalog_test_db2", "catalog_test_db2_set3", errMsg)) {
      cout << "Not able to create set: " + errMsg;
      exit(-1);
  }

  pdbClient.removeSet("catalog_test_db", "catalog_test_db_set1", errMsg);
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db", errMsg);

  pdbClient.removeSet("catalog_test_db", "catalog_test_db_set2", errMsg);
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db", errMsg);

  pdbClient.removeSet("catalog_test_db", "catalog_test_db_set3", errMsg);
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db", errMsg);

  pdbClient.removeSet("catalog_test_db2", "catalog_test_db2_set1", errMsg);
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2", errMsg);

  pdbClient.removeSet("catalog_test_db2", "catalog_test_db2_set2", errMsg);
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2", errMsg);

  pdbClient.removeSet("catalog_test_db2", "catalog_test_db2_set3", errMsg);
  pdbClient.listRegisteredSetsForADatabase("catalog_test_db2", errMsg);

  pdbClient.removeDatabase("catalog_test_db", errMsg);
  pdbClient.listRegisteredDatabases(errMsg);

  pdbClient.removeDatabase("catalog_test_db2", errMsg);
  pdbClient.listRegisteredDatabases(errMsg);

  pdbClient.listUserDefinedTypes(errMsg);

  pdbClient.registerType("libraries/libSillyJoin.so", errMsg);
  pdbClient.registerType("libraries/libScanIntSet.so", errMsg);
  pdbClient.listUserDefinedTypes(errMsg);

  pdbClient.registerType("libraries/libScanStringIntPairSet.so", errMsg);
  pdbClient.registerType("libraries/libScanStringSet.so", errMsg);

  pdbClient.listUserDefinedTypes(errMsg);
  pdbClient.registerType("libraries/libWriteStringSet.so", errMsg);
  pdbClient.listUserDefinedTypes(errMsg);

}

