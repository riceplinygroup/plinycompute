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
#include <sstream>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <map>
#include <ctype.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>




#include "Configuration.h"
#include "PDBLogger.h"
#include "SharedMem.h"
#include "BuiltInObjectTypeIDs.h"
#include "CatalogServer.h"
#include "CatalogClient.h"
#include "StorageClient.h"
#include "PangeaStorageServer.h"
#include "FrontendQueryTestServer.h"
#include "HermesExecutionServer.h"

#include "PDBLogger.h"
#include "LogLevel.h"

#include "PDBServer.h"
#include "CatalogServer.h"
#include "CatalogClient.h"

#include "DistributionManagerServer.h"
#include "DistributionManagerClient.h"
#include "PDBDistributionManager.h"

#include "InterfaceFunctions.h"
#include "NodeInfo.h"


using namespace std;

map<string, string> readConfigFile(const char* m_configFile) {
	map<string, string> keyValues;

	std::ifstream infile(m_configFile);

	std::string line;
	while (std::getline(infile, line)) {
		// check if the first charachter of the line is a comment starting with #
		char found = line.find_first_not_of(" \t");
		if (found != string::npos) {
			if (line[found] == '#')
				continue;
			else {
				std::istringstream is_line(line);
				std::string key;
				// looking for = in the line and getting the keys and values
				if (std::getline(is_line, key, '=')) {
					std::string value;
					// this line removes the space at the end of the string if there are any sppaces
					key.erase(std::remove_if(key.begin(), key.end(), ::isspace), key.end());
					if (std::getline(is_line, value))
						// this line removes the space at the end of the string if there are any sppaces
						value.erase(std::remove_if(value.begin(), value.end(), ::isspace), value.end());

					// removes comma at the end of the line
					value.erase(std::remove(value.begin(), value.end(), ','), value.end());

					// Checks if the given character is a punctuation character as classified by the current C locale. The default C locale classifies
					// the characters !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~ as punctuation.
					// value.erase(std::remove_if(value.begin(), value.end(), ::ispunct), value.end());

					// print out all of the key/values
					// std::cout << key << "," << value << std::endl;

					//  store_line(key, value);
					keyValues.insert(std::make_pair(key, value));
				}
			}
		}
	}

	return keyValues;
}

// helper function to map string to boolean
bool to_bool(std::string str) {
	std::transform(str.begin(), str.end(), str.begin(), ::tolower);
	std::istringstream is(str);
	bool b;
	is >> std::boolalpha >> b;
	return b;
}

int main(int argc, char **argv) {

	string serverName;
	string myIP;
	string catalogHostname;
	string ipcFile;
	string backEndIpcFile;
	string logFile;
	string dataDirs;
	string metaDir;
	string tempDataDirs;
	string tempMetaDir;
	string masterNodeHostName;
	string queryPlannerPlace;
	string logLevelSt;
	string configurationFile;

	int port=8080;
	int masterNodePort=8080;
	int maxConnections=20;
	int numThreads;

	size_t pageSize=0;
	size_t sharedMemSize=0;


	bool isMaster = true;
	bool logEnabled = true;
	bool enableStorage = true;
	bool enableCatalog = true;
	bool enableDM = true;
	bool useUnixDomainSock=false;

	const char* m_configFile = "./conf/pdbSettings.conf";

	int c;
	opterr = 0;
	const char* MasterNodeAddress = "localhost";

	while ((c = getopt(argc, argv, "s:c:sc:cs:")) != -1)

		switch (c) {
		case 's':
			isMaster = false;
			MasterNodeAddress = optarg;
			break;
		case 'c':
			m_configFile = optarg;
			break;
		case '?':
			if (optopt == 'c')
				fprintf(stderr, "Option -%c requires an argument.\n", optopt);
			else if (isprint(optopt))
				fprintf(stderr, "Unknown option `-%c'.\n", optopt);
			else
				fprintf(stderr, "Unknown option character `\\x%x'.\n", optopt);
			return 1;
		default:
			abort();
		}

	std::string MasterNodeAddressString(MasterNodeAddress);
	cout << "isMaster: " << isMaster << endl;
	if(!isMaster)
	    cout << "Master Node Address is: " << MasterNodeAddressString << endl;


	// ##################################################
	// ###########                          #############
	// ###########    READING CONFIG FILE   #############
	// ###########                          #############
	// ##################################################




	// read the config file and create a map out of it.
	map<string, string> keyValues;
	keyValues = readConfigFile(m_configFile);

	// We go through the map and find the configuration keys.

	if (keyValues.find("port") != keyValues.end()) {
		port = stoi(keyValues["port"]);
		cout << "Server Port: " << port << endl;
	}


	if (keyValues.find("serverName") != keyValues.end()) {
		serverName = keyValues["serverName"];
		cout << "Server Name: " << serverName << endl;
	}


	if (keyValues.find("myIP") != keyValues.end()) {
		myIP = keyValues["myIP"];
		cout << "Server IP: " << myIP << endl;
	}

	// enableStorage
	if (keyValues.find("enableStorage") != keyValues.end()) {
		enableStorage = to_bool(keyValues["enableStorage"]);
		cout << "enableStorage: " << enableStorage << endl;
	}

	// enableCatalog
	if (keyValues.find("enableCatalog") != keyValues.end()) {
		enableCatalog = to_bool(keyValues["enableCatalog"]);
		cout << "enableCatalog: " << enableCatalog << endl;
	}

	// enableDM
	if (keyValues.find("enableDM") != keyValues.end()) {
		enableDM = to_bool(keyValues["enableDM"]);
		cout << "enableDM: " << enableDM << endl;
	}

	// useUnixDomainSock
	if (keyValues.find("useUnixDomainSock") != keyValues.end()) {
		useUnixDomainSock = to_bool(keyValues["useUnixDomainSock"]);
		cout << "useUnixDomainSock: " << useUnixDomainSock << endl;
	}

	// maxConnections
	if (keyValues.find("maxConnections") != keyValues.end()) {
		maxConnections = stoi(keyValues["maxConnections"]);
		cout << "maxConnections: " << maxConnections << endl;
	}

	// logFile
	if (keyValues.find("logFile") != keyValues.end()) {
		logFile = keyValues["logFile"];
		cout << "logFile: " << logFile << endl;
	}

	// pageSize
	if (keyValues.find("pageSize") != keyValues.end()) {
		unsigned int pageSizeInt = stoi(keyValues["pageSize"]);
		pageSize =(size_t) pageSizeInt;
		cout << "pageSize: " << pageSize << endl;
	}

	//sharedMemSize
	if (keyValues.find("sharedMemSize") != keyValues.end()) {
		unsigned long sharedMemSizeLong = stol(keyValues["sharedMemSize"]);
		sharedMemSize =(size_t) sharedMemSizeLong;
		cout << "sharedMemSize: " << sharedMemSize << endl;
	}

	// logEnabled
	if (keyValues.find("logEnabled") != keyValues.end()) {
		logEnabled = to_bool(keyValues["logEnabled"]);
		cout << "logEnabled: " << logEnabled << endl;
	}

	// logLevel
	if (keyValues.find("logLevel") != keyValues.end()) {
		logLevelSt = keyValues["logLevel"];
		cout << "logLevel: " << logLevelSt << endl;
	}

	// dataDirs
	if (keyValues.find("dataDirs") != keyValues.end()) {
		dataDirs = keyValues["dataDirs"];
		cout << "dataDirs: " << dataDirs << endl;
	}

	// metaDir
	if (keyValues.find("metaDir") != keyValues.end()) {
		metaDir = keyValues["metaDir"];
		cout << "metaDir: " << metaDir << endl;
	}

	// tempDataDirs
	if (keyValues.find("tempDataDirs") != keyValues.end()) {
		tempDataDirs = keyValues["tempDataDirs"];
		cout << "tempDataDirs: " << tempDataDirs << endl;
	}

	// tempMetaDir
	if (keyValues.find("tempMetaDir") != keyValues.end()) {
		tempMetaDir = keyValues["tempMetaDir"];
		cout << "tempMetaDir: " << tempMetaDir << endl;
	}

	// numThreads
	if (keyValues.find("numThreads") != keyValues.end()) {
		numThreads = stoi(keyValues["numThreads"]);
		cout << "numThreads: " << numThreads << endl;
	}

//	// isMaster
//	if (keyValues.find("isMaster") != keyValues.end()) {
//        // no need to do that - default is master
//		//		isMaster = to_bool(keyValues["isMaster"]);
//		cout << "isMaster: " << isMaster << endl;
//	}

	// masterNodeHostName
	if (keyValues.find("masterNodeHostName") != keyValues.end()) {
		masterNodeHostName = keyValues["masterNodeHostName"];
		cout << "masterNodeHostName: " << masterNodeHostName << endl;
	}


	// masterNodePort
	if (keyValues.find("masterNodePort") != keyValues.end()) {
		masterNodePort = stoi(keyValues["masterNodePort"]);
		cout << "masterNodePort: " << masterNodePort << endl;
	}

	// queryPlannerPlace
	if (keyValues.find("queryPlannerPlace") != keyValues.end()) {
		queryPlannerPlace = keyValues["queryPlannerPlace"];
		cout << "queryPlannerPlace: " << queryPlannerPlace << endl;
	}



	cout << "#################################################" << endl;
	cout << "######                                     ######" << endl;
	cout << "######            Pliny Database           ######" << endl;
	cout << "######                                     ######" << endl;
	cout << "#################################################" << endl;

	// Starting with all of the configuration parameters.
	ConfigurationPtr conf = make_shared<Configuration>();

	conf->setServerName("testServer");
	conf->setPort(port);
	conf->setIsMaster(isMaster);
	conf->setMasterNodeHostName(masterNodeHostName);
	conf->setMasterNodePort(masterNodePort);
	conf->setQueryPlannerPlace(queryPlannerPlace);
	conf->setUseUnixDomainSock(useUnixDomainSock);
	conf->setShmSize(sharedMemSize);

	// now print out the configurations
	conf->printOut();



	//START A FRONTEND SERVER and a Forked backend server
	PDBLoggerPtr logger = make_shared<PDBLogger>(logFile);
	logger->setEnabled(logEnabled);

	// 	OFF,
	//	FATAL, - this is the lowest level.
	//	ERROR,
	//	WARN,
	//	INFO,
	//	DEBUG,
	//	TRACE  - this is the highest level that outouts everything
	if (logLevelSt == "OFF")
		logger->setLoglevel(LogLevel::OFF);
	else if (logLevelSt == "FATAL")
		logger->setLoglevel(LogLevel::FATAL);
	else if (logLevelSt == "ERROR")
		logger->setLoglevel(LogLevel::ERROR);
	else if (logLevelSt == "WARN")
		logger->setLoglevel(LogLevel::WARN);
	else if (logLevelSt == "INFO")
		logger->setLoglevel(LogLevel::INFO);
	else if (logLevelSt == "DEBUG")
		logger->setLoglevel(LogLevel::DEBUG);
	else if (logLevelSt == "TRACE")
		logger->setLoglevel(LogLevel::TRACE);
	else
		logger->setLoglevel(LogLevel::TRACE);


	cout << "Log Level is set to " << logger->getLoglevel() << endl;

	SharedMemPtr shm = make_shared<SharedMem>(conf->getShmSize(), logger);



    //STORAGE
    if(enableStorage){
    	std :: cout << "Storage Server is enabled. Adding a Storage server!!\n\n";

        if(shm != nullptr) {
                pid_t child_pid = fork();
                if(child_pid == 0) {
                    //I'm the backend server
                    pdb :: PDBLoggerPtr logger = make_shared <pdb :: PDBLogger> ("backendLogFile.log");
                    pdb :: PDBServer backEnd (conf->getBackEndIpcFile(), 100, logger);
                    backEnd.addFunctionality<pdb :: HermesExecutionServer>(shm, backEnd.getWorkerQueue(), logger, conf);
                    bool usePangea = true;
                    backEnd.addFunctionality<pdb :: StorageClient> (port, myIP, make_shared <pdb :: PDBLogger> ("clientLog"), usePangea);
                    backEnd.startServer(nullptr);

                } else if (child_pid == -1) {
                    std :: cout << "Fatal Error: fork failed." << std :: endl;
                } else {
                    // allocates memory
                    makeObjectAllocatorBlock (1024 * 1024 * 24, true);

                    //I'm the frontend server
                    pdb :: PDBServer frontEnd (masterNodePort, 100, logger);
                    frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir", isMaster, masterNodeHostName, masterNodePort);
                    frontEnd.addFunctionality <pdb :: CatalogClient> (port, myIP, logger);
                    frontEnd.addFunctionality<pdb :: PangeaStorageServer> (shm, frontEnd.getWorkerQueue(), logger, conf);
                    frontEnd.getFunctionality<pdb :: PangeaStorageServer>().startFlushConsumerThreads();
                    frontEnd.addFunctionality<pdb :: FrontendQueryTestServer>();

                    // not sure if this is the right place to put the DM
                    // Distribution Manager
                    if(enableDM){
                    std :: cout << "Distribution Manager server is enabled. Adding a  Distribution Manager server!!\n";


                    if (!isMaster) {
                        std::cout << "Distribution Manager is a Slave Node.\n" << std::endl;


                        // TODO: find a way to not start the heart beat operation here.
                        // Get the functionality back to start the heart beat.

                        bool wasError;
                        std::string errMsg;

                        pdb::String hostname(myIP);
                        frontEnd.addFunctionality <pdb :: DistributionManagerClient>(hostname, port , logger);
                        pdb::DistributionManagerClient myDMClient = frontEnd.getFunctionality<pdb::DistributionManagerClient>();
                        myDMClient.sendHeartBeat(masterNodeHostName, masterNodePort, wasError, errMsg);
                        std::cout << errMsg << std::endl;

                    }else{
                        PDBDistributionManagerPtr myDM=make_shared<PDBDistributionManager>();
                        frontEnd.addFunctionality<pdb::DistributionManagerServer>(myDM);
                        // make the distribution and set it to the distribution manager server.
                //      frontEnd.getFunctionality<pdb::DistributionManagerServer>().setDistributionManager();
                    }

                    }else{
                        std :: cout << "Distribution Manager Server is disabled!\n\n";
                    }
                    std :: cout << "Staring up all Server functionalities.\n";
                    frontEnd.startServer (nullptr);

                }

        }
        //old ones
//    	frontEnd.addFunctionality <pdb :: StorageServer> (dataDirs, pageSize, 128);
//    	frontEnd.addFunctionality <pdb :: QueryServer> (8);
    }else{
    	std :: cout << "Storage Server is disabled!\n\n";
    }
	return 0;
}

