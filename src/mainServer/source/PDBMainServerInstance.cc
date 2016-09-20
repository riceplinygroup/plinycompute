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
#include <fstream>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <string>
#include <string.h>
#include <vector>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <stdio.h>

#include "Configuration.h"
#include "PDBLogger.h"
#include "SharedMem.h"
#include "BuiltInObjectTypeIDs.h"

#include "PDBLogger.h"
#include "LogLevel.h"



#include "PDBServer.h"
#include "CatalogServer.h"
#include "StorageServer.h"
#include "CatalogClient.h"
#include "QueryServer.h"

#include "DistributionManagerServer.h"
#include "DistributionManagerClient.h"
#include "PDBDistributionManager.h"

#include "InterfaceFunctions.h"
#include "NodeInfo.h"

namespace po = boost::program_options;

using namespace std;
using namespace pdb;


int main(int numArgs, const char *args[]) {

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


	int port;
	int masterNodePort;
	int maxConnections;
	int recvBufferSize;
	int inputBufferSize;
	int numThreads;

	size_t clientPageSize;
	size_t pageSize;
	size_t miniPageSize;
	size_t sharedMemSize;
	size_t memPerConnection;

	double flushThreshold;
	bool isMaster;
	bool logEnabled;
	bool useByteArray;
	bool useUnixDomainSock;

	bool enableStorage;
	bool enableCatalog;
	bool enableDM;

	// TODO: this list of config parameters should be checked - some of them might be depricated.
	po::options_description desc("Options");
	desc.add_options()
			("help", "produce help messages")
			("conf",     po::value<string>(&configurationFile), "configure files")
			("isMaster", po::value<bool>(&isMaster), "true if this node is master")
			("port",     po::value<int>(&port), "frontEnd server listening port, default is 8108. In the case of Client this is the frontEnd server to connect to")
			("serverName", po::value<string>(&serverName), "name of the node")
			("myIP", po::value<string>(&myIP), "public IP address of this server.")
			("ipcFile", po::value<string>(&ipcFile), "ipc file name for client-frontEnd communication, default is ipcFile")
			("useUnixDomainSock", po::value<bool>(&useUnixDomainSock), "whether to use local server for client-frontEnd communication, default is n")
			("backEndIpcFile", po::value<string>(&backEndIpcFile), "ipc file name for frontEnd-backEnd communication, default is backEndIpcFile")
			("recvBufferSize", po::value<int>(&recvBufferSize),	"maximal number of pages in the server receive buffer, default is 5")
			("maxConnections", po::value<int>(&maxConnections), "maximal concurrent connections, default is 50")
			("clientPageSize",	po::value<size_t>(&clientPageSize), "client page size, default is 4*1024*1024")
			("logFile", po::value<string>(&logFile), "log file name, default is serverLog")
			("pageSize", po::value<size_t>(&pageSize),"storage page size, default is 64*1024*1024")
			("miniPageSize", po::value<size_t>(&miniPageSize), "storage minipage size, default is 64")
			("sharedMemSize", po::value<size_t>(&sharedMemSize), "shared memory size, default is 768*1024*1024")
			("inputBufferSize", po::value<int>(&inputBufferSize), "maximum size of storage input buffer, default is 5")
			("logEnabled", po::value<bool>(&logEnabled), "y for enabling log, n for disabling log")
			("logLevel", po::value<string>(&logLevelSt), "Log Levels are: OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE")
			("useByteArray", po::value<bool>(&useByteArray), "y for useByteArray, n for use pdb::Object")
			("dataDirs", po::value<string>(&dataDirs), "a comma-separated list of local directories for storing data partitions")
			("metaDir", po::value<string>(&metaDir), "local directory to store meta partition")
			("tempDataDirs", po::value<string>(&tempDataDirs), "local directories to store temporary data")
			("tempMetaDir", po::value<string>(&tempMetaDir), "local directories to store temporary meta data")
			("flushThreshold", po::value<double>(&flushThreshold), "flush threshold, default is 0.1")
			("numThreads", po::value<int>(&numThreads), "number of threads for backEnd processing, default is 2")
			("memPerConnection", po::value<size_t>(&memPerConnection), "size of buffer per connection, default is 4*1024*1024")
			("queryPlannerPlace",	po::value<string>(&queryPlannerPlace), "hostname and port of the node that does query planner")
			("masterNodeHostName", po::value<string>(&masterNodeHostName), "hostname of the master node, default is localhost")
			("masterNodePort", po::value<int>(&masterNodePort), "port number of the master node, default is 8107")
			("catalogHostname", po::value<string>(&masterNodeHostName), "hostname of the catalog server, default is localhost")
			("enableStorage", po::value<bool>(&enableStorage), "y for enabling Storage, n for disabling Storage")
			("enableCatalog", po::value<bool>(&enableCatalog), "y for enabling Catalog, n for disabling Catalog")
			("enableDM", po::value<bool>(&enableDM), "y for enabling Distribution Manager, n for disabling Distribution Manager")
			;

	po::positional_options_description p;
	po::variables_map vm;
	po::store(po::command_line_parser(numArgs, args).options(desc).positional(p).run(), vm);
	po::notify(vm);


	if (vm.count("conf")) {
		configurationFile = vm["conf"].as<string>();
		cout << "Configuration file is" << configurationFile << ".\n";
	} else {
		configurationFile = "pdbSettings.conf";
	}

	// Load from configuration file.
	po::variables_map vm1;
	ifstream settings_file(configurationFile, fstream::in);

	po::store(po::parse_config_file(settings_file, desc), vm1);
	settings_file.close();
	po::notify(vm1);


	// Starting with all of the configuration parameters.
	ConfigurationPtr conf = make_shared<Configuration>();

	if (vm.count("help")) {
		cout << desc << "\n";
		return 1;
	}

	if (vm.count("port")) {
		port = vm["port"].as<int>();
		cout << "port was set to " << port << ".\n";
	}
	conf->setPort(port);

	if (vm.count("serverName")) {
		std::string serverName = vm["serverName"].as<std::string>();
		cout << "serverName was set to " << serverName << ".\n";
	}
	conf->setServerName("testServer");


	if (vm.count("isMaster")) {
		isMaster = vm["isMaster"].as<bool>();
		cout << "isMaster was set to " << isMaster << ".\n";
	}
	conf->setIsMaster(isMaster);

	if (vm.count("masterNodeHostName")) {
		masterNodeHostName = vm["masterNodeHostName"].as<string>();
		cout << "masterNodeHostName was set to " << masterNodeHostName << ".\n";
	}
	conf->setMasterNodeHostName(masterNodeHostName);

	if (vm.count("masterNodePort")) {
		masterNodePort = vm["masterNodePort"].as<int>();
		cout << "masterNodePort was set to " << masterNodePort << ".\n";
	}
	conf->setMasterNodePort(masterNodePort);

	if (vm.count("queryPlannerPlace")) {
		std::string queryPlannerPlace = vm["queryPlannerPlace"].as<std::string>();
		cout << "queryPlannerPlace was set to " << queryPlannerPlace << ".\n";
	}
	conf->setQueryPlannerPlace(queryPlannerPlace);



	if (vm.count("maxConnections")) {
		maxConnections = vm["maxConnections"].as<int>();
		cout << "maxConnections was set to " << maxConnections << ".\n";
	}
	conf->setMaxConnections(maxConnections);

	if (vm.count("recvBufferSize")) {
		recvBufferSize = vm["recvBUfferSize"].as<int>();
		cout << "recvBUfferSize was set to " << recvBufferSize << ".\n";
	}
//	conf->setRecvBufferSize(recvBufferSize);

	if (vm.count("inputBufferSize")) {
		inputBufferSize = vm["inputBufferSize"].as<int>();
		cout << "inputBufferSize was set to " << inputBufferSize << ".\n";
	}
//	conf->setInputBufferSize(inputBufferSize);

	if (vm.count("ipcFile")) {
		ipcFile = vm["ipcFile"].as<std::string>();
		cout << "ipcFile was set to " << ipcFile << ".\n";
	}
//	conf->setIpcFile(ipcFile);

	if (vm.count("backEndIpcFile")) {
		backEndIpcFile = vm["backEndIpcFile"].as<std::string>();
		cout << "backEndIpcFile was set to " << backEndIpcFile << ".\n";
	}
//	conf->setBackEndIpcFile(backEndIpcFile);

	if (vm.count("clientPageSize")) {
		clientPageSize = vm["clientPageSize"].as<size_t>();
		cout << "clientPageSize was set to " << clientPageSize << ".\n";
	}
//	conf->setClientPageSize(clientPageSize);

	if (vm.count("logFile")) {
		logFile = vm["logFile"].as<std::string>();
		cout << "logFile was set to " << logFile << ".\n";
	}
//	conf->setLogFile(logFile);

	if (vm.count("logLevel")) {
		logLevelSt = vm["logLevel"].as<std::string>();
		cout << "logLevel was set to " << logLevelSt << ".\n";
	}

	cout << "Log Level is set to " << logLevelSt << endl;

	if (vm.count("dataDirs")) {
		dataDirs = vm["dataDirs"].as<std::string>();
		cout << "directories for data persistence was set to " << dataDirs << ".\n";
	}
//	conf->setDataDirs(dataDirs);

	if (vm.count("metaDir")) {
		metaDir = vm["metaDir"].as<std::string>();
		cout << "directory for meta data persistence was set to " << metaDir << ".\n";
	}
//	conf->setMetaDir(metaDir);

	if (vm.count("tempDataDirs")) {
		tempDataDirs = vm["tempDataDirs"].as<std::string>();
		cout << "directories for temporary data persistence was set to " << tempDataDirs << ".\n";
	}
//	conf->setDataTempDirs(tempDataDirs);

	if (vm.count("tempMetaDir")) {
		tempMetaDir = vm["tempMetaDir"].as<std::string>();
		cout << "directory for temporary data persistence was set to " << tempMetaDir << ".\n";
	}
//	conf->setMetaTempDir(tempMetaDir);

	if (vm.count("pageSize")) {
		pageSize = vm["pageSize"].as<size_t>();
		cout << "pageSize was set to " << pageSize << ".\n";
	}
	conf->setPageSize(pageSize);

	if (vm.count("miniPageSize")) {
		miniPageSize = vm["miniPageSize"].as<size_t>();
		cout << "miniPageSize was set to " << miniPageSize << ".\n";
	}
//	conf->setMiniPageSize(miniPageSize);

	if (vm.count("sharedMemSize")) {
		sharedMemSize = vm["sharedMemSize"].as<size_t>();
		cout << "sharedMemSize was set to " << sharedMemSize << ".\n";
	}
	conf->setShmSize(sharedMemSize);

	if (vm.count("numThreads")) {
		numThreads = vm["numThreads"].as<int>();
		cout << "numThreads was set to " << numThreads << ".\n";
	}
	conf->setNumThreads(numThreads);

	if (vm.count("memPerConnection")) {
		memPerConnection = vm["memPerConnection"].as<size_t>();
		cout << "memPerConnection was set to " << memPerConnection << ".\n";
	}
//	conf->setMemPerConnection(memPerConnection);

	if (vm.count("useUnixDomainSock")) {
		useUnixDomainSock = vm["useUnixDomainSock"].as<bool>();
		cout << "useUnixDomainSock was set to " << useUnixDomainSock << ".\n";
	}
	conf->setUseUnixDomainSock(useUnixDomainSock);

	if (vm.count("logEnabled")) {
		logEnabled = vm["logEnabled"].as<bool>();
		cout << "logEnabled was set to " << logEnabled << ".\n";

	}
	conf->setLogEnabled(logEnabled);

	if (vm.count("useByteArray")) {
		useByteArray = vm["useByteArray"].as<bool>();
		cout << "useByteArray was set to " << useByteArray << ".\n";

	}
//  conf->setUseByteArray(useByteArray);

	if (vm.count("flushThreshold")) {
		flushThreshold = vm["flushThreshold"].as<double>();
		cout << "flushThreshold was set to " << flushThreshold << ".\n";
	}

	if (vm.count("enableStorage")) {
		enableStorage = vm["enableStorage"].as<bool>();
		cout << "enableStorage was set to " << logEnabled << ".\n";

	}

	if (vm.count("enableCatalog")) {
		enableCatalog = vm["enableCatalog"].as<bool>();
		cout << "enableCatalog was set to " << logEnabled << ".\n";
	}


	if (vm.count("enableDM")) {
		enableDM = vm["enableDM"].as<bool>();
		cout << "enableDM was set to " << logEnabled << ".\n";
	}

	if (vm.count("myIP")) {
		myIP = vm["myIP"].as<std::string>();
		cout << "Public IP address of this server  was set to " << myIP << ".\n";
	}

//	conf->setFlushThreshold(flushThreshold);

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


	cout << "#################################################" << endl;
	cout << "######                                     ######" << endl;
	cout << "######            Pliny Database           ######" << endl;
	cout << "######                                     ######" << endl;
	cout << "#################################################" << endl;


    pdb :: PDBServer frontEnd (port, maxConnections, logger);

    // CATALOG
    if(enableCatalog){
    	std :: cout << "Catalog Server is enabled. Adding a Catalog server!!\n\n";
    	frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir");
    	frontEnd.addFunctionality <pdb :: CatalogClient> (port, "localhost", logger);
    }else{
    	std :: cout << "Catalog Server is disabled!\n\n";
    }


    //STORAGE
    if(enableStorage){
    	std :: cout << "Storage Server is enabled. Adding a Storage server!!\n\n";
    	frontEnd.addFunctionality <pdb :: StorageServer> (dataDirs, pageSize, 128);
    	frontEnd.addFunctionality <pdb :: QueryServer> (8);
    }else{
    	std :: cout << "Storage Server is disabled!\n\n";
    }

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
//		frontEnd.getFunctionality<pdb::DistributionManagerServer>().setDistributionManager();
	}

    }else{
    	std :: cout << "Distribution Manager Server is disabled!\n\n";
    }



	std :: cout << "Staring up all Server functionalities.\n";
    frontEnd.startServer (nullptr);
}

