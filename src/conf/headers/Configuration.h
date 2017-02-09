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
/* 
 * File:   Configuration.h
 * Author: Jia
 *
 * Created on September 27, 2015, 12:42 PM
 */

#ifndef CONFIGURATION_H
#define	CONFIGURATION_H

#include <memory>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include "DataTypes.h"

#include "LogLevel.h"


using namespace std;

#ifndef DEFAULT_PAGE_SIZE
#define DEFAULT_PAGE_SIZE (64*1024*1024)
#endif

#ifndef DEFAULT_NET_PAGE_SIZE
#define DEFAULT_NET_PAGE_SIZE DEFAULT_PAGE_SIZE-(sizeof(NodeID) + sizeof(DatabaseID) + sizeof(UserTypeID) + sizeof(SetID) + sizeof(PageID))
#endif

#ifndef DEFAULT_MAX_CONNECTIONS
#define DEFAULT_MAX_CONNECTIONS 200
#endif

#ifndef DEFAULT_SHAREDMEM_SIZE
#define DEFAULT_SHAREDMEM_SIZE ((size_t)12*(size_t)1024*(size_t)1024*(size_t)1024)
#endif

#ifndef DEFAULT_NUM_THREADS
#define DEFAULT_NUM_THREADS 2
#endif

// create a smart pointer for Configuration objects
class Configuration;
typedef shared_ptr<Configuration> ConfigurationPtr;

class Configuration {
private:
	NodeID nodeId;
	string serverName;
        string serverAddress;
        bool isMasterCatalogServer;
        bool usePangea;
	int port;
	int maxConnections;
	string ipcFile;
	string logFile;
	unsigned int pageSize;
	bool useUnixDomainSock;
	size_t shmSize;
	bool logEnabled;
	string dataDirs;
	string metaDir;
	string metaTempDir;
	string dataTempDirs;
	unsigned int numThreads;
	string backEndIpcFile;


	bool isMaster;
	string masterNodeHostName;
	int masterNodePort;
	string queryPlannerPlace;
	LogLevel logLevel;
        string rootDir;
public:

	Configuration() {
	    //set default values.
	    this->nodeId = 0;
            this->logLevel = LogLevel::ERROR; 
	    serverName = "testServer";
	    serverAddress = "localhost";
	    isMasterCatalogServer = false;
	    usePangea = true;
	    port = 8108;
	    maxConnections = DEFAULT_MAX_CONNECTIONS;
	    logFile = "serverLog";
	    pageSize = DEFAULT_PAGE_SIZE;
	    useUnixDomainSock = false;
	    shmSize = DEFAULT_SHAREDMEM_SIZE;
	    logEnabled = false;
	    numThreads = DEFAULT_NUM_THREADS;
	    ipcFile = "/tmp/ipcFile";
	    backEndIpcFile = "/tmp/backEndIpcFile";
            isMaster = false;
            initDirs();
	}

        void initDirs() {
            rootDir = std::string("pdbRoot_")+serverAddress+std::string("_")+std::to_string(port);
            //temporarily added for unit tests
            this->createDir(rootDir);
            //dataDirs = "pdbRoot/data1,pdbRoot/data2,pdbRoot/data3,pdbRoot/data4,pdbRoot/data5,pdbRoot/data6,pdbRoot/data7,pdbRoot/data8,pdbRoot/data9,pdbRoot/data10,pdbRoot/data11,pdbRoot/data12";
            //dataDirs = "/data/data,/mnt/data";
            dataDirs = rootDir+std::string("/data");
            metaDir = rootDir+std::string("/meta");
            metaTempDir = rootDir+std::string("/metaTmp");
            //dataTempDirs = "/data/tmp,/mnt/tmp";
            dataTempDirs = rootDir+std::string("/tmp");
            //dataTempDirs = "/data10/tmp,/mnt/tmp";
            //dataTempDirs = "/data1/tmp,/data2/tmp,/data3/tmp,/data4/tmp,/data5/tmp,/data6/tmp,/data7/tmp,/data8/tmp,/data9/tmp,pdbRoot/tmp,/data10/tmp,/mnt/tmp";

        }


	NodeID getNodeID() const {
		return nodeId;
	}

	string getServerName() const {
		return serverName;
	}

	string getIpcFile() const {
		return ipcFile;
	}

	string getLogFile() const {
		return logFile;
	}

	int getMaxConnections() const {
		return maxConnections;
	}

	unsigned int getPageSize() const {
		return pageSize;
	}

	int getPort() const {
		return port;
	}

	size_t getShmSize() const {
		return shmSize;
	}

	bool isLogEnabled() const {
		return logEnabled;
	}

	bool isUseUnixDomainSock() const {
		return useUnixDomainSock;
	}

	string getDataDirs() const {
		return dataDirs;
	}

	string getMetaDir() const {
		return metaDir;
	}

	string getMetaTempDir() const {
		return metaTempDir;
	}

	string getDataTempDirs() const {
		return dataTempDirs;
	}

	unsigned int getNumThreads() const {
		return numThreads;
	}

	string getBackEndIpcFile() const {
		return backEndIpcFile;
	}

	void setNodeId(NodeID nodeId) {
		this->nodeId = nodeId;
	}

	void setServerName(string serverName) {
		this->serverName = serverName;
	}

	void setIpcFile(string ipcFile) {
		this->ipcFile = ipcFile;
	}

	void setLogFile(string logFile) {
		this->logFile = logFile;
	}

	void setMaxConnections(int maxConnections) {
		this->maxConnections = maxConnections;
	}

	void setPageSize(unsigned int pageSize) {
		this->pageSize = pageSize;
	}

	void setPort(int port) {
		this->port = port;
	}

	void setShmSize(size_t shmSize) {
		this->shmSize = shmSize;
	}

	void setUseUnixDomainSock(bool useUnixDomainSock) {
		this->useUnixDomainSock = useUnixDomainSock;
	}

	void setLogEnabled(bool logEnabled) {
		this->logEnabled = logEnabled;
	}

	void setDataDirs(string dataDirs) {
		this->dataDirs = dataDirs;
	}

	void setMetaDir(string metaDir) {
		this->metaDir = metaDir;
	}

	void setMetaTempDir(string tempDir) {
		this->metaTempDir = tempDir;
	}

	void setDataTempDirs(string tempDirs) {
		this->dataTempDirs = tempDirs;
	}

	void setNumThreads(unsigned int numThreads) {
		this->numThreads = numThreads;
	}

	void setBackEndIpcFile(string backEndIpcFile) {
		this->backEndIpcFile = backEndIpcFile;
	}

	void createDir(string path) {
		struct stat st = { 0 };
		if (stat(path.c_str(), &st) == -1) {
			mkdir(path.c_str(), 0777);
			//cout << "Created dir:" << path <<"\n";
		}
	}

	bool getIsMaster() const {
		return isMaster;
	}

	void setIsMaster(bool isMaster) {
		this->isMaster = isMaster;
	}

	string getMasterNodeHostName() const {
		return masterNodeHostName;
	}

	void setMasterNodeHostName(string masterNodeHostName) {
		this->masterNodeHostName = masterNodeHostName;
	}

	int getMasterNodePort() const {
		return masterNodePort;
	}

	void setMasterNodePort(int masterNodePort) {
		this->masterNodePort = masterNodePort;
	}

	const string getQueryPlannerPlace() const {
		return queryPlannerPlace;
	}

	void setQueryPlannerPlace(const string queryPlannerPlace) {
		this->queryPlannerPlace = queryPlannerPlace;
	}

	LogLevel getLogLevel() const {
		return logLevel;
	}

	void setLogLevel(LogLevel logLevel) {
		this->logLevel = logLevel;
	}

    void setMasterCatalogServer(bool isMasterCatalogServer) {
        this->isMasterCatalogServer = isMasterCatalogServer;
    }

    bool getMasterCatalogServer() {
        return this->isMasterCatalogServer;
    }

    void setServerAddress(string serverAddress) {
        this->serverAddress = serverAddress;
    }

    string getServerAddress() {
        return this->serverAddress;
    }

    void setUsePangea(bool usePangea) {
        this->usePangea = usePangea;
    }

    bool getUsePangea() {
        return this->usePangea;
    }



    void printOut(){
		cout<< "nodeID: "  << nodeId<<endl;
		cout<< "serverName: "  << serverName<<endl;
		cout<< "serverAddress: "  << serverAddress<<endl;
		cout<< "isMasterCatalogServer: "  << isMasterCatalogServer<<endl;
                cout<< "usePangea: "  << usePangea<<endl;
		cout<< "port: "  << port<<endl;
		cout<< "maxConnections: "  << maxConnections<<endl;
		cout<< "ipcFile: "  << ipcFile<<endl;
		cout<< "logFile: "  << logFile<<endl;
		cout<< "pageSize: "  << pageSize<<endl;
		cout<< "useUnixDomainSock: "  << useUnixDomainSock<<endl;
		cout<< "shmSize: "  << shmSize<<endl;
		cout<< "dataDirs: "  << dataDirs<<endl;
		cout<< "metaDir: "  << metaDir<<endl;
		cout<< "metaTempDir: "  << metaTempDir<<endl;
		cout<< "dataTempDirs: "  << dataTempDirs<<endl;
		cout<< "numThreads: "  << numThreads<<endl;
		cout<< "backEndIpcFile: "  << backEndIpcFile<<endl;
		cout<< "isMaster: "  << isMaster<<endl;
		cout<< "masterNodeHostName: "  << masterNodeHostName<<endl;
		cout<< "masterNodePort: "  << masterNodePort<<endl;
		cout<< "logEnabled: "  << logEnabled<<endl;


	}
};

#endif	/* CONFIGURATION_H */

