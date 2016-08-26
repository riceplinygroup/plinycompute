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
#ifndef SERVER_MAIN_CC
#define SERVER_MAIN_CC

#include <iostream>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include "PDBServer.h"
#include "CatalogClient.h"
#include "CatalogServer.h"

using namespace std;

int main(int numArgs, const char *args[]) {

	int port = 8108, serverNumConnections = 5, numDatabaseThreads = 5;
	string hostName("localhost"), IPCfile("/var/tmp/IPCFile");
	string frontendLogFile("frontendLogFile.log"), backendLogFile("backendLogFile.log");

	if (numArgs == 8) {
		port = atoi(args[1]);
		string tempHostName(args[2]);
		hostName = tempHostName;
		string tempIPC(args[3]);
		IPCfile = tempIPC;
		serverNumConnections = atoi(args[4]);
		numDatabaseThreads = atoi(args[5]);
		string tempFrontLog(args[6]);
		frontendLogFile = tempFrontLog;
		string tempBackEndLog(args[7]);
		backendLogFile = tempBackEndLog;

	} else if (numArgs != 1) {
		cout << "usage: ./PDBServer port hostName fileForLocalIPC serverNumThreads";
		cout << "  numDatabaseThreeads frontendLogFile backEndLogFile\n";
		return (0);
	}

	// start by forking off a server frontend
	pid_t pid = fork();

	if (pid < 0) {
		cout << "Error forking process to start up the frontend server.\n";
		perror("Error was: ");
		return (0);
	}

	// see if we are in the child process (this will be the server frontend)
	if (pid == 0) {

		// start up the server frontend
		cout << "Starting up the frontend server.\n";

		//set if this FrontEndServer is a Master Server or slave
		pdb :: PDBLoggerPtr myLogger = make_shared <pdb :: PDBLogger> (frontendLogFile);
		pdb :: PDBServer frontEnd (port, serverNumConnections, myLogger);
		frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir");
		frontEnd.addFunctionality <pdb :: CatalogClient> (port, hostName, myLogger);
		frontEnd.startServer (nullptr);
		return 0;
	}

	pid_t frontEndPID = pid;
	cout << "Frontend PID was " << frontEndPID << "\n";

	// now we just keep on forking off copies of the backend server, as long as it keeps crashing
	while (true) {

		pid = fork();
		if (pid < 0) {
			cout << "Error forking process to start up the backend server.\n";
			perror("Error was: ");
			cout << "\n";
			return (0);
		}

		// if we are in the child, then fork off a backend server
		if (pid == 0) {
			cout << "Starting up the backend server.\n";
			unlink(args[3]);
			pdb :: PDBLoggerPtr myLogger = make_shared <pdb :: PDBLogger> (backendLogFile);
			pdb :: PDBServer backEnd (IPCfile, numDatabaseThreads, myLogger);
			backEnd.addFunctionality <pdb :: CatalogClient> (port, hostName, myLogger);
			return 0;

			// otherwise, we are in the parent, so wait on the child to finish
		} else {

			cout << "Backend PID was " << pid << "\n";

			// loop until the server dies
			while (true) {

				// now, wait for the backend server to stop
				int result;
				pid_t outPID = waitpid(pid, &result, 0);

				if (outPID < 0) {
					cout << "Error waiting on child.\n";
					perror("Error was");
					cout << "So I am done.  Goodbye.\n";
					return 0;
				}

				if (WIFEXITED(result)) {
					cout << "It appears that the backend server exited normally.\n";
					cout << "So I am done.  Goodbye.\n";
					return 0;
				}

				if (WIFSIGNALED(result)) {
					cout << "It appears that the server backend has died.\n";
					cout << "I will start it up again.\n";
					break;
				}
			}

			// signal the frontend that the backend has died... in this case, all of the
			// threads in the frontend that were waiting for something from the backend
			cout << "Letting the frontend server know that the backend has died.\n";
			if (kill(frontEndPID, SIGUSR1) != 0) {

				cout << "This is bad: I could not let the frontend ";
				cout << " server know that the backend has died.\n";
				perror("Error was ");
				cout << "\n";
				return 0;
			}
		}
	}
}

#endif

