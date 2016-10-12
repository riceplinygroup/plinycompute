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

#ifndef PDB_LOGGER_C
#define PDB_LOGGER_C

#include <iostream>
#include "LockGuard.h"
#include "PDBLogger.h"
#include <stdio.h>

#include "LogLevel.h"

namespace pdb {

PDBLogger::PDBLogger(std :: string fName) {
    outputFile = fopen(std :: string("logs/"+fName).c_str(), "a");
    if (outputFile == nullptr) {
        std :: cout << "Unable to open logging file.\n";
        perror(nullptr);
        exit(-1);
    }
    pthread_mutex_init(&fileLock, nullptr);
    this->enabled = true;
    loglevel = TRACE;
}

void PDBLogger::open(std :: string fName) {
    const LockGuard guard{fileLock};
    if (outputFile != nullptr) {
        fclose(outputFile);
    }
    outputFile = fopen((std :: string("logs/")+fName).c_str(), "a");
    if (outputFile == nullptr) {
        std :: cout << "Unable to open logging file.\n";
        perror(nullptr);
        exit(-1);
    }
}

PDBLogger::PDBLogger() {
    pthread_mutex_init(&fileLock, nullptr);
    loglevel = TRACE;
}

PDBLogger::~PDBLogger() {

    if(outputFile != nullptr)
        fclose(outputFile);

    pthread_mutex_destroy(&fileLock);
}

//void PDBLogger::writeLn(std :: string writeMe) {
//    if (!this->enabled) {
//        return;
//    }
//    const LockGuard guard{fileLock};
//    if (writeMe[writeMe.length() - 1] != '\n') {
//        fprintf(outputFile, "%s\n", writeMe.c_str());
//    } else {
//        fprintf(outputFile, "%s", writeMe.c_str());
//    }
//    fflush(outputFile);
//}

//added by Jia
void PDBLogger::writeInt(int writeMe) {
    if (!this->enabled) {
        return;
    }
      writeLn(std :: to_string(writeMe));
//    const LockGuard guard{fileLock};
//    fprintf(outputFile, "%d\n", writeMe);
//    fflush(outputFile);
}


// Log Levels are:
//
//	OFF,
//	FATAL,
//	ERROR,
//	WARN,
//	INFO,
//	DEBUG,
//	TRACE

void PDBLogger::trace(std :: string writeMe) {
    if (!this->enabled || this->loglevel==OFF || this->loglevel==FATAL || this->loglevel==ERROR || this->loglevel==INFO || this->loglevel==DEBUG) {
        return;
    }
	this->writeLn("[TRACE] "+writeMe);
}

void PDBLogger::debug(std :: string writeMe) {
    if (!this->enabled || this->loglevel==OFF || this->loglevel==FATAL || this->loglevel==ERROR|| this->loglevel==INFO) {
        return;
    }
	this->writeLn("[DEBUG] "+writeMe);
}


void PDBLogger::info(std :: string writeMe) {
    if (!this->enabled || this->loglevel==OFF || this->loglevel==FATAL || this->loglevel==ERROR) {
        return;
    }
	this->writeLn("[INFO] "+writeMe);
}


void PDBLogger::warn(std :: string writeMe) {
    if (!this->enabled || this->loglevel==OFF || this->loglevel==FATAL || this->loglevel==ERROR) {
        return;
    }
	this->writeLn("[WARN] "+writeMe);
}


void PDBLogger::error(std :: string writeMe) {
    if (!this->enabled || this->loglevel==OFF || this->loglevel==FATAL ) {
        return;
    }
	this->writeLn("[ERROR] "+writeMe);
}


void PDBLogger::fatal(std :: string writeMe) {
    if (!this->enabled || this->loglevel==OFF) {
        return;
    }
	this->writeLn("[FATAL] "+writeMe);
}


// Added date/time to the logger
void PDBLogger::writeLn(std :: string writeMe) {

    if (!this->enabled || this->loglevel==OFF) {
        return;
    }

	const LockGuard guard { fileLock };

	// get the current time and date
	time_t now = time(0);
	struct tm tstruct;
	char buf[80];
	tstruct = *localtime(&now);
	strftime(buf, sizeof(buf), "[%Y-%m-%d-%X] ", &tstruct);

	// add date and time to the log
	writeMe = buf + writeMe;

	if (writeMe[writeMe.length() - 1] != '\n') {
		fprintf(outputFile, "%s\n", writeMe.c_str());
	} else {
		fprintf(outputFile, "%s", writeMe.c_str());
	}
	fflush(outputFile);
}


//added by Jia

void PDBLogger::write(char* data, unsigned int length) {
    if (!this->enabled) {
        return;
    }
    const LockGuard guard{fileLock};
    fwrite(data, sizeof (char), length, outputFile);


}

//added by Jia
void PDBLogger::setEnabled(bool enabled) {
    this->enabled = enabled;
}

LogLevel PDBLogger::getLoglevel() {
	return this->loglevel;
}

void PDBLogger::setLoglevel(LogLevel loglevel) {
	this->loglevel = loglevel;
}

}

#endif


