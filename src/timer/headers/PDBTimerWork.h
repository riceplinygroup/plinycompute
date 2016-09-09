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
 * File:   PDBTimerWork.h
 * Author: Jia
 *
 * Created on October 19, 2015, 10:15 PM
 */

#ifndef PDBTIMERWORK_H
#define	PDBTIMERWORK_H

#include <memory>
using namespace std;



#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include "PDBTimer.h"
#include <list>
#include "PDBWorkerQueue.h"
#include "PDBBuzzer.h"

/**
 * This class implements PDBTimerWork class that will
 * be executed in a thread to manage timers for the PDB server.
 */
namespace pdb {

class PDBTimerWork;
typedef shared_ptr<PDBTimerWork> PDBTimerWorkPtr;


class PDBTimerWork: public PDBWork {
public:
	/**
	 * Create a PDBTimerWork instance.
	 */
	PDBTimerWork();

	/**
	 * Create a PDBTimerWork instance and specify the worker queue and timer resolution.
	 * - PDBWorkerQueuePtr workers: a smart pointer pointing to thread pool;
	 * - tickFrequency: how many ticks in a second, which defines the timer resolution.
	 */
	PDBTimerWork(PDBWorkerQueuePtr workers, long long tickFrequency);


	~PDBTimerWork();

	/**
	 * Work to do.
	 */
	void execute(PDBBuzzerPtr callerBuzzer);

	/**
	 * Add a new timer to allow worker to manage it.
	 */
	void addTimer(PDBTimerPtr timer);

protected:
	/**
	 * The main loop to check and execute work for expired timer periodically.
	 */
	void timerHandler();

	/**
	 * Update the current ticks, so that we can compare it with timer's expire ticks
	 * to see whether a timer has expired.
	 */
	void updateTicks();

private:
	list<PDBTimerPtr> * timerList;
	PDBWorkerQueuePtr workerQueue;

	/*specify how many ticks in a second.
	 *This controls the resolution of timer. By default a tick represents a second.
	 */
	long long tickFrequency;
	ticks_t ticks;
	ticks_t prevTicks; // last time we ran the timer
	bool runTimer;
	struct timeval lastTime;

};
}

#endif	/* PDBTIMERWORK_H */

