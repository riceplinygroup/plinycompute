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

#include <JobTracker.h>

#include "JobTracker.h"

bool JobTracker::startJob(string &jobID) {

  // lock
  std::unique_lock<std::mutex> lck (mutex);

  // create a job pointer
  CatalogStandardJobMetadataPtr job = std::make_shared<CatalogStandardJobMetadata>();

  // set the job id
  job->id = jobID;
  job->status = JobStatus::RUNNING;
  job->started = time(nullptr);

  // check if there is a job with this id
  if(jobs.find(jobID) != jobs.end()) {
    return false;
  }

  // set the job
  jobs[jobID] = job;

  return true;
}

bool JobTracker::finishJob(string &jobID) {

  // lock
  std::unique_lock<std::mutex> lck (mutex);

  // check if there is a job with this id
  if(jobs.find(jobID) == jobs.end()) {
    return false;
  }

  // set the status
  jobs[jobID]->status = JobStatus::FINISHED;
  jobs[jobID]->ended = time(nullptr);

  return true;
}

bool JobTracker::setJobTCAP(std::string &jobID, std::string &tcap) {

  // lock
  std::unique_lock<std::mutex> lck (mutex);

  // check if there is a job with this id
  if(jobs.find(jobID) == jobs.end()) {
    return false;
  }

  // set the status
  jobs[jobID]->tcapString = tcap;

  return true;
}

std::vector<CatalogStandardJobMetadata> JobTracker::getAllJobs() {

  // the return value
  vector<CatalogStandardJobMetadata> ret;

  // lock
  std::unique_lock<std::mutex> lck (mutex);

  // go through each job
  for(auto it : this->jobs) {
    ret.push_back(*it.second);
  }

  // return the value
  return std::move(ret);
}

void JobTracker::registerHandlers(PDBServer &forMe) {}

std::pair<CatalogStandardJobMetadata, bool> JobTracker::getJob(string &jobID) {

  // lock
  std::unique_lock<std::mutex> lck (mutex);

  if(jobs.find(jobID) == jobs.end()) {
    return std::make_pair<CatalogStandardJobMetadata, bool>(CatalogStandardJobMetadata(), false);
  }

  // return the value
  return std::make_pair(*jobs[jobID], true);
}

void JobTracker::logJobStage(TupleSetJobStage jobStage) {

}

void JobTracker::logJobStage(AggregationJobStage jobStage) {

}

void JobTracker::logJobStage(BroadcastJoinBuildHTJobStage jobStage) {

}

void JobTracker::logJobStage(HashPartitionedJoinBuildHTJobStage jobStage) {

}


