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

#ifndef PDB_JOBTRACKER_H
#define PDB_JOBTRACKER_H

#include <ServerFunctionality.h>
#include <TupleSetJobStage.h>
#include <AggregationJobStage.h>
#include <BroadcastJoinBuildHTJobStage.h>
#include <HashPartitionedJoinBuildHTJobStage.h>
#include <CatalogStandardJobMetadata.h>
#include <mutex>

namespace pdb {

class JobTracker : public ServerFunctionality {

public:

  JobTracker() = default;

  /**
   * Starts tracking a job
   * @param jobID - the job id of the job we are tracking
   */
  bool startJob(string &jobID);

  /**
   * Finish the tracking of the job
   * @param jobID - the job id of the job we are ending
   */
  bool finishJob(string &jobID);

  /**
   * Sets the tcap graph for a job we are tracking
   * @param tcap
   */
  bool setJobTCAP(std::string &jobID, std::string &tcap);

  /**
   * Return all the jobs
   * @return the jobs returned
   */
  std::vector<CatalogStandardJobMetadata> getAllJobs();

  /**
   * Returns the job with a particluar job id
   * @return the job returned
   */
  std::pair<CatalogStandardJobMetadata, bool> getJob(string &jobID);

  /**
   * Log a TupleSetJobStage job stage we are for the job tracker to store
   */
  void logJobStage(TupleSetJobStage jobStage);

  /**
   * Log a AggregationJobStage job stage we are for the job tracker to store
   */
  void logJobStage(AggregationJobStage jobStage);

  /**
   * Log a BroadcastJoinBuildHTJobStage job stage we are for the job tracker to store
   */
  void logJobStage(BroadcastJoinBuildHTJobStage jobStage);

  /**
   * Log a HashPartitionedJoinBuildHTJobStage job stage we are for the job tracker to store
   */
  void logJobStage(HashPartitionedJoinBuildHTJobStage jobStage);

  /**
   * Registers the handles currently does nothing...
   * @param forMe - the server
   */
  void registerHandlers(PDBServer &forMe) override;

 private:

  /**
   * Map of all the jobs. Maps the name of the job to the job
   */
  std::map<std::string, CatalogStandardJobMetadataPtr> jobs;

  /**
   * Used to synchronize the access to the job tracker
   */
  std::mutex mutex;

};

}

#endif //PDB_JOBTRACKER_H
