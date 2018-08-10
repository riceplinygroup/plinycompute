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

#include <JobModel.h>

#include "JobModel.h"
#include <boost/algorithm/string/replace.hpp>


JobModel::JobModel(JobTracker *jobTracker) : jobTracker(jobTracker) {}


mustache::data JobModel::getJobs() {

  // the return value
  mustache::data jobLists = mustache::data::type::list;

  // go through each job
  auto jobs = jobTracker->getAllJobs();
  for(auto &job : jobs) {

    // the job
    mustache::data jobData = mustache::data();

    // set the job data
    jobData.set("id", job.id);
    jobData.set("status", job.status == JobStatus::RUNNING ? "running" : "finished");
    jobData.set("started", std::to_string(job.started));
    jobData.set("ended", std::to_string(job.ended));
    jobData.set("is-last", jobLists.list_value().size() == (jobs.size() - 1));

    // add the job
    jobLists.push_back(jobData);
  }

  // return the thing
  mustache::data ret = mustache::data();
  ret.set("jobs", jobLists);
  ret["success"] = true;

  return ret;
}

mustache::data JobModel::getJob(std::string jobID) {

  mustache::data ret = mustache::data();

  // the job
  mustache::data jobData = mustache::data();

  // grab the job
  auto searchResult = jobTracker->getJob(jobID);

  // return the result
  if(!searchResult.second) {
    ret.set("success", false);
    return ret;
  }

  // grab the job since we found one
  auto job = searchResult.first;

  // set the job data
  jobData.set("id", job.id);
  jobData.set("status", job.status == JobStatus::RUNNING ? "running" : "finished");
  jobData.set("started", std::to_string(job.started));
  jobData.set("ended", std::to_string(job.ended));
  std::string tmp = boost::replace_all_copy(job.tcapString, "\n", "\\n");
  jobData.set("tcap-string", tmp);

  ret.set("job", jobData);
  ret.set("success", true);

  return ret;
}
