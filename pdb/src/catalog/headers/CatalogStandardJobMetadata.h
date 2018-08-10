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
#ifndef PDB_CATALOGSTANDARDJOBMETADATA_H
#define PDB_CATALOGSTANDARDJOBMETADATA_H

#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include "CatalogStandardJobStageMetadata.h"

namespace pdb {

/**
 * The status of the job
 */
enum JobStatus {
  RUNNING,
  FINISHED
};

/**
 * Just a typedef to the shared pointer for convenience
 */
struct CatalogStandardJobMetadata;
typedef std::shared_ptr<CatalogStandardJobMetadata> CatalogStandardJobMetadataPtr;

struct CatalogStandardJobMetadata {

  /**
   * The job name
   */
  std::string id;

  /**
   * The tcap string of this job
   */
  std::string tcapString;

  /**
   * The status of the job
   */
  JobStatus status;

  /**
   * The timestamp when the job started
   */
  int64_t started;

  /**
   * The timestamp when the job ended
   */
  int64_t ended;

  /**
   * The job stages of this job
   */
  std::vector<CatalogStandardJobStageMetadata> jobStages;

};

}

#endif //PDB_CATALOGSTANDARDJOBMETADATA_H
