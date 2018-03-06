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
#include "JobStageBuilders/AggregationJobStageBuilder.h"

namespace pdb {

AggregationJobStageBuilder::AggregationJobStageBuilder() {

}

void AggregationJobStageBuilder::setJobId(const string &jobId) {
    AggregationJobStageBuilder::jobId = jobId;
}

void AggregationJobStageBuilder::setJobStageId(int jobStageId) {
    AggregationJobStageBuilder::jobStageId = jobStageId;
}

void AggregationJobStageBuilder::setAggComp(const Handle<AbstractAggregateComp> &aggComp) {
    AggregationJobStageBuilder::aggComp = aggComp;
}

void AggregationJobStageBuilder::setSourceContext(const Handle<SetIdentifier> &sourceContext) {
    AggregationJobStageBuilder::sourceContext = sourceContext;
}

void AggregationJobStageBuilder::setSinkContext(const Handle<SetIdentifier> &sinkContext) {
    AggregationJobStageBuilder::sinkContext = sinkContext;
}

void AggregationJobStageBuilder::setMaterializeOrNot(bool materializeOrNot) {
    AggregationJobStageBuilder::materializeOrNot = materializeOrNot;
}

Handle<AggregationJobStage> AggregationJobStageBuilder::build() {
    // create an instance of the AggregationJobStage
    Handle<AggregationJobStage> aggStage = makeObject<AggregationJobStage>(jobStageId,
                                                                           materializeOrNot,
                                                                           aggComp);
    // increase the job stage id since we are creating a new stage
    jobStageId++;

    // set the parameters
    aggStage->setSourceContext(sourceContext);
    aggStage->setSinkContext(sinkContext);
    aggStage->setOutputTypeName(aggComp->getOutputType());
    aggStage->setJobId(this->jobId);

    PDB_COUT << "TCAPAnalyzer generates AggregationJobStage:" << "\n";

    return aggStage;
}

}

