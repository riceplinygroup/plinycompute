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

#ifndef PIPELINE_H
#define PIPELINE_H

#include "ComputeSource.h"
#include "ComputeSink.h"
#include "UseTemporaryAllocationBlock.h"
#include "Handle.h"
#include <queue>

#ifndef MIN_BATCH_SIZE
#define MIN_BATCH_SIZE 10
#endif


namespace pdb {

// this is used to buffer unwritten pages
struct MemoryHolder {

    // the output vector that this guy stores
    Handle<Object> outputSink;

    // his memory
    void* location;

    // the iteration where he was last written...
    // we use this beause we canot delete
    int iteration;

    void setIteration(int iterationIn) {
        if (outputSink != nullptr)
            getRecord(outputSink);
        iteration = iterationIn;
    }

    MemoryHolder(std::pair<void*, size_t> buildMe) {
        location = buildMe.first;
        makeObjectAllocatorBlock(location, buildMe.second, true);
        outputSink = nullptr;
    }
};

typedef std::shared_ptr<MemoryHolder> MemoryHolderPtr;

// this is a prototype for the pipeline
class Pipeline {

private:
    // this is a function that the pipeline calls to obtain a new page to
    // write output to.  The function returns a pair.  The first item in
    // the pair is the page, the second is the number of bytes in the page
    std::function<std::pair<void*, size_t>()> getNewPage;

    // this is a function that the pipeline calls to write back a page.
    // The first arg is the page to write back (and free), and the second
    // is the size of the page
    std::function<void(void*)> writeBackPage;

    // this is a function that the pipieline calls to free a page, without
    // writing it back (because it has no useful data)
    std::function<void(void*)> discardPage;

    // this is the source of data in the pipeline
    ComputeSourcePtr dataSource;

    // this is where the pipeline goes to write the data
    ComputeSinkPtr dataSink;

    // here is our pipeline
    std::vector<ComputeExecutorPtr> pipeline;

    // and here is all of the pages we've not yet written back
    std::queue<MemoryHolderPtr> unwrittenPages;

public:
    // the first argument is a function to call that gets a new output page...
    // the second arguement is a function to call that deals with a full output page
    // the third argument is the iterator that will create TupleSets to process
    Pipeline(std::function<std::pair<void*, size_t>()> getNewPage,
             std::function<void(void*)> discardPage,
             std::function<void(void*)> writeBackPage,
             ComputeSourcePtr dataSource,
             ComputeSinkPtr tupleSink)
        : getNewPage(getNewPage),
          writeBackPage(writeBackPage),
          discardPage(discardPage),
          dataSource(dataSource),
          dataSink(tupleSink) {}

    // adds a stage to the pipeline
    void addStage(ComputeExecutorPtr addMe) {
        pipeline.push_back(addMe);
    }

    ~Pipeline() {

        // kill all of the pipeline stages
        while (pipeline.size())
            pipeline.pop_back();

        // first, reverse the queue so we go oldest to newest
        // this ensures that everything is deleted in the reverse order that it was created
        std::vector<MemoryHolderPtr> reverser;
        while (unwrittenPages.size() > 0) {
            reverser.push_back(unwrittenPages.front());
            unwrittenPages.pop();
        }

        while (reverser.size() > 0) {
            unwrittenPages.push(reverser.back());
            reverser.pop_back();
        }

        // write back all of the pages
        cleanPages(999999999);

        if (unwrittenPages.size() != 0)
            std::cout
                << "This is bad: in destructor for pipeline, still some pages with objects!!\n";
    }

    // writes back any unwritten pages
    void cleanPages(int iteration) {

        // take care of getting rid of any pages... but only get rid of those from two iterations
        // ago...
        // pages from the last iteration may still have pointers into them
        PDB_COUT << "to clean page for iteration-" << iteration << std::endl;
        PDB_COUT << "unwrittenPages.size() =" << unwrittenPages.size() << std::endl;

        while (unwrittenPages.size() > 0 && iteration > unwrittenPages.front()->iteration + 1) {
            PDB_COUT << "unwrittenPages.front()->iteration=" << unwrittenPages.front()->iteration
                     << std::endl;
            // in this case, the page did not have any output data written to it... it only had
            // intermediate results, and so we will just discard it
            if (unwrittenPages.front()->outputSink == nullptr) {
                if (getNumObjectsInAllocatorBlock(unwrittenPages.front()->location) != 0) {

                    // this is bad... there should not be any objects here because this memory
                    // chunk does not store an output vector
                    emptyOutContainingBlock(unwrittenPages.front()->location);

                    std::cout << "This is Strange... how did I find a page with objects??\n";
                }

                discardPage(unwrittenPages.front()->location);
                unwrittenPages.pop();

                // in this case, the page DID have some data written to it
            } else {
                // and force the reference count for this guy to go to zero
                PDB_COUT << "to empty out containing block" << std::endl;
                unwrittenPages.front()->outputSink.emptyOutContainingBlock();

                // OK, because we will have invalidated the current object allocator block, we need
                // to
                // create a new one, or this could cause a lot of problems!!
                if (iteration == 999999999)
                    makeObjectAllocatorBlock(1024, true);

                // make sure he is written
                writeBackPage(unwrittenPages.front()->location);

                // and get ridda him
                unwrittenPages.pop();
            }
        }
    }

    // runs the pipeline
    void run() {
        // this is where we are outputting all of our results to
        MemoryHolderPtr myRAM = std::make_shared<MemoryHolder>(getNewPage());
        // Jia Note: this is not perfect to always create a container in every new page, but 
        // doing this can avoid a memory copy
        if (myRAM->location == nullptr) {
            std::cout << "ERROR: insufficient memory in heap" << std::endl;
            return;
        }
        if (myRAM->outputSink == nullptr) {
            myRAM->outputSink = dataSink->createNewOutputContainer();
        }

        // and here is the chunk
        TupleSetPtr curChunk;

        // the iteration counter
        int iteration = 0;

        // while there is still data
        // Jia Note: dataSource->getNextTupleSet() can throw exception for certain data sources like
        // MapTupleSetIterator
        while (true) {

            try {
                curChunk = dataSource->getNextTupleSet();
            } catch (NotEnoughSpace& n) {
                myRAM->setIteration(iteration);
                unwrittenPages.push(myRAM);
                myRAM = std::make_shared<MemoryHolder>(getNewPage());
                if (myRAM->location == nullptr) {
                    std::cout << "ERROR: insufficient memory in heap" << std::endl;
                    return;
                }
                if (myRAM->outputSink == nullptr) {
                    myRAM->outputSink = dataSink->createNewOutputContainer();
                }
                // then try again
                try {
                    curChunk = dataSource->getNextTupleSet();
                } catch (NotEnoughSpace& n) {
                    // consider to reduce batch size" << std :: endl;
                    std::cout << "batch size tuned to be " << MIN_BATCH_SIZE << std::endl;
                    dataSource->setChunkSize(MIN_BATCH_SIZE);
                    try {
                        curChunk = dataSource->getNextTupleSet();
                    } catch (NotEnoughSpace& n) {
                        std::cout << "batch size tuned to be " << MIN_BATCH_SIZE << std::endl;
                        dataSource->setChunkSize(1);
                        try {
                            curChunk = dataSource->getNextTupleSet();
                        } catch (NotEnoughSpace& n) {
                            std::cout << "batch size tuned to be " << MIN_BATCH_SIZE << std::endl;
                            return;
                        }
                    }
                }
            }
            if (curChunk == nullptr) {
                break;
            }
            // go through all of the pipeline stages
            for (ComputeExecutorPtr& q : pipeline) {

                try {
                    curChunk = q->process(curChunk);

                } catch (NotEnoughSpace& n) {
                    // and get a new page
                    myRAM->setIteration(iteration);
                    unwrittenPages.push(myRAM);
                    myRAM = std::make_shared<MemoryHolder>(getNewPage());
                    if (myRAM->location == nullptr) {
                        std::cout << "ERROR: insufficient memory in heap" << std::endl;
                        return;
                    }
                    if (myRAM->outputSink == nullptr) {
                         myRAM->outputSink = dataSink->createNewOutputContainer();
                    }
                    // then try again
                    try {
                        curChunk = q->process(curChunk);
                    } catch (NotEnoughSpace& n) {
                        std::cout << "Pipeline Error: Batch processing memory exceeds page size "
                                     "for executor type: "
                                  << q->getType() << ", consider to reduce batch size" << std::endl;
                        return;
                    }
                }
            }

            try {

                if (myRAM->outputSink == nullptr) {
                    myRAM->outputSink = dataSink->createNewOutputContainer();
                }
                dataSink->writeOut(curChunk, myRAM->outputSink);

            } catch (NotEnoughSpace& n) {

                // again, we ran out of RAM here, so write back the page and then create a new
                // output page
                std::cout << "pipeline runs out of RAM" << std::endl;
                myRAM->setIteration(iteration);
                unwrittenPages.push(myRAM);
                myRAM = std::make_shared<MemoryHolder>(getNewPage());

                // and again, try to write back the output
                myRAM->outputSink = dataSink->createNewOutputContainer();
                dataSink->writeOut(curChunk, myRAM->outputSink);
            }

            // lastly, write back all of the output pages
            iteration++;
            cleanPages(iteration);
        }

        // set the iteration
        myRAM->setIteration(iteration);

        // and remember the page
        unwrittenPages.push(myRAM);
    }
};

typedef std::shared_ptr<Pipeline> PipelinePtr;
}

#endif
