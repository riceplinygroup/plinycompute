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
 * PDBFlushConsumerWork.cc
 *
 *  Created on: Dec 27, 2015
 *      Author: Jia
 */


#include "PDBFlushConsumerWork.h"
#include "PageCircularBuffer.h"
#include <chrono>
#include <ctime>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
PDBFlushConsumerWork::PDBFlushConsumerWork(FilePartitionID partitionId, pdb :: PangeaStorageServer* server) {
	this->partitionId = partitionId;
        this->server = server;
        this->isStopped = false;
}

void PDBFlushConsumerWork :: stop() {
        this->isStopped = true;
}


void PDBFlushConsumerWork::execute(PDBBuzzerPtr callerBuzzer) {
	PageCircularBufferPtr flushBuffer = this->server->getFlushBuffer();
	PDBPagePtr page;
        SetPtr set;
	while(!isStopped) {
		if((page = flushBuffer->popPageFromHead()) != nullptr) {
			//got a page from flush buffer
			//find the set of the page
                        //this->server->getLogger()->writeLn("Got a page with PageID=");
                        //this->server->getLogger()->writeInt(page->getPageID());
                        //this->server->getLogger()->writeLn("PartitionId=");
                        //this->server->getLogger()->writeInt(this->partitionId);
			//cout <<"Got a page with PageID "<<page->getPageID()<<" for partition:"<<this->partitionId<<"\n";
                        //cout << "page dbId=" << page->getDbID()<<"\n";
                        //cout << "page typeId=" << page->getTypeID()<<"\n";
                        //cout << "page setId=" << page->getSetID()<<"\n";
                        bool isTempSet = false;
                        if((page->getDbID()==0)&&(page->getTypeID()== 0)){
                            set = this->server->getTempSet(page->getSetID());
                            isTempSet = true;
                        } else {
			    set = this->server->getSet(page->getDbID(), page->getTypeID(), page->getSetID());
                        }
                        CacheKey key;
                        key.dbId = page->getDbID();
                        key.typeId = page->getTypeID();
                        key.setId =page->getSetID();
                        key.pageId = page->getPageID();
                        //cout << "to get lock\n";
                        //this->server->getLogger()->writeLn("PDBFlushConsumerWork: to get lock for flushLock()...");
                        this->server->getCache()->flushLock();
                        //this->server->getLogger()->writeLn("PDBFlushConsumerWork: got lock for flushLock()...");
                        //cout << "got lock!\n";
			if((set != nullptr)&&(page->getRawBytes()!=nullptr)) {
                                
     				//append the page to the partition
			        //cout<<"start flushing at partition:"<<this->partitionId<<"\n";
                                //this->server->getLogger()->writeLn("PDBFlushConsumerWork: to get lock for lockDirtyPageSet()...");
                                //this->server->getLogger()->writeLn("PDBFlushConsumerWork: got lock for lockDirtyPageSet()...");
                                //auto begin = std::chrono::high_resolution_clock::now();
				int ret = set->getFile()->appendPage(this->partitionId, page);
                                //auto end = std::chrono::high_resolution_clock::now();
                                //std::cout << "append page latency:"<< std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;
                                if(isTempSet == false) {
                                     set->getFile()->writeMeta();
                                }
                                if (ret < 0) {
                                    cout << "Can't write page with below info:\n";
                                    cout <<"Got a page with PageID "<<page->getPageID()<<" for partition:"<<this->partitionId<<"\n";
                                    cout << "page dbId=" << page->getDbID()<<"\n";
                                    cout << "page typeId=" << page->getTypeID()<<"\n";
                                    cout << "page setId=" << page->getSetID()<<"\n";

                                }
                                set->lockDirtyPageSet();                
                                set->removePageFromDirtyPageSet(page->getPageID(), this->partitionId, ret);
                                set->unlockDirtyPageSet();
                                //this->server->getLogger()->writeLn("PDBFlushConsumerWork: unlocked for lockDirtyPageSet()...");
				//cout<<"page with PageID "<<page->getPageID() <<" appended to partition with PartitionID "<<this->partitionId<<"\n";
                         }
                         if((page->getRawBytes() != nullptr)&&(page->getRefCount()==0)&&(page->isInEviction()==true)) {
                             //remove the page from cache!
                             //cout << "to free the page!\n";
		             this->server->getSharedMem()->free(page->getRawBytes()-page->getInternalOffset());
                             //cout << "internalOffset="<<page->getInternalOffset()<< "\n";
		             page->setOffset(0);
		             page->setRawBytes(nullptr);
                         }
                         /*
                         else {
                             //cout << "page->getRefCount()="<<page->getRefCount()<<"\n";
                             if(page->isInEviction()==false) {
                                 cout << "page is not in eviction!\n";
                             }
                         }
                         */
                         //remove the page from cache!
                         if((page->getRefCount()==0)&&(page->isInEviction()==true)) {
                             this->server->getCache()->removePage(key);
                         } 
                         else {
                             page->setInFlush(false);
                             page->setDirty(false);
                         }
                                
                         this->server->getCache()->flushUnlock();
                         //this->server->getLogger()->writeLn("PDBFlushConsumerWork: unlocked for flushUnlock()...");
/*
                         if(set!=nullptr) {
                                if((set->getLastFlushedPageId()==(unsigned int)(-1)) || (key.pageId > set->getLastFlushedPageId())) {
                                	set->setLastFlushedPageId(key.pageId);
                                }
			}*/
		}

	}
	cout<<"flushing thread stopped running for partition: "<<partitionId<<"\n";
}


