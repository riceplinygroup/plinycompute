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

#ifndef PAGE_HANDLE_CC
#define PAGE_HANDLE_CC

#include "PageHandle.h"
#include <iostream>


PageHandle::PageHandle(DataProxyPtr proxy, PDBPagePtr page) {
    this->proxy = proxy;
    this->page = page;
}

PageHandle::~PageHandle() {}

void PageHandle::pin() {
    if (this->page->isPinned()) {
        return;
    }
    // temp page
    if ((this->page->getDbID() == 0) && (this->page->getTypeID() == 0)) {
        proxy->pinTempPage(page->getSetID(), page->getPageID(), this->page);
    }
    // user page
    else {
        proxy->pinUserPage(page->getNodeID(),
                           page->getDbID(),
                           page->getTypeID(),
                           page->getSetID(),
                           page->getPageID(),
                           this->page);
    }
}

void PageHandle::unpin() {
    if (!this->page->isPinned()) {
        cout << "PageHandle: can not unpin because page is already unpinned.\n";
        return;
    }
    // temp page
    if ((this->page->getDbID() == 0) && (this->page->getTypeID() == 0)) {
        proxy->unpinTempPage(page->getSetID(), page);
    }
    // user page
    else {
        proxy->unpinUserPage(
            page->getNodeID(), page->getDbID(), page->getTypeID(), page->getSetID(), page);
    }
    this->page->setPinned(false);
}


void* PageHandle::getRAM() {
    return this->page->getRawBytes();
}

void* PageHandle::getWritableBytes() {
    return this->page->getBytes();
}

size_t PageHandle::getSize() {
    return this->page->getRawSize();
}

size_t PageHandle::getWritableSize() {
    return this->page->getSize();
}

PageID PageHandle::getPageID() {
    return this->page->getPageID();
}

#endif
