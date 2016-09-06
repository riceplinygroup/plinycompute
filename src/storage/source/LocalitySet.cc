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
 *  LocalitySet.cc
 *
 *  Created on: May 29, 2016
 *  Author: Jia
 */

#ifndef LOCALITY_SET_CC
#define LOCALITY_SET_CC

#include "LocalitySet.h"
#include <iostream>
LocalitySet::LocalitySet(LocalityType localityType, LocalitySetReplacementPolicy replacementPolicy, OperationType operationType, DurabilityType durabilityType, PersistenceType persistenceType) {
    cachedPages = new list<PDBPagePtr> ();
    this->localityType = localityType;
    this->replacementPolicy = replacementPolicy;
    this->operationType = operationType;
    this->durabilityType = durabilityType;
    this->persistenceType = persistenceType;
    this->lifetimeEnded = false;
}

LocalitySet::~LocalitySet() {
    cachedPages->clear();
    delete cachedPages;
}

void LocalitySet::addCachedPage(PDBPagePtr page) {
    //cout << "to add page to cache with pageId=" << page->getPageID() << ", setId="<< page->getSetID() << "\n";
    cachedPages->push_back(page);
}

void LocalitySet::updateCachedPage(PDBPagePtr page) {
    //cout << "to update page in cache with pageId=" << page->getPageID() << ", setId=" << page->getSetID() << "\n";
    for (list<PDBPagePtr>::iterator it = cachedPages->begin(); it != cachedPages->end(); ++it) {
        if((*it) == page) {
           cachedPages->erase(it);
           break;
        }
    }
    cachedPages->push_back(page);   
}

void LocalitySet::removeCachedPage (PDBPagePtr page) {
    //cout << "to remove page from cache with pageId=" << page->getPageID() << ", setId=" << page->getSetID() << "\n";
    for (list<PDBPagePtr>::iterator it = cachedPages->begin(); it != cachedPages->end(); ++it) {
        if((*it) == page) {
           cachedPages->erase(it);
           break;
        }
    }
}

PDBPagePtr LocalitySet::selectPageForReplacement() {
    PDBPagePtr retPage = nullptr;
    if(this->replacementPolicy == MRU) {
        for (list<PDBPagePtr>::reverse_iterator it = cachedPages->rbegin(); it != cachedPages->rend(); ++it) {
             if((*it)->getRefCount()==0) {
                 retPage = (*it);
                 //cachedPages->erase(it);
                 break;
             }
        }
    } else {
        for (list<PDBPagePtr>::iterator it = cachedPages->begin(); it != cachedPages->end(); ++it) {
             if((*it)->getRefCount()==0) {
                 retPage = (*it);
                 //cachedPages->erase(it);
                 break;
             }
        }
    }
    return retPage;
}

vector<PDBPagePtr> * LocalitySet::selectPagesForReplacement() {
    vector<PDBPagePtr> * retPages = new vector<PDBPagePtr>();
    int totalPages = cachedPages->size();
    if(totalPages == 0) {
        delete retPages;
        return nullptr;
    }
    //cout << "totalPages="<<totalPages<<"\n";
    int numPages = 0;
    if(this->replacementPolicy == MRU) {
        for (list<PDBPagePtr>::reverse_iterator it = cachedPages->rbegin(); it != cachedPages->rend(); ++it) {
             if((*it)->getRefCount()==0) {
                 retPages->push_back(*it);
                 numPages ++;
                 if(this->operationType == Write) {
                     break;
                 }
                 else {
                     
                     if((double)numPages/(double)totalPages >= 0.1) {
                         break;
                     }
                 }
             }
        } 
    } else {
        for (list<PDBPagePtr>::iterator it = cachedPages->begin(); it != cachedPages->end(); ++it) {
             if((*it)->getRefCount()==0) {
                 retPages->push_back(*it);
                 numPages ++;
                 if(this->operationType == Write) {
                     break;
                 }
                 else {
                     if((double)numPages/(double)totalPages >= 0.1) {
                         break;
                     }
                 }
             }
        }
    }
    if(numPages == 0) {
       delete retPages;
       return nullptr;
    } else {
       return retPages;
    }
}


void LocalitySet::pin(LocalitySetReplacementPolicy policy, OperationType operationType) {
    this->replacementPolicy = policy;
    this->operationType = operationType;
    this->lifetimeEnded = false;
}

void LocalitySet::unpin() {
    this->lifetimeEnded = true;
}


LocalityType LocalitySet::getLocalityType() {
    return this->localityType;
}

void LocalitySet::setLocalityType (LocalityType type) {
    this->localityType = type;
}

LocalitySetReplacementPolicy LocalitySet::getReplacementPolicy() {
    return this->replacementPolicy;
}

void LocalitySet::setReplacementPolicy (LocalitySetReplacementPolicy policy) {
    this->replacementPolicy = policy;
}

OperationType LocalitySet::getOperationType() {
    return this->operationType;
}

void LocalitySet::setOperationType (OperationType type) {
    this->operationType = type;
}


DurabilityType LocalitySet::getDurabilityType () {
    return this->durabilityType;
}

void LocalitySet::setDurabilityType (DurabilityType type) {
    this->durabilityType = type;
}

PersistenceType LocalitySet::getPersistenceType() {
    return this->persistenceType;
}

void LocalitySet::setPersistenceType (PersistenceType type) {
    this->persistenceType = type;
}

bool LocalitySet::isLifetimeEnded () {
    return this->lifetimeEnded;
}

void LocalitySet::setLifetimeEnd (bool lifetimeEnded) {
    this->lifetimeEnded = lifetimeEnded;
}


#endif
