/**
 * Code is modified from memcached slabs allocator by Jia.
 * memcached is using BSD license.
 */

#ifndef SLAB_ALLOCATOR_CC
#define SLAB_ALLOCATOR_CC

/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Slabs memory allocation, based on powers-of-N. Slabs are up to 1MB in size
 * and are divided into chunks. The chunk sizes start off at the size of the
 * "item" structure plus space for a small key and value. They increase by
 * a multiplier factor from there, up to half the maximum slab size. The last
 * slab size is always 1MB, since that's the maximum item size allowed by the
 * memcached protocol.
 */
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <iostream>
#include "SlabAllocator.h"

SlabAllocator::SlabAllocator(const size_t limit, bool opt4hashmap) {
    this->opt4hashmap = opt4hashmap;
    if (this->opt4hashmap == true) {
        settings.verbose = 0;
        settings.factor = 3 * 128 * 1024;
        settings.chunk_size = 24;
        settings.item_size_max = 9 * 1024 * 1024;
        settings.slab_reassign = false;
    } else {
        settings.verbose = 0;
        settings.factor = 1.25;
        settings.chunk_size = 8;
        settings.item_size_max = 1024;
        settings.slab_reassign = false;
    }
    this->init(limit, settings.factor, true);
    pthread_mutex_init(&slabs_lock, nullptr);
}

SlabAllocator::SlabAllocator(void* memPool, const size_t limit, size_t pageSize, size_t alignment) {
    settings.verbose = 0;
    settings.factor = 2;
    settings.chunk_size = pageSize + alignment;
    settings.item_size_max = pageSize + alignment;
    settings.slab_reassign = false;
    this->opt4sharedmem = true;
    this->mem_base = memPool;
    this->init(limit, settings.factor, true);
    pthread_mutex_init(&slabs_lock, nullptr);
}

SlabAllocator::SlabAllocator(void* memPool, const size_t limit, bool opt4hashmap) {
    this->opt4hashmap = opt4hashmap;
    if (this->opt4hashmap == true) {
        settings.verbose = 0;
        settings.factor = 3 * 128 * 1024;
        settings.chunk_size = 24;
        settings.item_size_max = 9 * 1024 * 1024;
        settings.slab_reassign = false;
    } else {
        settings.verbose = 0;
        settings.factor = 2;
        settings.chunk_size = 8;
        settings.item_size_max = 1 * 1024 * 1024;
        settings.slab_reassign = false;
    }
    this->useExternalMemory = true;
    this->mem_base = memPool;
    this->init(limit, settings.factor, true);
    pthread_mutex_init(&slabs_lock, nullptr);
}


SlabAllocator::~SlabAllocator() {
    if ((mem_base != nullptr) && (useExternalMemory == false)) {
        free(mem_base);
    }
    for (int i = POWER_SMALLEST; i < MAX_NUMBER_OF_SLAB_CLASSES - 1; i++) {
        slabclass_t* p = &slabclass[i];
        if (p->slab_list != nullptr) {
            free(p->slab_list);
        }
    }
    pthread_mutex_destroy(&slabs_lock);
}

void SlabAllocator::init(const size_t limit, const double factor, const bool prealloc) {
    int i = POWER_SMALLEST - 1;
    unsigned int size = sizeof(item) + settings.chunk_size;
    // cout << "sizeof(item)="<<sizeof(item)<<"\n";
    // cout << "settings.chunk_size="<<settings.chunk_size<<"\n";
    mem_limit = limit;

    if (prealloc) {
        /* Allocate everything in a big chunk with malloc */
        if (this->mem_base == nullptr) {
            mem_base = malloc(mem_limit);
        }
        if (mem_base != nullptr) {
            mem_current = mem_base;
            mem_avail = mem_limit;
        } else {
            fprintf(stderr,
                    "Warning: Failed to allocate requested memory in"
                    " one large chunk.\nWill allocate in smaller chunks\n");
        }
    }
    memset(slabclass, 0, sizeof(slabclass));
    while (++i < MAX_NUMBER_OF_SLAB_CLASSES - 1 &&
           (size - sizeof(item)) <= settings.item_size_max / factor) {
        /* Make sure items are always n-byte aligned */
        if (size % CHUNK_ALIGN_BYTES)
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        //        cout << "initialize slab i="<<i<<"\n";

        slabclass[i].size = size;
        if (opt4sharedmem == true) {
            slabclass[i].perslab = mem_limit / (2 * (settings.item_size_max + sizeof(item)));
        } else {
            if (size <= 64) {
                if (this->opt4hashmap == false) {
                    slabclass[i].perslab = settings.item_size_max / slabclass[i].size;
                } else {
                    slabclass[i].perslab = 10000;
                }
            } else {
                slabclass[i].perslab = 1;
            }
        }
        //        cout << "size="<<size<<"\n";
        //        cout << "perslab="<<slabclass[i].perslab<<"\n";
        size *= factor;
        if (settings.verbose > 1) {
            fprintf(stderr,
                    "slab class %3d: chunk size %9u perslab %7u\n",
                    i,
                    slabclass[i].size,
                    slabclass[i].perslab);
        }
    }
    power_largest = i;
    slabclass[power_largest].size = settings.item_size_max;
    slabclass[power_largest].perslab = 1;
    //    cout << "initialize for slab i="<< i<<"\n";
    //    cout << "size="<<slabclass[i].size<<"\n";
    //    cout << "perslab="<<slabclass[i].perslab<<"\n";
    if (settings.verbose > 1) {
        fprintf(stderr,
                "slab class %3d: chunk size %9u perslab %7u\n",
                i,
                slabclass[i].size,
                slabclass[i].perslab);
    }
    /*
    if (prealloc) {
        slabs_preallocate(power_largest);
    }
    */
}

void SlabAllocator::slabs_preallocate(const unsigned int maxslabs) {
    int i;
    unsigned int prealloc = 0;

    /* pre-allocate a 1MB slab in every size class so people don't get
       confused by non-intuitive "SERVER_ERROR out of memory"
       messages.  this is the most common question on the mailing
       list.  if you really don't want this, you can rebuild without
       these three lines.  */

    for (i = POWER_SMALLEST; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
        if (++prealloc > maxslabs)
            return;
        if (do_slabs_newslab(i) == 0) {
            fprintf(stderr,
                    "Error while preallocating slab memory!\n"
                    "If using -L or other prealloc options, max memory must be "
                    "at least %d megabytes.\n",
                    power_largest);
            std::cout << "Fatal Error: SlabAllocator::slabs_preallocate()" << std::endl;
            exit(1);
        }
    }
}

int SlabAllocator::do_slabs_newslab(const unsigned int id) {
    slabclass_t* p = &slabclass[id];
    slabclass_t* g = &slabclass[SLAB_GLOBAL_PAGE_POOL];
    int len = p->size * p->perslab;
    // cout << "len for newslab="<<len<<"\n";
    char* ptr;

    if ((mem_limit && mem_malloced + len > mem_limit && p->slabs > 0 && g->slabs == 0)) {
        mem_limit_reached = true;
        return 0;
    }

    if ((grow_slab_list(id) == 0) || (((ptr = (char*)get_page_from_global_pool()) == nullptr) &&
                                      ((ptr = (char*)memory_allocate((size_t)len)) == 0))) {

        return 0;
    }

    memset(ptr, 0, (size_t)len);
    split_slab_page_into_freelist(ptr, id);

    p->slab_list[p->slabs++] = ptr;

    return 1;
}

int SlabAllocator::grow_slab_list(const unsigned int id) {
    slabclass_t* p = &slabclass[id];
    if (p->slabs == p->list_size) {
        size_t new_size = (p->list_size != 0) ? p->list_size * 2 : 16;
        void* new_list = realloc(p->slab_list, new_size * sizeof(void*));
        if (new_list == 0)
            return 0;
        p->list_size = new_size;
        p->slab_list = (void**)new_list;
    }
    return 1;
}

/* Fast FIFO queue */
void* SlabAllocator::get_page_from_global_pool(void) {
    slabclass_t* p = &slabclass[SLAB_GLOBAL_PAGE_POOL];
    if (p->slabs < 1) {
        return nullptr;
    }
    void* ret = p->slab_list[p->slabs - 1];
    p->slabs--;
    return ret;
}

void SlabAllocator::split_slab_page_into_freelist(char* ptr, const unsigned int id) {
    slabclass_t* p = &slabclass[id];
    int x;
    for (x = 0; x < p->perslab; x++) {
        do_slabs_free(ptr, 0, id);
        ptr += p->size;
    }
}

void* SlabAllocator::memory_allocate(size_t size) {
    void* ret;

    if (mem_base == nullptr) {
        /* We are not using a preallocated large memory chunk */
        ret = malloc(size);
    } else {
        ret = mem_current;

        if (size > mem_avail) {
            return nullptr;
        }

        /* mem_current pointer _must_ be aligned!!! */
        if (size % CHUNK_ALIGN_BYTES) {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }

        mem_current = ((char*)mem_current) + size;
        if (size < mem_avail) {
            mem_avail -= size;
        } else {
            mem_avail = 0;
        }
    }
    mem_malloced += size;

    return ret;
}

void* SlabAllocator::slabs_alloc(const size_t size) {
    pthread_mutex_lock(&slabs_lock);
    void* ret = slabs_alloc_unsafe(size);
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}

void* SlabAllocator::slabs_alloc_unsafe(const size_t size) {
    void* ret;
    ret = do_slabs_alloc(size);
    return ret;
}
unsigned int SlabAllocator::slabs_clsid(const size_t size) {
    int res = POWER_SMALLEST;
    if (size == 0) {
        return 0;
    }
    while (size > slabclass[res].size) {
        if (res++ == power_largest) {
            return 0;
        }
    }
    return res;
}
void* SlabAllocator::do_slabs_alloc(const size_t size) {
    // cout << "size to alloc="<< size <<"\n";
    slabclass_t* p;
    void* ret = nullptr;
    item* it = nullptr;
    unsigned int id = slabs_clsid(size);
    // cout << "slab id=" << id << "\n";
    if (id < POWER_SMALLEST || id > power_largest) {
        return nullptr;
    }
    p = &slabclass[id];
    // assert(p->sl_curr == 0 || ((item *)p->slots)->slabs_clsid == 0);
    // assert(p->sl_curr == 0);
    /* fail unless we have space at the end of a recently allocated page,
       we have something on our freelist, or we could allocate a new page */
    if (p->sl_curr == 0) {
        do_slabs_newslab(id);
    }

    if (p->sl_curr != 0) {
        /* return off our freelist */
        it = (item*)p->slots;
        p->slots = it->next;
        if (it->next)
            it->next->prev = 0;
        p->sl_curr--;
        ret = (void*)it;
    } else {
        ret = nullptr;
    }

    if (ret) {
        p->requested += size;
    }

    return ret;
}

void SlabAllocator::slabs_free(void* ptr, size_t size) {
    pthread_mutex_lock(&slabs_lock);
    do_slabs_free(ptr, size);
    pthread_mutex_unlock(&slabs_lock);
}

void SlabAllocator::slabs_free_unsafe(void* ptr, size_t size) {
    do_slabs_free(ptr, size);
}

void SlabAllocator::do_slabs_free(void* ptr, const size_t size, unsigned int id) {
    slabclass_t* p;
    item* it;

    assert(id >= POWER_SMALLEST && id <= power_largest);
    if (id < POWER_SMALLEST || id > power_largest)
        return;

    p = &slabclass[id];

    it = (item*)ptr;
    it->prev = 0;
    it->next = (item*)p->slots;
    if (it->next)
        it->next->prev = it;
    p->slots = it;

    p->sl_curr++;
    p->requested -= size;
    return;
}

void SlabAllocator::do_slabs_free(void* ptr, const size_t size) {
    slabclass_t* p;
    item* it;
    unsigned int id = slabs_clsid(size);
    assert(id >= POWER_SMALLEST && id <= power_largest);
    if (id < POWER_SMALLEST || id > power_largest)
        return;

    p = &slabclass[id];

    it = (item*)ptr;
    // it->slabs_clsid = 0;
    it->prev = 0;
    it->next = (item*)p->slots;
    if (it->next)
        it->next->prev = it;
    p->slots = it;

    p->sl_curr++;
    p->requested -= size;
    return;
}


#endif
