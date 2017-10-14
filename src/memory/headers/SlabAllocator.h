/**
 * Code is modified from memcached slabs allocator by Jia.
 * memcached is using BSD license.
 */


#ifndef SLAB_ALLOCATOR_H
#define SLAB_ALLOCATOR_H
#include <memory>
using namespace std;

class SlabAllocator;
typedef shared_ptr<SlabAllocator> SlabAllocatorPtr;

/* Slab sizing definitions. */
#define POWER_SMALLEST 1
#define POWER_LARGEST 256       /* actual cap is 255 */
#define SLAB_GLOBAL_PAGE_POOL 0 /* magic slab class for storing pages for reassignment */
#define CHUNK_ALIGN_BYTES 8
/* slab class max is a 6-bit number, -1. */
#define MAX_NUMBER_OF_SLAB_CLASSES (63 + 1) /* Slab sizing definitions. */
#define POWER_SMALLEST 1
#define POWER_LARGEST 256       /* actual cap is 255 */
#define SLAB_GLOBAL_PAGE_POOL 0 /* magic slab class for storing pages for reassignment */
#define CHUNK_ALIGN_BYTES 8
/* slab class max is a 6-bit number, -1. */
#define MAX_NUMBER_OF_SLAB_CLASSES (63 + 1)

/* When adding a setting, be sure to update process_stat_settings */
/**
 * Globally accessible settings as derived from the commandline.
 */
struct settings_t {
    int verbose = 0;
    double factor = 2;                    /* chunk size growth factor */
    int chunk_size = 24;                  /* space for a modest key and value */
    int item_size_max = 10 * 1024 * 1024; /* Maximum item size, and upper end for slabs */
    bool slab_reassign = false;           /* Whether or not slab reassignment is allowed */
    // int slab_automove;     /* Whether or not to automatically move slabs */
};

/**
 * Structure for storing items within memcached.
 */
typedef struct _stritem {
    /* Protected by LRU locks */
    struct _stritem* next;
    struct _stritem* prev;
    // uint8_t slabs_clsid; /* which slab class we are in */
    // int nBytes; /* size of data */
    char data[]; /* data */
} item;


/* powers-of-N allocation structures */

typedef struct {
    unsigned int size;    /* sizes of items */
    unsigned int perslab; /* how many items per slab */

    void* slots;          /* list of item ptrs */
    unsigned int sl_curr; /* total free items in list */

    unsigned int slabs; /* how many slabs were allocated for this class */

    void** slab_list;       /* array of slab pointers */
    unsigned int list_size; /* size of prev array */

    size_t requested; /* The number of requested bytes */
} slabclass_t;


class SlabAllocator {

public:
    SlabAllocator(const size_t limit, bool opt4hashmap = false);
    SlabAllocator(void* memPool, const size_t limit, size_t pageSize, size_t alignment);
    SlabAllocator(void* memPool, const size_t limit, bool opt4hashmap = false);
    ~SlabAllocator();

    /** Init the subsystem. 1st argument is the limit on no. of bytes to allocate,
        0 if no limit. 2nd argument is the growth factor; each slab will use a chunk
        size equal to the previous slab's chunk size times this factor.
        3rd argument specifies if the slab allocator should allocate all memory
        up front (if true), or allocate memory in chunks as it is needed (if false)
    */
    void init(const size_t limit, const double factor, const bool prealloc);

    /** Allocate object of given length. 0 on error */ /*@null@*/
#define SLABS_ALLOC_NO_NEWPAGE 1
    void* slabs_alloc(const size_t size);
    void* slabs_alloc_unsafe(const size_t size);
    /** Free previously allocated object */
    void slabs_free(void* ptr, size_t size);
    void slabs_free_unsafe(void* ptr, size_t size);
    void* do_slabs_alloc(const size_t size);
    void do_slabs_free(void* ptr, const size_t size);
    void do_slabs_free(void* ptr, const size_t size, unsigned int id);
    unsigned int slabs_clsid(const size_t size);
    int do_slabs_newslab(const unsigned int id);
    void* get_page_from_global_pool(void);
    int grow_slab_list(const unsigned int id);
    void* memory_allocate(size_t size);
    void slabs_preallocate(const unsigned int maxslabs);
    void split_slab_page_into_freelist(char* ptr, const unsigned int id);

private:
    size_t mem_limit = 0;
    size_t mem_malloced = 0;
    bool mem_limit_reached = false;
    int power_largest;
    void* mem_base = NULL;
    void* mem_current = NULL;
    size_t mem_avail = 0;
    pthread_mutex_t slabs_lock;
    struct settings_t settings;
    slabclass_t slabclass[MAX_NUMBER_OF_SLAB_CLASSES];
    bool opt4hashmap = false;
    bool opt4sharedmem = false;
    bool useExternalMemory = false;
};

#endif
