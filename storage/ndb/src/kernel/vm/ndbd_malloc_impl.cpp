/*
   Copyright (c) 2006, 2021, Oracle and/or its affiliates.
   Copyright (c) 2021, 2021, iClaustron AB and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/


#include "ndbd_malloc_impl.hpp"

#include <time.h>

#include <ndb_global.h>
#include <portlib/NdbMem.h>
#include <portlib/NdbThread.h>
#include <portlib/NdbTick.h>
#include <atomic>

#define JAM_FILE_ID 296

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
#define DEBUG_MEM_ALLOC 1
#endif

#ifdef DEBUG_MEM_ALLOC
#define DEB_MEM_ALLOC(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_MEM_ALLOC(arglist) do { } while (0)
#endif

#define PAGES_PER_REGION_LOG BPP_2LOG
#define ALLOC_PAGES_PER_REGION ((1 << PAGES_PER_REGION_LOG) - 2)

#ifdef _WIN32
void *sbrk(int increment)
{
  return (void*)-1;
}
#endif


static int f_method_idx = 0;
#ifdef NDBD_MALLOC_METHOD_SBRK
static const char * f_method = "SMsm";
#else
static const char * f_method = "MSms";
#endif
#define MAX_CHUNKS 10

/*
 * For muti-threaded ndbd, these calls are used for locking around
 * memory allocation operations.
 *
 * For single-threaded ndbd, they are no-ops (but still called, to avoid
 * having to compile this file twice).
 */
extern void mt_mem_manager_init();
extern void mt_mem_manager_lock();
extern void mt_mem_manager_unlock();

#include <NdbOut.hpp>

extern void ndbd_alloc_touch_mem(void * p, size_t sz, volatile Uint32 * watchCounter);

const Uint32 Ndbd_mem_manager::zone_bound[ZONE_COUNT] =
{ /* bound in regions */
  ZONE_19_BOUND >> PAGES_PER_REGION_LOG,
  ZONE_27_BOUND >> PAGES_PER_REGION_LOG,
  ZONE_30_BOUND >> PAGES_PER_REGION_LOG,
  ZONE_32_BOUND >> PAGES_PER_REGION_LOG
};

/**
 * do_virtual_alloc uses debug functions NdbMem_ReserveSpace and
 * NdbMem_PopulateSpace to be able to use as high page numbers as possible for
 * each memory region.  Using high page numbers will likely lure bugs due to
 * storing not all required bits of page numbers.
 */

/**
   Disable on Solaris:
   Bug #32575486 NDBMTD CONSUMES ALL AVAILABLE MEMORY IN DEBUG ON SOLARIS
*/
#if defined(VM_TRACE) && !defined(__sun)
#if defined(_WIN32) || \
    (defined(MADV_DONTDUMP) && \
     defined(MAP_NORESERVE)) || \
    defined(MAP_GUARD)
/*
 * Only activate use of do_virtual_alloc() if build platform allows reserving
 * address space only without reserving space on swap nor include memory in
 * core files dumped, since we start by trying to reserve 128TB of address
 * space.
 *
 * For Windows one uses VirtualAlloc(MEM_RESERVE).
 *
 * On Linux and Solaris (since 11.4 SRU 12) one uses mmap(MAP_NORESERVE) and
 * madvise(MADV_DONTDUMP).
 *
 * On FreeBSD one uses mmap(MAP_GUARD).
 *
 * For other OS do_virtual_alloc should not be used since it will produce huge
 * core dumps if crashing.
 */
#define USE_DO_VIRTUAL_ALLOC
#elif defined(USE_DO_VIRTUAL_ALLOC)
#error do_virtual_alloc is not supported, please undefine USE_DO_VIRTUAL_ALLOC.
#endif
#endif

#ifdef USE_DO_VIRTUAL_ALLOC

/*
 * To verify that the maximum of 16383 regions can be reserved without failure
 * in do_virtual_alloc define NDB_TEST_128TB_VIRTUAL_MEMORY, data node should
 * exit early with exit status 0, anything else is an error.
 * Look for NdbMem printouts in data node log.
 *
 * Also see Bug#28961597.
 */
//#define NDB_TEST_128TB_VIRTUAL_MEMORY
#ifdef NDB_TEST_128TB_VIRTUAL_MEMORY

static inline int
log_and_fake_success(const char func[], int line,
                     const char msg[], void* p, size_t s)
{
  g_eventLogger->info("DEBUG: %s: %u: %s: p %p: len %zu", func, line, msg, p,
                      s);
  return 0;
}

#define NdbMem_ReserveSpace(x,y) \
  log_and_fake_success(__func__, __LINE__, "NdbMem_ReserveSpace", (x), (y))

#define NdbMem_PopulateSpace(x,y) \
  log_and_fake_success(__func__, __LINE__, "NdbMem_PopulateSpace", (x), (y))

#endif

bool
Ndbd_mem_manager::do_virtual_alloc(Uint32 pages,
                                   InitChunk chunks[ZONE_COUNT],
                                   Uint32* watchCounter,
                                   Alloc_page** base_address)
{
  if (watchCounter)
    *watchCounter = 9;
  const Uint32 max_regions = zone_bound[ZONE_COUNT - 1];
  const Uint32 max_pages = max_regions << PAGES_PER_REGION_LOG;
  require(max_regions == (max_pages >> PAGES_PER_REGION_LOG));
  require(max_regions > 0); // TODO static_assert
  if (pages > max_pages)
  {
    return false;
  }
  const bool half_space = (pages <= (max_pages >> 1));

  /* Find out page count per zone */
  Uint32 page_count[ZONE_COUNT];
  Uint32 region_count[ZONE_COUNT];
  Uint32 prev_bound = 0;
  for (int i = 0; i < ZONE_COUNT; i++)
  {
    Uint32 n = pages / (ZONE_COUNT - i);
    if (half_space && n > (zone_bound[i] << (PAGES_PER_REGION_LOG - 1)))
    {
      n = zone_bound[i] << (PAGES_PER_REGION_LOG - 1);
    }
    else if (n > ((zone_bound[i] - prev_bound) << PAGES_PER_REGION_LOG))
    {
      n = (zone_bound[i] - prev_bound) << PAGES_PER_REGION_LOG;
    }
    page_count[i] = n;
    region_count[i] = (n + 256 * 1024 - 1) / (256 * 1024);
    prev_bound = zone_bound[i];
    pages -= n;
  }
  require(pages == 0);

  /* Reserve big enough continuous address space */
  require(ZONE_COUNT >= 2); // TODO static assert
  const Uint32 highest_low = zone_bound[0] - region_count[0];
  const Uint32 lowest_high = zone_bound[ZONE_COUNT - 2] +
                             region_count[ZONE_COUNT - 1];
  const Uint32 least_region_count = lowest_high - highest_low;
  Uint32 space_regions = max_regions;
  Alloc_page *space = nullptr;
  int rc = -1;
  while (space_regions >= least_region_count)
  {
    if (watchCounter)
      *watchCounter = 9;
    rc = NdbMem_ReserveSpace(
           (void**)&space,
           (space_regions << PAGES_PER_REGION_LOG) * Uint64(32768));
    if (watchCounter)
      *watchCounter = 9;
    if (rc == 0)
    {
      g_eventLogger->info(
          "%s: Reserved address space for %u 8GiB regions at %p.", __func__,
          space_regions, space);
      break;
    }
    space_regions = (space_regions - 1 + least_region_count) / 2;
  }
  if (rc == -1)
  {
    g_eventLogger->info(
        "%s: Failed reserved address space for at least %u 8GiB regions.",
        __func__, least_region_count);
    return false;
  }

#ifdef NDBD_RANDOM_START_PAGE
  Uint32 range = highest_low;
  for (int i = 0; i < ZONE_COUNT; i++)
  {
    Uint32 rmax = (zone_bound[i] << PAGES_PER_REGION_LOG) - page_count[i];
    if (i > 0)
    {
      rmax -= zone_bound[i - 1] << PAGES_PER_REGION_LOG;
    }
    if (half_space)
    {
      rmax -= 1 << 17; /* lower half of region */
    }
    if (range > rmax)
    {
      rmax = range;
    }
  }
  m_random_start_page_id = rand() % range;
#endif

  Uint32 first_region[ZONE_COUNT];
  for (int i = 0; i < ZONE_COUNT; i++)
  {
    first_region[i] = (i < ZONE_COUNT - 1)
                      ? zone_bound[i]
                      : MIN(first_region[0] + space_regions, max_regions);
    first_region[i] -= ((page_count[i] +
#ifdef NDBD_RANDOM_START_PAGE
                         m_random_start_page_id +
#endif
                         ((1 << PAGES_PER_REGION_LOG) - 1))
                        >> PAGES_PER_REGION_LOG);

    chunks[i].m_cnt = page_count[i];
    chunks[i].m_ptr = space + ((first_region[i] - first_region[0])
                                << PAGES_PER_REGION_LOG);
#ifndef NDBD_RANDOM_START_PAGE
    const Uint32 first_page = first_region[i] << PAGES_PER_REGION_LOG;
#else
    const Uint32 first_page = (first_region[i] << PAGES_PER_REGION_LOG) +
                              m_random_start_page_id;
#endif
    const Uint32 last_page = first_page + chunks[i].m_cnt - 1;
    g_eventLogger->info("%s: Populated space with pages %u to %u at %p.",
                        __func__, first_page, last_page, chunks[i].m_ptr);
    require(last_page < (zone_bound[i] << PAGES_PER_REGION_LOG));
  }
  *base_address = space - first_region[0] * 8 * Uint64(32768);
  if (watchCounter)
    *watchCounter = 9;
#ifdef NDB_TEST_128TB_VIRTUAL_MEMORY
  exit(0); // No memory mapped only faking no meaning to continue.
#endif
  return true;
}
#endif

static
bool
do_malloc(Uint32 pages,
          InitChunk* chunk,
          Uint32 *watchCounter,
          void * baseaddress)
{
  void * ptr = 0;
  Uint32 sz = pages;

retry:
  if (watchCounter)
    *watchCounter = 9;

  char method = f_method[f_method_idx];
  switch(method){
  case 0:
    return false;
  case 'S':
  case 's':
  {
    ptr = 0;
    while (ptr == 0)
    {
      if (watchCounter)
        *watchCounter = 9;

      ptr = sbrk(sizeof(Alloc_page) * sz);
      
      if (ptr == (void*)-1)
      {
	if (method == 'S')
	{
	  f_method_idx++;
	  goto retry;
	}
	
	ptr = 0;
	sz = 1 + (9 * sz) / 10;
	if (pages >= 32 && sz < 32)
	{
	  sz = pages;
	  f_method_idx++;
	  goto retry;
	}
      }
      else if (UintPtr(ptr) < UintPtr(baseaddress))
      {
        /**
         * Unusable memory :(
         */
        g_eventLogger->info(
            "sbrk(%lluMb) => %p which is less than baseaddress!!",
            Uint64((sizeof(Alloc_page) * sz) >> 20), ptr);
        f_method_idx++;
        goto retry;
      }
    }
    break;
  }
  case 'M':
  case 'm':
  {
    ptr = 0;
    while (ptr == 0)
    {
      if (watchCounter)
        *watchCounter = 9;

      ptr = NdbMem_AlignedAlloc(sizeof(Alloc_page), sizeof(Alloc_page) * sz);
      if (UintPtr(ptr) < UintPtr(baseaddress))
      {
        g_eventLogger->info(
            "malloc(%lluMb) => %p which is less than baseaddress!!",
            Uint64((sizeof(Alloc_page) * sz) >> 20), ptr);
        free(ptr);
        ptr = 0;
      }

      if (ptr == 0)
      {
	if (method == 'M')
	{
	  f_method_idx++;
	  goto retry;
	}

	sz = 1 + (9 * sz) / 10;
	if (pages >= 32 && sz < 32)
	{
	  f_method_idx++;
	  goto retry;
	}
      }
    }
    break;
  }
  default:
    return false;
  }
  
  chunk->m_cnt = sz;
  chunk->m_ptr = (Alloc_page*)ptr;
  const UintPtr align = sizeof(Alloc_page) - 1;
  /*
   * Ensure aligned to 32KB boundary.
   * Unsure why that is needed.
   * NdbMem_PopulateSpace() in ndbd_alloc_touch_mem() need system page
   * alignment, typically 4KB or 8KB.
   */
  if (UintPtr(ptr) & align)
  {
    chunk->m_cnt--;
    chunk->m_ptr = (Alloc_page*)((UintPtr(ptr) + align) & ~align);
  }

#ifdef UNIT_TEST
  g_eventLogger->info("do_malloc(%d) -> %p %d", pages, ptr, chunk->m_cnt);
  if (1)
  {
    Uint32 sum = 0;
    Alloc_page* page = chunk->m_ptr;
    for (Uint32 i = 0; i<chunk->m_cnt; i++, page++)
    {
      page->m_data[0*1024] = 0;
      page->m_data[1*1024] = 0;
      page->m_data[2*1024] = 0;
      page->m_data[3*1024] = 0;
      page->m_data[4*1024] = 0;
      page->m_data[5*1024] = 0;
      page->m_data[6*1024] = 0;
      page->m_data[7*1024] = 0;
    }
  }
#endif
  
  return true;
}

/**
 * Resource_limits
 */

Resource_limits::Resource_limits()
{
  m_allocated = 0;
  m_free_reserved = 0;
  m_in_use = 0;
  m_spare = 0;
  m_untaken = 0;
  m_max_page = 0;
  // By default allow no low prio usage of shared
  m_prio_free_limit = UINT32_MAX;
  m_lent = 0;
  m_borrowed = 0;
  memset(m_limit, 0, sizeof(m_limit));
}

#ifndef VM_TRACE
inline
#endif
void
Resource_limits::check() const
{
#ifdef VM_TRACE
  const Resource_limit* rl = m_limit;
  Uint32 curr = 0;
  Uint32 spare = 0;
  Uint32 lent = 0;
  Uint32 borrowed = 0;
  Uint32 sumres_lent = 0;
  Uint32 sumres_alloc = 0; // includes spare and lent pages
  Uint32 shared_alloc = 0;
  Uint32 sumres = 0;
  for (Uint32 i = 0; i < MM_RG_COUNT; i++)
  {
    curr += rl[i].m_curr;
    spare += rl[i].m_spare;
    lent += rl[i].m_lent;
    borrowed += rl[i].m_borrowed;
    sumres_lent += rl[i].m_lent;
    sumres += rl[i].m_min;
    const Uint32 res_alloc = rl[i].m_curr + rl[i].m_spare + rl[i].m_lent;
    require(res_alloc <= rl[i].m_max);
    if (res_alloc > rl[i].m_min)
    {
      shared_alloc += res_alloc - rl[i].m_min;
      sumres_alloc += rl[i].m_min;
    }
    else
    {
      sumres_alloc += res_alloc;
    }
  }

  if(!((curr + m_untaken == get_in_use()) &&
       (spare == get_spare()) &&
       (sumres_alloc + shared_alloc == curr + spare + sumres_lent) &&
       (sumres == sumres_alloc + get_free_reserved()) &&
       (get_in_use() + get_spare() <= get_allocated()) &&
       (lent == m_lent) &&
       (borrowed == m_borrowed)))
  {
    dump();
  }

  require(curr + m_untaken == get_in_use());
  require(spare == get_spare());
  require(sumres_alloc + shared_alloc == curr + spare + sumres_lent);
  require(sumres == sumres_alloc + get_free_reserved());
  require(get_in_use() + get_spare() <= get_allocated());
  require(lent == m_lent);
  require(borrowed == m_borrowed);
#endif
}

void
Resource_limits::dump() const
{
  g_eventLogger->info(
      "ri: global "
      "max_page: %u free_reserved: %u in_use: %u allocated: %u spare: %u: "
      "untaken: %u: lent: %u: borrowed: %u",
      m_max_page, m_free_reserved, m_in_use, m_allocated, m_spare, m_untaken,
      m_lent, m_borrowed);
  for (Uint32 i = 0; i < MM_RG_COUNT; i++)
  {
    if (m_limit[i].m_resource_id == 0 &&
        m_limit[i].m_min == 0 &&
        m_limit[i].m_curr == 0 &&
        m_limit[i].m_max == 0 &&
        m_limit[i].m_lent == 0 &&
        m_limit[i].m_borrowed == 0 &&
        m_limit[i].m_spare == 0 &&
        m_limit[i].m_spare_pct == 0)
    {
      continue;
    }
    g_eventLogger->info(
        "ri: %u id: %u min: %u curr: %u max: %u lent: %u"
        " borrowed: %u spare: %u spare_pct: %u",
        i, m_limit[i].m_resource_id, m_limit[i].m_min, m_limit[i].m_curr,
        m_limit[i].m_max, m_limit[i].m_lent, m_limit[i].m_borrowed,
        m_limit[i].m_spare, m_limit[i].m_spare_pct);
  }
}

/**
 *
 * resource N has following semantics:
 *
 * m_min = reserved
 * m_curr = currently used
 * m_max = max alloc
 *
 */
void
Resource_limits::init_resource_limit(Uint32 id, Uint32 min, Uint32 max)
{
  assert(id > 0);
  assert(id <= MM_RG_COUNT);

  m_limit[id - 1].m_resource_id = id;
  m_limit[id - 1].m_curr = 0;
  m_limit[id - 1].m_max = max;

  m_limit[id - 1].m_min = min;

  Uint32 reserve = min;
  Uint32 current_reserved = get_free_reserved();
  set_free_reserved(current_reserved + reserve);
}

void
Resource_limits::init_resource_spare(Uint32 id, Uint32 pct)
{
  require(m_limit[id - 1].m_spare_pct == 0);
  m_limit[id - 1].m_spare_pct = pct;

  (void) alloc_resource_spare(id, 0);
}

/**
 * Ndbd_mem_manager
 */

int
Ndbd_mem_manager::PageInterval::compare(const void* px, const void* py)
{
  const PageInterval* x = static_cast<const PageInterval*>(px);
  const PageInterval* y = static_cast<const PageInterval*>(py);

  if (x->start < y->start)
  {
    return -1;
  }
  if (x->start > y->start)
  {
    return +1;
  }
  if (x->end < y->end)
  {
    return -1;
  }
  if (x->end > y->end)
  {
    return +1;
  }
  return 0;
}

Uint32
Ndbd_mem_manager::ndb_log2(Uint32 input)
{
  if (input > 65535)
    return 16;
  input = input | (input >> 8);
  input = input | (input >> 4);
  input = input | (input >> 2);
  input = input | (input >> 1);
  Uint32 output = (input & 0x5555) + ((input >> 1) & 0x5555);
  output = (output & 0x3333) + ((output >> 2) & 0x3333);
  output = output + (output >> 4);
  output = (output & 0xf) + ((output >> 8) & 0xf);
  return output;
}

Ndbd_mem_manager::Ndbd_mem_manager()
: m_base_page(NULL),
  m_dump_on_alloc_fail(false),
  m_mapped_pages_count(0),
  m_mapped_pages_new_count(0)
{
  memset(m_buddy_lists, 0, sizeof(m_buddy_lists));

  if (sizeof(Free_page_data) != (4 * (1 << FPD_2LOG)))
  {
    g_eventLogger->error("Invalid build, ndbd_malloc_impl.cpp:%d", __LINE__);
    abort();
  }
  mt_mem_manager_init();
}

void*
Ndbd_mem_manager::get_memroot() const
{
#ifdef NDBD_RANDOM_START_PAGE
  return (void*)(m_base_page - m_random_start_page_id);
#else
  return (void*)m_base_page;
#endif
}

/**
 *
 * resource N has following semantics:
 *
 * m_min = reserved
 * m_curr = currently used including spare pages
 * m_max = max alloc
 * m_spare = pages reserved for restart or special use
 *
 */
void
Ndbd_mem_manager::set_resource_limit(const Resource_limit& rl)
{
  require(rl.m_resource_id > 0);
  mt_mem_manager_lock();
  m_resource_limits.init_resource_limit(rl.m_resource_id, rl.m_min, rl.m_max);
  mt_mem_manager_unlock();
}

bool
Ndbd_mem_manager::get_resource_limit(Uint32 id, Resource_limit& rl) const
{
  /**
   * DUMP DumpPageMemory(1000) is agnostic about what resource groups exists.
   * Allowing use of any id.
   */
  if (1 <= id && id <= MM_RG_COUNT)
  {
    mt_mem_manager_lock();
    m_resource_limits.get_resource_limit(id, rl);
    mt_mem_manager_unlock();
    return true;
  }
  return false;
}

bool
Ndbd_mem_manager::get_resource_limit_nolock(Uint32 id, Resource_limit& rl) const
{
  assert(id > 0);
  if (id <= MM_RG_COUNT)
  {
    m_resource_limits.get_resource_limit(id, rl);
    return true;
  }
  return false;
}

Uint32
Ndbd_mem_manager::get_allocated() const
{
  mt_mem_manager_lock();
  const Uint32 val = m_resource_limits.get_allocated();
  mt_mem_manager_unlock();
  return val;
}

Uint32
Ndbd_mem_manager::get_reserved() const
{
  mt_mem_manager_lock();
  const Uint32 val = m_resource_limits.get_reserved();
  mt_mem_manager_unlock();
  return val;
}

Uint32
Ndbd_mem_manager::get_shared() const
{
  mt_mem_manager_lock();
  const Uint32 val = m_resource_limits.get_shared();
  mt_mem_manager_unlock();
  return val;
}

Uint32
Ndbd_mem_manager::get_free_shared() const
{
  mt_mem_manager_lock();
  const Uint32 val = m_resource_limits.get_free_shared();
  mt_mem_manager_unlock();
  return val;
}

Uint32
Ndbd_mem_manager::get_free_shared_nolock() const
{
  /* Used by mt_getSendBufferLevel for quick read. */
  const Uint32 val = m_resource_limits.get_free_shared(); // racy
  return val;
}

Uint32
Ndbd_mem_manager::get_spare() const
{
  mt_mem_manager_lock();
  const Uint32 val = m_resource_limits.get_spare();
  mt_mem_manager_unlock();
  return val;
}

Uint32
Ndbd_mem_manager::get_in_use() const
{
  mt_mem_manager_lock();
  const Uint32 val = m_resource_limits.get_in_use();
  mt_mem_manager_unlock();
  return val;
}

Uint32
Ndbd_mem_manager::get_reserved_in_use() const
{
  mt_mem_manager_lock();
  const Uint32 val = m_resource_limits.get_reserved_in_use();
  mt_mem_manager_unlock();
  return val;
}

Uint32
Ndbd_mem_manager::get_shared_in_use() const
{
  mt_mem_manager_lock();
  const Uint32 val = m_resource_limits.get_shared_in_use();
  mt_mem_manager_unlock();
  return val;
}

int
cmp_chunk(const void * chunk_vptr_1, const void * chunk_vptr_2)
{
  InitChunk * ptr1 = (InitChunk*)chunk_vptr_1;
  InitChunk * ptr2 = (InitChunk*)chunk_vptr_2;
  if (ptr1->m_ptr < ptr2->m_ptr)
    return -1;
  if (ptr1->m_ptr > ptr2->m_ptr)
    return 1;
  assert(false);
  return 0;
}

bool
Ndbd_mem_manager::init(Uint32 *watchCounter,
                       Uint32 max_pages,
                       bool alloc_less_memory)
{
  assert(m_base_page == 0);
  assert(max_pages > 0);
  assert(m_resource_limits.get_allocated() == 0);

  DEB_MEM_ALLOC(("Allocating %u pages", max_pages));

  if (watchCounter)
    *watchCounter = 9;

  Uint32 pages = max_pages;
  Uint32 max_page = 0;
  
  const Uint64 pg = Uint64(sizeof(Alloc_page));
  if (pages == 0)
  {
    return false;
  }

#if SIZEOF_CHARP == 4
  Uint64 sum = (pg*pages); 
  if (sum >= (Uint64(1) << 32))
  {
    g_eventLogger->error("Trying to allocate more that 4Gb with 32-bit binary!!");
    return false;
  }
#endif

  Uint32 allocated = 0;
  m_base_page = NULL;

#ifdef USE_DO_VIRTUAL_ALLOC
  {
    // Add one page per extra ZONE used due to using all zones even if not needed.
    int zones_needed = 1;
    for (zones_needed = 1; zones_needed <= ZONE_COUNT; zones_needed++)
    {
      if (pages < (zone_bound[zones_needed - 1] << PAGES_PER_REGION_LOG))
        break;
    }
    pages += ZONE_COUNT - zones_needed;
    InitChunk chunks[ZONE_COUNT];
    if (do_virtual_alloc(pages, chunks, watchCounter, &m_base_page))
    {
      for (int i = 0; i < ZONE_COUNT; i++)
      {
        m_unmapped_chunks.push_back(chunks[i]);
        DEB_MEM_ALLOC(("Adding one more chunk with %u pages",
                       chunks[i].m_cnt));
        allocated += chunks[i].m_cnt;
      }
      require(allocated == pages);
    }
  }
#endif

#ifdef NDBD_RANDOM_START_PAGE
  if (m_base_page == NULL)
  {
    /**
     * In order to find bad-users of page-id's
     *   we add a random offset to the page-id's returned
     *   however, due to ZONE_19 that offset can't be that big
     *   (since we at get_page don't know if it's a HI/LO page)
     */
    Uint32 max_rand_start = ZONE_19_BOUND - 1;
    if (max_rand_start > pages)
    {
      max_rand_start -= pages;
      if (max_rand_start > 0x10000)
        m_random_start_page_id =
          0x10000 + (rand() % (max_rand_start - 0x10000));
      else if (max_rand_start)
        m_random_start_page_id = rand() % max_rand_start;

      assert(Uint64(pages) + Uint64(m_random_start_page_id) <= 0xFFFFFFFF);

      g_eventLogger->info("using m_random_start_page_id: %u (%.8x)",
                          m_random_start_page_id, m_random_start_page_id);
    }
  }
#endif

  /**
   * Do malloc
   */
  while (m_unmapped_chunks.size() < MAX_CHUNKS && allocated < pages)
  {
    InitChunk chunk;
    memset(&chunk, 0, sizeof(chunk));
    
    if (do_malloc(pages - allocated, &chunk, watchCounter, m_base_page))
    {
      if (watchCounter)
        *watchCounter = 9;

      m_unmapped_chunks.push_back(chunk);
      allocated += chunk.m_cnt;
      DEB_MEM_ALLOC(("malloc of a chunk of %u pages", chunk.m_cnt));
      if (allocated < pages)
      {
        /* Add one more page for another chunk */
        pages++;
      }
    }
    else
    {
      break;
    }
  }
  
  if (allocated < m_resource_limits.get_free_reserved())
  {
    g_eventLogger->
      error("Unable to alloc min memory from OS: min: %lldMb "
            " allocated: %lldMb",
            (Uint64)(sizeof(Alloc_page)*m_resource_limits.get_free_reserved()) >> 20,
            (Uint64)(sizeof(Alloc_page)*allocated) >> 20);
    return false;
  }
  else if (allocated < pages)
  {
    g_eventLogger->
      warning("Unable to alloc requested memory from OS: min: %lldMb"
              " requested: %lldMb allocated: %lldMb",
              (Uint64)(sizeof(Alloc_page)*m_resource_limits.get_free_reserved())>>20,
              (Uint64)(sizeof(Alloc_page)*max_pages)>>20,
              (Uint64)(sizeof(Alloc_page)*allocated)>>20);
    if (!alloc_less_memory)
      return false;
  }

  if (m_base_page == NULL)
  {
    /**
     * Sort chunks...
     */
    qsort(m_unmapped_chunks.getBase(), m_unmapped_chunks.size(),
          sizeof(InitChunk), cmp_chunk);

    m_base_page = m_unmapped_chunks[0].m_ptr;
  }

  for (Uint32 i = 0; i<m_unmapped_chunks.size(); i++)
  {
    UintPtr start = UintPtr(m_unmapped_chunks[i].m_ptr) - UintPtr(m_base_page);
    start >>= (2 + BMW_2LOG);
    assert((Uint64(start) >> 32) == 0);
    m_unmapped_chunks[i].m_start = Uint32(start);
    Uint64 last64 = start + m_unmapped_chunks[i].m_cnt;
    assert((last64 >> 32) == 0);
    Uint32 last = Uint32(last64);

    if (last > max_page)
      max_page = last;
  }

  g_eventLogger->info("Ndbd_mem_manager::init(%d) min: %lluMb initial: %lluMb",
                      alloc_less_memory,
                      (pg*m_resource_limits.get_free_reserved())>>20,
                      (pg*pages) >> 20);

  m_resource_limits.set_max_page(max_page);
  m_resource_limits.set_allocated(0);

  return true;
}

void
Ndbd_mem_manager::map(Uint32 * watchCounter, bool memlock, Uint32 resources[])
{
  require(watchCounter != nullptr);
  Uint32 limit = ~(Uint32)0;
  Uint32 sofar = 0;

  if (resources != 0)
  {
    limit = 0;
    for (Uint32 i = 0; resources[i] ; i++)
    {
      limit += m_resource_limits.get_resource_reserved(resources[i]);
    }
  }

  while (m_unmapped_chunks.size() && sofar < limit)
  {
    Uint32 remain = limit - sofar;

    unsigned idx = m_unmapped_chunks.size() - 1;
    InitChunk * chunk = &m_unmapped_chunks[idx];
    if (watchCounter)
      *watchCounter = 9;

    if (chunk->m_cnt > remain)
    {
      /**
       * Split chunk
       */
      Uint32 extra = chunk->m_cnt - remain;
      chunk->m_cnt = remain;

      InitChunk newchunk;
      newchunk.m_start = chunk->m_start + remain;
      newchunk.m_ptr = m_base_page + newchunk.m_start;
      newchunk.m_cnt = extra;
      m_unmapped_chunks.push_back(newchunk);

      // pointer could have changed after m_unmapped_chunks.push_back
      chunk = &m_unmapped_chunks[idx];
    }

    g_eventLogger->info("Touch Memory Starting, %u pages, page size = %d",
                        chunk->m_cnt,
                        (int)sizeof(Alloc_page));

    ndbd_alloc_touch_mem(chunk->m_ptr,
                         chunk->m_cnt * sizeof(Alloc_page),
                         watchCounter);

    g_eventLogger->info("Touch Memory Completed");

    if (memlock)
    {
      /**
       * memlock pages that I added...
       */
      if (watchCounter)
        *watchCounter = 9;

      /**
       * Don't memlock everything in one go...
       *   cause then process won't be killable
       */
      const Alloc_page * start = chunk->m_ptr;
      Uint32 cnt = chunk->m_cnt;
      g_eventLogger->info("Lock Memory Starting, %u pages, page size = %d",
                          chunk->m_cnt,
                          (int)sizeof(Alloc_page));

      while (cnt > 32768) // 1G
      {
        if (watchCounter)
          *watchCounter = 9;

        NdbMem_MemLock(start, 32768 * sizeof(Alloc_page));
        start += 32768;
        cnt -= 32768;
      }
      if (watchCounter)
        *watchCounter = 9;

      NdbMem_MemLock(start, cnt * sizeof(Alloc_page));

      g_eventLogger->info("Lock memory Completed");
    }

    DEB_MEM_ALLOC(("grow %u pages", chunk->m_cnt));
    grow(chunk->m_start, chunk->m_cnt);
    sofar += chunk->m_cnt;

    m_unmapped_chunks.erase(idx);
  }
  
  mt_mem_manager_lock();
  if (resources == nullptr)
  {
    // Allow low prio use of shared only when all memory is mapped.
    m_resource_limits.update_low_prio_shared_limit();
  }
  m_resource_limits.check();
  mt_mem_manager_unlock();

  if (resources == 0 && memlock)
  {
    NdbMem_MemLockAll(1);
  }

  /* Note: calls to map() must be serialized by other means. */
  m_mapped_pages_lock.write_lock();
  if (m_mapped_pages_new_count != m_mapped_pages_count)
  {
    /* Do not support shrinking memory */
    require(m_mapped_pages_new_count > m_mapped_pages_count);

    qsort(m_mapped_pages,
          m_mapped_pages_new_count,
          sizeof(m_mapped_pages[0]),
          PageInterval::compare);

    /* Validate no overlapping intervals */
    for (Uint32 i = 1; i < m_mapped_pages_new_count; i++)
    {
      require(m_mapped_pages[i - 1].end <= m_mapped_pages[i].start);
    }

    m_mapped_pages_count = m_mapped_pages_new_count;
  }
  m_mapped_pages_lock.write_unlock();
}

void
Ndbd_mem_manager::init_resource_spare(Uint32 id, Uint32 pct)
{
  mt_mem_manager_lock();
  m_resource_limits.init_resource_spare(id, pct);
  mt_mem_manager_unlock();
}

#include <NdbOut.hpp>

void
Ndbd_mem_manager::grow(Uint32 start, Uint32 cnt)
{
  assert(cnt);
  Uint32 start_bmp = start >> BPP_2LOG;
  Uint32 last_bmp = (start + cnt - 1) >> BPP_2LOG;
  
#if SIZEOF_CHARP == 4
  assert(start_bmp == 0 && last_bmp == 0);
#endif
  
  if (start_bmp != last_bmp)
  {
    Uint32 tmp = ((start_bmp + 1) << BPP_2LOG) - start;
    grow(start, tmp);
    grow((start_bmp + 1) << BPP_2LOG, cnt - tmp);
    return;
  }

  for (Uint32 i = 0; i<m_used_bitmap_pages.size(); i++)
    if (m_used_bitmap_pages[i] == start_bmp)
    {
      /* m_mapped_pages should contain the ranges of allocated pages.
       * In release build there will typically be one big range.
       * In debug build there are typically four ranges, one per allocation
       * zone.
       * Not all ranges passed to grow() may be used, but for a big range it
       * is only the first partial range that can not be used.
       * This part of code will be called with the range passed to top call to
       * grow() broken up in 8GB regions by recursion above, and the ranges
       * will always be passed with increasing addresses, and the start will
       * match end of previous calls range.
       * To keep use as few entries as possible in m_mapped_pages these
       * adjacent ranges are combined.
       */
      if (m_mapped_pages_new_count > 0 &&
          m_mapped_pages[m_mapped_pages_new_count - 1].end == start)
      {
        m_mapped_pages[m_mapped_pages_new_count - 1].end = start + cnt;
      }
      else
      {
        require(m_mapped_pages_new_count < NDB_ARRAY_SIZE(m_mapped_pages));
        m_mapped_pages[m_mapped_pages_new_count].start = start;
        m_mapped_pages[m_mapped_pages_new_count].end = start + cnt;
        m_mapped_pages_new_count++;
      }
      goto found;
    }

  if (start != (start_bmp << BPP_2LOG))
  {
    g_eventLogger->info(
        "ndbd_malloc_impl.cpp:%d:grow(%d, %d) %d!=%d not using %uMb"
        " - Unable to use due to bitmap pages missaligned!!",
        __LINE__, start, cnt, start, (start_bmp << BPP_2LOG),
        (cnt >> (20 - 15)));
    g_eventLogger->error("ndbd_malloc_impl.cpp:%d:grow(%d, %d) not using %uMb"
                         " - Unable to use due to bitmap pages missaligned!!",
                         __LINE__, start, cnt,
                         (cnt >> (20 - 15)));

    dump(false);
    return;
  }
  
#ifdef UNIT_TEST
  g_eventLogger->info("creating bitmap page %d", start_bmp);
#endif

  if (m_mapped_pages_new_count > 0 &&
      m_mapped_pages[m_mapped_pages_new_count - 1].end == start)
  {
    m_mapped_pages[m_mapped_pages_new_count - 1].end = start + cnt;
  }
  else
  {
    require(m_mapped_pages_new_count < NDB_ARRAY_SIZE(m_mapped_pages));
    m_mapped_pages[m_mapped_pages_new_count].start = start;
    m_mapped_pages[m_mapped_pages_new_count].end = start + cnt;
    m_mapped_pages_new_count++;
  }

  {
    Alloc_page* bmp = m_base_page + start;
    memset(bmp, 0, sizeof(Alloc_page));
    cnt--;
    start++;
  }
  m_used_bitmap_pages.push_back(start_bmp);

found:
  if ((start + cnt) == ((start_bmp + 1) << BPP_2LOG))
  {
    cnt--; // last page is always marked as empty
  }

  if (cnt)
  {
    mt_mem_manager_lock();
    const Uint32 allocated = m_resource_limits.get_allocated();
    m_resource_limits.set_allocated(allocated + cnt);
    const Uint64 mbytes = ((Uint64(cnt)*32) + 1023) / 1024;
    /**
     * grow first split large page ranges to ranges completely within
     * a BPP regions.
     * Boundary between lo and high zone coincide with a BPP region
     * boundary.
     */
    NDB_STATIC_ASSERT((ZONE_19_BOUND & ((1 << BPP_2LOG) - 1)) == 0);
    if (start < ZONE_19_BOUND)
    {
      require(start + cnt < ZONE_19_BOUND);
      g_eventLogger->info("Adding %uMb to ZONE_19 (%u, %u)",
                          (Uint32)mbytes,
                          start,
                          cnt);
    }
    else if (start < ZONE_27_BOUND)
    {
      require(start + cnt < ZONE_27_BOUND);
      g_eventLogger->info("Adding %uMb to ZONE_27 (%u, %u)",
                          (Uint32)mbytes,
                          start,
                          cnt);
    }
    else if (start < ZONE_30_BOUND)
    {
      require(start + cnt < ZONE_30_BOUND);
      g_eventLogger->info("Adding %uMb to ZONE_30 (%u, %u)",
                          (Uint32)mbytes,
                          start,
                          cnt);
    }
    else
    {
      g_eventLogger->info("Adding %uMb to ZONE_32 (%u, %u)",
                          (Uint32)mbytes,
                          start,
                          cnt);
    }
    release(start, cnt);
    mt_mem_manager_unlock();
  }
}

void
Ndbd_mem_manager::release(Uint32 start, Uint32 cnt)
{
  assert(start);
#if defined VM_TRACE || defined ERROR_INSERT
  memset(m_base_page + start, 0xF5, cnt * sizeof(m_base_page[0]));
#endif

  set(start, start+cnt-1);

  Uint32 zone = get_page_zone(start);
  release_impl(zone, start, cnt);
}

void
Ndbd_mem_manager::release_impl(Uint32 zone, Uint32 start, Uint32 cnt)
{
  assert(start);

  Uint32 test = check(start-1, start+cnt);
  if (test & 1)
  {
    Free_page_data *fd = get_free_page_data(m_base_page + start - 1,
					    start - 1);
    Uint32 sz = fd->m_size;
    Uint32 left = start - sz;
    remove_free_list(zone, left, fd->m_list);
    cnt += sz;
    start = left;
  }

  Uint32 right = start + cnt;
  if (test & 2)
  {
    Free_page_data *fd = get_free_page_data(m_base_page+right, right);
    Uint32 sz = fd->m_size;
    remove_free_list(zone, right, fd->m_list);
    cnt += sz;
  }

  insert_free_list(zone, start, cnt);
}

void
Ndbd_mem_manager::alloc(AllocZone zone,
                        Uint32* ret,
                        Uint32 *pages,
                        Uint32 min)
{
  const Uint32 save = * pages;
  for (Uint32 z = zone; ; z--)
  {
    alloc_impl(z, ret, pages, min);
    if (*pages)
    {
#if defined VM_TRACE || defined ERROR_INSERT
      memset(m_base_page + *ret, 0xF6, *pages * sizeof(m_base_page[0]));
#endif
      return;
    }
    if (z == 0)
    {
      if (unlikely(m_dump_on_alloc_fail))
      {
        g_eventLogger->info(
            "Page allocation failed in %s: zone=%u pages=%u (at least %u)",
            __func__, zone, save, min);
        dump(true);
      }
      return;
    }
    * pages = save;
  }
}

void
Ndbd_mem_manager::alloc_impl(Uint32 zone,
                             Uint32* ret,
                             Uint32 *pages,
                             Uint32 min)
{
  Int32 i;
  Uint32 start;
  Uint32 cnt = * pages;
  Uint32 list = ndb_log2(cnt - 1);

  assert(cnt);
  assert(list <= 16);

  for (i = list; i < 16; i++)
  {
    if ((start = m_buddy_lists[zone][i]))
    {
/* ---------------------------------------------------------------- */
/*       PROPER AMOUNT OF PAGES WERE FOUND. NOW SPLIT THE FOUND     */
/*       AREA AND RETURN THE PART NOT NEEDED.                       */
/* ---------------------------------------------------------------- */

      Uint32 sz = remove_free_list(zone, start, i);
      Uint32 extra = sz - cnt;
      assert(sz >= cnt);
      if (extra)
      {
	insert_free_list(zone, start + cnt, extra);
	clear_and_set(start, start+cnt-1);
      }
      else
      {
	clear(start, start+cnt-1);
      }
      * ret = start;
      assert(m_resource_limits.get_in_use() + cnt <=
             m_resource_limits.get_allocated());
      return;
    }
  }

  /**
   * Could not find in guaranteed list...
   *   search in other lists...
   */

  Int32 min_list = ndb_log2(min - 1);
  assert((Int32)list >= min_list);
  for (i = list - 1; i >= min_list; i--) 
  {
    if ((start = m_buddy_lists[zone][i]))
    {
      Uint32 sz = remove_free_list(zone, start, i);
      Uint32 extra = sz - cnt;
      if (sz > cnt)
      {
	insert_free_list(zone, start + cnt, extra);	
	sz -= extra;
	clear_and_set(start, start+sz-1);
      }
      else
      {
	clear(start, start+sz-1);
      }

      * ret = start;
      * pages = sz;
      assert(m_resource_limits.get_in_use() + sz <=
             m_resource_limits.get_allocated());
      return;
    }
  }
  * pages = 0;
}

void
Ndbd_mem_manager::insert_free_list(Uint32 zone, Uint32 start, Uint32 size)
{
  Uint32 list = ndb_log2(size) - 1;
  Uint32 last = start + size - 1;

  Uint32 head = m_buddy_lists[zone][list];
  Free_page_data* fd_first = get_free_page_data(m_base_page+start, 
						start);
  fd_first->m_list = list;
  fd_first->m_next = head;
  fd_first->m_prev = 0;
  fd_first->m_size = size;

  Free_page_data* fd_last = get_free_page_data(m_base_page+last, last);
  fd_last->m_list = list;
  fd_last->m_next = head;
  fd_last->m_prev = 0;
  fd_last->m_size = size;
  
  if (head)
  {
    Free_page_data* fd = get_free_page_data(m_base_page+head, head);
    assert(fd->m_prev == 0);
    assert(fd->m_list == list);
    fd->m_prev = start;
  }
  
  m_buddy_lists[zone][list] = start;
}

Uint32 
Ndbd_mem_manager::remove_free_list(Uint32 zone, Uint32 start, Uint32 list)
{
  Free_page_data* fd = get_free_page_data(m_base_page+start, start);
  Uint32 size = fd->m_size;
  Uint32 next = fd->m_next;
  Uint32 prev = fd->m_prev;
  assert(fd->m_list == list);
  
  if (prev)
  {
    assert(m_buddy_lists[zone][list] != start);
    fd = get_free_page_data(m_base_page+prev, prev);
    assert(fd->m_next == start);
    assert(fd->m_list == list);
    fd->m_next = next;
  }
  else
  {
    assert(m_buddy_lists[zone][list] == start);
    m_buddy_lists[zone][list] = next;
  }
  
  if (next)
  {
    fd = get_free_page_data(m_base_page+next, next);
    assert(fd->m_list == list);
    assert(fd->m_prev == start);
    fd->m_prev = prev;
  }

  return size;
}

void
Ndbd_mem_manager::dump(bool locked) const
{
  if (!locked)
    mt_mem_manager_lock();
  g_eventLogger->info("Begin Ndbd_mem_manager::dump");
  for (Uint32 zone = 0; zone < ZONE_COUNT; zone ++)
  {
    g_eventLogger->info("zone %u", zone);
    for (Uint32 i = 0; i<16; i++)
    {
      Uint32 head = m_buddy_lists[zone][i];
      if (head == 0)
        continue;
      g_eventLogger->info(" list: %d - ", i);
      while(head)
      {
        Free_page_data* fd = get_free_page_data(m_base_page+head, head);
        g_eventLogger->info("[ i: %d prev %d next %d list %d size %d ] ", head,
                            fd->m_prev, fd->m_next, fd->m_list, fd->m_size);
        head = fd->m_next;
      }
      g_eventLogger->info("EOL");
    }
  }
  m_resource_limits.dump();
  g_eventLogger->info("End Ndbd_mem_manager::dump");
  if (!locked)
    mt_mem_manager_unlock();
}

void
Ndbd_mem_manager::dump_on_alloc_fail(bool on)
{
  m_dump_on_alloc_fail = on;
}

void
Ndbd_mem_manager::lock()
{
  mt_mem_manager_lock();
}

void
Ndbd_mem_manager::unlock()
{
  mt_mem_manager_unlock();
}

void*
Ndbd_mem_manager::alloc_page(Uint32 type,
                             Uint32* i,
                             AllocZone zone,
                             bool locked,
                             bool use_max_part)
{
  Uint32 idx = type & RG_MASK;
  assert(idx && idx <= MM_RG_COUNT);
  if (!locked)
    mt_mem_manager_lock();

  m_resource_limits.reclaim_lent_pages(idx, 1);

  Uint32 cnt = 1;
  const Uint32 min = 1;
  const Uint32 free_res = m_resource_limits.get_resource_free_reserved(idx);
  if (free_res < cnt)
  {
    if (use_max_part)
    {
      const Uint32 free_shr = m_resource_limits.get_resource_free_shared(idx);
      const Uint32 free = m_resource_limits.get_resource_free(idx);
      if (free < min || (free_shr + free_res < min))
      {
        if (unlikely(m_dump_on_alloc_fail))
        {
          g_eventLogger->info(
              "Page allocation failed in %s: no free resource page.", __func__);
          dump(true);
        }
        if (!locked)
          mt_mem_manager_unlock();
        return NULL;
      }
    }
    else
    {
      if (unlikely(m_dump_on_alloc_fail))
      {
        g_eventLogger->info(
            "Page allocation failed in %s: no free reserved resource page.",
            __func__);
        dump(true);
      }
      if (!locked)
        mt_mem_manager_unlock();
      return NULL;
    }
  }
  alloc(zone, i, &cnt, min);
  if (likely(cnt))
  {
    const Uint32 spare_taken =
      m_resource_limits.post_alloc_resource_pages(idx, cnt);
    if (spare_taken > 0)
    {
      require(spare_taken == cnt);
      release(*i, spare_taken);
      m_resource_limits.check();
      if (unlikely(m_dump_on_alloc_fail))
      {
        g_eventLogger->info(
            "Page allocation failed in %s: no free non-spare resource page.",
            __func__);
        dump(true);
      }
      if (!locked)
        mt_mem_manager_unlock();
      *i = RNIL;
      return NULL;
    }
    m_resource_limits.check();
    if (!locked)
      mt_mem_manager_unlock();
#ifdef NDBD_RANDOM_START_PAGE
    *i += m_random_start_page_id;
    return m_base_page + *i - m_random_start_page_id;
#else
    return m_base_page + *i;
#endif
  }
  if (unlikely(m_dump_on_alloc_fail))
  {
    g_eventLogger->info(
        "Page allocation failed in %s: no page available in zone %d.", __func__,
        zone);
    dump(true);
  }
  if (!locked)
    mt_mem_manager_unlock();
  return 0;
}

void*
Ndbd_mem_manager::alloc_spare_page(Uint32 type, Uint32* i, AllocZone zone)
{
  Uint32 idx = type & RG_MASK;
  assert(idx && idx <= MM_RG_COUNT);
  mt_mem_manager_lock();

  Uint32 cnt = 1;
  const Uint32 min = 1;
  if (m_resource_limits.get_resource_spare(idx) >= min)
  {
    alloc(zone, i, &cnt, min);
    if (likely(cnt))
    {
      assert(cnt == min);
      m_resource_limits.post_alloc_resource_spare(idx, cnt);
      m_resource_limits.check();
      mt_mem_manager_unlock();
#ifdef NDBD_RANDOM_START_PAGE
      *i += m_random_start_page_id;
      return m_base_page + *i - m_random_start_page_id;
#else
      return m_base_page + *i;
#endif
    }
  }
  if (unlikely(m_dump_on_alloc_fail))
  {
    g_eventLogger->info("Page allocation failed in %s: no spare page.",
                        __func__);
    dump(true);
  }
  mt_mem_manager_unlock();
  return 0;
}

void
Ndbd_mem_manager::release_page(Uint32 type, Uint32 i, bool locked)
{
  Uint32 idx = type & RG_MASK;
  assert(idx && idx <= MM_RG_COUNT);
  if (!locked)
    mt_mem_manager_lock();

#ifdef NDBD_RANDOM_START_PAGE
  i -= m_random_start_page_id;
#endif

  release(i, 1);
  m_resource_limits.post_release_resource_pages(idx, 1);

  m_resource_limits.check();
  if (!locked)
    mt_mem_manager_unlock();
}

void
Ndbd_mem_manager::alloc_pages(Uint32 type,
                              Uint32* i,
                              Uint32 *cnt,
                              Uint32 min,
                              AllocZone zone,
                              bool locked)
{
  Uint32 idx = type & RG_MASK;
  assert(idx && idx <= MM_RG_COUNT);
  if (!locked)
    mt_mem_manager_lock();

  Uint32 req = *cnt;
  m_resource_limits.reclaim_lent_pages(idx, req);

  const Uint32 free_res = m_resource_limits.get_resource_free_reserved(idx);
  if (free_res < req)
  {
    const Uint32 free = m_resource_limits.get_resource_free(idx);
    if (free < req)
    {
      req = free;
    }
    const Uint32 free_shr = m_resource_limits.get_free_shared();
    if (free_shr + free_res < req)
    {
      req = free_shr + free_res;
    }
    if (req < min)
    {
      *cnt = 0;
      if (unlikely(m_dump_on_alloc_fail))
      {
        g_eventLogger->info(
            "Page allocation failed in %s: not enough free resource pages.",
            __func__);
        dump(true);
      }
      if (!locked)
        mt_mem_manager_unlock();
      return;
    }
  }

  // Hi order allocations can always use any zone
  alloc(zone, i, &req, min);
  const Uint32 spare_taken = m_resource_limits.post_alloc_resource_pages(idx, req);
  if (spare_taken > 0)
  {
    req -= spare_taken;
    release(*i + req, spare_taken);
  }
  if (0 < req && req < min)
  {
    release(*i, req);
    m_resource_limits.post_release_resource_pages(idx, req);
    req = 0;
  }
  * cnt = req;
  m_resource_limits.check();
  if (req == 0 && unlikely(m_dump_on_alloc_fail))
  {
    g_eventLogger->info(
        "Page allocation failed in %s: no page available in zone %d.", __func__,
        zone);
    dump(true);
  }
  if (!locked)
    mt_mem_manager_unlock();
#ifdef NDBD_RANDOM_START_PAGE
  *i += m_random_start_page_id;
#endif
}

void
Ndbd_mem_manager::release_pages(Uint32 type, Uint32 i, Uint32 cnt, bool locked)
{
  Uint32 idx = type & RG_MASK;
  assert(idx && idx <= MM_RG_COUNT);
  if (!locked)
    mt_mem_manager_lock();

#ifdef NDBD_RANDOM_START_PAGE
  i -= m_random_start_page_id;
#endif

  release(i, cnt);
  m_resource_limits.post_release_resource_pages(idx, cnt);
  m_resource_limits.check();
  if (!locked)
    mt_mem_manager_unlock();
}

/** Transfer pages between resource groups without risk that some other
 * resource gets them in betweeen.
 *
 * In some cases allocating pages fail.  Preferable the application can handle
 * the allocation failure gracefully.
 * In other cases application really need to have those pages.
 * For that the memory manager support giving up and taking pages.
 *
 * The allocation may fail, either because there are no free pages at all, or
 * that all free pages are reserved by other resources, or that the current
 * resource have reached it upper limit of allowed allocations.
 *
 * One can use a combination of give_up_pages() and take_pages() instead of
 * release_pages() and alloc_pages() to avoid that the pages are put into the
 * global free list of pages but rather only the book keeping about how many
 * pages are used in what way.
 *
 * An examples transferring pages from DM to TM.
 *
 * 1) Try do an ordinary alloc_pages(TM) first. If that succeed there is no
 *    need for special page transfer.  Follow up with release_pages(DM).
 *
 * 2) When alloc_pages(TM) fail, do give_up_pages(DM) instead of
 *    release_pages(DM).  This function should never fail.
 *    All given up pages will be counted as lent.
 *    These pages may not be further used by DM until lent count is decreased.
 *    See point 5) how lent pages are reclaimed.
 *
 * 3) Call take_pages(TM).  This will increase the count of pages in use for
 *    TM, as a normal alloc_pages() would do.  And the borrowed pages count is
 *    increased.
 *
 * 4) When later calling release_pages(TM), it will decrease both the global
 *    and the TM resource borrow count.  This will eventually allow reclaim of
 *    lent DM pages, see next point.
 *
 * 5) When later calling alloc_pages(DM) it will first try to reclaim lent out
 *    pages.
 *    If the global counts for untaken and borrowed toghether is less than the
 *    global lent count, that means that some lent pages have been
 *    taken/borrowed and also released and those we may reclaim that many lent
 *    pages.
 *    If DM has lent pages, The minimum of globally reclaimable lent pages and
 *    request count of pages and the number of lent pages in resource are
 *    reclaimed.
 *
 * Code example:
 *
    ...
    Uint32 page_count = 3;
    Uint32 DM_page_no;
    Uint32 DM_page_count = page_count;
    mem.alloc_pages(RG_DM, &DM_page_no, &DM_page_count, page_count);
    ...
    assert(DM_page_count == page_count);
    Uint32 TM_page_no;
    Uint32 TM_page_count = page_count;
    mem.alloc_pages(RG_TM, &TM_page_no, &TM_page_count, page_count);
    if (TM_page_count != 0)
    {
      mem.release_pages(RG_DM, DM_page_no, page_count);
    }
    else
    {
      require(mem.give_up_pages(RG_DM, page_count));
      require(mem.take_pages(RG_TM, page_count));
      DM_page_no = TM_page_no;
      TM_page_count = page_count;
    }
    ...
    mem.release_pages(RG_TM, TM_page_no, TM_page_count);
    ...
    DM_page_count = 1;
    // Typically will reclaim one lent out DM page
    mem.alloc_pages(RG_DM, &DM_page_no, &DM_page_count, 1);
    ...
    mem.release_pages(RG_DM, DM_page_no, DM_page_count);
    ...
 */

bool Resource_limits::give_up_pages(Uint32 id, Uint32 cnt)
{
  const Resource_limit& rl = m_limit[id - 1];

  /* Only support give up pages for resources with only reserved pages to
   * simplify logic.
   */

  require(rl.m_min == rl.m_max);

  if (get_resource_in_use(id) < cnt)
  {
    // Can not pass more pages than actually in use!
    return false;
  }

  post_release_resource_pages(id, cnt);
  inc_untaken(cnt);
  inc_resource_lent(id, cnt);
  inc_lent(cnt);
  dec_free_reserved(cnt);

  return true;
}

bool Ndbd_mem_manager::give_up_pages(Uint32 type, Uint32 cnt)
{
  Uint32 idx = type & RG_MASK;
  assert(idx && idx <= MM_RG_COUNT);
  mt_mem_manager_lock();

  if (!m_resource_limits.give_up_pages(idx, cnt))
  {
    m_resource_limits.dump();
    mt_mem_manager_unlock();
    return false;
  }

  m_resource_limits.check();
  mt_mem_manager_unlock();
  return true;
}

bool Resource_limits::take_pages(Uint32 id, Uint32 cnt)
{
  const Resource_limit& rl = m_limit[id - 1];

  /* Support take pages only for "unlimited" resources (m_max == HIGHEST_LIMIT)
   * and with no spare pages (m_spare_pct == 0) to simplify logic.
   */

  require(rl.m_max == Resource_limit::HIGHEST_LIMIT);
  require(rl.m_spare_pct == 0);

  if (m_untaken < cnt)
  {
    return false;
  }

  inc_resource_borrowed(id, cnt);
  inc_borrowed(cnt);
  dec_untaken(cnt);
  const Uint32 spare_taken = post_alloc_resource_pages(id, cnt);
  require(spare_taken == 0);

  return true;
}

bool Ndbd_mem_manager::take_pages(Uint32 type, Uint32 cnt)
{
  Uint32 idx = type & RG_MASK;
  assert(idx && idx <= MM_RG_COUNT);
  mt_mem_manager_lock();

  if (!m_resource_limits.take_pages(idx, cnt))
  {
    m_resource_limits.dump();
    mt_mem_manager_unlock();
    return false;
  }

  m_resource_limits.check();
  mt_mem_manager_unlock();
  return true;
}

template class Vector<InitChunk>;

#if defined(TEST_NDBD_MALLOC)

#include <Vector.hpp>
#include <NdbHost.h>
#include "portlib/ndb_stacktrace.h"
#include "portlib/NdbTick.h"

struct Chunk {
  Uint32 pageId;
  Uint32 pageCount;
};

struct Timer
{
  Uint64 sum;
  Uint32 cnt;

  Timer() { sum = cnt = 0;}

  NDB_TICKS st;

  void start() {
    st = NdbTick_getCurrentTicks();
  }

  Uint64 calc_diff() {
    const NDB_TICKS st2 = NdbTick_getCurrentTicks();
    const NdbDuration dur = NdbTick_Elapsed(st, st2);
    return dur.microSec();
  }
  
  void stop() {
    add(calc_diff());
  }
  
  void add(Uint64 diff) { sum += diff; cnt++;}

  void print(const char * title) const {
    float ps = sum;
    ps /= cnt;
    printf("%s %fus/call %lld %d\n", title, ps, sum, cnt);
  }
};

void abort_handler(int signum)
{
  ndb_print_stacktrace();
  signal(SIGABRT, SIG_DFL);
  abort();
}

class Test_mem_manager: public Ndbd_mem_manager
{
public:
  static constexpr Uint32 ZONE_COUNT = Ndbd_mem_manager::ZONE_COUNT;
  Test_mem_manager(Uint32 tot_mem,
                   Uint32 data_mem,
                   Uint32 trans_mem,
                   Uint32 data_mem2 = 0,
                   Uint32 trans_mem2 = 0);
  ~Test_mem_manager();
};

enum Resource_groups {
  RG_DM = 1,
  RG_TM = 2,
  RG_QM = 3,
  RG_DM2 = 4,
  RG_TM2 = 5,
  RG_QM2 = 6
};

Test_mem_manager::Test_mem_manager(Uint32 tot_mem,
                                   Uint32 data_mem,
                                   Uint32 trans_mem,
                                   Uint32 data_mem2,
                                   Uint32 trans_mem2)
{
  assert(tot_mem >= data_mem + trans_mem + data_mem2 + trans_mem2);

  Resource_limit rl;
  // Data memory
  rl.m_min = data_mem;
  rl.m_max = rl.m_min;
  rl.m_resource_id = RG_DM;
  set_resource_limit(rl);

  // Transaction memory
  rl.m_min = trans_mem;
  rl.m_max = Resource_limit::HIGHEST_LIMIT;
  rl.m_resource_id = RG_TM;
  set_resource_limit(rl);

  // Query memory
  rl.m_min = 0;
  rl.m_max = Resource_limit::HIGHEST_LIMIT;
  rl.m_resource_id = RG_QM;
  set_resource_limit(rl);

  // Data memory
  rl.m_min = data_mem2;
  rl.m_max = rl.m_min;
  rl.m_resource_id = RG_DM2;
  set_resource_limit(rl);

  // Transaction memory
  rl.m_min = trans_mem2;
  rl.m_max = Resource_limit::HIGHEST_LIMIT;
  rl.m_resource_id = RG_TM2;
  set_resource_limit(rl);

  // Query memory
  rl.m_min = 0;
  rl.m_max = Resource_limit::HIGHEST_LIMIT;
  rl.m_resource_id = RG_QM2;
  set_resource_limit(rl);

  /*
   * Add one extra page for the initial bitmap page and the final empty page
   * for each complete region (8GiB).
   * And one extra page for initial page of last region which do not need an
   * empty page.
   */
  require(tot_mem > 0);
  tot_mem += 2 * ((tot_mem - 1) / ALLOC_PAGES_PER_REGION) + 1;
  init(NULL, tot_mem);
  Uint32 dummy_watchdog_counter_marking_page_mem = 0;
  map(&dummy_watchdog_counter_marking_page_mem);
}

Test_mem_manager::~Test_mem_manager()
{
  require(m_resource_limits.get_in_use() == 0);
}

#define NDBD_MALLOC_PERF_TEST 0
static void perf_test(int sz, int run_time);
static void transfer_test();
static void ic_ndbd_malloc_test();

int 
main(int argc, char** argv)
{
  ndb_init();
  ndb_init_stacktrace();
  signal(SIGABRT, abort_handler);

  int sz = 1 * 32768;
  int run_time = 30;
  if (argc > 1)
    sz = 32 * atoi(argv[1]);

  if (argc > 2)
    run_time = atoi(argv[2]);

  g_eventLogger->createConsoleHandler();
  g_eventLogger->setCategory("ndbd_malloc-t");
  g_eventLogger->enable(Logger::LL_ON, Logger::LL_INFO);
  g_eventLogger->enable(Logger::LL_ON, Logger::LL_CRITICAL);
  g_eventLogger->enable(Logger::LL_ON, Logger::LL_ERROR);
  g_eventLogger->enable(Logger::LL_ON, Logger::LL_WARNING);

  transfer_test();

  if (NDBD_MALLOC_PERF_TEST)
  {
    perf_test(sz, run_time);
  }
  ic_ndbd_malloc_test();
  ndb_end(0);
}

#define DEBUG 0

void transfer_test()
{
  const Uint32 data_pages = 18;
  Test_mem_manager mem(data_pages, 4, 4, 4, 4);
  Ndbd_mem_manager::AllocZone zone = Ndbd_mem_manager::NDB_ZONE_LE_32;

  Uint32 dm[4 + 1];
  Uint32 dm2[4];
  Uint32 tm[6];
  Uint32 tm2[6];

  if (DEBUG) mem.dump(false);

  // Allocate 4 pages each from DM and DM2 resources.
  for (int i = 0; i < 4; i++)
  {
    require(mem.alloc_page(RG_DM, &dm[i], zone));
    require(mem.alloc_page(RG_DM2, &dm2[i], zone));
  }

  // Allocate 5 pages each from TM and TM2 resources.
  for (int i = 0; i < 5; i++)
  {
    require(mem.alloc_page(RG_TM, &tm[i], zone));
    require(mem.alloc_page(RG_TM2, &tm2[i], zone));
  }

  // Allocating a 6th page for TM should fail since all 18 pages are allocated.
  require(mem.alloc_page(RG_TM, &tm[5], zone) == nullptr);

  // Start transfer of pages from RG_DM to RG_TM
  require(mem.give_up_pages(RG_DM, 1));

  /* Start and complete transfer between RG_DM2 to RG_TM2 before completing
   * transfer from RG_DM to RG_TM started above.
   */
  require(mem.alloc_page(RG_TM2, &tm2[5], zone) == nullptr);
  require(mem.give_up_pages(RG_DM2, 1));
  require(mem.take_pages(RG_TM2, 1));
  tm2[5] = dm2[3];
  dm2[3] = RNIL;
  mem.release_page(RG_TM2, tm2[5]);

  /* Verify that one can not allocate a page for RG_DM since it already have
   * reached its maximum of 4 (including the lent page)
   */
  require(mem.alloc_page(RG_DM, &dm[4], zone) != nullptr);

  // Proceed with taking over the page to RG_TM
  require(mem.take_pages(RG_TM, 1));
  tm[5] = dm[3];
  dm[3] = RNIL;

  require(mem.alloc_page(RG_DM, &dm[3], zone) == nullptr);

  mem.release_page(RG_DM, dm[4]);
  mem.release_page(RG_TM, tm[5]);

  require(mem.alloc_page(RG_DM, &dm[3], zone));
  require(mem.alloc_page(RG_DM2, &dm2[3], zone));

  // Cleanup, release all allocated pages.
  for (int i = 0; i < 4; i++)
  {
    mem.release_page(RG_DM, dm[i]);
    mem.release_page(RG_DM2, dm2[i]);
  }

  for (int i = 0; i < 5; i++)
  {
    mem.release_page(RG_TM, tm[i]);
    mem.release_page(RG_TM2, tm2[i]);
  }

  if (DEBUG) mem.dump(false);
}

void perf_test(int sz, int run_time)
{
  char buf[255];
  Timer timer[4];
  printf("Startar modul test av Page Manager %dMb %ds\n",
         (sz >> 5), run_time);

  const Uint32 data_sz = sz / 3;
  const Uint32 trans_sz = sz / 3;
  Test_mem_manager mem(sz, data_sz, trans_sz);
  mem.dump(false);

  printf("pid: %d press enter to continue\n", NdbHost_GetProcessId());
  fgets(buf, sizeof(buf), stdin);

  Vector<Chunk> chunks;
  Ndbd_mem_manager::AllocZone zone = Ndbd_mem_manager::NDB_ZONE_LE_32;
  time_t stop = time(0) + run_time;
  for (Uint32 i = 0; time(0) < stop; i++)
  {
    mem.dump(false);
    printf("pid: %d press enter to continue\n", NdbHost_GetProcessId());
    fgets(buf, sizeof(buf), stdin);
    time_t stop = time(0) + run_time;
    for (Uint32 i = 0; time(0) < stop; i++)
    {
      // Case
      Uint32 c = (rand() % 100);
      if (c < 50)
      {
        c = 0;
      }
      else if (c < 93)
      {
        c = 1;
      }
      else
      {
        c = 2;
      }

      Uint32 alloc = 1 + rand() % 3200;

      if (chunks.size() == 0 && c == 0)
      {
        c = 1 + rand() % 2;
      }

      if (DEBUG)
      {
        printf("loop=%d ", i);
      }
      switch (c)
      {
      case 0:
      { // Release
        const int ch = rand() % chunks.size();
        Chunk chunk = chunks[ch];
        chunks.erase(ch);
        timer[0].start();
        mem.release_pages(RG_DM, chunk.pageId, chunk.pageCount);
        timer[0].stop();
        if (DEBUG)
        {
          printf(" release %d %d\n", chunk.pageId, chunk.pageCount);
        }
      }
      break;
      case 2:
      { // Seize(n) - fail
        alloc += sz;
      }
      // Fall through
      case 1:
      { // Seize(n) (success)
        Chunk chunk;
        chunk.pageCount = alloc;
        if (DEBUG)
        {
          printf(" alloc %d -> ", alloc);
          fflush(stdout);
        }
        timer[0].start();
        mem.alloc_pages(RG_DM, &chunk.pageId, &chunk.pageCount, 1, zone);
        Uint64 diff = timer[0].calc_diff();

        if (DEBUG)
        {
          printf("%d %d", chunk.pageId, chunk.pageCount);
        }
        assert(chunk.pageCount <= alloc);
        if (chunk.pageCount != 0)
        {
          chunks.push_back(chunk);
          if (chunk.pageCount != alloc)
          {
            timer[2].add(diff);
            if (DEBUG)
            {
              printf(" -  Tried to allocate %d - only allocated %d - free: %d",
                     alloc, chunk.pageCount, 0);
            }
          }
          else
          {
            timer[1].add(diff);
          }
        }
        else
        {
          timer[3].add(diff);
          if (DEBUG)
          {
            printf("  Failed to alloc %d pages with %d pages free",
                   alloc, 0);
          }
        }
        if (DEBUG)
        {
          printf("\n");
        }
      }
      break;
      }
    }
  }
  if (!DEBUG)
  {
    while (chunks.size() > 0)
    {
      Chunk chunk = chunks.back();
      mem.release_pages(RG_DM, chunk.pageId, chunk.pageCount);
      chunks.erase(chunks.size() - 1);
    }
  }

  const char *title[] = {
    "release   ",
    "alloc full",
    "alloc part",
    "alloc fail"
  };
  for (Uint32 i = 0; i < 4; i++)
  {
    timer[i].print(title[i]);
  }
  mem.dump(false);
}

/**
 * iClaustron memory pool
 * ----------------------
 * This module implements a malloc/free API that can use the global memory
 * pools where most of the memory in the NDB data node resides. This makes it
 * possible to dynamically allocate memory in the data nodes without actually
 * having to call malloc and free with all the implications that has on
 * real-time properties, swapping and many other things.
 *
 * All the memory in NDB data nodes are allocated at startup, after that we
 * manage memory internally using global memory pools and a whole range of
 * data structures such as RWPool, RWPool64, ArrayPool, TransientPool and so
 * forth.
 *
 * This memory module is intended to be used to handle memory areas that are
 * dynamic in size. E.g. each table has a mapping from fragment id to fragment
 * pointer. This area should be dynamic in nature, this interface enables this.
 * There are a few other similar cases.
 *
 * Also to handle larger signals and larger requests than 32 kBytes we need
 * to be able to handle larger memory segments without having to split the
 * memory and thus requiring the memory to be copied a lot of extra times
 * when large requests are handled.
 *
 * This is a very generic problem to solve in most applications and DBMSs.
 *
 * We provide two malloc APIs.
 * The first one is for long-lived malloc's, this interface is very
 * simple and use the following methods:
 *
 * void *ic_ndbd_pool_malloc(size_t size, Uint32 pool_id, bool clear_flag)
 *
 * Thus a normal malloc interface with a clear flag instead of two function
 * calls. In addition we need a pool id to ensure that we know which memory
 * area the memory is requested for.
 *
 * The free call requires only the pointer we got from the malloc call.
 * There is no need for a pool id in this call, the memory pointed to
 * will contain references to the pool id and its memory segments.
 *
 * The second interface is designed specifically for handling very short-lived
 * allocations. The idea is that these allocations happens in the receive
 * thread and eventually released from a separate thread and when all
 * parts of a memory segments are released the memory segments is free again.
 *
 * This interface thus have two calls to allocate memory. The first allocates
 * a memory segment using the call:
 *
 * void *ic_ndbd_pool_min_malloc(size_t size,
 *                               size_t *alloc_size,
 *                               Uint32 pool_id)
 *
 * This returns a reference to a memory segment. Thus this memory segment is
 * not intended to be used. The actual memory used is retrieved using another
 * call:
 *
 * void *ic_ndbd_split_malloc(void *memory_segment,
 *                            size_t size)
 *
 * We provide the memory segment and the size we want to allocate. The memory
 * returned will be aligned on a 8 byte boundary and will always be a
 * multiple of 64 bytes. However all details on handling the memory is taken
 * care of by the memory handler.
 *
 * The pool id is mapped to the memory pools in RonDB. Thus at least the
 * following memory areas:
 *
 * TransactionMemory
 * SchemaMemory
 * ReplicationMemory
 * DataMemory
 *
 * Not all memory pools are going to provide dynamic memory allocation.
 * So only those requiring that will have such support.
 *
 * Memory is retrieved from the global memory pool in chunks of 1 MByte of
 * consecutive memory.
 * Each pool will manage a set of such 1 MByte memory segments. For the
 * long lived allocations, the basic data structure for this is the
 * IC_LONG_LIVED_MEMORY_BASE. There is one such data structure for each
 * pool.
 *
 * The 1 MByte memory segments are handled through a data structure called
 * IC_LONG_LIVED_MEMORY_AREA.
 *
 * The IC_LONG_LIVED_MEMORY_BASE is used to find a memory segment that
 * contains enough consecutive memory for the allocation. This uses an
 * array of 8 linked list where each linked list contains memory segments
 * that have a minimum free consecutive area based on its position in the
 * the array.
 *
 * The list[0] has a minimum of 16 bytes
 * list[1] has a minimum of 64 bytes
 * list[2] has a minimum of 256 bytes
 * list[3] has a minimum of 1024 bytes
 * list[4] has a minimum of 4096 bytes
 * list[5] has a minimum of 16384 bytes
 * list[6] has a minimum of 65536 bytes
 * list[7] has a minimum of 262144 bytes
 *
 * Each memory segment also has an array of linked list organised in the
 * same fashion. In this we handle free memory areas. Thus the linked
 * list are linked objects of type FREE_AREA_STRUCT.
 *
 * Each memory segment that is returned to the requester has 8 bytes of
 * managed memory before the actual returned memory and 8 bytes right
 * after the end of the memory.
 *
 * Thus each returned memory area has the following layout.
 *   |----------------------|
 *   | Size        4B       |
 *   | Offset      4B       |
 *   | Memory area          |
 *   | Magic       4B       |
 *   | Status info 4B       |
 *   |----------------------|
 *
 * The size parameter is a pointer to the end of the memory area and is
 * thus required to find the Magic number and the Status information about
 * the memory area. The status info among other things contains the
 * Pool id and is thus used to calculate the magic number. If for some
 * reason we have a memory overrun it is thus quickly possible to verify
 * that we haven't written in memory areas not allowed.
 *
 * The minimum size allocated is a 16B memory area and there is no
 * specific maximum. However if the requested size is larger than
 * what is handled by any Memory segments, then the request will simply
 * be handed off directly to the backend memory allocator.
 *
 * ic_mempool_tester
 * -----------------
 * This includes a unit memory testing of the memory pool.
 *
 * ic_mempool_backend
 * ------------------
 * This module provides the request for large memory areas from the
 * backend allocator, this backend allocator is application specific.
 *
 * ic_mempool_mapper
 * -----------------
 * This module has a function that takes the pool id and transforms it
 * into an internal pool id. There is also a function that calculates
 * a magic number provided the internal pool id.
 *
 * Data structure of a long lived memory area:
 * -------------------------------------------
 *
 * At first we have a statically allocated memory that contains for
 * each pool we support an IC_LONG_LIVED_MEMORY_BASE struct. This
 * struct contains the mutex protecting this memory structure.
 * Second it contains an array of lists of free memory segments.
 *
 * In this area we have the following memory segments:
 * [0]: Contains memory segments at least 16 bytes and not 64 bytes.
 * [1]: From 64 bytes to smaller than 256 bytes.
 * [2]: From 256 bytes to smaller than 1024 bytes.
 * [3]: From 1024 bytes to smaller than 4096 bytes.
 * [4]: From 4096 bytes to smaller than 16384 bytes.
 * [5]: From 16384 bytes to smaller than 65536 bytes.
 * [6]: From 65536 bytes to smaller than 262144 bytes.
 * [7]: From 262144 bytes to smaller than 1 MByte
 *
 * When we have found a memory segment that is large enough we
 * call ic_memseg_malloc to retrieve a memory area from the
 * memory segment.
 *
 * The memory area data structure is the following:
 * First of all we have a pointer to the IC_LONG_LIVED_MEMORY_BASE
 * structure from each memory segment.
 * Second we have the same type of free list as the memory base
 * have, but here we have memory areas belonging to a specific
 * memory area.
 *
 * Next we have the total memory area size.
 * Next we have current free area in this memory area.
 * After that we have some unused space to ensure that we the
 * start of the first memory area is on a cache line boundary.
 * The memory area data structure is 128 bytes.
 */

typedef unsigned int ic_uint32;
typedef unsigned long long ic_uint64;

typedef ic_uint32 (*IC_MAP_POOL_ID) (ic_uint32);
typedef ic_uint32 (*IC_MAKE_MAGIC) (ic_uint32);
typedef void* (*IC_MALLOC_BACKEND) (size_t, ic_uint32);
typedef void (*IC_FREE_BACKEND) (void*, ic_uint32, ic_uint32);
typedef void* (*IC_MEMPOOL_MALLOC) (size_t, ic_uint32, ic_uint32);
typedef void* (*IC_MEMPOOL_MIN_MALLOC) (size_t, ic_uint32*, ic_uint32);
typedef void (*IC_MEMPOOL_FREE) (void*, ic_uint32);

struct ic_mempool_mapper
{
  IC_MAP_POOL_ID ic_map_pool_id;
  IC_MAKE_MAGIC ic_make_magic;
};

struct ic_mempool_backend
{
  IC_MALLOC_BACKEND ic_malloc_backend;
  IC_FREE_BACKEND ic_free_backend;
};

struct free_area_struct;
typedef struct free_area_struct FREE_AREA_STRUCT;
struct free_area_struct
{
  ic_uint32 size_area;
  ic_uint32 area_offset;
  FREE_AREA_STRUCT *m_next_ptr;
  FREE_AREA_STRUCT *m_prev_ptr;
};

struct ic_mempool_mapper glob_long_mempool_mapper;
struct ic_mempool_backend glob_long_mempool_backend;
ic_uint32 glob_long_num_high_pools;

struct ic_long_lived_memory_area;
typedef struct ic_long_lived_memory_area IC_LONG_LIVED_MEMORY_AREA;
struct ic_long_lived_memory_base;
typedef struct ic_long_lived_memory_base IC_LONG_LIVED_MEMORY_BASE;

#define MAX_LONG_LIVED_POOLS 16
#define MAX_SHORT_LIVED_POOLS 64

#define MEMORY_SEGMENT_SIZE (1024 * 1024)
#define MEMORY_SEGMENT_SIZE_IN_WORDS (MEMORY_SEGMENT_SIZE/4)
#define MALLOC_OVERHEAD 16
#define MALLOC_OVERHEAD_IN_WORDS 4

#define MIN_LONG_AREA_SIZE 16
#define MIN_LONG_AREA_SIZE_IN_WORDS 4
#define MIN_SHORT_AREA_SIZE 64
#define MIN_SHORT_AREA_SIZE_IN_WORDS 16

#define MAX_FREE_SHORT_AREAS 64
#define NUM_FREE_AREA_LISTS 9
#define POS_MEMORY_AREA_EMPTY 255
#define INFO_BIT_MASK (0xFF)
#define ALLOC_SIZE_SHIFT 12
#define ALLOC_BIT_SHIFT 8
/* Bit 9-11 not used in status_info */


struct ic_long_lived_memory_area
{
  IC_LONG_LIVED_MEMORY_BASE *m_base_ptr;
  FREE_AREA_STRUCT *m_first_free[NUM_FREE_AREA_LISTS];
  IC_LONG_LIVED_MEMORY_AREA *m_next_ptr;
  IC_LONG_LIVED_MEMORY_AREA *m_prev_ptr;
  ic_uint32 m_mem_area_size;
  ic_uint32 m_current_pos;
  ic_uint32 m_unused_mem_area[8];
  ic_uint32 m_first_mem_area[0];
};

#define MAX_MEMORY_ALLOC_SIZE_IN_WORDS \
  ((MEMORY_SEGMENT_SIZE_IN_WORDS - MALLOC_OVERHEAD_IN_WORDS) - \
   (sizeof(ic_long_lived_memory_area) / 4))
struct ic_long_lived_memory_base
{
  ic_uint32 m_num_active_global_malloc;
  IC_LONG_LIVED_MEMORY_AREA *m_first_free[NUM_FREE_AREA_LISTS];
  NdbMutex m_mutex;
};
IC_LONG_LIVED_MEMORY_BASE glob_long_lived_memory_base[MAX_LONG_LIVED_POOLS];

static void*
ic_mempool_long_lived_pool_malloc(size_t size_in_words,
                                  ic_uint32 pool_id,
                                  bool clear_flag);
static void*
ic_memseg_malloc(size_t size_in_words,
                 IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr,
                 ic_uint32 *check_pos);
static void
remove_from_base_area(IC_LONG_LIVED_MEMORY_BASE *base_ptr,
                      ic_uint32 old_pos,
                      IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr);
static void
remove_from_memory_area(IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr,
                        ic_uint32 pos,
                        FREE_AREA_STRUCT *mem_free_ptr);
static void
insert_into_base_area(IC_LONG_LIVED_MEMORY_BASE *base_ptr,
                      ic_uint32 old_pos,
                      IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr);
static void
insert_into_memory_area(IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr,
                        ic_uint32 pos,
                        FREE_AREA_STRUCT *mem_ins_ptr);
static void
ic_pool_check_memory(void *mem);
static void
check_memory_area_pos(IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr,
                      ic_uint32 *check_pos);

static ic_uint32
get_min_size_given_array_pos(ic_uint32 pos)
{
  assert(pos < NUM_FREE_AREA_LISTS);
  ic_uint32 min_size = MIN_LONG_AREA_SIZE << (2 * pos);
  return min_size;
}

static int
Ndb_fls(Uint32 val)
{
  if (val == 0) return 0;
#if defined(__GNUC__) || defined(__clang__)
  int num_zeros = __builtin_clz(val);
  return 31 - num_zeros;
#else
  /* Binary search for highest bit set */
  Uint32 pos  = 16;
  Uint32 step = pos;
  do
  {
    /* Will always run in 4 loops */
    step >>= 1; //Divide by 2
    if (val >> pos)
     pos += step;
   else
     pos -= step;
  } while (step > 1);
  if (val >> pos)
    pos++;
  return (pos - 1);
#endif
}

static ic_uint32
get_array_pos(ic_uint32 size_in_words)
{
  /**
   * We calculate the size in 16-byte chunks, next we get the most
   * significant bit position of the number we got. So e.g. if the
   * number is 127 (should use array pos 1 since it is 64 bytes or
   * larger and smaller than 128).
   * 127 / 16 = 7
   * fls(7) = 3 (bits counted from 1 in fls, 0 returned from fls(0)
   * We need to add one since we can be in the second part of the
   * range.
   * We divide by 2 since we multiply by 4 for each new array pos
   * and finally we subtract 1 such that we get back to position 0
   * for the lowest position.
   * In the example we thus get ((3 + 1) / 2) - 1 = 1
   */
  assert(size_in_words > 0);
  size_in_words--;
  size_in_words /= MALLOC_OVERHEAD_IN_WORDS;
  if (size_in_words == 0)
    return 0;
  int bit_pos = Ndb_fls(size_in_words);
  bit_pos = ((bit_pos + 4) / 2) - 1;
  return (ic_uint32)bit_pos;
}

static void
init_memory_area(IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr, ic_uint32 pool_id)
{
  /* Set up memory area before inserting it into free lists */
  IC_LONG_LIVED_MEMORY_BASE *base_ptr =
    &glob_long_lived_memory_base[pool_id];
  mem_area_ptr->m_base_ptr = base_ptr;
  for (ic_uint32 i = 0; i < NUM_FREE_AREA_LISTS; i++)
  {
    mem_area_ptr->m_first_free[i] = nullptr;
  }
  ic_uint32 size_long_lived_memory_area_header =
    sizeof(IC_LONG_LIVED_MEMORY_AREA);
  ic_uint32 free_memory_size = MAX_MEMORY_ALLOC_SIZE_IN_WORDS;
  mem_area_ptr->m_mem_area_size = free_memory_size;
  FREE_AREA_STRUCT *mem_free_ptr =
    (FREE_AREA_STRUCT*)&mem_area_ptr->m_first_mem_area[0];
  mem_area_ptr->m_first_mem_area[0] = free_memory_size;
  mem_area_ptr->m_first_mem_area[1] = (size_long_lived_memory_area_header / 4);
  ic_uint32 magic = glob_long_mempool_mapper.ic_make_magic(pool_id);
  mem_area_ptr->m_first_mem_area[free_memory_size + 2] = magic;
  mem_area_ptr->m_first_mem_area[free_memory_size + 3] =
    (free_memory_size << ALLOC_SIZE_SHIFT) | pool_id;
  ic_uint32 pos = NUM_FREE_AREA_LISTS - 1;
  insert_into_memory_area(mem_area_ptr, pos, mem_free_ptr);
}

/* This method is part of external interface */
void*
ic_ndbd_pool_malloc(size_t size, ic_uint32 pool_id, bool clear_flag)
{
  if (size > (MAX_MEMORY_ALLOC_SIZE_IN_WORDS * 4) || size == 0)
    return nullptr;
  /* Ensure that we allocate a multiple of 16 bytes. */
  size_t size_in_bytes =
    ((size + (MIN_LONG_AREA_SIZE - 1)) / MIN_LONG_AREA_SIZE) *
     MIN_LONG_AREA_SIZE;
  size_t size_in_words = size_in_bytes / 4;
  return
    ic_mempool_long_lived_pool_malloc(size_in_words, pool_id, clear_flag);
}

static void*
ic_mempool_long_lived_pool_malloc(size_t size_in_words,
                                  ic_uint32 pool_id,
                                  bool clear_flag)
{
  bool first = true;
  /* Map the pool id to the internal pool id */
  ic_uint32  map_pool_id = glob_long_mempool_mapper.ic_map_pool_id(pool_id);
  /* Retrieve the memory base for this pool */
  IC_LONG_LIVED_MEMORY_BASE *base_ptr =
    &glob_long_lived_memory_base[map_pool_id];
  /* Lock this memory base during the allocation process */
  ic_uint32 start_pos = get_array_pos(size_in_words);
  ic_uint32 start_min_size_in_words = get_min_size_given_array_pos(start_pos);
  NdbMutex_Lock(&base_ptr->m_mutex);
  do
  {
    ic_uint32 min_size_in_words = start_min_size_in_words;
    for (Uint32 i = start_pos;
         i < NUM_FREE_AREA_LISTS;
         i++, min_size_in_words *= 4)
    {
      if (min_size_in_words < size_in_words)
      {
        /**
         * Continue until we reached an index that will have a chance
         * to contain the area sought for.
         * Should never happen since we already ensured that we start
         * from the first possible.
         */
        abort();
      }
      if (base_ptr->m_first_free[i] == nullptr)
      {
        /* No memory segment have so small areas, move to larger areas */
        continue;
      }
      /**
       * We have found a memory segment that will provide the memory
       * requested. Now retrieve this memory from the memory area
       * using the ic_memseg_malloc call.
       *
       * This call will return the new position in the array of linked
       * lists of free memory segments. If required we will move the
       * memory segment to a new free list of memory areas.
       */
      IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr = base_ptr->m_first_free[i];
      ic_uint32 new_pos = i;
      ic_uint32 old_pos = new_pos;
      void *ret_mem = ic_memseg_malloc(size_in_words,
                                       mem_area_ptr,
                                       &new_pos);
      if (ret_mem != nullptr)
      {
        if (new_pos != old_pos)
        {
          /**
           * New position, start by removing it from old position
           * If new position is higher than old position it is
           * an indication that the memory segment has no more
           * free areas. Thus it won't be inserted into any
           * free area. The whole memory area is thus allocated
           * and the memory area data structure will notice when
           * such a memory area has memory freed which makes it
           * necessary to reinsert it into the base area free lists.
           */
          remove_from_base_area(base_ptr, old_pos, mem_area_ptr);
          if (new_pos < old_pos)
          {
            insert_into_base_area(base_ptr, new_pos, mem_area_ptr);
          }
          mem_area_ptr->m_current_pos = new_pos;
        }
      }
      else
      {
        /* Should never happen that we find no memory in this case. */
        abort();
        return nullptr;
      }
      /**
       * We have successfully allocated memory, setting up memory before
       * it is returned requires no mutex. Neither does clearing the
       * memory before returning it.
       */
      NdbMutex_Unlock(&base_ptr->m_mutex);
      if (clear_flag)
      {
        memset(ret_mem, 0, 4 * size_in_words);
      }
      return ret_mem;
    }
    /**
     * No memory was found in existing memory segments.
     * We need to allocate more memory from the global memory pool.
     * If first is false, we have already allocated memory from
     * the pool and still failed to allocate memory, this should
     * never happen.
     *
     * What could happen though is that we allocate memory from a different
     * memory segment than the one we allocated from the global memory
     * pool. This can happen since we released the lock and thus someone
     * could have freed memory while we allocated memory from the global
     * memory pool.
     */
    if (!first)
    {
      abort();
    }
    first = false;
    /**
     * Release lock before allocating from global memory pool to avoid
     * holding two hot mutexes concurrently.
     *
     * One problem that comes from releasing the lock before allocating
     * from global memory pool is that we can have multiple threads
     * allocating memory from the global memory pool concurrently.
     * A solution to this could be to use some kind of booking with
     * a conditional variable waking up the waiter on memory.
     * If such a scheme is used, then also new arrivals must wait before
     * they start checking for free memory.
     *
     * Another solution that we will use here is to instead only release
     * the mutex if someone isn't already allocating a memory area, or
     * that a certain number of allocations are already progressing.
     * This will give this requester priority before the other requesters,
     * but will ensure that we don't have hundreds of threads doing
     * large allocations at the same time and thus wasting a lot of memory
     * space.
     */
    bool locked = true;
    if (base_ptr->m_num_active_global_malloc == 0)
    {
      base_ptr->m_num_active_global_malloc++;
      NdbMutex_Unlock(&base_ptr->m_mutex);
      locked = false;
    }
    IC_LONG_LIVED_MEMORY_AREA *new_mem_area_ptr = (IC_LONG_LIVED_MEMORY_AREA*)
      glob_long_mempool_backend.ic_malloc_backend(MEMORY_SEGMENT_SIZE,
                                                  map_pool_id);
    if (new_mem_area_ptr != nullptr)
    {
      init_memory_area(new_mem_area_ptr, map_pool_id);
      /* Acquire lock again before inserting it into free list */
      if (!locked)
      {
        NdbMutex_Lock(&base_ptr->m_mutex);
        assert(base_ptr->m_num_active_global_malloc > 0);
        base_ptr->m_num_active_global_malloc--;
      }
      //printf("Allocating memory segment, mem_area_ptr: %p\n", new_mem_area_ptr);
      new_mem_area_ptr->m_current_pos = NUM_FREE_AREA_LISTS - 1;
      insert_into_base_area(base_ptr,
                            new_mem_area_ptr->m_current_pos,
                            new_mem_area_ptr);
    }
    else
    {
      if (locked)
      {
        NdbMutex_Unlock(&base_ptr->m_mutex);
      }
      return (void*) nullptr;
    }
  } while (true);
  assert(false);
  return nullptr; //Should never arrive here
}

static void
insert_into_base_area(IC_LONG_LIVED_MEMORY_BASE *mem_base_ptr,
                      ic_uint32 pos,
                      IC_LONG_LIVED_MEMORY_AREA *mem_ins_ptr)
{
  IC_LONG_LIVED_MEMORY_AREA *first_free_ptr = mem_base_ptr->m_first_free[pos];
  mem_ins_ptr->m_prev_ptr = nullptr;
  mem_ins_ptr->m_next_ptr = first_free_ptr;
  if (first_free_ptr != nullptr)
  {
    first_free_ptr->m_prev_ptr = mem_ins_ptr;
  }
  mem_base_ptr->m_first_free[pos] = mem_ins_ptr;
}

static void
insert_into_memory_area(IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr,
                        ic_uint32 pos,
                        FREE_AREA_STRUCT *mem_ins_ptr)
{
  FREE_AREA_STRUCT *first_free_ptr =
    (FREE_AREA_STRUCT*)mem_area_ptr->m_first_free[pos];
  mem_ins_ptr->m_prev_ptr = nullptr;
  mem_ins_ptr->m_next_ptr = first_free_ptr;
  if (first_free_ptr != nullptr)
  {
    first_free_ptr->m_prev_ptr = mem_ins_ptr;
  }
  mem_area_ptr->m_first_free[pos] = mem_ins_ptr;
}

static void
remove_from_base_area(IC_LONG_LIVED_MEMORY_BASE *mem_base_ptr,
                      ic_uint32 pos,
                      IC_LONG_LIVED_MEMORY_AREA *mem_free_ptr)
{
  IC_LONG_LIVED_MEMORY_AREA *first_free_ptr = mem_base_ptr->m_first_free[pos];
  if (first_free_ptr == mem_free_ptr)
  {
    if (first_free_ptr->m_next_ptr != nullptr)
    {
      first_free_ptr->m_next_ptr->m_prev_ptr = nullptr;
    }
    mem_base_ptr->m_first_free[pos] = mem_free_ptr->m_next_ptr;
  }
  else
  {
    if (mem_free_ptr->m_next_ptr != nullptr)
    {
      mem_free_ptr->m_next_ptr->m_prev_ptr = mem_free_ptr->m_prev_ptr;
    }
    mem_free_ptr->m_prev_ptr->m_next_ptr = mem_free_ptr->m_next_ptr;
  }
  mem_free_ptr->m_next_ptr = nullptr;
  mem_free_ptr->m_prev_ptr = nullptr;
}

static void
remove_from_memory_area(IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr,
                        ic_uint32 pos,
                        FREE_AREA_STRUCT *mem_free_ptr)
{
  FREE_AREA_STRUCT *first_free_ptr =
    (FREE_AREA_STRUCT*)mem_area_ptr->m_first_free[pos];
  if (first_free_ptr == mem_free_ptr)
  {
    if (first_free_ptr->m_next_ptr != nullptr)
    {
      first_free_ptr->m_next_ptr->m_prev_ptr = nullptr;
    }
    mem_area_ptr->m_first_free[pos] = mem_free_ptr->m_next_ptr;
  }
  else
  {
    if (mem_free_ptr->m_next_ptr != nullptr)
    {
      mem_free_ptr->m_next_ptr->m_prev_ptr = mem_free_ptr->m_prev_ptr;
    }
    mem_free_ptr->m_prev_ptr->m_next_ptr = mem_free_ptr->m_next_ptr;
  }
  mem_free_ptr->m_next_ptr = nullptr;
  mem_free_ptr->m_prev_ptr = nullptr;
}

static void*
ic_split_malloc_spec(void *mem,
                     size_t size_in_words,
                     ic_uint32 *remaining_area,
                     ic_uint32 area_size_in_words)
{
  /**
   * We come here with a memory region, the total size of this area is
   * size + min_size. We want to split off min_size from this (min_size
   * includes the 16 byte overhead plus the actual allocated space. The
   * actual allocated space is always a multiple of area_size). area_size
   * is 16 bytes for the long lived memory allocations and 64 bytes for
   * the short lived parts.
   *
   * The part that we return for allocation is the last part, this ensures
   * that the we can retain the position in the linked list of free parts
   * for the long lived allocations if we still have enough free memory
   * to retain our position in the linked list we currently belong to.

   Below is an image of the area as it looks when we enter this method.
   S = Size of the Free Memory Area
   O = Offset from the beginning of the Memory Area to the start of
       the Free Memory Area
   M = Magic number
   I = Status information, including a pool id
   ---------------------------------------------------------------------
   | S | O |  Free memory area                                 | M | I |
   ---------------------------------------------------------------------

   This routine creates an area at the end of Free Memory Area which it
   sets up as an Alloc area with the same header information. The old
   Free Memory Area decreases in size.
   ---------------------------------------------------------------------
   | S | O |  Free memory area   | M | I | S | O | Alloc area  | M | I |
   ---------------------------------------------------------------------
   */
  ic_uint32 alloc_size_in_words =
    (size_in_words + MALLOC_OVERHEAD_IN_WORDS + (area_size_in_words - 1)) /
     area_size_in_words;
  alloc_size_in_words *= area_size_in_words;

  ic_uint32 *uint32_mem = (ic_uint32*)mem;
  ic_uint32 mem_size_in_words = uint32_mem[-2];
  ic_uint32 mem_offset_in_words = uint32_mem[-1];
  ic_uint32 status_info = uint32_mem[mem_size_in_words + 1];
  ic_uint32 magic = uint32_mem[mem_size_in_words];
  ic_uint32 pool_id = status_info & INFO_BIT_MASK;
  ic_uint32 expected_magic = glob_long_mempool_mapper.ic_make_magic(pool_id);
  if (unlikely(magic != expected_magic))
  {
    abort();
  }
  if (unlikely(mem_size_in_words < alloc_size_in_words))
  {
    *remaining_area = mem_size_in_words;
    return nullptr;
  }
  ic_uint32 new_mem_size_in_words = mem_size_in_words - alloc_size_in_words;
  ic_uint32 *ret_mem =
    &uint32_mem[new_mem_size_in_words+MALLOC_OVERHEAD_IN_WORDS];
  ic_uint32 alloc_bit = ((status_info >> ALLOC_BIT_SHIFT) & 1);
  if (unlikely(alloc_bit))
  {
    abort();
  }
  if (unlikely(new_mem_size_in_words <= MALLOC_OVERHEAD_IN_WORDS))
  {
    /**
     * We need to allocate the full memory area, there is not enough space
     * to split the area. This ensures that we don't overwrite the
     * Free Memory Area which is part of a FREE_AREA_STRUCT when we arrive
     * here from the long lived memory allocation.
     * We must however set the Alloc bit in the status info before we return.
     */
    *remaining_area = 0;
    status_info |= (1 << ALLOC_BIT_SHIFT);
    uint32_mem[mem_size_in_words + 1] = status_info;
    return mem;
  }
  /* Split the Free Memory Area */
  ic_uint32 free_status_info = (status_info & INFO_BIT_MASK);
  free_status_info |= (new_mem_size_in_words << ALLOC_SIZE_SHIFT);
  /* Alloc bit isn't set, so no need to do anything with it. */
  uint32_mem[-2] = new_mem_size_in_words;
  uint32_mem[-1] = mem_offset_in_words;
  uint32_mem[new_mem_size_in_words] = magic;
  uint32_mem[new_mem_size_in_words + 1] = free_status_info;

  ic_uint32 new_offset_in_words =
    mem_offset_in_words +
    new_mem_size_in_words +
    MALLOC_OVERHEAD_IN_WORDS;
  ic_uint32 real_alloc_size_in_words =
    alloc_size_in_words - MALLOC_OVERHEAD_IN_WORDS;
  ic_uint32 alloc_status_info = (status_info & INFO_BIT_MASK);
  alloc_status_info |= (real_alloc_size_in_words << ALLOC_SIZE_SHIFT);
  alloc_status_info |= (1 << ALLOC_BIT_SHIFT);
  ret_mem[-2] = real_alloc_size_in_words;
  ret_mem[-1] = new_offset_in_words;
  ret_mem[real_alloc_size_in_words] = magic;
  ret_mem[real_alloc_size_in_words + 1] = alloc_status_info;
  *remaining_area = new_mem_size_in_words;
  return (void*)ret_mem;
}

static void*
ic_memseg_malloc(size_t size_in_words,
                 IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr,
                 ic_uint32 *check_pos)
{
  ic_uint32 min_size_in_words = get_min_size_given_array_pos(*check_pos);
  ic_uint32 i = *check_pos;
  if (unlikely(min_size_in_words < size_in_words ||
               mem_area_ptr->m_first_free[i] == nullptr))
  {
    /* Size of free parts too small, should never happen */
    /* No free parts in this linked list, should never happen */
    abort();
  }
  /**
   * Found a linked list with free elements that are large enough
   * Every part in the linked list should be large enough, so
   * simply grab the first one and return this.
   *
   * In addition we need to split the memory into the part that
   * we allocate and the part that goes back into the free list.
   */
  ic_uint32 *mem_ptr = (ic_uint32*)mem_area_ptr->m_first_free[i];
  ic_uint32 pos = i;
  ic_uint32 remaining_area = 0;
  void *ret_mem = ic_split_malloc_spec(&mem_ptr[2],
                                       size_in_words,
                                       &remaining_area,
                                       MIN_LONG_AREA_SIZE_IN_WORDS);
  if (unlikely(ret_mem == nullptr))
  {
    abort();
  }
  if (likely(remaining_area > 0))
  {
    ic_uint32 new_pos = get_array_pos(remaining_area);
    if (new_pos != pos)
    {
      /**
       * There is still memory available in the memory we retrieved
       * memory from, check if it needs to move to another position
       * in the free area array.
       */
      remove_from_memory_area(mem_area_ptr,
                              pos,
                              (FREE_AREA_STRUCT*)mem_ptr);
      insert_into_memory_area(mem_area_ptr,
                              new_pos,
                              (FREE_AREA_STRUCT*)mem_ptr);
    }
    else
    {
      /* Position haven't changed, return immediately */
      return ret_mem;
    }
  }
  else
  {
    /**
     * There is no free space left in the memory area, remove the
     * memory from the free array pool and since it is now fully
     * used, there is no place to insert it into.
     *
     * We also flag the memory area as not any longer containing any
     * free area. This ensures that we know what to do when memory
     * is free'd from this memory area.
     */
    remove_from_memory_area(mem_area_ptr,
                            pos,
                            (FREE_AREA_STRUCT*)mem_ptr);
  }
  check_memory_area_pos(mem_area_ptr, check_pos);
  return ret_mem;
}

static void
check_memory_area_pos(IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr,
                      ic_uint32 *check_pos)
{
  for (ic_uint32 i = (*check_pos) + 1; i > 0; i--)
  {
    if (mem_area_ptr->m_first_free[i - 1] != nullptr)
    {
      if (i != (*check_pos) + 1)
      {
        *check_pos = i - 1;
        return;
      }
    }
  }
  *check_pos = POS_MEMORY_AREA_EMPTY;
}

static void
ic_mempool_long_lived_pool_free(ic_uint32 *mem,
                                IC_LONG_LIVED_MEMORY_BASE *base_ptr,
                                IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr,
                                ic_uint32 mem_size_in_words,
                                ic_uint32 offset_in_words,
                                ic_uint32 pool_id);
static ic_uint32*
ic_merge_free(ic_uint32 *left_mem,
              ic_uint32 *right_mem,
              ic_uint32 left_size_in_words,
              ic_uint32 right_size_in_words,
              ic_uint32 pool_id);

/* This method is part of external interface */
void
ic_ndbd_pool_free(void *mem)
{
  ic_uint32 *uint32_mem = (ic_uint32*)mem;
  ic_uint32 mem_size_in_words = uint32_mem[-2];
  ic_pool_check_memory(mem);
  ic_uint32 status_info = uint32_mem[mem_size_in_words + 1];
  ic_uint32 magic = uint32_mem[mem_size_in_words];
  ic_uint32 pool_id = (status_info & INFO_BIT_MASK);
  ic_uint32 mem_offset_in_words = uint32_mem[-1];
  IC_LONG_LIVED_MEMORY_BASE *base_ptr =
    &glob_long_lived_memory_base[pool_id];
  IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr = (IC_LONG_LIVED_MEMORY_AREA*)
    &uint32_mem[-int(mem_offset_in_words + 2)];
  ic_uint32 check_magic = glob_long_mempool_mapper.ic_make_magic(pool_id);
#ifdef VM_TRACE
  IC_LONG_LIVED_MEMORY_BASE *base_ptr_check = mem_area_ptr->m_base_ptr;
  if (unlikely(base_ptr_check != base_ptr))
  {
    abort();
  }
#endif
  if (unlikely(magic != check_magic))
  {
    abort();
  }
  ic_mempool_long_lived_pool_free(uint32_mem,
                                  base_ptr,
                                  mem_area_ptr,
                                  mem_size_in_words,
                                  mem_offset_in_words,
                                  pool_id);
}

static void
ic_mempool_long_lived_pool_free(ic_uint32 *mem,
                                IC_LONG_LIVED_MEMORY_BASE *base_ptr,
                                IC_LONG_LIVED_MEMORY_AREA *mem_area_ptr,
                                ic_uint32 mem_size_in_words,
                                ic_uint32 offset_in_words,
                                ic_uint32 pool_id)
{
  ic_uint32 size_long_lived_memory_area_header_in_words =
    sizeof(IC_LONG_LIVED_MEMORY_AREA) / 4;
  bool check_left = true;
  if (unlikely(offset_in_words <=
               size_long_lived_memory_area_header_in_words))
  {
    if (offset_in_words < size_long_lived_memory_area_header_in_words)
    {
      abort();
    }
    check_left = false;
  }
  ic_uint32 right_limit_in_words = MEMORY_SEGMENT_SIZE / 4;
  ic_uint32 right_index =
    offset_in_words + mem_size_in_words + MALLOC_OVERHEAD_IN_WORDS;
  bool check_right = true;
  if (right_index >= right_limit_in_words)
  {
    if (unlikely(right_index > right_limit_in_words))
    {
      abort();
    }
    check_right = false;
  }
  NdbMutex_Lock(&base_ptr->m_mutex);
  if (check_left)
  {
    /* Read status info from left neighbour */
    ic_uint32 left_status_info = mem[-3];
    ic_uint32 is_left_allocated = (left_status_info >> ALLOC_BIT_SHIFT) & 1;
    if (is_left_allocated == 0)
    {
      ic_uint32 left_len_in_words = left_status_info >> ALLOC_SIZE_SHIFT;
      ic_uint32 left_pos = get_array_pos(left_len_in_words);
      ic_uint32 *left_mem =
        &mem[-int(left_len_in_words + MALLOC_OVERHEAD_IN_WORDS)];
      remove_from_memory_area(mem_area_ptr,
        left_pos,
        (FREE_AREA_STRUCT*)&left_mem[-2]);
      mem =
        ic_merge_free(left_mem,
                      mem,
                      left_len_in_words,
                      mem_size_in_words,
                      pool_id);
      mem_size_in_words = mem[-2];
    }
  }
  if (check_right)
  {
    ic_uint32 *right_mem = &mem[mem_size_in_words + MALLOC_OVERHEAD_IN_WORDS];
    ic_uint32 right_len_in_words = right_mem[-2];
    ic_uint32 right_status_info = right_mem[right_len_in_words + 1];
    ic_uint32 is_right_allocated = (right_status_info >> ALLOC_BIT_SHIFT) & 1;
    if (is_right_allocated == 0)
    {
      ic_uint32 right_pos = get_array_pos(right_len_in_words);
      remove_from_memory_area(mem_area_ptr,
        right_pos,
        (FREE_AREA_STRUCT*)&right_mem[-2]);
      mem = ic_merge_free(mem,
                          right_mem,
                          mem_size_in_words,
                          right_len_in_words,
                          pool_id);
    }
  }
  FREE_AREA_STRUCT *mem_free_ptr = (FREE_AREA_STRUCT*)&mem[-2];
  ic_uint32 len_in_words = mem[-2];
  ic_uint32 pos = get_array_pos(len_in_words);
  insert_into_memory_area(mem_area_ptr, pos, mem_free_ptr);
  if (unlikely(len_in_words >= MAX_MEMORY_ALLOC_SIZE_IN_WORDS))
  {
    if (len_in_words > MAX_MEMORY_ALLOC_SIZE_IN_WORDS)
    {
      abort();
    }
    /**
     * All the memory of the memory area released, it is possible to
     * release the entire memory area if so is desired.
     */
    if (mem_area_ptr->m_current_pos != POS_MEMORY_AREA_EMPTY)
    {
      remove_from_base_area(base_ptr,
                            mem_area_ptr->m_current_pos,
                            mem_area_ptr);
    }
    //printf("Release memory segment, mem_area_ptr: %p\n", mem_area_ptr);
    NdbMutex_Unlock(&base_ptr->m_mutex);
    glob_long_mempool_backend.ic_free_backend(mem_area_ptr,
                                              pool_id,
                                              MEMORY_SEGMENT_SIZE);
    return;
  }
  if (pos > mem_area_ptr->m_current_pos)
  {
    remove_from_base_area(base_ptr, mem_area_ptr->m_current_pos, mem_area_ptr);
    insert_into_base_area(base_ptr, pos, mem_area_ptr);
    mem_area_ptr->m_current_pos = pos;
  }
  else if (mem_area_ptr->m_current_pos == POS_MEMORY_AREA_EMPTY)
  {
    insert_into_base_area(base_ptr, pos, mem_area_ptr);
    mem_area_ptr->m_current_pos = pos;
  }
  //printf("free memory len_in_words: %u, mem_area_ptr: %p\n", len_in_words, mem_area_ptr);
  ic_uint32 status_info = ((len_in_words) << ALLOC_SIZE_SHIFT);
  status_info |= pool_id;
  mem[len_in_words+1] = status_info;
  NdbMutex_Unlock(&base_ptr->m_mutex);
}

static ic_uint32*
ic_merge_free(ic_uint32 *left_mem,
              ic_uint32 *right_mem,
              ic_uint32 left_size_in_words,
              ic_uint32 right_size_in_words,
              ic_uint32 pool_id)
{
  ic_uint32 new_size_in_words = left_size_in_words +
                                right_size_in_words +
                                MALLOC_OVERHEAD_IN_WORDS;
  left_mem[-2] = new_size_in_words;
  ic_uint32 status_info = ((new_size_in_words) << ALLOC_SIZE_SHIFT);
  status_info |= pool_id;
  left_mem[new_size_in_words + 1] = status_info;
  return left_mem;
}

static void
ic_pool_check_memory(void *mem)
{
  ic_uint32 *uint32_mem = (ic_uint32*)mem;
  ic_uint32 mem_size_in_words = uint32_mem[-2];
  ic_uint32 stored_magic = uint32_mem[mem_size_in_words];
  ic_uint32 status_info = uint32_mem[mem_size_in_words + 1];
  ic_uint32 pool_id = status_info & INFO_BIT_MASK;
  ic_uint32 alloc_bit = (status_info >> ALLOC_BIT_SHIFT) & 1;
  ic_uint32 size_in_words = (status_info >> ALLOC_SIZE_SHIFT);
  ic_uint32 calc_magic = glob_long_mempool_mapper.ic_make_magic(pool_id);
  if (stored_magic != calc_magic ||
      (!alloc_bit) ||
      size_in_words != mem_size_in_words)
  {
    abort();
  }
}

static ic_uint32
default_make_magic(ic_uint32 pool_id)
{
  ic_uint32 magic = 0xfdb97530 + pool_id;
  return magic;
}

static ic_uint32
default_map_pool_id(ic_uint32 in)
{
  return in;
}

static void*
default_malloc_backend(size_t size, ic_uint32 pool_id)
{
  (void)pool_id;
  return malloc(size);
}

static void
default_free_backend(void *mem,
                     ic_uint32 pool_id,
                     ic_uint32 size)
{
  (void)pool_id;
  free(mem);
}

static
void ic_init_long_lived_memory_pool(IC_MAP_POOL_ID map_pool_id,
                                    IC_MAKE_MAGIC make_magic,
                                    IC_MALLOC_BACKEND malloc_backend,
                                    IC_FREE_BACKEND free_backend)
{
  if (make_magic == nullptr)
    glob_long_mempool_mapper.ic_make_magic = default_make_magic;
  else
    glob_long_mempool_mapper.ic_make_magic = make_magic;
  if (map_pool_id == nullptr)
    glob_long_mempool_mapper.ic_map_pool_id = default_map_pool_id;
  else
    glob_long_mempool_mapper.ic_make_magic = map_pool_id;

  if (malloc_backend == nullptr)
    glob_long_mempool_backend.ic_malloc_backend = default_malloc_backend;
  else
    glob_long_mempool_backend.ic_malloc_backend = malloc_backend;
  if (free_backend == nullptr)
    glob_long_mempool_backend.ic_free_backend = default_free_backend;
  else
    glob_long_mempool_backend.ic_free_backend = free_backend;

  for (ic_uint32 pool_id = 0; pool_id < MAX_LONG_LIVED_POOLS; pool_id++)
  {
    for (ic_uint32 i = 0; i < NUM_FREE_AREA_LISTS; i++)
    {
      glob_long_lived_memory_base[pool_id].m_first_free[i] = nullptr;
    }
    glob_long_lived_memory_base[pool_id].m_num_active_global_malloc = 0;
    NdbMutex_Init(&glob_long_lived_memory_base[pool_id].m_mutex);
  }
}

/**
 * iClaustron short lived memory handler
 *
 * This malloc/free implementation is based on the use case where the memory
 * is used to handle short-lived messages that pass through in a matter of
 * a few microseconds or at most a few milliseconds.
 *
 * Thus the use case here is a flow of messages arriving from somewhere,
 * most likely from the network. These memory areas are used to store those
 * incoming messages. Later they are passed to an execution thread and after
 * execution the memory is freed.
 *
 * This particular model makes it possible to implement malloc/free using a
 * very simple approach where a large memory area is allocated at first,
 * each time this area is used for a message the part used by the message
 * one calls ic_split_malloc to divide the large memory into a two parts,
 * one with the prescribed size and the rest using the remainder of the
 * memory.
 *
 * For efficiency purpose each of those memory segments will be at least
 * 64 bytes or a multiple of 64 bytes to ensure that different messages
 * do not share CPU cache lines if possible.
 *
 * When freeing the memory one simply decreases an atomic counter and
 * when this reaches zero one knows that all memory areas have been
 * free'd and it is time to return the full memory area to the free list.
 */


ic_uint32 glob_short_num_high_pools;
ic_uint32 glob_short_available_words;

struct ic_short_lived_memory_area
{
  ic_uint32 m_start[0];
  bool m_is_memory_available;
  struct ic_short_lived_memory_area *m_next_area;
  ic_uint32 m_mem_area_total;
  std::atomic<unsigned int> m_mem_area_used;
};
typedef struct ic_short_lived_memory_area IC_SHORT_LIVED_MEMORY_AREA;

struct ic_short_lived_memory_base
{
  IC_SHORT_LIVED_MEMORY_AREA *m_first_free;
  ic_uint32 m_num_free_areas;
  NdbMutex m_mutex;
};
typedef struct ic_short_lived_memory_base IC_SHORT_LIVED_MEMORY_BASE;
IC_SHORT_LIVED_MEMORY_BASE glob_short_lived_memory_base[MAX_SHORT_LIVED_POOLS];

struct ic_mempool_mapper glob_short_mempool_mapper;
struct ic_mempool_backend glob_short_mempool_backend;

static IC_SHORT_LIVED_MEMORY_AREA*
ic_pool_short_lived_malloc(Uint32 pool_id)
{
  ic_uint32 *alloc_uint32;
  ic_uint32 available_area = MEMORY_SEGMENT_SIZE_IN_WORDS -
                             MIN_SHORT_AREA_SIZE_IN_WORDS;
  ic_uint32 mem_size_in_words = available_area - MALLOC_OVERHEAD_IN_WORDS;
  IC_SHORT_LIVED_MEMORY_BASE *base_ptr =
    &glob_short_lived_memory_base[pool_id];
  IC_SHORT_LIVED_MEMORY_AREA *mem_area_ptr = nullptr;
  if (unlikely(base_ptr->m_first_free == nullptr))
  {
    ic_uint32 map_pool_id = glob_short_mempool_mapper.ic_map_pool_id(pool_id);
    void *alloc_mem =
      glob_short_mempool_backend.ic_malloc_backend(
        MEMORY_SEGMENT_SIZE, map_pool_id);
    if (unlikely(alloc_mem == nullptr))
    {
      return nullptr;
    }
    mem_area_ptr = (IC_SHORT_LIVED_MEMORY_AREA*)alloc_mem;
    alloc_uint32 = (ic_uint32*)alloc_mem;
  }
  else
  {
    mem_area_ptr = (IC_SHORT_LIVED_MEMORY_AREA*)base_ptr->m_first_free;
    alloc_uint32 = (ic_uint32*)base_ptr->m_first_free;
    base_ptr->m_first_free = mem_area_ptr->m_next_area;
    if (unlikely(base_ptr->m_num_free_areas == 0))
    {
      abort();
    }
    base_ptr->m_num_free_areas--;
  }
  mem_area_ptr->m_mem_area_used = mem_size_in_words;
  mem_area_ptr->m_mem_area_total = mem_size_in_words;
  mem_area_ptr->m_next_area = nullptr;
  mem_area_ptr->m_is_memory_available = true;
  ic_uint32 *mem_area = &alloc_uint32[MIN_SHORT_AREA_SIZE_IN_WORDS];
  mem_area[0] = mem_size_in_words;
  mem_area[1] = MIN_SHORT_AREA_SIZE_IN_WORDS;
  ic_uint32 map_pool_id = glob_short_mempool_mapper.ic_map_pool_id(pool_id);
  ic_uint32 magic = glob_short_mempool_mapper.ic_make_magic(map_pool_id);
  ic_uint32 status_info = map_pool_id & INFO_BIT_MASK;
  status_info |= (mem_size_in_words << ALLOC_SIZE_SHIFT);
  mem_area[available_area - 2] = magic;
  mem_area[available_area - 1] = status_info;
  return mem_area_ptr;
}

static void
release_memory_area(IC_SHORT_LIVED_MEMORY_BASE *base_ptr,
                    IC_SHORT_LIVED_MEMORY_AREA *mem_area_ptr,
                    ic_uint32 map_pool_id)
{
  unsigned int new_remaining =
    mem_area_ptr->m_mem_area_used.load();
  if (mem_area_ptr->m_is_memory_available || new_remaining)
  {
    /**
     * We are still allocating from it, too early to return
     */
    NdbMutex_Unlock(&base_ptr->m_mutex);
    return;
  }
  if (base_ptr->m_num_free_areas < MAX_FREE_SHORT_AREAS)
  {
    mem_area_ptr->m_next_area = base_ptr->m_first_free;
    base_ptr->m_first_free = mem_area_ptr;
    base_ptr->m_num_free_areas++;
    NdbMutex_Unlock(&base_ptr->m_mutex);
    return;
  }
  NdbMutex_Unlock(&base_ptr->m_mutex);
  glob_short_mempool_backend.ic_free_backend((void*)mem_area_ptr,
                                             map_pool_id,
                                             MEMORY_SEGMENT_SIZE);
}

/* This method is part of external interface */
void
ic_ndbd_pool_split_free(void *mem)
{
  ic_pool_check_memory(mem);
  ic_uint32 *uint32_mem = (ic_uint32*)mem;
  ic_uint32 mem_size_in_words = uint32_mem[-2];
  ic_uint32 mem_offset_in_words = uint32_mem[-1];
  IC_SHORT_LIVED_MEMORY_AREA *mem_area_ptr =
    (IC_SHORT_LIVED_MEMORY_AREA*)&uint32_mem[-int(mem_offset_in_words + 2)];
  ic_uint32 size_in_words = mem_size_in_words + MALLOC_OVERHEAD_IN_WORDS;
  unsigned int remaining =
    mem_area_ptr->m_mem_area_used.fetch_sub(size_in_words);
  if (unlikely(remaining > mem_area_ptr->m_mem_area_total))
  {
    abort();
  }
  if (likely(remaining > 0))
  {
    return;
  }
  ic_uint32 status_info = uint32_mem[mem_size_in_words + 1];
  ic_uint32 pool_id = status_info & INFO_BIT_MASK;
  ic_uint32 map_pool_id = glob_short_mempool_mapper.ic_map_pool_id(pool_id);
  if (unlikely(map_pool_id >= MAX_SHORT_LIVED_POOLS))
  {
    abort();
  }
  IC_SHORT_LIVED_MEMORY_BASE *base_ptr =
    &glob_short_lived_memory_base[map_pool_id];
  NdbMutex_Lock(&base_ptr->m_mutex);
  release_memory_area(base_ptr, mem_area_ptr, map_pool_id);
}

static void*
ic_ndbd_pool_min_malloc(ic_uint32 map_pool_id)
{
  IC_SHORT_LIVED_MEMORY_BASE *base_ptr =
    &glob_short_lived_memory_base[map_pool_id];
  NdbMutex_Lock(&base_ptr->m_mutex);
  void *ret_mem = (void*)ic_pool_short_lived_malloc(map_pool_id);
  NdbMutex_Unlock(&base_ptr->m_mutex);
  return ret_mem;
}

/* This method is part of external interface */
void*
ic_ndbd_split_malloc(unsigned int _pool_id, void **mem, size_t size)
{
  ic_uint32 pool_id = (ic_uint32)_pool_id;
  ic_uint32 map_pool_id = glob_short_mempool_mapper.ic_map_pool_id(pool_id);
  void *mem_area = *mem;
  bool first = true;

  if (unlikely(size > MEMORY_SEGMENT_SIZE || size == 0))
    return nullptr;

  do
  {
    if (unlikely(mem_area == nullptr))
    {
      mem_area = ic_ndbd_pool_min_malloc(map_pool_id);
      if (unlikely(mem_area == nullptr))
        return nullptr;
      *mem = mem_area;
    }
    IC_SHORT_LIVED_MEMORY_AREA *mem_area_ptr =
      (IC_SHORT_LIVED_MEMORY_AREA*)mem_area;
    ic_uint32 *mem_start_ptr = (ic_uint32*)mem_area;
    mem_start_ptr = &mem_start_ptr[MIN_SHORT_AREA_SIZE_IN_WORDS + 2];
    if (likely(mem_area_ptr->m_is_memory_available))
    {
      ic_uint32 remaining_area = 0;
      void *ret_mem = ic_split_malloc_spec(mem_start_ptr,
                                           size,
                                           &remaining_area,
                                           MIN_SHORT_AREA_SIZE);
      if (likely(ret_mem != nullptr))
      {
        ic_pool_check_memory((ic_uint32*)ret_mem);
        if (unlikely(remaining_area == 0))
        {
          IC_SHORT_LIVED_MEMORY_BASE *base_ptr =
            &glob_short_lived_memory_base[map_pool_id];
          NdbMutex_Lock(&base_ptr->m_mutex);
          mem_area_ptr->m_is_memory_available = false;
          NdbMutex_Unlock(&base_ptr->m_mutex);
          *mem = nullptr;
        }
        return ret_mem;
      }
      else
      {
        mem_area_ptr->m_mem_area_used.fetch_sub(remaining_area);
        IC_SHORT_LIVED_MEMORY_BASE *base_ptr =
          &glob_short_lived_memory_base[map_pool_id];
        NdbMutex_Lock(&base_ptr->m_mutex);
        mem_area_ptr->m_is_memory_available = false;
        release_memory_area(base_ptr, mem_area_ptr, map_pool_id);
        *mem = nullptr;
      }
    }
    if (unlikely(!first))
      abort();
    mem_area = nullptr;
    first = false;
  } while (true);
  return nullptr; 
}

static
void ic_init_short_lived_memory_pool(IC_MAP_POOL_ID map_pool_id,
                                     IC_MAKE_MAGIC make_magic,
                                     IC_MALLOC_BACKEND malloc_backend,
                                     IC_FREE_BACKEND free_backend)
{
  if (make_magic == nullptr)
    glob_short_mempool_mapper.ic_make_magic = default_make_magic;
  else
    glob_short_mempool_mapper.ic_make_magic = make_magic;
  if (map_pool_id == nullptr)
    glob_short_mempool_mapper.ic_map_pool_id = default_map_pool_id;
  else
    glob_short_mempool_mapper.ic_make_magic = map_pool_id;
  if (malloc_backend == nullptr)
    glob_short_mempool_backend.ic_malloc_backend = default_malloc_backend;
  else
    glob_short_mempool_backend.ic_malloc_backend = malloc_backend;
  if (free_backend == nullptr)
    glob_short_mempool_backend.ic_free_backend = default_free_backend;
  else
    glob_short_mempool_backend.ic_free_backend = free_backend;

  for (ic_uint32 pool_id = 0; pool_id < MAX_SHORT_LIVED_POOLS; pool_id++)
  {
    glob_short_lived_memory_base[pool_id].m_first_free = nullptr;
    glob_short_lived_memory_base[pool_id].m_num_free_areas = 0;
    NdbMutex_Init(&glob_short_lived_memory_base[pool_id].m_mutex);
  }
}

/* This method is part of external interface */
void
init_ic_ndbd_memory_pool()
{
  ic_init_short_lived_memory_pool(nullptr, nullptr, nullptr, nullptr);
  ic_init_long_lived_memory_pool(nullptr, nullptr, nullptr, nullptr);
}

/**
 * Test program
 */
#define NUM_ROUNDS 64000000
#define NUM_THREADS 8

static ic_uint32
get_random(ic_uint32 max)
{
  long rand_number = random();
  ic_uint64 rand_max = ic_uint64(1 << 31);
  ic_uint64 rand_calc = rand_number * ic_uint64(max);
  rand_calc /= rand_max;
  return ic_uint32(rand_calc);
}

static size_t
get_malloc_size()
{
  return size_t(32 + get_random(136));
}

static void
swap_ptrs(void **ptrs, ic_uint32 source, ic_uint32 dest)
{
  void *source_ptr = ptrs[source];
  void *dest_ptr = ptrs[dest];
  ptrs[dest] = source_ptr;
  ptrs[source] = dest_ptr;
}

static void
simple_single_thread_short_test()
{
  ic_uint32 num_rounds = 128;
  void *ptrs[128];
  ic_uint32 malloc_size = 32768;
  printf("Simple Single-threaded short malloc test\n");
  void *mem_area = nullptr;
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ptrs[i] = ic_ndbd_split_malloc(0, &mem_area, malloc_size);
    if (ptrs[i] == nullptr)
    {
      abort();
    }
  }
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    for (ic_uint32 j = i + 1; j < num_rounds; j++)
    {
      if (ptrs[i] == ptrs[j])
      {
        abort();
      }
    }
  }
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_ndbd_pool_split_free(ptrs[i]);
  }
}

static void
simple_single_thread_short_test_random()
{
  ic_uint32 num_rounds = 128;
  void *ptrs[128];
  ic_uint32 malloc_size = 32768;
  printf("Simple Single-threaded short malloc random test\n");
  void *mem_area = nullptr;
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ptrs[i] = ic_ndbd_split_malloc(0, &mem_area, malloc_size);
    if (ptrs[i] == nullptr)
    {
      abort();
    }
  }
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    for (ic_uint32 j = i + 1; j < num_rounds; j++)
    {
      if (ptrs[i] == ptrs[j])
      {
        abort();
      }
    }
  }
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_ndbd_pool_split_free(ptrs[i]);
  }
}

static void
many_single_thread_short_test()
{
  ic_uint32 num_rounds = NUM_ROUNDS;
  void **ptrs;
  ptrs = (void**)malloc(sizeof(void*) * num_rounds);
  printf("Many Single-threaded short malloc test\n");
  void *mem_area = nullptr;
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    size_t malloc_size = get_malloc_size();
    ptrs[i] = ic_ndbd_split_malloc(0, &mem_area, malloc_size);
    if (ptrs[i] == nullptr)
    {
      abort();
    }
  }
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_ndbd_pool_split_free(ptrs[i]);
  }
  free(ptrs);
}

static void
many_single_thread_short_test_random()
{
  ic_uint32 num_rounds = NUM_ROUNDS;
  void **ptrs;
  ptrs = (void**)malloc(sizeof(void*) * num_rounds);
  printf("Many Single-threaded short malloc random test\n");
  void *mem_area = nullptr;

  NDB_TICKS start = NdbTick_getCurrentTicks();
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    size_t malloc_size = get_malloc_size();
    ptrs[i] = ic_ndbd_split_malloc(0, &mem_area, malloc_size);
    if (ptrs[i] == nullptr)
    {
      abort();
    }
  }
  NDB_TICKS end_malloc = NdbTick_getCurrentTicks();
  Uint64 micros = NdbTick_Elapsed(start, end_malloc).microSec();
  printf("Allocation took %llu microseconds\n", micros);

  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_uint32 source = get_random(num_rounds);
    ic_uint32 dest = get_random(num_rounds);
    swap_ptrs(ptrs, source, dest);
  }
  start = NdbTick_getCurrentTicks();
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_ndbd_pool_split_free(ptrs[i]);
  }
  NDB_TICKS end_free = NdbTick_getCurrentTicks();
  micros = NdbTick_Elapsed(start, end_free).microSec();
  printf("Free took %llu microseconds\n", micros);
  free(ptrs);
}

static void **glob_ptrs;

extern "C"
void*
release_split_malloc(void* arg)
{
  ic_uint64 thread_id = (ic_uint64)arg;
  ic_uint64 num_frees = NUM_ROUNDS / NUM_THREADS;
  ic_uint64 first_free = thread_id * num_frees;
  for (ic_uint64 i = first_free; i < (first_free + num_frees); i++)
  {
    ic_ndbd_pool_split_free(glob_ptrs[i]);
  }
  return nullptr;
}

extern "C"
void*
alloc_split_malloc(void* arg)
{
  ic_uint64 thread_id = (ic_uint64)arg;
  ic_uint64 num_frees = NUM_ROUNDS / NUM_THREADS;
  ic_uint64 first_free = thread_id * num_frees;
  void *mem_area = nullptr;
  for (ic_uint64 i = first_free; i < (first_free + num_frees); i++)
  {
    size_t malloc_size = get_malloc_size();
    glob_ptrs[i] = ic_ndbd_split_malloc(0, &mem_area, malloc_size);
    if (glob_ptrs[i] == nullptr)
    {
      abort();
    }
  }
  return nullptr;
}

static void
many_multi_thread_short_test_random()
{
  ic_uint32 num_rounds = NUM_ROUNDS;
  glob_ptrs = (void**)malloc(sizeof(void*) * num_rounds);
  printf("Many multi-threaded short malloc random test\n");

  NDB_TICKS start = NdbTick_getCurrentTicks();
  NdbThread *alloc_thr_ptrs[NUM_THREADS];
  for (ic_uint64 i = 0; i < NUM_THREADS; i++)
  {
    struct NdbThread *thr_ptr =
      NdbThread_Create(alloc_split_malloc,
                       (void**)i,
                       64 * 1024,
                       "alloc_split_malloc",
                       NDB_THREAD_PRIO_MEAN);
    alloc_thr_ptrs[i] = thr_ptr;
  }

  for (ic_uint32 i = 0; i < NUM_THREADS; i++)
  {
    void *dummy;
    NdbThread_WaitFor(alloc_thr_ptrs[i], &dummy);
  }
  NDB_TICKS end_malloc = NdbTick_getCurrentTicks();
  Uint64 micros = NdbTick_Elapsed(start, end_malloc).microSec();
  printf("Allocation took %llu microseconds\n", micros);
  for (ic_uint32 i = 0; i < NUM_THREADS; i++)
  {
    NdbThread_Destroy(&alloc_thr_ptrs[i]);
  }

  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_uint32 source = get_random(num_rounds);
    ic_uint32 dest = get_random(num_rounds);
    swap_ptrs(glob_ptrs, source, dest);
  }

  start = NdbTick_getCurrentTicks();
  NdbThread *release_thr_ptrs[NUM_THREADS];
  for (ic_uint64 i = 0; i < NUM_THREADS; i++)
  {
    struct NdbThread *thr_ptr =
      NdbThread_Create(release_split_malloc,
                       (void**)i,
                       64 * 1024,
                       "release_split_malloc",
                       NDB_THREAD_PRIO_MEAN);
    release_thr_ptrs[i] = thr_ptr;
  }
  for (ic_uint32 i = 0; i < NUM_THREADS; i++)
  {
    void *dummy;
    NdbThread_WaitFor(release_thr_ptrs[i], &dummy);
    NdbThread_Destroy(&release_thr_ptrs[i]);
  }
  NDB_TICKS end_free = NdbTick_getCurrentTicks();
  micros = NdbTick_Elapsed(start, end_free).microSec();
  printf("Allocation took %llu microseconds\n", micros);
  free(glob_ptrs);
}

static void
simple_single_thread_long_test()
{
  ic_uint32 num_rounds = 16;
  void *ptrs[16];
  printf("Simple Single-threaded long malloc test\n");
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ptrs[i] = ic_ndbd_pool_malloc(1 << (i+1), 0, 1);
  }
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    for (ic_uint32 j = i + 1; j < num_rounds; j++)
    {
      if (ptrs[i] == ptrs[j])
      {
        abort();
      }
    }
  }
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_ndbd_pool_free(ptrs[i]);
  }
}

static void
many_malloc_single_thread_long_test()
{
  ic_uint32 num_rounds = NUM_ROUNDS;
  void **ptrs;
  printf("Many malloc's Single-threaded long malloc test\n");

  ptrs = (void**)malloc(sizeof(void*) * num_rounds);
  
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    size_t malloc_size = get_malloc_size();
    ptrs[i] = ic_ndbd_pool_malloc(malloc_size, 0, 1);
  }
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_ndbd_pool_free(ptrs[i]);
  }
  free(ptrs);
}

static void
many_malloc_single_thread_long_test_random()
{
  ic_uint32 num_rounds = NUM_ROUNDS;
  void **ptrs;
  printf("Many malloc's Single-threaded long malloc random test\n");

  ptrs = (void**)malloc(sizeof(void*) * num_rounds);
  
  NDB_TICKS start = NdbTick_getCurrentTicks();
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    size_t malloc_size = get_malloc_size();
    ptrs[i] = ic_ndbd_pool_malloc(malloc_size, 0, 1);
  }
  NDB_TICKS end_malloc = NdbTick_getCurrentTicks();
  Uint64 micros = NdbTick_Elapsed(start, end_malloc).microSec();
  printf("Allocation took %llu microseconds\n", micros);

  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_uint32 source = get_random(num_rounds);
    ic_uint32 dest = get_random(num_rounds);
    swap_ptrs(ptrs, source, dest);
  }
  start = NdbTick_getCurrentTicks();
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_ndbd_pool_free(ptrs[i]);
  }
  NDB_TICKS end_free = NdbTick_getCurrentTicks();
  micros = NdbTick_Elapsed(start, end_free).microSec();
  printf("Free took %llu microseconds\n", micros);
  free(ptrs);
}

extern "C"
void*
release_pool_malloc(void* arg)
{
  ic_uint32 thread_id = (ic_uint64)arg;
  ic_uint32 num_frees = NUM_ROUNDS / NUM_THREADS;
  ic_uint32 first_free = thread_id * num_frees;
  for (ic_uint32 i = first_free; i < (first_free + num_frees); i++)
  {
    ic_ndbd_pool_free(glob_ptrs[i]);
  }
  return nullptr;
}

extern "C"
void*
alloc_pool_malloc(void* arg)
{
  ic_uint32 thread_id = (ic_uint64)arg;
  ic_uint32 num_frees = NUM_ROUNDS / NUM_THREADS;
  ic_uint32 first_free = thread_id * num_frees;
  for (ic_uint32 i = first_free; i < (first_free + num_frees); i++)
  {
    size_t malloc_size = get_malloc_size();
    glob_ptrs[i] = ic_ndbd_pool_malloc(malloc_size, 0, 1);
    if (glob_ptrs[i] == nullptr)
    {
      abort();
    }
  }
  return nullptr;
}

static void
many_malloc_multi_thread_long_test_random()
{
  ic_uint32 num_rounds = NUM_ROUNDS;
  glob_ptrs = (void**)malloc(sizeof(void*) * num_rounds);
  printf("Many multi-threaded long malloc random test\n");

  NDB_TICKS start = NdbTick_getCurrentTicks();
  NdbThread *alloc_thr_ptrs[NUM_THREADS];
  for (ic_uint64 i = 0; i < NUM_THREADS; i++)
  {
    struct NdbThread *thr_ptr =
      NdbThread_Create(alloc_pool_malloc,
                       (void**)i,
                       64 * 1024,
                       "alloc_pool_malloc",
                       NDB_THREAD_PRIO_MEAN);
    alloc_thr_ptrs[i] = thr_ptr;
  }
  for (ic_uint32 i = 0; i < NUM_THREADS; i++)
  {
    void *dummy;
    NdbThread_WaitFor(alloc_thr_ptrs[i], &dummy);
    NdbThread_Destroy(&alloc_thr_ptrs[i]);
  }
  NDB_TICKS end_malloc = NdbTick_getCurrentTicks();
  Uint64 micros = NdbTick_Elapsed(start, end_malloc).microSec();
  printf("Allocation took %llu microseconds\n", micros);
  for (ic_uint32 i = 0; i < num_rounds; i++)
  {
    ic_uint32 source = get_random(num_rounds);
    ic_uint32 dest = get_random(num_rounds);
    swap_ptrs(glob_ptrs, source, dest);
  }

  start = NdbTick_getCurrentTicks();
  NdbThread *release_thr_ptrs[NUM_THREADS];
  for (ic_uint64 i = 0; i < NUM_THREADS; i++)
  {
    struct NdbThread *thr_ptr =
      NdbThread_Create(release_pool_malloc,
                       (void**)i,
                       64 * 1024,
                       "release_pool_malloc",
                       NDB_THREAD_PRIO_MEAN);
    release_thr_ptrs[i] = thr_ptr;
  }
  for (ic_uint32 i = 0; i < NUM_THREADS; i++)
  {
    void *dummy;
    NdbThread_WaitFor(release_thr_ptrs[i], &dummy);
    NdbThread_Destroy(&release_thr_ptrs[i]);
  }
  NDB_TICKS end_free = NdbTick_getCurrentTicks();
  micros = NdbTick_Elapsed(start, end_free).microSec();
  printf("Free took %llu microseconds\n", micros);
  free(glob_ptrs);
}

static void
test_get_array_pos()
{
  printf("Test iClaustron get_array_pos\n");
  size_t size_array[27];
  ic_uint32 pos_array[27];
  ic_uint32 i = 0;
  size_array[0] = 1;
  pos_array[0] = 0;
  size_array[++i] = 3;
  pos_array[i] = 0;
  size_array[++i] = 4;
  pos_array[i] = 0;
  size_array[++i] = 5;
  pos_array[i] = 1;
  size_array[++i] = 15;
  pos_array[i] = 1;
  size_array[++i] = 16;
  pos_array[i] = 1;
  size_array[++i] = 17;
  pos_array[i] = 2;
  size_array[++i] = 63;
  pos_array[i] = 2;
  size_array[++i] = 64;
  pos_array[i] = 2;
  size_array[++i] = 65;
  pos_array[i] = 3;
  size_array[++i] = 255;
  pos_array[i] = 3;
  size_array[++i] = 256;
  pos_array[i] = 3;
  size_array[++i] = 257;
  pos_array[i] = 4;
  size_array[++i] = 1023;
  pos_array[i] = 4;
  size_array[++i] = 1024;
  pos_array[i] = 4;
  size_array[++i] = 1025;
  pos_array[i] = 5;
  size_array[++i] = 4095;
  pos_array[i] = 5;
  size_array[++i] = 4096;
  pos_array[i] = 5;
  size_array[++i] = 4097;
  pos_array[i] = 6;
  size_array[++i] = 16383;
  pos_array[i] = 6;
  size_array[++i] = 16384;
  pos_array[i] = 6;
  size_array[++i] = 16385;
  pos_array[i] = 7;
  size_array[++i] = 65535;
  pos_array[i] = 7;
  size_array[++i] = 65536;
  pos_array[i] = 7;
  size_array[++i] = 65537;
  pos_array[i] = 8;
  size_array[++i] = 262143;
  pos_array[i] = 8;
  size_array[++i] = 262144;
  pos_array[i] = 8;
  assert(i == 26);
  ic_uint32 pos;
  ic_uint32 error = 0;
  for (ic_uint32 i = 0; i < 27; i++)
  {
    pos = get_array_pos(size_array[i]);
    if (pos != pos_array[i])
    {
      error = i;
      break;
    }
  }
  if (error)
  {
    abort();
  }
}

static void
ic_ndbd_malloc_test()
{
  /* Testing iClaustron memory pool for ndbmtd */
  printf("Test iClaustron memory pool for ndbmtd\n");
  init_ic_ndbd_memory_pool();
  test_get_array_pos();
  simple_single_thread_short_test();
  simple_single_thread_short_test_random();
  many_single_thread_short_test();
  many_single_thread_short_test_random();
  many_multi_thread_short_test_random();
  many_single_thread_short_test();
  many_single_thread_short_test_random();
  many_multi_thread_short_test_random();
  simple_single_thread_long_test();
  many_malloc_single_thread_long_test();
  many_malloc_single_thread_long_test_random();
  many_malloc_multi_thread_long_test_random();
  printf("Successful Test of iClaustron memory pool for ndbmtd\n");
}

template class Vector<Chunk>;

#endif
