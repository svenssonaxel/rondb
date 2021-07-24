/*
   Copyright (c) 2005, 2021, Oracle and/or its affiliates.
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

#ifndef NDBD_MALLOC_H
#define NDBD_MALLOC_H

#include <stddef.h>

#include "ndb_types.h"

#define JAM_FILE_ID 234

/**
 * These functions can be used directly by blocks and other entities
 * to manage memory in an efficient manner.
 *
 * init_ic_ndbd_memory_pool
 *    No parameters
 *
 *   This call is used to initialise the memory to handle the various
 *   pools that use this interface. It is called at startup before
 *   any calls to allocate memory through its interfaces have been done.
 *
 *  General malloc/free functions for NDB kernel
 *  --------------------------------------------
 *
 * ic_ndbd_pool_malloc
 *   Parammeters:
 *     size:       [IN] The size of the memory to allocate
 *     pool_id:    [IN] The memory pool to use in the allocation
 *   Return value:
 *     == 0     Unsuccessful allocation of memory
 *     != 0     The address of the memory allocated
 *
 *   This call allocates a memory area of exactly the requested size.
 *   It carries a pool id that identifies the global memory pool to
 *   allocate the memory from.
 *
 * ic_ndbd_pool_free
 *   Parammeters:
 *     mem:        [IN] The start of the memory area to free
 *   Return value:
 *     None         The call will crash the data node if unsuccessful
 *
 *  This call returns the memory area to the pool where it was acquired
 *  from.
 *
 *  Specialized malloc/free functions for very short-lived memory segments
 *  ----------------------------------------------------------------------
 *
 * ic_ndbd_pool_min_malloc
 *   Parammeters:
 *     size:       [IN] The size of the memory to allocate
 *     alloc_size: [OUT] The actual size of the memory allocated
 *                       The size is at least the requested size
 *                       but could also be biggger.
 *     pool_id:    [IN] The memory pool to use in the allocation
 *   Return value:
 *     == 0     Unsuccessful allocation of memory
 *     != 0     The address of the memory allocated
 *
 *   This call is used in situations where we want to allocate a large
 *   chunk of memory, but has a minimum requirement on the size of this
 *   memory area, but in reality wants a bigger memory area such that
 *   many allocations can be performed as one allocation.
 *
 * ic_ndbd_split_malloc
 *   Parammeters:
 *     mem:        [IN] The start of the memory area to split
 *     size:       [IN] The size of the memory area that should be kept at
 *                      the start address.
 *     min_size    [IN] The minimum size of the new memory area split off
 *                      from the input memory.
 *     remain_size [OUT] The size of the new memory area, 0 if no new area
 *                       is created.
 *   Return value:
 *     == 0     Unsuccessful split of memory
 *     != 0     The address of the new memory split off
 *
 *   This call is used to split a memory area into two memory areas that
 *   can later be returned in a ndbd_pool_free call independent of each
 *   other. Thus it doesn't involve any allocation of memory and also
 *   no release of memory. It simply keeps the memory pointed by the
 *   mem parameter and creates a new memory area of the remaining
 *   memory. If the remaining memory is not large enough to create a memory
 *   area of minsize, then 0 is returned and no changes to the input
 *   memory area is done. If the memory is large enough to create a new
 *   memory area of at least minsize, then the original area gets the
 *   size specified in the size parameter and the remainder is put into
 *   the new memory area which address is returned. The size of this
 *   area is returned in the remaining_size parameter.
 *
 *   This call together with ic_ndbd_pool_min_malloc can be used to allocate
 *   large chunks of memory and use those chunks a piece at a time without
 *   requiring to allocate or free any more memory.
 *   The chunks have to be free'd separately.
 *
 *   This call does not use any mutexes or other protection against
 *   concurrent access, thus the caller has to certify that the memory
 *   area isn't used by another thread concurrently. The memory area
 *   from the start of the memory to the size will not be neither read
 *   nor written to.
 *
 * ic_ndbd_pool_split_free
 *   Parammeters:
 *     mem:        [IN] The start of the split memory area to free
 *   Return value:
 *     None         The call will crash the data node if unsuccessful
 *
 *  This call returns the memory area to the pool where it was acquired
 *  from. But it waits for the entire area to be released before this
 *  can occur. Thus it is imperative that users of this interface do not
 *  hold onto memory for any longer periods.
 *
 * The above provides the details guaranteed by the API. The implementation
 * of those calls can vary dependent on which pool_id that is used.
 * There are very different use scenarios for different pools. E.g. the
 * SchemaMemory contains objects that are kept for a very long time and
 * that are only allocated and freed when a new metadata object is
 * created, altered or droppped. At the other extreme we have memory areas
 * used by signals to carry the signal payload. These are extremely short
 * lived and uses very frequent allocations and frees of fairly small
 * memory areas.
 */

/**
 * common memory allocation function for ndbd kernel
 *
 * There are two different sets of functions.
 * ic_ndbd_pool_malloc/free for generic malloc towards a pool
 *
 * ic_ndbd_pool_min_malloc, ic_ndbd_split_malloc, ic_ndbd_split_free
 * to handle short lived memory allocations that are provided in a
 * consecutive memory space.
 */

/**
 * common memory allocation function for ndbd kernel
 */
void *ndbd_malloc(size_t size);
bool ndbd_malloc_need_watchdog(size_t size);
void *ndbd_malloc_watched(size_t size, volatile Uint32* watch_dog);
void ndbd_free(void *p, size_t size);

/**
 * The ic_ndbd_pool_malloc/free is a traditional malloc/free API.
 * The malloc call carries a pool id to ensure that we know from
 * which memory pool to request the memory. There is also a clear
 * flag to ensure that we don't need a specific calloc call.
 */
void *ic_ndbd_pool_malloc(size_t size, Uint32 pool_id, bool clear_flag);
void ic_ndbd_pool_free(void *mem);

/**
 * The interface to
 * - ic_ndbd_split_malloc
 * - ic_ndbd_split_free
 * is a bit different.
 *
 * The user of this interface will start by declaring a void* variable
 * which is assigned nullptr. This variable is sent into ic_ndbd_split_malloc
 * by reference, thus ic_ndbd_split_malloc will update with a reference to
 * a memory area from where to allocate the next memory area. If the
 * method ic_ndbd_split_malloc finds that this memory area is exhausted it
 * will allocate a new area and update the referenced memory area pointer.
 *
 * The method ic_ndbd_split_malloc needs the pool_id variable to allocate new
 * memory areas when needed.
 *
 * The memory area returned by ic_ndbd_split_malloc can be returned by any
 * thread using ic_ndbd_split_free. However the interface requires those
 * memory allocations to be short memory allocations since the entire
 * memory area is kept allocated until all parts have been returned.
 *
 * There is no call to return a memory area, it is expected that a thread
 * that uses this interface runs until the program is stopped and it is
 * up to higher levels in the software stack to ensure that any memory
 * allocated is returned to the OS.
 *
 * When all memory areas returned from ic_ndbd_split_malloc using one
 * void* pointer has called ic_ndbd_split_free, then it will be
 * discovered that the entire area is free'd and the area will be
 * available for a new ic_ndbd_split_malloc calls.
 *
 * It is vital that memory allocated in this interface is never held
 * onto for any extended periods of time. The normal use is to allocate
 * memory in one thread receiving messages, store the message in this
 * memory and send the message to its destination. The memory is free'd
 * as soon as the message have been handled.
 */
void *ic_ndbd_split_malloc(unsigned int pool_id, void **mem, size_t size);
void ic_ndbd_pool_split_free(void *mem);

void init_ic_ndbd_memory_pool();
#undef JAM_FILE_ID

#endif 
