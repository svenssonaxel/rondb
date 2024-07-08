/*
   Copyright (c) 2005, 2023, Oracle and/or its affiliates.
   Copyright (c) 2021, 2023, Hopsworks and/or its affiliates.

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

#define DBTUP_C
#define DBTUP_VAR_ALLOC_CPP
#include "../dblqh/Dblqh.hpp"
#include "Dbtup.hpp"

#define JAM_FILE_ID 405

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_ELEM_COUNT 1
#endif

#ifdef DEBUG_ELEM_COUNT
#define DEB_ELEM_COUNT(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_ELEM_COUNT(arglist) do { } while (0)
#endif

void Dbtup::init_list_sizes(void) {
  c_min_list_size[0] = 200;
  c_max_list_size[0] = 499;

  c_min_list_size[1] = 500;
  c_max_list_size[1] = 999;

  c_min_list_size[2] = 1000;
  c_max_list_size[2] = 4079;

  c_min_list_size[3] = 4080;
  c_max_list_size[3] = 7783;

  /* The last free list must guarantee space for biggest possible column
   * size.
   * Assume varsize may take up the whole row (a slight exaggeration).
   */
  static_assert(MAX_EXPANDED_TUPLE_SIZE_IN_WORDS <= 7784);
  c_min_list_size[4] = 7784;
  c_max_list_size[4] = 8159;

  static_assert(MAX_FREE_LIST == 5);
  c_min_list_size[5] = 0;
  c_max_list_size[5] = 199;
}

Uint32*
Dbtup::alloc_var_part(Uint32 * err,
                      Fragrecord* fragPtr,
		      Tablerec* tabPtr,
		      Uint32 alloc_size,
		      Local_key* key,
                      Uint32 from_line,
                      bool insert_flag)
{
  PagePtr pagePtr;
  (void)from_line;
  pagePtr.i= get_alloc_page(fragPtr, (alloc_size + 1));
  if (pagePtr.i == RNIL) { 
    jam();
    if ((pagePtr.i= get_empty_var_page(fragPtr, tabPtr)) == RNIL)
    {
      jam();
      * err = ZMEM_NOMEM_ERROR;
      return 0;
    }
    c_page_pool.getPtr(pagePtr);
    ((Var_page*)pagePtr.p)->init();
    fragPtr->m_varWordsFree += ((Var_page*)pagePtr.p)->free_space;
    pagePtr.p->list_index = MAX_FREE_LIST - 1;
    Local_Page_list list(c_page_pool,
			   fragPtr->free_var_page_array[MAX_FREE_LIST-1]);
    list.addFirst(pagePtr);
  } else {
    c_page_pool.getPtr(pagePtr);
    jam();
  }  
  /*
    First we remove the current free space on this page from 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
fragment total.
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
allocated
*/
Uint32* Dbtup::alloc_var_rec(Uint32 * err,
// RONDB-624 todo: Glue these lines together ^v
=======
allocated
*/
Uint32 *Dbtup::alloc_var_rec(Uint32 *err, Fragrecord *fragPtr, Tablerec *tabPtr,
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36

    Then we calculate a new free space value for the page. Finally we call
    update_free_page_list() which adds this new value to the 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
fragment
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
Fragrecord*
// RONDB-624 todo: Glue these lines together ^v
=======
Uint32
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
total.
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
fragPtr,
// RONDB-624 todo: Glue these lines together ^v
=======
alloc_size, Local_key *key,
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36

  
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
*/
||||||| Common ancestor
   Tablerec* tabPtr,
			 
// RONDB-624 todo: Glue these lines together ^v
=======
     
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
  
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
ndbassert(fragPtr->m_varWordsFree
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
  Uint32 alloc_size,
			   
// RONDB-624 todo: Glue these lines together ^v
=======
      
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 >= 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
((Var_page*)pagePtr.p)->free_space);
||||||| Common ancestor
Local_key* key,
			 
// RONDB-624 todo: Glue these lines together ^v
=======
        
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
  fragPtr->m_varWordsFree -= ((Var_page*)pagePtr.p)->free_space;

  bool upgrade_exclusive = false;
  if (alloc_size >= ((Var_page*)pagePtr.p)->largest_frag_size() &&
      c_lqh->get_fragment_lock_status() !=
        Dblqh::FRAGMENT_LOCKED_IN_EXCLUSIVE_MODE &&
      insert_flag)
  {
    jam();
    /**
     * Page will be reorganised to fit in the new row, this requires
     * exclusive access to the fragment.
     */
    upgrade_exclusive = true;
    c_lqh->upgrade_to_exclusive_frag_access();
  }
  Uint32 idx= ((Var_
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
page*)pagePtr.p)
 
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
part_ref*
// RONDB-624 todo: Glue these lines together ^v
=======
part_ref
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
dst
// RONDB-624 todo: Glue these lines together ^v
=======
*dst
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
  ->alloc_record(alloc_size, (Var_page*)ctemp_page, Var_page::CHAIN);

  if (upgrade_exclusive) {
    c_lqh->downgrade_from_exclusive_frag_access();
  }

  fragPtr->m_varElemCount++;
  DEB_ELEM_COUNT(("(%u) 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
Inc m_varElemCount: now %llu tab(%u,%u),"
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
!= 0))
// RONDB-624 todo: Glue these lines together ^v
=======
!=
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36

    
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
{
=======
           0)) {
>>>>>>> MySQL 8.0.36
              " line: %u, called from %u",
                  instance(),
                  fragPtr->m_varElemCount,
            
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
 
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
* err,
=======
*err, Fragrecord *fragPtr,
>>>>>>> MySQL 8.0.36
     fragPtr->fragTableId,
                  
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
fragPtr->fragmentId,
||||||| Common ancestor
fragPtr,
		
// RONDB-624 todo: Glue these lines together ^v
=======
 
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
      
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
Tablerec*
// RONDB-624 todo: Glue these lines together ^v
=======
Tablerec
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
     
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
tabPtr,
		     
// RONDB-624 todo: Glue these lines together ^v
=======
*tabPtr,
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
      __LINE__,
      
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
Local_key*
// RONDB-624 todo: Glue these lines together ^v
=======
Local_key
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
key)
{
=======
*key) {
>>>>>>> MySQL 8.0.36
           from_line));
  key->m_page_no = pagePtr.i;
  key->m_page_idx = idx;
  
  update_free_page_list(fragPtr, pagePtr);  
  return ((Var_page*)pagePtr.p)->get_ptr(idx);
}

/*
  free_var_part is used to free the variable length storage associated
  with the passed local key.
  It is not assumed that there is a corresponding fixed-length part.
  // TODO : Any need for tabPtr?
*/
void Dbtup::free_var_part(Fragrecord* fragPtr,
                          Tablerec* tabPtr,
                          Local_key* key)
{
  Ptr<Page> pagePtr;
  if (key->m_page_no != RNIL)
  {
    ndbrequire(c_page_pool.getPtr(pagePtr, key->m_page_no));
    ndbassert(fragPtr->m_varWordsFree >= ((Var_page *)pagePtr.p)->free_space);
    fragPtr->m_varWordsFree -= ((Var_page *)pagePtr.p)->free_space;
    ((Var_page *)pagePtr.p)->free_record(key->m_page_idx, Var_page::CHAIN);
    ndbassert(fragPtr->m_varElemCount > 0);
    fragPtr->m_varElemCount--;
    DEB_ELEM_COUNT(("(%u) Dec m_varElemCount: now %llu tab(%u,%u),"
                    " line: %u",
                
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
Dbtup::free_var_part(Fragrecord*
// RONDB-624 todo: Glue these lines together ^v
=======
Dbtup::free_var_part(Fragrecord
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
   instance()
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
fragPtr
// RONDB-624 todo: Glue these lines together ^v
=======
*fragPtr
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
,
                    fragPtr->m_varElemCount,
      
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
Tablerec*
// RONDB-624 todo: Glue these lines together ^v
=======
Tablerec
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
tabPtr,
=======
*tabPtr,
>>>>>>> MySQL 8.0.36
             fragPtr->fragTableId,
             
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
Local_key*
// RONDB-624 todo: Glue these lines together ^v
=======
Local_key
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
key)
{
=======
*key) {
>>>>>>> MySQL 8.0.36
      fragPtr->fragmentId,
                    __LINE__));

    ndbassert(pagePtr.p->free_space <= Var_page::DATA_WORDS);
    if (pagePtr.p->free_space == Var_page::DATA_WORDS - 1) {
      jam();
      Uint32 idx = pagePtr.p->list_index;
      Local_Page_list list(c_page_pool, fragPtr->free_var_page_array[idx]);
      list.remove(pagePtr);
      returnCommonArea(pagePtr.i, 1);
      fragPtr->noOfVarPages--;
    } else {
      jam();
      // Adds the new free space value for the page to the fragment total.
      update_free_page_list(fragPtr, pagePtr);
    }
    ndbassert(fragPtr->verifyVarSpace());
  }
  return;
}

/*
  Deallocator for variable sized segments
  Part of the external interface for variable sized segments

  SYNOPSIS
    fragPtr         A pointer to the fragment description
    tabPtr          A pointer to the table description
    signal           The signal object to be used if a signal needs to
                     be sent
    page_ptr         A reference to the page of the variable sized
                     segment
    free_page_index  Page index on page of variable sized segment
                     which is freed
  RETURN VALUES
    Returns true if deallocation was successful otherwise false
*/
void Dbtup::free_var_rec(Fragrecord *fragPtr, Tablerec *tabPtr, Local_key *key,
                         Ptr<Page> pagePtr) {
  /**
   * TODO free fix + var part
   */
  Uint32 *ptr = ((Fix_page *)pagePtr.p)->get_ptr(key->m_page_idx, 0);
  Tuple_header *tuple = (Tuple_header *)ptr;

  Local_key ref;
  Var_part_ref *varref = tuple->get_var_part_ref_ptr(tabPtr);
  varref->copyout(&ref);

  free_fix_rec(fragPtr, tabPtr, key, (Fix_page *)pagePtr.p);

  if (ref.m_page_no != RNIL) {
    jam();
    ndbrequire(c_page_pool.getPtr(pagePtr, ref.m_page_no));
    free_var_part(fragPtr, pagePtr, ref.m_page_idx);
  }
  return;
}

void Dbtup::free_var_part(Fragrecord *fragPtr, PagePtr pagePtr,
                          Uint32 page_idx) {
  ndbassert(fragPtr->m_varWordsFree >= ((Var_page *)pagePtr.p)->free_space);
  fragPtr->m_varWordsFree -= ((Var_page *)pagePtr.p)->free_space;
  ((Var_page *)pagePtr.p)->free_record(page_idx, Var_page::CHAIN);
  ndbassert(fragPtr->m_varElemCount > 0);
  fragPtr->m_varElemCount--;

  DEB_ELEM_COUNT(("(%u) Dec m_varElemCount: now %llu tab(%u,%u),"
                  " line: %u",
                  instance(),
                  fragPtr->m_varElemCount,
                  fragPtr->fragTableId, Fragrecord *fragPtr,
                  
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
fragPtr->fragmentId,
||||||| Common ancestor
      Fragrecord* fragPtr,
// RONDB-624 todo: Glue these lines together ^v
=======
             
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
   
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
pagePtr,
			Var_part_ref*
// RONDB-624 todo: Glue these lines together ^v
=======
pagePtr,
                                Var_part_ref
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
refptr,
// RONDB-624 todo: Glue these lines together ^v
=======
*refptr,
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
              __LINE__));
  ndbassert(pagePtr.p->free_space <= Var_page::DATA_WORDS);
  if (pagePtr.p->free_space == Var_page::DATA_WORDS - 1) {
    jam();
    Uint32 idx = pagePtr.p->list_index;
    Local_Page_list list(c_page_pool, fragPtr->free_var_page_array[idx]);
    list.remove(pagePtr);
    returnCommonArea(pagePtr.i, 1);
    fragPtr->noOfVarPages --;
  }
  else
  {
    jam();
    // Adds the new free space value for the page to the fragment total.
    update_free_page_list(fragPtr, pagePtr);
  }
  ndbassert(fragPtr->verifyVarSpace());
}

Uint32 *
Dbtup::realloc_var_part(Uint32 * err,
                        Fragrecord* fragPtr, Tablerec* tabPtr, PagePtr pagePtr,
			Var_part_ref* refptr, Uint32 oldsz, Uint32 newsz)
{
  Uint32 add = newsz - oldsz;
  Uint32 *new_var_ptr;
  Var_page* pageP = (Var_page*)pagePtr.p;
  Local_key oldref;
  refptr->copyout(&oldref);
  
  ndbassert(newsz);
  ndbassert(add);

  if (oldsz && pageP->free_space >= add)
  {
    jam();
    new_var_ptr = pageP->get_ptr(oldref.m_page_idx);
    {
      if(0) printf("extra reorg");
      jam();
      /**
       * In this case we need to reorganise the page to fit. To ensure we
       * don't complicate matters we make a little trick here where we
       * fool the reorg_page to avoid copying the entry at hand and copy
       * that separately at the end. This means we need to copy it out of
       * the page before reorg_page to save the entry contents.
       */
      Uint32* copyBuffer= cinBuffer;
      memcpy(copyBuffer, new_var_ptr, 4*oldsz);
      pageP->set_entry_len(oldref.m_page_idx, 0);
      pageP->free_space += oldsz;
      fragPtr->m_varWordsFree += oldsz;
      pageP->reorg((Var_page*)ctemp_page);
      new_var_ptr= pageP->get_free_space_ptr();
      memcpy(new_var_ptr, copyBuffer, 4*oldsz);
      pageP->set_entry_offset(oldref.m_page_idx, pageP->insert_pos);
      add += oldsz;
    }
    ndbassert(fragPtr->m_varWordsFree >= pageP->free_space);
    fragPtr->m_varWordsFree -= pageP->free_space;

   
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
 pageP->grow_entry(oldref.m_page_idx, add);
    // Adds the new free space value for the page to
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
PagePtr pagePtr,
              
// RONDB-624 todo: Glue these lines together ^v
=======
>>>>>>> MySQL 8.0.36
 the fragment total.
  PagePtr pagePtr, update_free_page_list(fragPtr, pagePtr);
  }
  else
  {
    jam();
   Uint32 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
Local_key
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
=======
size,
>>>>>>> MySQL 8.0.36
 newref;
    
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
new_var_ptr = alloc_var_part(err,
||||||| Common ancestor
Uint32 size,
=======
>>>>>>> MySQL 8.0.36
                           
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
Var_page*
// RONDB-624 todo: Glue these lines together ^v
=======
Var_page
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
pageP
// RONDB-624 todo: Glue these lines together ^v
=======
*pageP
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
     fragPtr,
                                 tabPtr,
                                 newsz,
                                 &newref,
                                 __LINE__,
                                 false);
    if (unlikely(new_var_ptr == 0))
      return NULL;

    if (oldsz)
    {
      jam();
      Uint32 *src = pageP->get_ptr(oldref.m_page_idx);
      ndbassert(oldref.m_page_no != newref.m_page_no);
      ndbassert(pageP->get_entry_len(oldref.m_page_idx) == oldsz);
      memcpy(new_var_ptr, src, 4 *oldsz);
      free_var_part(fragPtr, pagePtr, oldref.m_page_idx);
    }

    refptr->assign(&newref);
  }
  
  return new_var_ptr;
}

void
Dbtup::move_var_part(Fragrecord* fragPtr,
                     Tablerec* tabPtr,
                     PagePtr pagePtr,
                     Var_part_ref* refptr,
                     Uint32 size,
                     Tuple_header *org)
{
  jam();

  ndbassert(size);
  Var_page* pageP = (Var_page*)pagePtr.p;
  Local_key oldref;
  refptr->copyout(&oldref);

  /**
   * to find destination page index of free list
   */
  Uint32 new_index = calculate_free_list_impl(size);

  /**
   * do not move tuple from big-free-size page list
   * to small-free-size page list
   */
  if (new_index > pageP->list_index)
  {
    jam();
    return;
  }

  PagePtr new_pagePtr;
  new_pagePtr.i = get_alloc_page(fragPtr, size + 1);

  if (new_pagePtr.i == RNIL)
  {
    jam();
    return;
  }

  /**
   * do not move varpart if new var part page is same as old
   */
  if (new_pagePtr.i == pagePtr.i)
  {
    jam();
    return;
  }

  c_page_pool.getPtr(new_pagePtr);

  ndbassert(fragPtr->m_varWordsFree >= ((Var_page*)new_pagePtr.p)->free_space);
  fragPtr->m_varWordsFree -= ((Var_page*)new_pagePtr.p)->free_space;

  /**
   * At his point we need to upgrade to exclusive fragment access.
   * The variable sized part might be used for reading in query
   * thread at this point in time. To avoid having to use a mutex
   * to protect reads of rows we ensure that all places where we
   * reorganize pages and rows are done with exclusive fragment
   * access.
   *
   * Since we change the reference to the variable part we also
   * need to recalculate while being in exclusive mode.
   *
   * We could need this exclusive access already in alloc_record
   * since that could potentially reorganise the row.
   */
  c_lqh->upgrade_to_exclusive_frag_access();

  Uint32 idx= ((Var_page*)new_pagePtr.p)
    ->alloc_record(size,(Var_page*)ctemp_page, Var_page::CHAIN);

  /**
   * update new page into new free list after alloc_record
   */
  update_free_page_list(fragPtr, new_pagePtr);

  Uint32 *dst = ((Var_page*)new_pagePtr.p)->get_ptr(idx);
  const Uint32 *src = pageP->get_ptr(oldref.m_page_idx);

  /**
   * copy old varpart to new position
   */
  memcpy(dst, src, 4*size);

  fragPtr->m_varElemCount++;
  DEB_ELEM_COUNT(("(%u) Inc m_varElemCount: now %llu tab(%u,%u),"
                  " line: %u",
                  instance(),
                  fragPtr->m_varElemCount,
                  fragPtr->fragTableId,
                  fragPtr->fragmentId,
                  __LINE__));
  /**
   * remove old var part of tuple (and decrement m_varElemCount).
   */
  free_var_part(fragPtr, pagePtr, oldref.m_page_idx);
  /**
   * update var part ref of fix part tuple to newref
   */
  Local_key newref;
  newref.m_page_no = new_pagePtr.i;
  newref.m_page_idx = idx;
  refptr->assign(&newref);
  setChecksum(org, tabPtr);
  c_lqh->downgrade_from_exclusive_frag_access();
}

/* ------------------------------------------------------------------------ */
// Get a page from one of free lists. If the desired free list is empty we
// try with the next until we have tried all possible lists.
/* ------------------------------------------------------------------------ */
Uint32
Dbtup::get_alloc_page(Fragrecord* fragPtr, Uint32 alloc_size)
{
  Uint32 start_index;
  PagePtr pagePtr;
  
  start_index= calculate_free_list_for_alloc(alloc_size);
  ndbassert(start_index < MAX_FREE_LIST);
  for (Uint32 i = start_index; i < MAX_FREE_LIST; i++)
  {
    jam();
    if (!fragPtr->free_var_page_array[i].isEmpty()) 
    {
      jam();
      return fragPtr->free_var_page_array[i].getFirst();
    }
  }
  /* If no list with enough guaranteed size of free space is empty, fallback
   * checking the first 16 entries in the free list which may have an entry
   * with enough free space.
   */
  if (start_index == 0)
  {
    jam();
    return RNIL;
  }
  start_index--;
  Local_Page_list list(c_page_pool, fragPtr->free_var_page_array[start_index]);
  list.first(pagePtr);
  for(Uint32 loop = 0; !pagePtr.isNull() && loop < 16; loop++)
  {
    jam();
    if (pagePtr.p->free_space >= alloc_size)
    {
      jam();
      return pagePtr.i;
    }
    list.next(pagePtr);
  }
  return RNIL;
}

Uint32
Dbtup::get_empty_var_page(Fragrecord* fragPtr,
                          Tablerec* tabPtrP)
{
  PagePtr ptr;
  Uint32 cnt;
  allocConsPages(jamBuffer(), tabPtrP, 1, cnt, ptr.i);
  fragPtr->noOfVarPages+= cnt;
  if (unlikely(cnt == 0))
  {
    return RNIL;
  }

  c_page_pool.getPtr(ptr);
  ptr.p->physical_page_id = ptr.i;
  ptr.p->page_state = ~0;
  ptr.p->nextList = RNIL;
  ptr.p->prevList = RNIL;
  ptr.p->frag_page_id = RNIL;
  
  return ptr.i;
}

/* ------------------------------------------------------------------------ */
// Check if the page needs to go to a new free page list.
/* ------------------------------------------------------------------------ */
void Dbtup::update_free_page_list(Fragrecord *fragPtr,
                                  Ptr<Page> pagePtr) {
  Uint32 free_space, list_index;
  free_space= pagePtr.p->free_space;
  list_index= pagePtr.p->list_index;
  fragPtr->m_varWordsFree+= free_space;
  ndbassert(fragPtr->verifyVarSpace());

  if ((free_space < c_min_list_size[list_index]) ||
      (free_space > c_max_list_size[list_index])) {
    Uint32 new_list_index= calculate_free_list_impl(free_space);

    {
      /**
       * Remove from free list
       */
      Local_Page_list
        list(c_page_pool, fragPtr->free_var_page_array[list_index]);
      list.remove(pagePtr);
    }
    if (free_space < c_min_list_size[new_list_index])
    {
      /*
	We have not sufficient amount of free space to put it into any
	free list. Thus the page will not be available for new inserts.
	This can only happen for the free list with least guaranteed 
	free space.

        Put in on MAX_FREE_LIST-list (i.e full pages)     */
      jam();
      ndbrequire(new_list_index == 0);
      new_list_index = MAX_FREE_LIST;
    }

    {
      Local_Page_list list(c_page_pool,
                             fragPtr->free_var_page_array[new_list_index]);
      list.addFirst(pagePtr);
      pagePtr.p->list_index = new_list_index;
    }
  }
}

/* ------------------------------------------------------------------------ */
// Given size of free space, calculate the free list to put it into
/* ------------------------------------------------------------------------ */
Uint32 Dbtup::calculate_free_list_impl(Uint32 free_space_size) const
{
  Uint32 i;
  for (i = 0; i < MAX_FREE_LIST; i++) {
    jam();
    if (free_space_size <= c_max_list_size[i]) {
      jam();
      return i;
    }
  }
  ndbabort();
  return 0;
}

Uint32 Dbtup::calculate_free_list_for_alloc(
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
Uint32
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
Fragrecord*
// RONDB-624 todo: Glue these lines together ^v
=======
Fragrecord
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
alloc_size) const
{
  ndbassert(alloc_size <= MAX_EXPANDED_TUPLE_SIZE_IN_WORDS);
  for (Uint32 i = 0; i < MAX_FREE_LIST; i++)
  {
    jam();
    if (alloc_size <= c_min_list_size[i])
    {
      jam();
      return i;
   
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
fragPtr,
                                 
// RONDB-624 todo: Glue these lines together ^v
=======
*fragPtr,
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 }
  }
  /* Allocation too big, last free list page should always have space for
   * biggest possible allocation.
   */
  ndbabort();
}

Uint64 Dbtup::calculate_used_var_words(Fragrecord* fragPtr)
{
  /* Loop over all VarSize pages in this fragment, summing
   * their used space
   */
  Uint64 totalUsed= 0;
  for (Uint32 freeList= 0; freeList <= MAX_FREE_LIST; freeList++)
  {
    Local_Page_list list(c_page_pool,
                           fragPtr->free_var_page_array[freeList]);
    Ptr<Page> pagePtr;

    if (list.first(pagePtr))
    {
      do
      {
        totalUsed+= (Tup_varsize_page::DATA_WORDS - pagePtr.p->free_space);
      } while (list.next(pagePtr));
    };
  };

  return totalUsed;
}

/*
  Allocator for variable sized rows, allocates both the fixed size
  part and the variable sized part. To ensure that we don't hold
  any mutex while allocating varsize part we allocate that first.
  We might need exclusive access while allocating varsized part.

  This method is used both to allocate from a specific rowid or from
  any rowid.

  SYNOPSIS
    err             Error object
    fragPtr         A pointer to the fragment description
    tabPtr          A pointer to the table description
    alloc_size      Size of the allocated varsize record
    out_frag_page_id Row id of new row
    use_rowid       Use the row id as input as well
  RETURN VALUES
    Return 0 if unsuccessful, otherwise pointer to fixed part.
*/
Uint32* 
Dbtup::alloc_var_row(Uint32 * err,
                     Fragrecord* const fragPtr,
		     Tablerec* const tabPtr,
		     Uint32 alloc_size,
		     Local_key* key,
		     Uint32 * out_frag_page_id,
                     bool use_rowid)
{
  Local_key varref;
  if (likely(alloc_size))
  {
    if (unlikely(alloc_var_part(err,
                                fragPtr,
                                tabPtr,
                     
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
Dbtup::calculate_used_var_words(Fragrecord*
// RONDB-624 todo: Glue these lines together ^v
=======
Dbtup::calculate_used_var_words(Fragrecord
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
fragPtr)
{
=======
*fragPtr) {
>>>>>>> MySQL 8.0.36
          alloc_size,
                                &varref,
                                __LINE__,
                                true) == 0))
    {
      return 0;
    }
  }
  Uint32 *ptr;
  if (!use_rowid)
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
Uint32*
// RONDB-624 todo: Glue these lines together ^v
=======
Uint32
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
 {
    ptr = 
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor

Dbtup::
// RONDB-624 todo: Glue these lines together ^v
=======
*Dbtup::
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
alloc_fix_rec(jamBuffer(), err,
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
 fragPtr, tabPtr, key,
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
=======
 Fragrecord *fragPtr,
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36

                        
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
 out_frag_page_id);
||||||| Common ancestor
fragPtr,
		
// RONDB-624 todo: Glue these lines together ^v
=======
>>>>>>> MySQL 8.0.36
  }
  else
  {
    
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
ptr = alloc_fix_rowid(err, fragPtr,
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
Tablerec*
// RONDB-624 todo: Glue these lines together ^v
=======
Tablerec
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 *tabPtr,
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
 key, out_frag_page_id);
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
=======
 Uint32 alloc_size,
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36

  }
  if (unlikely(ptr == 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
0))
  {
||||||| Common ancestor
Uint32
// RONDB-624 todo: Glue these lines together ^v
=======
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
   if (alloc_size)
||||||| Common ancestor
alloc_size,
		
// RONDB-624 todo: Glue these lines together ^v
=======
>>>>>>> MySQL 8.0.36
    {
      PagePtr pagePtr;
    
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
Uint32
// RONDB-624 todo: Glue these lines together ^v
=======
        Local_key
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
*
// RONDB-624 todo: Glue these lines together ^v
=======
*key, Uint32
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
 
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
ndbrequire(c
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
out
// RONDB-624 todo: Glue these lines together ^v
=======
*out
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
_page_pool.getPtr(pagePtr, varref.m_page_no));
      free_var_part(fragPtr, pagePtr, varref.m_page_idx);
    }
    return 0;
  }
  Tuple_header *tuple = (Tuple_header *)ptr;
  Var_part_ref *dst = (Var_part_ref *)tuple->get_var_part_ref_ptr(tabPtr);
  if (likely(alloc_size
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
))
// RONDB-624 todo: Glue these lines together ^v
||||||| Common ancestor
, &varref) != 0))
// RONDB-624 todo: Glue these lines together ^v
=======
, &varref) !=
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36

  
// RONDB-624 todo: Glue these lines together ^v
<<<<<<< RonDB // RONDB-624 todo
||||||| Common ancestor
  
// RONDB-624 todo: Glue these lines together ^v
=======
             0)) 
// RONDB-624 todo: Glue these lines together ^v
>>>>>>> MySQL 8.0.36
{
    dst->assign(&varref);
  } else {
    varref.m_page_no = RNIL;
    dst->assign(&varref);
  }
  return ptr;
}
