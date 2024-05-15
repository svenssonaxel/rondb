/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

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

#include <cstring> // memcpy
#include "ArenaAllocator.hpp"

ArenaAllocator::ArenaAllocator()
{
  m_point = &m_initial_stack_allocated_page[0];
  m_stop = &m_initial_stack_allocated_page[INITIAL_PAGE_SIZE];
}

ArenaAllocator::~ArenaAllocator()
{
  while (m_current_page)
  {
    Page* next = (Page*)m_current_page->next;
    free(m_current_page);
    m_current_page = next;
  }
# ifdef ARENA_ALLOCATOR_DEBUG
  std::cerr << "In ~ArenaAllocator" << std::endl
            << "  Total allocated by us: " << m_allocated_by_us << std::endl
            << "  Total allocated by user: " << m_allocated_by_user << std::endl
            << "  Efficiency: " << 100 * m_allocated_by_user / m_allocated_by_us << "%" << std::endl;
# endif
}

void*
ArenaAllocator::alloc(size_t size)
{
  byte* new_point = m_point + size;
  if (new_point > m_stop)
  {
    if (0x40000000 <= 2 * size + OVERHEAD)
    {
      throw std::runtime_error(
        "ArenaAllocator: Requested allocation size too large"
      );
    }
    while (m_page_data_size < 2 * size + OVERHEAD)
    {
      m_page_data_size *= 2;
    }
    Page* new_page = (Page*)malloc(m_page_data_size);
    if (!new_page)
    {
      throw std::runtime_error("ArenaAllocator: Out of memory");
    }
#   ifdef ARENA_ALLOCATOR_DEBUG
    m_allocated_by_us += m_page_data_size;
#   endif
    new_page->next = m_current_page;
    m_current_page = new_page;
    m_point = new_page->data;
    m_stop = ((byte*)new_page) + m_page_data_size;
    new_point = m_point + size;
    assert(new_point < m_stop);
  }
  void* ret = m_point;
  m_point = new_point;
# ifdef ARENA_ALLOCATOR_DEBUG
  m_allocated_by_user += size;
# endif
  return ret;
}

/*
 * WARNING: ArenaAllocator::realloc can return a non-const pointer to the same
 *          memory as the argument `const void* ptr`. Make sure not to write to
 *          ptr[X] for any X<original_size.
 *
 * This realloc differs from the standard by requiring the size of the original
 * allocation. This gives several advantages:
 * 1) ArenaAllocator has no need to keep track of allocation sizes.
 * 2) ArenaAllocator can reallocate memory that was allocated by other means
 *    than itself, on the condition that no free() is necessary.
 * 3) ArenaAllocator can potentially reallocate in-place a block of memory that
 *    is the result of concatenating several allocations.
 */
void*
ArenaAllocator::realloc(const void* ptr, size_t size, size_t original_size)
{
  const byte* byte_ptr = static_cast<const byte*>(ptr);
  if (&byte_ptr[original_size] == m_point &&
     size >= original_size &&
     &byte_ptr[size] <= m_stop)
  {
    // The original allocation ends precisely where our unused memory begins,
    // the reallocation will increase the size, and the unused memory on the
    // current page is sufficient to accommodate the new allocation. Therefore,
    // we can reallocate in-place.
    m_point += (size - original_size);
    assert(m_point == &byte_ptr[size]);
    void* nonconst_ptr = &m_point[-size];
    assert((const void*)nonconst_ptr == ptr);
    return nonconst_ptr;
  }
  // Do not reallocate in-place.
  void* new_alloc = alloc(size);
  size_t cplen = size < original_size ? size : original_size;
  memcpy(new_alloc, ptr, cplen);
  return new_alloc;
}
