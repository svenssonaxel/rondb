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

#ifndef ArenaAllocator_hpp_included
#define ArenaAllocator_hpp_included 1

#include <assert.h>
#include <stdexcept>

//#define ARENA_ALLOCATOR_DEBUG 1

class ArenaAllocator
{
private:
  enum class byte : uint8_t {};
  /*
   * todo: These two parameters could be dynamic. With some statistics, we
   * should be able to tune these as a function of SQL statement length, which
   * we'll probably know before we create the arena allocator.
   */
  static const size_t DEFAULT_PAGE_SIZE = 256;
  static const size_t INITIAL_PAGE_SIZE = 80;
  size_t m_page_data_size = DEFAULT_PAGE_SIZE;
  struct Page
  {
    struct Page* next = NULL;
    byte data[1]; // Actually an arbitrary amount
  };
  static const size_t OVERHEAD = offsetof(struct Page, data);
  static_assert(OVERHEAD < DEFAULT_PAGE_SIZE, "default page size too small");
  struct Page* m_current_page = NULL;
  byte* m_point = NULL;
  byte* m_stop = NULL;
# ifdef ARENA_ALLOCATOR_DEBUG
  unsigned long int m_allocated_by_us = sizeof(ArenaAllocator);
  unsigned long int m_allocated_by_user = 0;
# endif
  byte m_initial_stack_allocated_page[INITIAL_PAGE_SIZE]; // MUST be last!
public:
  ArenaAllocator();
  ~ArenaAllocator();
  void* alloc(size_t size);
  void* realloc(const void* ptr, size_t size, size_t original_size);
};

#endif
