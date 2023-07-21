/*
   Copyright (c) 2023, 2023, Hopsworks and/or its affiliates.

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

#ifndef DBHAST_H
#define DBHAST_H

#include <pc.hpp>
#include "Emulator.hpp"
#include <ndbd_malloc.hpp>

class Dbacc;

class Hast {

  // Utility types
public:
  typedef Uint32 Hash;
  typedef Uint64 Value; // todoas 32 for page id + 13 bits for page idx, + 1 bit for locked status, so perhaps only guarantee 46 or 48 bits. Some implementation might want to pack it in 6 bytes and use 1 bit for presence tracking, so 47 bits.
  typedef Dbacc* Block;
  class Cursor {
    friend class Hast;
  private:
    Hash m_hash;
    Uint32 m_bucketIndex;
    Uint32 m_entryIndex;
    Value* m_valueptr;
  };
private:
  class Entry {
    friend class Hast;
  private:
    Hash m_hash;
    Value m_value;
  };
  class Bucket {
    friend class Hast;
  private:
    Uint32 m_numberOfEntries;
    Entry* m_entries; // todoas not correct since this could potentially become larger than 1MB. Use a nextBucket pointer.
  };

  // Public interface
public:
  Hast();
  void initialize(Block acc);
  void release(Block acc);
  bool isEntryCursor(Block acc, Cursor& cursor) const;
  bool isInsertCursor(Block acc, Cursor& cursor) const;
  // Given a hash, return a cursor pointing to the first entry for that hash if
  // such exists, otherwise an insert cursor for that hash.
  // A cursor is valid for as long as the lock is held and there is no call to
  // insertEntry or deleteEntry.
  Cursor getCursorFirst(Block acc, Hash hash) const;
  // Given a cursor pointing to an entry, make it point to the next entry for
  // the same hash if such exists, otherwise make it an insert cursor for
  // that hash.
  void cursorNext(Block acc, Cursor& cursor) const;
  // Given a cursor pointing to an entry, get the value of that entry.
  Value getValue(Block acc, Cursor& cursor) const;
  // Given a cursor pointing to an entry, set the value of that entry.
  void setValue(Block acc, Cursor& cursor, Value value);
  // Given an insert cursor, insert an entry with the given value.
  void insertEntry(Block acc, Cursor& cursor, Value value);
  // Given a cursor pointing to an entry, delete that entry.
  void deleteEntry(Block acc, Cursor& cursor);

  // Internals
private:
  void insertEntryIntoBucket(Block acc, Bucket& bucket, Hash hash, Value value);
  Uint32 computeBucketIndex(Hash hash, Uint32 numberOfBuckets) const;
  void* malloc(Block acc, size_t size);
  void free(void* ptr);
  // todoas expose growing/shrinking and make async
  bool shouldGrow() const;
  bool shouldShrink() const;
  void grow(Block acc);
  void shrink(Block acc);
  static constexpr size_t MAX_NUMBER_OF_BUCKETS =
      (NDBD_MALLOC_MAX_MEMORY_ALLOC_SIZE_IN_BYTES / sizeof(Bucket));
  static constexpr Uint64 HIGH_NUMBER_OF_ENTRIES_PER_BUCKET = 18;
  static constexpr Uint64 LOW_NUMBER_OF_ENTRIES_PER_BUCKET = 14;

  // Validation & debugging
  void validateAll(Block acc) const;
  void validateHastRoot(Block acc) const;
  void validateB(Block acc) const;
  void validateValue(Block acc, Value value) const;
  void validateCursor(Block acc, Cursor& cursor) const;
  void validateBucket(Block acc, Bucket& bucket, Uint32 bucketIndex) const;
  void progError(int line, int err_code, const char* extra, const char* check) const;
  EmulatedJamBuffer* jamBuffer() const;

  // Data
  Uint32 m_numberOfBuckets;
  Bucket* m_buckets; // todoas Eventually, use some kind of dynamic array
  Uint64 m_numberOfEntries;
  Block m_bptr;
  Uint32 m_threadId;
};

#endif
