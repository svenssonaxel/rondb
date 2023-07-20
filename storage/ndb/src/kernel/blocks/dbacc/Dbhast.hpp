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
  typedef Uint64 Value;
  typedef Dbacc* B;
  class Cursor {
    friend class Hast;
  private:
    Hash hash;
    Uint32 bucketIndex;
    Uint32 entryIndex;
    Value* valueptr;
  };
private:
  class Entry {
    friend class Hast;
  private:
    Hash hash;
    Value value;
  };
  class Bucket {
    friend class Hast;
  private:
    Uint32 numberOfEntries;
    Entry* entries;
  };

  // Public interface
public:
  Hast();
  void initialize(B b);
  void release(B b);
  bool isEntryCursor(B b, Cursor& cursor) const;
  bool isInsertCursor(B b, Cursor& cursor) const;
  // Given a hash, return a cursor pointing to the first entry for that hash if
  // such exists, otherwise an insert cursor for that hash.
  // A cursor is valid for as long as the lock is held and there is no call to
  // insertEntry or deleteEntry.
  Cursor getCursorFirst(B b, Hash hash) const;
  // Given a cursor pointing to an entry, make it point to the next entry for
  // the same hash if such exists, otherwise make it an insert cursor for
  // that hash.
  void cursorNext(B b, Cursor& cursor) const;
  // Given a cursor pointing to an entry, get the value of that entry.
  Value getValue(B b, Cursor& cursor) const;
  // Given a cursor pointing to an entry, set the value of that entry.
  void setValue(B b, Cursor& cursor, Value value);
  // Given an insert cursor, insert an entry with the given value.
  void insertEntry(B b, Cursor& cursor, Value value);
  // Given a cursor pointing to an entry, delete that entry.
  void deleteEntry(B b, Cursor& cursor);

  // Internals
private:
  void insertEntryIntoBucket(B b, Bucket& bucket, Hash hash, Value value);
  Uint32 computeBucketIndex(Hash hash, Uint32 numberOfBuckets) const;
  void* malloc(B b, size_t size);
  void free(void* ptr);
  // todoas expose growing/shrinking and make async
  bool shouldGrow() const;
  bool shouldShrink() const;
  void grow(B b);
  void shrink(B b);
  static constexpr size_t max_number_of_buckets =
      (NDBD_MALLOC_MAX_MEMORY_ALLOC_SIZE_IN_BYTES / sizeof(Bucket));
  static constexpr Uint64 high_number_of_entries_per_bucket = 18;
  static constexpr Uint64 low_number_of_entries_per_bucket = 14;

  // Validation & debugging
  void validateAll(B b) const;
  void validateHastRoot(B b) const;
  void validateB(B b) const;
  void validateValue(B b, Value value) const;
  void validateCursor(B b, Cursor& cursor) const;
  void validateBucket(B b, Bucket& bucket, Uint32 bucketIndex) const;
  void progError(int line, int err_code, const char* extra, const char* check) const;
  EmulatedJamBuffer* jamBuffer() const;

  // Data
  Uint32 numberOfBuckets;
  Bucket* buckets;
  Uint64 numberOfEntries;
  B bptr;
  Uint32 threadId;
};

#endif
