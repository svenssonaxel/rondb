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
#include <ndbd_malloc.hpp>

//#define DEBUG_HAST 1

class Dbacc;

class Hast {

  // Utility types
public:
  typedef Uint32 Hash;
  typedef Uint64 Value; // todoas 32 for page id + 13 bits for page idx, + 1 bit for locked status, so perhaps only guarantee 46 or 48 bits. Some implementation might want to pack it in 6 bytes and use 1 bit for presence tracking, so 47 bits.
  typedef Dbacc* Block;
  typedef const Dbacc* CBlock;
  class Cursor {
    friend class Hast;
  private:
    Hash m_hash;
    Uint32 m_bucketIndex;
    Uint32 m_entryIndex;
    Value* m_valueptr;
    Uint32 m_valid;
    enum { VALID = 0x7d5be15d,
           INVALID = 0x37f93878
    };
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
  class Root {
    friend class Hast;
  private:
    Root();
    void* seize_mem(Block acc, size_t size);
    void release_mem(void* ptr);
    void initialize(Block acc, Uint32 dbg_tableId, Uint32 dbg_threadId, Uint32 dbg_inx);
    void release(Block acc);
    void insertEntryIntoBucket(Block acc, Bucket& bucket, Uint32 hash, Value value);
    Uint32 computeBucketIndex(CBlock acc, Hash hash, Uint32 numberOfBuckets) const;
    Uint32 siblingBucketIndex(Block acc, Uint32 bucketIndex) const;
    // todoas expose expanding/shrinking and make async
    bool shouldExpand() const;
    bool shouldShrink() const;
    void expand(Block acc);
    void shrink(Block acc);
    void updateOperationRecords(Block acc, Bucket &bucket, Uint32 bucketIndex);
    static constexpr size_t MAX_NUMBER_OF_BUCKETS =
        (NDBD_MALLOC_MAX_MEMORY_ALLOC_SIZE_IN_BYTES / sizeof(Bucket));
    static constexpr Uint64 HIGH_NUMBER_OF_ENTRIES_PER_BUCKET = 18;
    static constexpr Uint64 LOW_NUMBER_OF_ENTRIES_PER_BUCKET = 14;

    // Proxied from public interface
    bool isEntryCursor(CBlock acc, Cursor& cursor) const;
    bool isInsertCursor(CBlock acc, Cursor& cursor) const;
    Cursor getCursorFirst(Block acc, Hash hash) const;
    void cursorNext(Block acc, Cursor& cursor) const;
    Value getValue(CBlock acc, Cursor& cursor) const;
    void setValue(Block acc, Cursor& cursor, Value value);
    void insertEntry(Block acc, Cursor& cursor, Value value);
    void deleteEntry(Block acc, Cursor& cursor);

    // Validation & debugging
    void validateAll(CBlock acc) const;
    void validateHastRoot(CBlock acc) const;
    void validateB(CBlock acc) const;
    void validateValue(CBlock acc, Value value) const;
    void validateCursor(CBlock acc, Cursor& cursor) const;
    void validateBucket(CBlock acc, Bucket& bucket, Uint32 bucketIndex) const;
    void progError(int line, int err_code, const char* extra, const char* check) const;
    void debug_dump_root() const;
    void debug_dump_bucket(Bucket& bucket, Uint32 bucketIndex, const char* bucketPrefix, const char* entryPrefix) const;

    // Data
    Uint32 m_numberOfBuckets;
    Bucket* m_buckets; // todoas Eventually, use some kind of dynamic array
    Uint64 m_numberOfEntries;
    Uint32 m_dbg_tableId;
    Uint32 m_dbg_fragId;
    Uint32 m_dbg_inx;
  };

  // Public interface
public:
  void initialize(Block acc, Uint32 dbg_tableId, Uint32 dbg_threadId);
  void release(Block acc);
  bool isEntryCursor(CBlock acc, Cursor& cursor) const;
  bool isInsertCursor(CBlock acc, Cursor& cursor) const;
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
  Value getValue(CBlock acc, Cursor& cursor) const;
  // Given a cursor pointing to an entry, set the value of that entry.
  void setValue(Block acc, Cursor& cursor, Value value);
  // Given an insert cursor, insert an entry with the given value.
  void insertEntry(Block acc, Cursor& cursor, Value value);
  // Given a cursor pointing to an entry, delete that entry.
  void deleteEntry(Block acc, Cursor& cursor);

  // Internals
private:
  Root& getRoot(Hash hash);
  const Root& getRoot(Hash hash) const;
  Root& getRoot(Cursor& cursor);
  const Root& getRoot(Cursor& cursor) const;

  // Data
  Root m_roots[4];
};

#endif
