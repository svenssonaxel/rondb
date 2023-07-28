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
private:
  class Value {
    friend class Hast;
  private:
    // 31 bit Operation Pointer I-value
    Uint32 m_opptri;
    // Local key from kernel_types, from which we use
    // - 13 bit Dbtup page index (m_page_idx)
    // - 32 bit Dbtup page number (m_page_no)
    Local_key m_lk;
    // 1 bit locked status
    bool m_locked;
    bool equals(const Value& other) const;
  };
  class Root;
public:
  typedef Uint32 Hash;
  typedef Dbacc* Block;
  typedef const Dbacc* CBlock;
  class Cursor {
    friend class Hast;
  public:
    // A cursor can be
    // - Insert cursor: Not pointing to any entry but usable for inserting one
    // - Entry cursor: Pointing to an entry
    //   - Unlocked cursor: Pointing to an entry that is not locked
    //   - Locked cursor: Pointing to an entry that is locked

    // Return true if the cursor is an insert cursor.
    bool isInsertCursor(CBlock acc) const;
    // Return true if the cursor is an entry cursor.
    bool isEntryCursor(CBlock acc) const;

    /*
     * Methods for insert cursors:
     */
    void insertLockedOpptriLk(Block acc, Uint32 opptri, Local_key lk);

    /*
     * Methods for entry cursors:
     */
    // Given an entry cursor, make it point to the next entry for the same hash
    // if such exists, otherwise make it an insert cursor for that hash.
    void next(CBlock acc);
    // Return true if the entry is locked.
    bool getLocked(CBlock acc) const;
    // Delete the entry and invalidate the cursor.
    void deleteEntry(Block acc);

    /*
     * Methods for unlocked cursors:
     */
    Local_key getLkWhenUnlocked(CBlock acc) const;
    void setLockedOpptriWhenUnlocked(CBlock acc, Uint32 opptri);
    void setLkWhenUnlocked(CBlock acc, Local_key lk);
    /*
     * Methods for locked cursors:
     */
    Uint32 getOpptriWhenLocked(CBlock acc) const;
    Local_key getLkWhenLocked(CBlock acc) const;
    void setOpptriWhenLocked(CBlock acc, Uint32 opptri);
    void setLkWhenLocked(CBlock acc, Local_key lk);
    void setUnlocked(CBlock acc);
  private:
    void validateCursor(CBlock acc) const;
    void validateInsertCursor(CBlock acc) const;
    void validateEntryCursor(CBlock acc) const;
    void validateLockedCursor(CBlock acc) const;
    void validateUnlockedCursor(CBlock acc) const;
    static void progError(int line, int err_code, const char* extra, const char* check);
    Hash m_hash;
    Root* m_root;
    Uint32 m_bucketIndex;
    Uint32 m_entryIndex;
    Value* m_valueptr;
    Value m_dbg_value; // Used for validation
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
    void initialize(Block acc, Uint32 dbg_tableId, Uint32 dbg_threadId, Uint32 dbg_inx);
    void release(Block acc);
    void insertEntryIntoBucket(CBlock acc, Bucket& bucket, Uint32 hash, Value value);
    Uint32 computeBucketIndex(CBlock acc, Hash hash, Uint32 numberOfBuckets) const;
    Uint32 siblingBucketIndex(Block acc, Uint32 bucketIndex) const;
    // todoas expose expanding/shrinking and make async
    bool shouldExpand() const;
    bool shouldShrink() const;
    void expand(Block acc);
    void shrink(Block acc);
    void updateOperationRecords(Block acc, Bucket &bucket, Uint32 bucketIndex);
    static void progError(int line, int err_code, const char* extra, const char* check);
    static constexpr size_t MAX_NUMBER_OF_BUCKETS =
        (NDBD_MALLOC_MAX_MEMORY_ALLOC_SIZE_IN_BYTES / sizeof(Bucket));
    static constexpr Uint64 HIGH_NUMBER_OF_ENTRIES_PER_BUCKET = 18;
    static constexpr Uint64 LOW_NUMBER_OF_ENTRIES_PER_BUCKET = 14;

    // Validation & debugging
    void validateRoot(CBlock acc) const;
    void validateB(CBlock acc) const;
    void validateBucket(CBlock acc, Bucket& bucket, Uint32 bucketIndex) const;
    void debug_dump_root(CBlock acc) const;
    void debug_dump_bucket(CBlock acc,
                           Bucket& bucket,
                           Uint32 bucketIndex,
                           const char* bucketPrefix,
                           const char* entryPrefix) const;

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
  // Given a hash, return a cursor pointing to the first entry for that hash if
  // such exists, otherwise an insert cursor for that hash.
  Cursor getCursorFirst(Block acc, Hash hash);

  // Internals
private:
  static void* seize_mem(CBlock acc, size_t size);
  static void release_mem(void* ptr);
  static void progError(int line, int err_code, const char* extra, const char* check);
  void validateAll(CBlock acc) const;
  static void validateValue(CBlock acc, const Value& value);

  // Data
  Root m_roots[4];
};

#endif
