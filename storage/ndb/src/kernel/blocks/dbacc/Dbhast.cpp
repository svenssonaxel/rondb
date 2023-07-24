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

#include "util/require.h"
#include "Dbacc.hpp"
#include "Dbhast.hpp"
#define JAM_FILE_ID 550

// We make a hard-coded assumption that we have 4 fragment mutexes.
static_assert(NUM_ACC_FRAGMENT_MUTEXES == 4);

// Debug logging
#ifdef DEBUG_HAST
#define DEB_HAST(msg, ...) do { \
    g_eventLogger->info \
      ( \
        "DEBUG Dbhast.cpp:%3d Table %04x, Fragment %04x, Root %x, Thread %u: " msg, \
        __LINE__, \
        m_dbg_tableId, \
        m_dbg_fragId, \
        m_dbg_inx, \
        m_threadId, \
        ##__VA_ARGS__); \
  } while (0)
#define DEB_HASTC(cursor, msg, ...) do {         \
    g_eventLogger->info \
      ( \
        "DEBUG Dbhast.cpp:%3d Table %04x, Fragment %04x, Root %x, Thread %u, Cursor(m_bucketIndex=%04x, m_entryIndex=%04x, m_hash=%08x, m_valid=%s %08x, m_valueptr=%p)" msg, \
        __LINE__, \
        m_dbg_tableId, \
        m_dbg_fragId, \
        m_dbg_inx, \
        m_threadId, \
        cursor.m_bucketIndex, \
        cursor.m_entryIndex, \
        cursor.m_hash, \
        (cursor.m_valid == Cursor::VALID ? "YES" : \
         (cursor.m_valid == Cursor::INVALID ? "NO!" : "???")),  \
        cursor.m_valid, \
        cursor.m_valueptr, \
        ##__VA_ARGS__); \
  } while (0)
void Hast::Root::debug_dump_root() const {
  DEB_HAST("Dumping Root %u: m_numberOfBuckets=%u, m_numberOfEntries=%llu", m_dbg_inx, m_numberOfBuckets, m_numberOfEntries);
  for(Uint32 i = 0; i < m_numberOfBuckets; i++) {
    debug_dump_bucket(m_buckets[i], i, "- ", "  - ");
  }
}
void Hast::Root::debug_dump_bucket(Hast::Bucket& bucket, Uint32 bucketIndex, const char* bucketPrefix, const char* entryPrefix) const {
  DEB_HAST("%sBucket %u (%u entries):", bucketPrefix, bucketIndex, bucket.m_numberOfEntries);
  for(Uint32 i = 0; i < bucket.m_numberOfEntries; i++) {
    DEB_HAST("%sEntry %u: m_hash=%08x, m_value=%016llx, &m_value=%p", entryPrefix, i, bucket.m_entries[i].m_hash, bucket.m_entries[i].m_value, &bucket.m_entries[i].m_value);
  }
}
#else
#define DEB_HAST(msg, ...) do { } while (0)
#define DEB_HASTC(cursor, msg, ...) do { } while (0)
void Hast::Root::debug_dump_root() const {}
void Hast::Root::debug_dump_bucket(Hast::Bucket& bucket, Uint32 bucketIndex, const char* bucketPrefix, const char* entryPrefix) const {}
#endif

/*
 * Public interface
 */

Hast::Root::Root():
  m_numberOfBuckets(0),
  m_buckets(nullptr),
  m_numberOfEntries(0),
  m_bptr(nullptr),
  m_threadId(0)
{}

void Hast::initialize(Block acc, Uint32 dbg_tableId, Uint32 dbg_fragId) {
  for (Uint32 inx = 0; inx < 4; inx++) {
    m_roots[inx].initialize(acc, dbg_tableId, dbg_fragId, inx);
  }
}
void Hast::Root::initialize(Block acc,
                            Uint32 dbg_tableId,
                            Uint32 dbg_fragId,
                            Uint32 dbg_inx) {
  ndbrequire(m_bptr == nullptr);
  ndbrequire(acc != nullptr);
  m_bptr = acc;
  jam();
  m_threadId = m_bptr->getThreadId();
  ndbrequire(m_threadId != 0);
  m_numberOfBuckets = 1;
  m_numberOfEntries = 0;
  m_buckets = (Bucket*)seize_mem(acc, sizeof(Bucket));
  m_buckets[0].m_numberOfEntries = 0;
  m_buckets[0].m_entries = nullptr;
  m_dbg_tableId = dbg_tableId;
  m_dbg_fragId = dbg_fragId;
  m_dbg_inx = dbg_inx;
  validateAll(acc);
  jamDebug();
  DEB_HAST("initialize() done");
}

void Hast::release(Block acc) {
  for (Uint32 inx = 0; inx < 4; inx++) {
    m_roots[inx].release(acc);
  }
}
void Hast::Root::release(Block acc) {
  jamDebug();
  DEB_HAST("release()");
  validateAll(acc);
  for (Uint32 i = 0; i < m_numberOfBuckets; i++) {
    Bucket& bucket = m_buckets[i];
    if (bucket.m_entries != nullptr) {
      release_mem(bucket.m_entries);
    }
  }
  release_mem(m_buckets);
  jamDebug();
  DEB_HAST("release() done");
}

bool Hast::isEntryCursor(CBlock acc, Cursor& cursor) const {
  return getRoot(cursor).isEntryCursor(acc, cursor);
}
bool Hast::Root::isEntryCursor(CBlock acc, Cursor& cursor) const {
  jamDebug();
  validateCursor(acc, cursor);
  validateAll(acc);
  return cursor.m_valueptr != nullptr;
}

bool Hast::isInsertCursor(CBlock acc, Cursor& cursor) const {
  return getRoot(cursor).isInsertCursor(acc, cursor);
}
bool Hast::Root::isInsertCursor(CBlock acc, Cursor& cursor) const {
  jamDebug();
  validateAll(acc);
  validateCursor(acc, cursor);
  return cursor.m_valueptr == nullptr;
}

Uint32 Hast::Root::computeBucketIndex(Uint32 hash, Uint32 numberOfBuckets) const {
  jamDebug();
  Uint32 usableHash = hash >> 2; // The 2 least significant bits are used to designate fragment mutex and select the root.
  Uint32 mask = 0x3fffffff;
  while ((mask & usableHash) >= numberOfBuckets) {
    mask >>= 1;
  }
  Uint32 bucketIndex = usableHash & mask;
  ndbassert(bucketIndex < numberOfBuckets);
  DEB_HAST("computeBucketIndex(hash=%08x, numberOfBuckets=%04x) -> %04x", hash, numberOfBuckets, bucketIndex);
  return bucketIndex;
}

Uint32 Hast::Root::siblingBucketIndex(Uint32 bucketIndex) const {
  jamDebug();
  Uint32 mask = 0xffffffff;
  while ((mask & bucketIndex) == bucketIndex) {
    mask >>= 1;
  }
  Uint32 siblingbucketIndex = bucketIndex & mask;
  DEB_HAST("siblingBucketIndex(bucketIndex=%04x) -> %04x", bucketIndex, siblingbucketIndex);
  return siblingbucketIndex;
}

Hast::Cursor Hast::getCursorFirst(Block acc, Uint32 hash) const {
  return getRoot(hash).getCursorFirst(acc, hash);
}
Hast::Cursor Hast::Root::getCursorFirst(Block acc, Uint32 hash) const {
  jamDebug();
  validateAll(acc);
  Cursor cursor = Cursor();
  cursor.m_hash = hash;
  cursor.m_bucketIndex = computeBucketIndex(hash, m_numberOfBuckets);
  cursor.m_entryIndex = 0;
  cursor.m_valueptr = nullptr;
  Bucket& bucket = m_buckets[cursor.m_bucketIndex];
  while (cursor.m_entryIndex < bucket.m_numberOfEntries) {
    if (bucket.m_entries[cursor.m_entryIndex].m_hash == hash) {
      cursor.m_valueptr = &bucket.m_entries[cursor.m_entryIndex].m_value;
      break;
    }
    cursor.m_entryIndex++;
  }
  cursor.m_valid = Hast::Cursor::VALID;
  DEB_HASTC(cursor, "<- getCursorFirst(hash=%08x)", hash);
  return cursor;
}

void Hast::cursorNext(Block acc, Cursor& cursor) const {
  getRoot(cursor).cursorNext(acc, cursor);
}
void Hast::Root::cursorNext(Block acc, Cursor& cursor) const {
  jamDebug();
  validateAll(acc);
  ndbrequire(isEntryCursor(acc, cursor));
  DEB_HASTC(cursor, ": Begin cursorNext()");
  Bucket& bucket = m_buckets[cursor.m_bucketIndex];
  cursor.m_entryIndex++;
  cursor.m_valueptr = nullptr;
  while (cursor.m_entryIndex < bucket.m_numberOfEntries) {
    if (bucket.m_entries[cursor.m_entryIndex].m_hash == cursor.m_hash) {
      cursor.m_valueptr = &bucket.m_entries[cursor.m_entryIndex].m_value;
      break;
    }
    cursor.m_entryIndex++;
  }
  DEB_HASTC(cursor, ": End cursorNext()");
}

Hast::Value Hast::getValue(CBlock acc, Cursor& cursor) const {
  return getRoot(cursor).getValue(acc, cursor);
}
Hast::Value Hast::Root::getValue(CBlock acc, Cursor& cursor) const {
  jamDebug();
  ndbassert(isEntryCursor(acc, cursor));
  Hast::Value value = *cursor.m_valueptr;
  DEB_HASTC(cursor, ": getValue() -> %016llx", value);
  return value;
}

void Hast::setValue(Block acc, Cursor& cursor, Value value) {
  getRoot(cursor).setValue(acc, cursor, value);
}
void Hast::Root::setValue(Block acc, Cursor& cursor, Value value) {
  jamDebug();
  ndbassert(isEntryCursor(acc, cursor));
  DEB_HASTC(cursor, ": setValue(value=%016llx)", value);
  *cursor.m_valueptr = value;
  DEB_HASTC(cursor, ": setValue() done");
}

void Hast::insertEntry(Block acc, Cursor& cursor, Value value) {
  getRoot(cursor).insertEntry(acc, cursor, value);
}
void Hast::Root::insertEntry(Block acc, Cursor& cursor, Value value) {
  jamDebug();
  validateAll(acc);
  ndbassert(isInsertCursor(acc, cursor));
  DEB_HASTC(cursor, ": insertEntry(value=%016llx)", value);
  Bucket& bucket = m_buckets[cursor.m_bucketIndex];
  insertEntryIntoBucket(acc, bucket, cursor.m_hash, value);
  DEB_HASTC(cursor, ": insertEntry(value=%016llx): after insertEntryIntoBucket", value);
  cursor.m_valueptr = &bucket.m_entries[cursor.m_entryIndex].m_value;
  DEB_HASTC(cursor, ": insertEntry(value=%016llx): after set m_valueptr", value);
  jamDebug();
  updateOperationRecords(bucket, cursor.m_bucketIndex);
  DEB_HASTC(cursor, ": insertEntry(value=%016llx): after updateOperationRecords", value);
  jamDebug();
  validateAll(acc);
  if(shouldExpand()) {
    jamDebug();
    expand(acc);
    DEB_HASTC(cursor, ": insertEntry(value=%016llx): after expand", value);
  }
}

void Hast::deleteEntry(Block acc, Cursor& cursor) {
  getRoot(cursor).deleteEntry(acc, cursor);
}
void Hast::Root::deleteEntry(Block acc, Cursor& cursor) {
  jamDebug();
  validateAll(acc);
  ndbassert(isEntryCursor(acc, cursor));
  DEB_HASTC(cursor, ": deleteEntry()");
  Uint32 bucketIndex = cursor.m_bucketIndex;
  Bucket& bucket = m_buckets[bucketIndex];
  Entry* newEntries = nullptr;
  if(bucket.m_numberOfEntries > 1) {
    newEntries = (Entry*)seize_mem(acc, (bucket.m_numberOfEntries - 1) * sizeof(Entry));
    if(cursor.m_entryIndex > 0) {
      memcpy(newEntries, bucket.m_entries, cursor.m_entryIndex * sizeof(Entry));
    }
    if(cursor.m_entryIndex < bucket.m_numberOfEntries - 1) {
      memcpy(newEntries + cursor.m_entryIndex, bucket.m_entries + cursor.m_entryIndex + 1,
             (bucket.m_numberOfEntries - cursor.m_entryIndex - 1) * sizeof(Entry));
    }
  }
  if (bucket.m_entries != nullptr) {
    release_mem(bucket.m_entries);
  }
  bucket.m_entries = newEntries;
  bucket.m_numberOfEntries--;
  m_numberOfEntries--;
  cursor.m_valueptr = nullptr;
  cursor.m_valid = Hast::Cursor::INVALID;
  updateOperationRecords(bucket, bucketIndex);
  DEB_HASTC(cursor, ": deleteEntry() after updateOperationRecords");
  jamDebug();
  validateAll(acc);
  if(shouldShrink()) {
    jamDebug();
    shrink(acc);
    DEB_HASTC(cursor, ": deleteEntry() after shrink");
  }
}

/*
 * Internals
 */

void Hast::Root::insertEntryIntoBucket(Block acc, Bucket& bucket, Uint32 hash, Value value) {
  jamDebug();
  validateAll(acc);
  DEB_HAST("insertEntryIntoBucket(bucket=%p, hash=%08x, value=%016llx)", &bucket, hash, value);
  Entry* newEntries = (Entry*)seize_mem(acc, (bucket.m_numberOfEntries + 1) * sizeof(Entry));
  if (bucket.m_numberOfEntries > 0) {
    jamDebug();
    memcpy(newEntries, bucket.m_entries, bucket.m_numberOfEntries * sizeof(Entry));
    release_mem(bucket.m_entries);
  }
  newEntries[bucket.m_numberOfEntries].m_hash = hash;
  newEntries[bucket.m_numberOfEntries].m_value = value;
  bucket.m_entries = newEntries;
  bucket.m_numberOfEntries++;
  m_numberOfEntries++;
  validateAll(acc);
  DEB_HAST("After insertEntryIntoBucket, entry hash=%08x, value=%016llx, valueptr=%p", newEntries[bucket.m_numberOfEntries - 1].m_hash, newEntries[bucket.m_numberOfEntries - 1].m_value, &newEntries[bucket.m_numberOfEntries - 1].m_value);
}

// todoas do not crash on OOM. Also, we must always be able to delete.
void* Hast::Root::seize_mem(Block acc, size_t size) {
  validateB(acc);
  // todoas Do I really need getThreadId() here, or can I use 0?
  void* ret = lc_ndbd_pool_malloc(size, RG_DATAMEM, acc->getThreadId(), false);
  ndbrequire(ret != nullptr);
  return ret;
}

void Hast::Root::release_mem(void *ptr) {
  ndbrequire(ptr != nullptr);
  lc_ndbd_pool_free(ptr);
}

bool Hast::Root::shouldExpand() const {
  return m_numberOfBuckets < MAX_NUMBER_OF_BUCKETS &&
                           m_numberOfEntries > Uint64(m_numberOfBuckets) * HIGH_NUMBER_OF_ENTRIES_PER_BUCKET;
}
bool Hast::Root::shouldShrink() const {
  return m_numberOfBuckets > 1 &&
    m_numberOfEntries < Uint64(m_numberOfBuckets) * LOW_NUMBER_OF_ENTRIES_PER_BUCKET;
}

void Hast::Root::expand(Block acc) {
  jamDebug();
  validateAll(acc);
  ndbassert(shouldExpand());
  DEB_HAST("Begin expand(), m_numberOfBuckets=%u, m_numberOfEntries=%llu", m_numberOfBuckets, m_numberOfEntries);
  Bucket* newBuckets = (Bucket*)seize_mem(acc, (m_numberOfBuckets + 1) * sizeof(Bucket));
  memcpy(newBuckets, m_buckets, m_numberOfBuckets * sizeof(Bucket));
  release_mem(m_buckets);
  m_buckets = newBuckets;
  Uint32 newBucketIndex = m_numberOfBuckets;
  Uint32 oldBucketIndex = siblingBucketIndex(newBucketIndex);
  m_numberOfBuckets++;
  Bucket splitBucket = m_buckets[oldBucketIndex];
  Uint32 m_entriesToMove = splitBucket.m_numberOfEntries;
  m_buckets[oldBucketIndex].m_numberOfEntries = 0;
  m_buckets[oldBucketIndex].m_entries = nullptr;
  m_buckets[newBucketIndex].m_numberOfEntries = 0;
  m_buckets[newBucketIndex].m_entries = nullptr;
  m_numberOfEntries -= m_entriesToMove;
  for (Uint32 i = 0; i < splitBucket.m_numberOfEntries; i++) {
    Entry &entry = splitBucket.m_entries[i];
    Uint32 bucketIndex = computeBucketIndex(entry.m_hash, m_numberOfBuckets);
    ndbassert(bucketIndex == oldBucketIndex || bucketIndex == newBucketIndex);
    insertEntryIntoBucket(acc, m_buckets[bucketIndex], entry.m_hash, entry.m_value);
  }
  if (splitBucket.m_entries != nullptr) {
    release_mem(splitBucket.m_entries);
  }
  splitBucket.m_numberOfEntries = 0;
  splitBucket.m_entries = nullptr;
  jamDebug();
  updateOperationRecords(m_buckets[oldBucketIndex], oldBucketIndex);
  jamDebug();
  updateOperationRecords(m_buckets[newBucketIndex], newBucketIndex);
  validateBucket(acc, splitBucket, oldBucketIndex);
  validateBucket(acc, m_buckets[oldBucketIndex], oldBucketIndex);
  validateBucket(acc, m_buckets[newBucketIndex], newBucketIndex);
  DEB_HAST("End expand(), m_numberOfBuckets=%u, m_numberOfEntries=%llu", m_numberOfBuckets, m_numberOfEntries);
  ndbassert(m_buckets[oldBucketIndex].m_numberOfEntries + m_buckets[newBucketIndex].m_numberOfEntries == m_entriesToMove);
  jamDebug();
  validateAll(acc);
}

void Hast::Root::shrink(Block acc) {
  jamDebug();
  validateAll(acc);
  ndbassert(shouldShrink());
  DEB_HAST("Begin shrink(), m_numberOfBuckets=%u, m_numberOfEntries=%llu", m_numberOfBuckets, m_numberOfEntries);
  Bucket* newBuckets = (Bucket*)seize_mem(acc, (m_numberOfBuckets - 1) * sizeof(Bucket));
  memcpy(newBuckets, m_buckets, (m_numberOfBuckets - 1) * sizeof(Bucket));
  Uint32 oldBucketIndex = m_numberOfBuckets - 1;
  Bucket oldBucket = m_buckets[oldBucketIndex];
  m_numberOfBuckets--;
  Uint32 newBucketIndex = siblingBucketIndex(oldBucketIndex);
  release_mem(m_buckets);
  m_buckets = newBuckets;
  Bucket& newBucket = m_buckets[newBucketIndex];
  if(oldBucket.m_numberOfEntries > 0) {
    Entry* newEntries = (Entry*)seize_mem(acc, (oldBucket.m_numberOfEntries + newBucket.m_numberOfEntries) * sizeof(Entry));
    if(newBucket.m_numberOfEntries > 0) {
      memcpy(newEntries, newBucket.m_entries, newBucket.m_numberOfEntries * sizeof(Entry));
      release_mem(newBucket.m_entries);
    }
    memcpy(newEntries + newBucket.m_numberOfEntries, oldBucket.m_entries, oldBucket.m_numberOfEntries * sizeof(Entry));
    newBucket.m_entries = newEntries;
    newBucket.m_numberOfEntries += oldBucket.m_numberOfEntries;
    release_mem(oldBucket.m_entries);
    oldBucket.m_entries = nullptr;
    oldBucket.m_numberOfEntries = 0;
  }
  jamDebug();
  updateOperationRecords(newBucket, newBucketIndex);
  validateBucket(acc, oldBucket, oldBucketIndex);
  validateBucket(acc, newBucket, newBucketIndex);
  DEB_HAST("End shrink(), m_numberOfBuckets=%u, m_numberOfEntries=%llu", m_numberOfBuckets, m_numberOfEntries);
  jamDebug();
  validateAll(acc);
}

void Hast::Root::updateOperationRecords(Bucket& bucket, Uint32 bucketIndex) {
  jamDebug();
  DEB_HAST("Begin updateOperationRecords(bucketIndex=%u): m_numberOfBuckets=%u, m_numberOfEntries=%llu", bucketIndex, m_numberOfBuckets, m_numberOfEntries);
  debug_dump_bucket(bucket, bucketIndex, "Before update Operation Records: ", "- ");
  for (Uint32 i = 0; i < bucket.m_numberOfEntries; i++) {
    jamDebug();
    Entry& entry = bucket.m_entries[i];
    DEB_HAST("updateOperationRecords(bucketIndex=%u): m_entries[%u]=Entry(m_hash=%08x, m_value=%016llx)", bucketIndex, i, entry.m_hash, entry.m_value);
    Uint32 locked = entry.m_value & 1;
    if(locked) {
      jamDebug();
      Uint32 operation_rec_i = (entry.m_value >> 1) & 0x7fffffff;
      Dbacc::Operationrec* oprec = m_bptr->get_operation_ptr(operation_rec_i);
      Cursor &cursor = oprec->m_hastCursor;
      jamDebug();
      DEB_HASTC(cursor, ", updateOperationRecords(bucketIndex=%u): m_entries[%u], operation_rec_i=%08x, will update cursor", bucketIndex, i, operation_rec_i);
      ndbassert(cursor.m_valueptr != nullptr); // Valid or invalid entry cursor
      ndbassert(cursor.m_hash == entry.m_hash);
      jamDebug();
      cursor.m_bucketIndex = bucketIndex;
      cursor.m_entryIndex = i;
      cursor.m_valueptr = &entry.m_value;
      DEB_HASTC(cursor, ", updateOperationRecords(bucketIndex=%u): m_entries[%u], operation_rec_i=%08x, after cursor update", bucketIndex, i, operation_rec_i);
    }
  }
  debug_dump_bucket(bucket, bucketIndex, "After update Operation Records: ", "- ");
  DEB_HAST("End updateOperationRecords(bucketIndex=%u)", bucketIndex);
}

/*
 * Validation
 */

void Hast::Root::validateAll(CBlock acc) const {
  validateHastRoot(acc);
  Uint64 totalNumberOfEntries = 0;
  for (Uint32 i = 0; i < m_numberOfBuckets; i++) {
    validateBucket(acc, m_buckets[i], i);
    totalNumberOfEntries += m_buckets[i].m_numberOfEntries;
  }
  if (totalNumberOfEntries != m_numberOfEntries) {
    DEB_HAST("validateAll(): FAILED CHECK: totalNumberOfEntries=%llu != m_numberOfEntries=%llu", totalNumberOfEntries, m_numberOfEntries);
  }
  ndbassert(totalNumberOfEntries == m_numberOfEntries);
}

void Hast::Root::validateHastRoot(CBlock acc) const {
  validateB(acc);
  ndbassert(m_numberOfBuckets >= 1);
  ndbassert(m_buckets != nullptr);
}

void Hast::Root::validateB(CBlock acc) const {
  ndbassert(acc == m_bptr);
  ndbassert(m_bptr != nullptr);
  ndbassert(m_threadId == acc->getThreadId());
  // todoas: validate m_bptr->fragrecptr
  //ndbassert(m_bptr->c_fragment_pool.getPtr(m_bptr->fragrecptr));
  //ndbassert(m_bptr->fragrecptr.p != nullptr);
  //ndbassert(Magic::match(m_bptr->fragrecptr.p->m_magic, Dbacc::Fragmentrec::TYPE_ID));
}

void Hast::Root::validateValue(CBlock acc, Value value) const {
  if ((value & 1) == 0) {
  ndbassert((value & 0x000000000000c000ULL) == 0);
  }
}

void Hast::Root::validateCursor(CBlock acc, Cursor& cursor) const {
  jamDebug();
  ndbassert(cursor.m_valid == Hast::Cursor::VALID);
  ndbassert(cursor.m_bucketIndex < m_numberOfBuckets);
  Bucket& bucket = m_buckets[cursor.m_bucketIndex];
  Uint32 tmp_bucket_indx = computeBucketIndex(cursor.m_hash, m_numberOfBuckets);
  if(tmp_bucket_indx != cursor.m_bucketIndex) {
    DEB_HASTC(cursor, ": FAILED CHECK in validateCursor(): m_numberOfBuckets=%u, bucket.m_numberOfEntries=%u", m_numberOfBuckets, bucket.m_numberOfEntries);
    debug_dump_root();
  }
  ndbassert(tmp_bucket_indx == cursor.m_bucketIndex);
  if(cursor.m_valueptr != nullptr) {
    ndbassert(cursor.m_entryIndex < bucket.m_numberOfEntries);
    Entry& entry = bucket.m_entries[cursor.m_entryIndex];
    ndbassert(cursor.m_hash == entry.m_hash);
    ndbassert(cursor.m_valueptr == &entry.m_value);
    validateValue(acc, entry.m_value);
  }
  else {
    ndbassert(cursor.m_entryIndex == bucket.m_numberOfEntries);
  }
}

void Hast::Root::validateBucket(CBlock acc, Bucket& bucket, Uint32 bucketIndex) const {
  if(bucket.m_numberOfEntries == 0) {
    ndbrequire(bucket.m_entries == nullptr);
    return;
  }
  ndbrequire(bucket.m_entries != nullptr);
  #if defined(VM_TRACE) || defined(ERROR_INSERT)
  for (Uint32 i = 0; i < bucket.m_numberOfEntries; i++) {
    Entry& entry = bucket.m_entries[i];
    Uint32 hash = entry.m_hash;
    ndbassert(computeBucketIndex(hash, m_numberOfBuckets) == bucketIndex);
    validateValue(acc, entry.m_value);
  }
  #endif
}

void Hast::Root::progError(int line, int err_code, const char* extra, const char* check) const {
  if(m_bptr != nullptr) {
    m_bptr->progError(line, err_code, extra, check);
    return;
  }
  globalData.theStopFlag = true;
  mb();
  jamNoBlock();
  /* Add line number and failed expression to block name */
  char buf[500];
  /*Add the check to the log message only if default value of ""
    is over-written. */
  if(native_strcasecmp(check,"") == 0)
    BaseString::snprintf(
      &buf[0], 100,
      "b/Dbhast.cpp (Line: %d)",
      line);
  else
    BaseString::snprintf(
      &buf[0], sizeof(buf),
      "b/Dbhast.cpp (Line: %d) Check %.400s failed",
      line, check);
  ErrorReporter::handleError(err_code, extra, buf);
}

EmulatedJamBuffer* Hast::Root::jamBuffer() const {
  ndbassert(m_bptr != nullptr);
  EmulatedJamBuffer* jamBuffer = m_bptr->jamBuffer();
  ndbassert(jamBuffer != nullptr);
  return jamBuffer;
}

// Hast private

Hast::Root& Hast::getRoot(Hash hash) {
  return m_roots[hash & 3];
}

const Hast::Root& Hast::getRoot(Hash hash) const {
  return m_roots[hash & 3];
}

Hast::Root& Hast::getRoot(Cursor& cursor) {
  return m_roots[cursor.m_hash & 3];
}

const Hast::Root& Hast::getRoot(Cursor& cursor) const {
  return m_roots[cursor.m_hash & 3];
}

#undef JAM_FILE_ID
