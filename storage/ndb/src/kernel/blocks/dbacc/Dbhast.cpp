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
#define hastJamDebug() jamBlock(acc)
#define DEB_HAST(msg, ...) do { \
    g_eventLogger->info \
      ( \
        "DEBUG Dbhast.cpp:%3d Table %04x, Fragment %04x, Root %x, Thread %u: " msg, \
        __LINE__, \
        m_dbg_tableId, \
        m_dbg_fragId, \
        m_dbg_inx, \
        acc->getThreadId(), \
        ##__VA_ARGS__); \
  } while (0)
#define DEB_HASTC(msg, ...) do { \
    g_eventLogger->info \
      ( \
        "DEBUG Dbhast.cpp:%3d Table %04x, Fragment %04x, Root %x, Thread %u, Cursor(m_bucketIndex=%04x, m_entryIndex=%04x, m_hash=%08x, m_valid=%s %08x, m_valueptr=%p)" msg, \
        __LINE__, \
        m_dbg_tableId, \
        m_dbg_fragId, \
        m_dbg_inx, \
        acc->getThreadId(), \
        m_bucketIndex, \
        m_entryIndex, \
        m_hash, \
        (m_valid == Cursor::VALID ? "YES" : \
         (m_valid == Cursor::INVALID ? "NO!" : "???")),  \
        m_valid, \
        m_valueptr, \
        ##__VA_ARGS__); \
  } while (0)
void Hast::Root::debug_dump_root(CBlock acc) const {
  DEB_HAST("Dumping Root %u: m_numberOfBuckets=%u, m_numberOfEntries=%llu", m_dbg_inx, m_numberOfBuckets, m_numberOfEntries);
  for(Uint32 i = 0; i < m_numberOfBuckets; i++) {
    debug_dump_bucket(acc, m_buckets[i], i, "- ", "  - ");
  }
}
void Hast::Root::debug_dump_bucket(CBlock acc, Hast::Bucket& bucket, Uint32 bucketIndex, const char* bucketPrefix, const char* entryPrefix) const {
  DEB_HAST("%sBucket %u (%u entries):", bucketPrefix, bucketIndex, bucket.m_numberOfEntries);
  for(Uint32 i = 0; i < bucket.m_numberOfEntries; i++) {
    DEB_HAST("%sEntry %u: m_hash=%08x, m_value=%016llx, &m_value=%p", entryPrefix, i, bucket.m_entries[i].m_hash, bucket.m_entries[i].m_value, &bucket.m_entries[i].m_value);
  }
}
#else
#define hastJamDebug() do { } while (0)
#define DEB_HAST(msg, ...) do { } while (0)
#define DEB_HASTC(msg, ...) do { } while (0)
void Hast::Root::debug_dump_root(CBlock acc) const {}
void Hast::Root::debug_dump_bucket(CBlock acc, Hast::Bucket& bucket, Uint32 bucketIndex, const char* bucketPrefix, const char* entryPrefix) const {}
#endif

/*
 * Public interface
 */

Hast::Root::Root():
  m_numberOfBuckets(0),
  m_buckets(nullptr),
  m_numberOfEntries(0)
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
  ndbrequire(acc != nullptr);
  jamBlock(acc);
  m_numberOfBuckets = 1;
  m_numberOfEntries = 0;
  m_buckets = (Bucket*)seize_mem(acc, sizeof(Bucket));
  m_buckets[0].m_numberOfEntries = 0;
  m_buckets[0].m_entries = nullptr;
  m_dbg_tableId = dbg_tableId;
  m_dbg_fragId = dbg_fragId;
  m_dbg_inx = dbg_inx;
  hastJamDebug();
  validateRoot(acc);
  DEB_HAST("initialize() done");
}

void Hast::release(Block acc) {
  for (Uint32 inx = 0; inx < 4; inx++) {
    m_roots[inx].release(acc);
  }
}
void Hast::Root::release(Block acc) {
  hastJamDebug();
  DEB_HAST("release()");
  validateRoot(acc);
  for (Uint32 i = 0; i < m_numberOfBuckets; i++) {
    Bucket& bucket = m_buckets[i];
    if (bucket.m_entries != nullptr) {
      release_mem(bucket.m_entries);
    }
  }
  release_mem(m_buckets);
  hastJamDebug();
  DEB_HAST("release() done");
}

bool Hast::Cursor::isEntryCursor(CBlock acc) const {
  hastJamDebug();
  m_root->validateRoot(acc);
  validateCursor(acc);
  return m_valueptr != nullptr;
}

bool Hast::Cursor::isInsertCursor(CBlock acc) const {
  hastJamDebug();
  validateCursor(acc);
  return m_valueptr == nullptr;
}

Uint32 Hast::Root::computeBucketIndex(CBlock acc, Uint32 hash, Uint32 numberOfBuckets) const {
  hastJamDebug();
  // The 2 least significant bits are used to designate fragment mutex and
  // select the root.
  Uint32 usableHash = hash >> 2;
  Uint32 expectedInx = hash & 0x3;
  ndbassert(expectedInx == m_dbg_inx);
  Uint32 mask = 0x3fffffff;
  while ((mask & usableHash) >= numberOfBuckets) {
    mask >>= 1;
  }
  Uint32 bucketIndex = usableHash & mask;
  ndbassert(bucketIndex < numberOfBuckets);
  return bucketIndex;
}

Uint32 Hast::Root::siblingBucketIndex(Block acc, Uint32 bucketIndex) const {
  hastJamDebug();
  Uint32 mask = 0xffffffff;
  while ((mask & bucketIndex) == bucketIndex) {
    mask >>= 1;
  }
  Uint32 siblingbucketIndex = bucketIndex & mask;
  DEB_HAST("siblingBucketIndex(bucketIndex=%04x) -> %04x", bucketIndex, siblingbucketIndex);
  return siblingbucketIndex;
}

Hast::Cursor Hast::getCursorFirst(Block acc, Uint32 hash) {
  return getRoot(hash).getCursorFirst(acc, hash);
}
Hast::Cursor Hast::Root::getCursorFirst(Block acc, Uint32 hash) {
  hastJamDebug();
  validateRoot(acc);
  Cursor cursor = Cursor();
  cursor.m_hash = hash;
  cursor.m_root = this;
  cursor.m_bucketIndex = computeBucketIndex(acc, hash, m_numberOfBuckets);
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
  DEB_HASTC("<- getCursorFirst(hash=%08x)", hash);
  return cursor;
}

void Hast::Cursor::next(CBlock acc) {
  hastJamDebug();
  m_root->validateRoot(acc);
  ndbrequire(isEntryCursor(acc));
  DEB_HASTC(": Begin cursorNext()");
  Bucket& bucket = m_root->m_buckets[m_bucketIndex];
  m_entryIndex++;
  m_valueptr = nullptr;
  while (m_entryIndex < bucket.m_numberOfEntries) {
    if (bucket.m_entries[m_entryIndex].m_hash == m_hash) {
      m_valueptr = &bucket.m_entries[m_entryIndex].m_value;
      break;
    }
    m_entryIndex++;
  }
  DEB_HASTC(": End cursorNext()");
}

bool Hast::Cursor::getLocked(CBlock acc) const {
  hastJamDebug();
  validateLockedCursor(acc);
  return m_valueptr->m_locked;
}
Uint32 Hast::Cursor::getOpptriWhenLocked(CBlock acc) const {
  hastJamDebug();
  validateLockedCursor(acc);
  return m_valueptr->m_opptri;
}
Uint16 Hast::Cursor::getPageidxWhenUnlocked(CBlock acc) const {
  hastJamDebug();
  validateUnlockedCursor(acc);
  return m_valueptr->m_pageidx;
}
Uint16 Hast::Cursor::getPageidxWhenLocked(CBlock acc) const {
  hastJamDebug();
  validateLockedCursor(acc);
  return m_valueptr->m_pageidx;
}
Uint32 Hast::Cursor::getPagenoWhenLocked(CBlock acc) const {
  hastJamDebug();
  validateLockedCursor(acc);
  return m_valueptr->m_pageno;
}
Uint32 Hast::Cursor::getPagenoWhenUnlocked(CBlock acc) const {
  hastJamDebug();
  validateUnlockedCursor(acc);
  return m_valueptr->m_pageno;
}
void Hast::Cursor::setLockedOpptriWhenUnlocked(CBlock acc, Uint32 opptri) {
  hastJamDebug();
  validateUnlockedCursor(acc);
  m_valueptr->m_locked = true;
  m_dbg_value.m_locked = true;
  m_valueptr->m_opptri = opptri;
  m_dbg_value.m_opptri = opptri;
  m_dbg_value.m_pageidx = m_valueptr->m_pageidx;
  m_dbg_value.m_pageno = m_valueptr->m_pageno;
  validateLockedCursor(acc);
}
void Hast::Cursor::setPagenoWhenUnlocked(CBlock acc, Uint32 pageno) {
  hastJamDebug();
  validateUnlockedCursor(acc);
  m_valueptr->m_pageno = pageno;
  validateUnlockedCursor(acc);
}
void Hast::Cursor::setUnlocked(CBlock acc) {
  hastJamDebug();
  validateLockedCursor(acc);
  m_valueptr->m_locked = false;
  m_dbg_value.m_locked = false;
  m_valueptr->m_opptri = 0;
  m_dbg_value.m_opptri = 0;
  m_dbg_value.m_pageidx = 0;
  m_dbg_value.m_pageno = 0;
  validateUnlockedCursor(acc);
}
void Hast::Cursor::setPagenoWhenLocked(CBlock acc, Uint32 pageno) {
  hastJamDebug();
  validateLockedCursor(acc);
  m_valueptr->m_pageno = pageno;
  m_dbg_value.m_pageno = pageno;
  validateLockedCursor(acc);
}
void Hast::Cursor::setPageidxPagenoWhenLocked(CBlock acc, Uint16 pageidx, Uint32 pageno) {
  hastJamDebug();
  validateLockedCursor(acc);
  m_valueptr->m_pageidx = pageidx;
  m_dbg_value.m_pageidx = pageidx;
  m_valueptr->m_pageno = pageno;
  m_dbg_value.m_pageno = pageno;
  validateLockedCursor(acc);
}
void Hast::Cursor::setOpptriWhenLocked(CBlock acc, Uint32 opptri) {
  hastJamDebug();
  validateLockedCursor(acc);
  m_valueptr->m_opptri = opptri;
  m_dbg_value.m_opptri = opptri;
  validateLockedCursor(acc);
}
//todoas remove unnecessary get/set functions

void Hast::Cursor::insertLockedOpptriPagenoPageidx(Block acc,
                                                   Uint32 opptri,
                                                   Uint32 pageno,
                                                   Uint32 pageidx) {
  hastJamDebug();
  m_root->validateRoot(acc);
  ndbassert(isInsertCursor(acc));
  DEB_HASTC(": insertEntry(opptri=%08x, pageno=%08x, pageidx=%04x)", opptri, pageno, pageidx);
  Bucket& bucket = m_root->m_buckets[m_bucketIndex];
  Value value;
  value.m_locked = true;
  value.m_opptri = opptri;
  value.m_pageno = pageno;
  value.m_pageidx = pageidx;
  Hast::validateValue(acc, value);
  m_root->insertEntryIntoBucket(acc, bucket, m_hash, value);
  DEB_HASTC(": insertEntry(opptri=%08x, pageno=%08x, pageidx=%04x): after insertEntryIntoBucket", opptri, pageno, pageidx);
  m_valueptr = &bucket.m_entries[m_entryIndex].m_value;
  hastJamDebug();
  m_root->updateOperationRecords(acc, bucket, m_bucketIndex);
  DEB_HASTC(": insertEntry(value=%016llx): after updateOperationRecords", value);
  hastJamDebug();
  m_root->validateRoot(acc);
  if(m_root->shouldExpand()) {
    hastJamDebug();
    m_root->expand(acc);
    DEB_HASTC(": insertEntry(value=%016llx): after expand", value);
  }
}

void Hast::Cursor::deleteEntry(Block acc) {
  hastJamDebug();
  m_root->validateRoot(acc);
  ndbassert(isEntryCursor(acc));
  DEB_HASTC(": deleteEntry()");
  Bucket& bucket = m_root->m_buckets[m_bucketIndex];
  Entry* newEntries = nullptr;
  if(bucket.m_numberOfEntries > 1) {
    newEntries = (Entry*)Hast::seize_mem(acc, (bucket.m_numberOfEntries - 1) * sizeof(Entry));
    if(m_entryIndex > 0) {
      memcpy(newEntries, bucket.m_entries, m_entryIndex * sizeof(Entry));
    }
    if(m_entryIndex < bucket.m_numberOfEntries - 1) {
      memcpy(newEntries + m_entryIndex, bucket.m_entries + m_entryIndex + 1,
             (bucket.m_numberOfEntries - m_entryIndex - 1) * sizeof(Entry));
    }
  }
  if (bucket.m_entries != nullptr) {
    release_mem(bucket.m_entries);
  }
  bucket.m_entries = newEntries;
  bucket.m_numberOfEntries--;
  m_root->m_numberOfEntries--;
  m_valueptr = nullptr;
  m_valid = Hast::Cursor::INVALID;
  m_root->updateOperationRecords(acc, bucket, m_bucketIndex);
  DEB_HASTC(": deleteEntry() after updateOperationRecords");
  hastJamDebug();
  m_root->validateRoot(acc);
  if(m_root->shouldShrink()) {
    hastJamDebug();
    m_root->shrink(acc);
    DEB_HASTC(": deleteEntry() after shrink");
  }
}

/*
 * Internals
 */

void Hast::Root::insertEntryIntoBucket(CBlock acc, Bucket& bucket, Uint32 hash, Value value) {
  hastJamDebug();
  validateRoot(acc);
  DEB_HAST("insertEntryIntoBucket(bucket=%p, hash=%08x, value=%016llx)", &bucket, hash, value);
  Entry* newEntries = (Entry*)seize_mem(acc, (bucket.m_numberOfEntries + 1) * sizeof(Entry));
  if (bucket.m_numberOfEntries > 0) {
    hastJamDebug();
    memcpy(newEntries, bucket.m_entries, bucket.m_numberOfEntries * sizeof(Entry));
    release_mem(bucket.m_entries);
  }
  newEntries[bucket.m_numberOfEntries].m_hash = hash;
  newEntries[bucket.m_numberOfEntries].m_value = value;
  bucket.m_entries = newEntries;
  bucket.m_numberOfEntries++;
  m_numberOfEntries++;
  hastJamDebug();
  validateRoot(acc);
  DEB_HAST("After insertEntryIntoBucket, entry hash=%08x, value=%016llx, valueptr=%p", newEntries[bucket.m_numberOfEntries - 1].m_hash, newEntries[bucket.m_numberOfEntries - 1].m_value, &newEntries[bucket.m_numberOfEntries - 1].m_value);
}

// todoas do not crash on OOM. Also, we must always be able to delete.
void* Hast::seize_mem(CBlock acc, size_t size) {
  hastJamDebug();
  // todoas Do I really need getThreadId() here, or can I use 0?
  void* ret = lc_ndbd_pool_malloc(size, RG_DATAMEM, acc->getThreadId(), false);
  ndbrequire(ret != nullptr);
  return ret;
}

void Hast::release_mem(void *ptr) {
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
  hastJamDebug();
  validateRoot(acc);
  ndbassert(shouldExpand());
  DEB_HAST("Begin expand(), m_numberOfBuckets=%u, m_numberOfEntries=%llu", m_numberOfBuckets, m_numberOfEntries);
  Bucket* newBuckets = (Bucket*)seize_mem(acc, (m_numberOfBuckets + 1) * sizeof(Bucket));
  memcpy(newBuckets, m_buckets, m_numberOfBuckets * sizeof(Bucket));
  release_mem(m_buckets);
  m_buckets = newBuckets;
  Uint32 newBucketIndex = m_numberOfBuckets;
  Uint32 oldBucketIndex = siblingBucketIndex(acc, newBucketIndex);
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
    Uint32 bucketIndex = computeBucketIndex(acc, entry.m_hash, m_numberOfBuckets);
    ndbassert(bucketIndex == oldBucketIndex || bucketIndex == newBucketIndex);
    insertEntryIntoBucket(acc, m_buckets[bucketIndex], entry.m_hash, entry.m_value);
  }
  if (splitBucket.m_entries != nullptr) {
    release_mem(splitBucket.m_entries);
  }
  splitBucket.m_numberOfEntries = 0;
  splitBucket.m_entries = nullptr;
  hastJamDebug();
  updateOperationRecords(acc, m_buckets[oldBucketIndex], oldBucketIndex);
  hastJamDebug();
  updateOperationRecords(acc, m_buckets[newBucketIndex], newBucketIndex);
  validateBucket(acc, splitBucket, oldBucketIndex);
  validateBucket(acc, m_buckets[oldBucketIndex], oldBucketIndex);
  validateBucket(acc, m_buckets[newBucketIndex], newBucketIndex);
  DEB_HAST("End expand(), m_numberOfBuckets=%u, m_numberOfEntries=%llu", m_numberOfBuckets, m_numberOfEntries);
  ndbassert(m_buckets[oldBucketIndex].m_numberOfEntries + m_buckets[newBucketIndex].m_numberOfEntries == m_entriesToMove);
  hastJamDebug();
  validateRoot(acc);
}

void Hast::Root::shrink(Block acc) {
  hastJamDebug();
  validateRoot(acc);
  ndbassert(shouldShrink());
  DEB_HAST("Begin shrink(), m_numberOfBuckets=%u, m_numberOfEntries=%llu", m_numberOfBuckets, m_numberOfEntries);
  Bucket* newBuckets = (Bucket*)seize_mem(acc, (m_numberOfBuckets - 1) * sizeof(Bucket));
  memcpy(newBuckets, m_buckets, (m_numberOfBuckets - 1) * sizeof(Bucket));
  Uint32 oldBucketIndex = m_numberOfBuckets - 1;
  Bucket oldBucket = m_buckets[oldBucketIndex];
  m_numberOfBuckets--;
  Uint32 newBucketIndex = siblingBucketIndex(acc, oldBucketIndex);
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
  hastJamDebug();
  updateOperationRecords(acc, newBucket, newBucketIndex);
  validateBucket(acc, oldBucket, oldBucketIndex);
  validateBucket(acc, newBucket, newBucketIndex);
  DEB_HAST("End shrink(), m_numberOfBuckets=%u, m_numberOfEntries=%llu", m_numberOfBuckets, m_numberOfEntries);
  hastJamDebug();
  validateRoot(acc);
}

void Hast::Root::updateOperationRecords(Block acc, Bucket& bucket, Uint32 bucketIndex) {
  hastJamDebug();
  DEB_HAST("Begin updateOperationRecords(bucketIndex=%u): m_numberOfBuckets=%u, m_numberOfEntries=%llu", bucketIndex, m_numberOfBuckets, m_numberOfEntries);
  debug_dump_bucket(acc, bucket, bucketIndex, "Before update Operation Records: ", "- ");
  for (Uint32 i = 0; i < bucket.m_numberOfEntries; i++) {
    hastJamDebug();
    Entry& entry = bucket.m_entries[i];
    DEB_HAST("updateOperationRecords(bucketIndex=%u): m_entries[%u]=Entry(m_hash=%08x, m_value=%016llx)", bucketIndex, i, entry.m_hash, entry.m_value);
    if(entry.m_value.m_locked) {
      hastJamDebug();
      Uint32 operation_rec_i = entry.m_value.m_opptri;
      Dbacc::Operationrec* oprec = acc->get_operation_ptr(operation_rec_i);
      Cursor &cursor = oprec->m_hastCursor;
      hastJamDebug();
      DEB_HASTC(", updateOperationRecords(bucketIndex=%u): m_entries[%u], operation_rec_i=%08x, will update cursor", bucketIndex, i, operation_rec_i);
      ndbassert(cursor.m_valueptr != nullptr); // Entry cursor or invalid cursor that otherwise looks like an entry cursor
      ndbassert(cursor.m_hash == entry.m_hash);
      hastJamDebug();
      cursor.m_bucketIndex = bucketIndex;
      cursor.m_entryIndex = i;
      cursor.m_valueptr = &entry.m_value;
      DEB_HASTC(", updateOperationRecords(bucketIndex=%u): m_entries[%u], operation_rec_i=%08x, after cursor update", bucketIndex, i, operation_rec_i);
    }
  }
  debug_dump_bucket(acc, bucket, bucketIndex, "After update Operation Records: ", "- ");
  DEB_HAST("End updateOperationRecords(bucketIndex=%u)", bucketIndex);
}

/*
 * Validation
 */

void Hast::Root::validateRoot(CBlock acc) const {
  validateB(acc);
  ndbassert(m_numberOfBuckets >= 1);
  ndbassert(m_buckets != nullptr);
  Uint64 totalNumberOfEntries = 0;
  for (Uint32 i = 0; i < m_numberOfBuckets; i++) {
    validateBucket(acc, m_buckets[i], i);
    totalNumberOfEntries += m_buckets[i].m_numberOfEntries;
  }
  if (totalNumberOfEntries != m_numberOfEntries) {
    DEB_HAST("validateRoot(): FAILED CHECK: totalNumberOfEntries=%llu != m_numberOfEntries=%llu", totalNumberOfEntries, m_numberOfEntries);
  }
  ndbassert(totalNumberOfEntries == m_numberOfEntries);
}

void Hast::Root::validateB(CBlock acc) const {
  ndbassert(acc != nullptr);
  ndbassert(acc->getThreadId() != 0);
  // todoas: validate m_bptr->fragrecptr
  //ndbassert(m_bptr->c_fragment_pool.getPtr(m_bptr->fragrecptr));
  //ndbassert(m_bptr->fragrecptr.p != nullptr);
  //ndbassert(Magic::match(m_bptr->fragrecptr.p->m_magic, Dbacc::Fragmentrec::TYPE_ID));
}

void Hast::validateValue(CBlock acc, const Value& value) {
  ndbassert((value.m_opptri & 0x7fffffff) == value.m_opptri);
  ndbassert((value.m_pageidx & 0x00001fff) == value.m_pageidx);
  if(!value.m_locked) {
    ndbassert(value.m_opptri == 0);
  }
}

void Hast::Cursor::validateLockedCursor(CBlock acc) const {
  validateEntryCursor(acc);
  ndbassert(m_dbg_value.m_locked);
}
void Hast::Cursor::validateUnlockedCursor(CBlock acc) const {
  validateEntryCursor(acc);
  ndbassert(!m_dbg_value.m_locked);
}
void Hast::Cursor::validateEntryCursor(CBlock acc) const {
  validateCursor(acc);
  ndbassert(m_valueptr != nullptr);
}
void Hast::Cursor::validateInsertCursor(CBlock acc) const {
  validateCursor(acc);
  ndbassert(m_valueptr == nullptr);
}
void Hast::Cursor::validateCursor(CBlock acc) const {
  //    ✓Hash m_hash;
  //    ✓Root* m_root;
  //    ✓Uint32 m_bucketIndex;
  //    ✓Uint32 m_entryIndex;
  //    Value* m_valueptr;
  //    Value m_dbg_value;// Used for validation
  //    ✓Uint32 m_valid;
  hastJamDebug();
  ndbassert(m_valid == Hast::Cursor::VALID);
  ndbassert(m_root != nullptr);
  Hast::Root& root = *m_root;
  ndbassert((m_hash & 3) == root.m_dbg_inx);
  //root.validateRoot(acc);
  Uint32 expected_bucket_index =
    root.computeBucketIndex(acc,
                            m_hash,
                            root.m_numberOfBuckets);
  ndbassert(m_bucketIndex ==expected_bucket_index);
  Bucket& bucket = root.m_buckets[m_bucketIndex];
  root.validateBucket(acc, bucket, m_bucketIndex);
  if(m_valueptr == nullptr)
  {
    // Insert cursor
    ndbassert(m_entryIndex == bucket.m_numberOfEntries);
    ndbassert(m_dbg_value.m_opptri == 0);
    ndbassert(m_dbg_value.m_pageidx == 0);
    ndbassert(m_dbg_value.m_pageno == 0);
    ndbassert(m_dbg_value.m_locked == false);
  }
  else
  {
    // Entry cursor
    ndbassert(m_entryIndex < bucket.m_numberOfEntries);
    Entry& entry = bucket.m_entries[m_entryIndex];
    ndbassert(m_hash == entry.m_hash);
    ndbassert(m_valueptr == &entry.m_value);
    validateValue(acc, entry.m_value);
    ndbassert(m_dbg_value.m_locked == entry.m_value.m_locked);
    if(entry.m_value.m_locked)
    {
      // Locked entry
      ndbassert(m_dbg_value.m_opptri == entry.m_value.m_opptri);
      ndbassert(m_dbg_value.m_pageidx == entry.m_value.m_pageidx);
      ndbassert(m_dbg_value.m_pageno == entry.m_value.m_pageno);
    }
    else
    {
      ndbassert(m_dbg_value.m_opptri == 0);
      ndbassert(m_dbg_value.m_pageidx == 0);
      ndbassert(m_dbg_value.m_pageno == 0);
    }
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
    ndbassert((hash & 3) == m_dbg_inx);
    ndbassert(computeBucketIndex(acc, hash, m_numberOfBuckets) == bucketIndex);
    validateValue(acc, entry.m_value);
  }
  #endif
}

void hast_progError(int line, int err_code, const char* extra, const char* check) {
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
void Hast::progError(int line, int err_code, const char* extra, const char* check) {
  hast_progError(line, err_code, extra, check);
}
void Hast::Root::progError(int line, int err_code, const char* extra, const char* check) {
  hast_progError(line, err_code, extra, check);
}
void Hast::Cursor::progError(int line, int err_code, const char* extra, const char* check) {
  hast_progError(line, err_code, extra, check);
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
