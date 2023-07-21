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

/*
 * Public interface
 */

Hast::Hast():
  m_numberOfBuckets(0),
  m_buckets(nullptr),
  m_numberOfEntries(0),
  m_bptr(nullptr),
  m_threadId(0)
{}

void Hast::initialize(Block acc) {
  ndbrequire(m_bptr == nullptr);
  ndbrequire(acc != nullptr);
  m_bptr = acc;
  jam();
  m_threadId = m_bptr->getThreadId();
  ndbrequire(m_threadId != 0);
  m_numberOfBuckets = 1; // todoas make this 0
  m_numberOfEntries = 0;
  m_buckets = (Bucket*)seize_mem(acc, sizeof(Bucket));
  m_buckets[0].m_numberOfEntries = 0;
  m_buckets[0].m_entries = nullptr;
  validateAll(acc);
  jamDebug();
}

void Hast::release(Block acc) {
  validateAll(acc);
  jamDebug();
  for (Uint32 i = 0; i < m_numberOfBuckets; i++) {
    Bucket& bucket = m_buckets[i];
    if (bucket.m_entries != nullptr) {
      release_mem(bucket.m_entries);
    }
  }
  release_mem(m_buckets);
  jamDebug();
}

bool Hast::isEntryCursor(Block acc, Cursor& cursor) const {
  validateB(acc);
  validateCursor(acc, cursor);
  jamDebug();
  return cursor.m_valueptr != nullptr;
}

bool Hast::isInsertCursor(Block acc, Cursor& cursor) const {
  validateB(acc);
  validateCursor(acc, cursor);
  jamDebug();
  return cursor.m_valueptr == nullptr;
}

Uint32 Hast::computeBucketIndex(Uint32 hash, Uint32 m_numberOfBuckets) const {
  Uint32 mask = 0xffffffff;
  while ((mask & hash) >= m_numberOfBuckets) {
    mask >>= 1;
  }
  ndbassert(mask != 0);
  Uint32 bucketIndex = hash & mask;
  ndbassert(bucketIndex < m_numberOfBuckets);
  return bucketIndex;
}

Hast::Cursor Hast::getCursorFirst(Block acc, Uint32 hash) const {
  validateB(acc);
  jamDebug();
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
  return cursor;
}

void Hast::cursorNext(Block acc, Cursor& cursor) const {
  validateB(acc);
  jamDebug();
  ndbrequire(isEntryCursor(acc, cursor));
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
}

Hast::Value Hast::getValue(Block acc, Cursor& cursor) const {
  ndbassert(isEntryCursor(acc, cursor));
  jamDebug();
  return *cursor.m_valueptr;
}

void Hast::setValue(Block acc, Cursor& cursor, Value value) {
  ndbassert(isEntryCursor(acc, cursor));
  jamDebug();
  *cursor.m_valueptr = value;
}

void Hast::insertEntry(Block acc, Cursor& cursor, Value value) {
  ndbassert(isInsertCursor(acc, cursor));
  jamDebug();
  insertEntryIntoBucket(acc, m_buckets[cursor.m_bucketIndex], cursor.m_hash, value);
  if(shouldExpand()) {
    jamDebug();
    expand(acc);
  }
}

void Hast::deleteEntry(Block acc, Cursor& cursor) {
  ndbassert(isEntryCursor(acc, cursor));
  jamDebug();
  Bucket& bucket = m_buckets[cursor.m_bucketIndex];
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
  if(shouldShrink()) {
    jamDebug();
    shrink(acc);
  }
}

/*
 * Internals
 */

void Hast::insertEntryIntoBucket(Block acc, Bucket& bucket, Uint32 hash, Value value) {
  jamDebug();
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
}

// todoas do not crash on OOM. Also, we must always be able to delete.
void* Hast::seize_mem(Block acc, size_t size) {
  validateB(acc);
  // todoas Do I really need getThreadId() here, or can I use 0?
  void* ret = lc_ndbd_pool_malloc(size, RG_DATAMEM, acc->getThreadId(), false);
  ndbrequire(ret != nullptr);
  return ret;
}

void Hast::release_mem(void *ptr) {
  ndbrequire(ptr != nullptr);
  lc_ndbd_pool_free(ptr);
}

bool Hast::shouldExpand() const {
  return m_numberOfBuckets < MAX_NUMBER_OF_BUCKETS &&
                           m_numberOfEntries > Uint64(m_numberOfBuckets) * HIGH_NUMBER_OF_ENTRIES_PER_BUCKET;
}
bool Hast::shouldShrink() const {
  return m_numberOfBuckets > 1 &&
    m_numberOfEntries < Uint64(m_numberOfBuckets) * LOW_NUMBER_OF_ENTRIES_PER_BUCKET;
}

void Hast::expand(Block acc) {
  validateB(acc);
  ndbassert(shouldExpand());
  jamDebug();
  Bucket* newBuckets = (Bucket*)seize_mem(acc, (m_numberOfBuckets + 1) * sizeof(Bucket));
  memcpy(newBuckets, m_buckets, m_numberOfBuckets * sizeof(Bucket));
  release_mem(m_buckets);
  m_buckets = newBuckets;
  Uint32 newBucketIndex = m_numberOfBuckets;
  Uint32 oldBucketIndex = computeBucketIndex(newBucketIndex, m_numberOfBuckets);
  m_numberOfBuckets++;
  Bucket splitBucket = m_buckets[oldBucketIndex];
  Uint32 m_entriesToMove = splitBucket.m_numberOfEntries;
  m_buckets[oldBucketIndex].m_numberOfEntries = 0;
  m_buckets[oldBucketIndex].m_entries = nullptr;
  m_buckets[newBucketIndex].m_numberOfEntries = 0;
  m_buckets[newBucketIndex].m_entries = nullptr;
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
  validateBucket(acc, splitBucket, oldBucketIndex);
  validateBucket(acc, m_buckets[oldBucketIndex], oldBucketIndex);
  validateBucket(acc, m_buckets[newBucketIndex], newBucketIndex);
  ndbassert(m_buckets[oldBucketIndex].m_numberOfEntries + m_buckets[newBucketIndex].m_numberOfEntries == m_entriesToMove);
}

void Hast::shrink(Block acc) {
  ndbassert(shouldShrink());
  jamDebug();
  Bucket* newBuckets = (Bucket*)seize_mem(acc, (m_numberOfBuckets - 1) * sizeof(Bucket));
  memcpy(newBuckets, m_buckets, (m_numberOfBuckets - 1) * sizeof(Bucket));
  Uint32 oldBucketIndex = m_numberOfBuckets - 1;
  Bucket oldBucket = m_buckets[oldBucketIndex];
  m_numberOfBuckets--;
  Uint32 newBucketIndex = computeBucketIndex(oldBucketIndex, m_numberOfBuckets);
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
    release_mem(oldBucket.m_entries);
    oldBucket.m_entries = nullptr;
    newBucket.m_numberOfEntries += oldBucket.m_numberOfEntries;
    oldBucket.m_numberOfEntries = 0;
  }
  validateBucket(acc, oldBucket, oldBucketIndex);
  validateBucket(acc, newBucket, newBucketIndex);
}

/*
 * Validation
 */

void Hast::validateAll(Block acc) const {
  validateHastRoot(acc);
  Uint64 totalNumberOfEntries = 0;
  for (Uint32 i = 0; i < m_numberOfBuckets; i++) {
    validateBucket(acc, m_buckets[i], i);
    totalNumberOfEntries += m_buckets[i].m_numberOfEntries;
  }
  ndbassert(totalNumberOfEntries == m_numberOfEntries);
}

void Hast::validateHastRoot(Block acc) const {
  validateB(acc);
  ndbassert(m_numberOfBuckets > 0);
  ndbassert(m_buckets != nullptr);
}

void Hast::validateB(Block acc) const {
  ndbassert(acc == m_bptr);
  ndbassert(m_bptr != nullptr);
  ndbassert(m_threadId == acc->getThreadId());
  // todoas: validate m_bptr->fragrecptr
  //ndbassert(m_bptr->c_fragment_pool.getPtr(m_bptr->fragrecptr));
  //ndbassert(m_bptr->fragrecptr.p != nullptr);
  //ndbassert(Magic::match(m_bptr->fragrecptr.p->m_magic, Dbacc::Fragmentrec::TYPE_ID));
}

void Hast::validateValue(Block acc, Value value) const {
  ndbassert(value == (value | 0xffffffffffffffffUL)); // todoas: How many bits are used for the value?
}

void Hast::validateCursor(Block acc, Cursor& cursor) const {
  ndbassert(cursor.m_bucketIndex < m_numberOfBuckets);
  Bucket& bucket = m_buckets[cursor.m_bucketIndex];
  ndbassert(computeBucketIndex(cursor.m_hash, m_numberOfBuckets) == cursor.m_bucketIndex);
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

void Hast::validateBucket(Block acc, Bucket& bucket, Uint32 bucketIndex) const {
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

void Hast::progError(int line, int err_code, const char* extra, const char* check) const {
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

EmulatedJamBuffer* Hast::jamBuffer() const {
  ndbassert(m_bptr != nullptr);
  EmulatedJamBuffer* jamBuffer = m_bptr->jamBuffer();
  ndbassert(jamBuffer != nullptr);
  return jamBuffer;
}

#undef JAM_FILE_ID
