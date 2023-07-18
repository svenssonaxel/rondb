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
  numberOfBuckets(0),
  buckets(nullptr),
  numberOfEntries(0),
  bptr(nullptr),
  threadId(0)
{}

void Hast::initialize(B b) {
  ndbrequire(bptr == nullptr);
  ndbrequire(b != nullptr);
  bptr = b;
  jam();
  threadId = bptr->getThreadId();
  ndbrequire(threadId != 0);
  numberOfBuckets = 1;
  numberOfEntries = 0;
  buckets = (Bucket*)malloc(b, sizeof(Bucket));
  buckets[0].numberOfEntries = 0;
  buckets[0].entries = nullptr;
  validateAll(b);
  jamDebug();
}

void Hast::release(B b) {
  validateAll(b);
  jamDebug();
  for (Uint32 i = 0; i < numberOfBuckets; i++) {
    Bucket& bucket = buckets[i];
    if (bucket.entries != nullptr) {
      free(bucket.entries);
    }
  }
  free(buckets);
  jamDebug();
}

bool Hast::isEntryCursor(B b, Cursor& cursor) const {
  validateB(b);
  validateCursor(b, cursor);
  jamDebug();
  return cursor.valueptr != nullptr;
}

bool Hast::isInsertCursor(B b, Cursor& cursor) const {
  validateB(b);
  validateCursor(b, cursor);
  jamDebug();
  return cursor.valueptr == nullptr;
}

Uint32 Hast::computeBucketNumber(Uint32 hash, Uint32 numberOfBuckets) const {
  Uint32 mask = 0xffffffff;
  while ((mask & hash) >= numberOfBuckets) {
    mask >>= 1;
  }
  ndbassert(mask != 0);
  Uint32 bucketNumber = hash & mask;
  ndbassert(bucketNumber < numberOfBuckets);
  return bucketNumber;
}

Hast::Cursor Hast::getCursorFirst(B b, Uint32 hash) const {
  validateB(b);
  jamDebug();
  Cursor cursor = Cursor();
  cursor.hash = hash;
  cursor.bucketNumber = computeBucketNumber(hash, numberOfBuckets);
  cursor.indexInBucket = 0;
  cursor.valueptr = nullptr;
  Bucket& bucket = buckets[cursor.bucketNumber];
  while (cursor.indexInBucket < bucket.numberOfEntries) {
    if (bucket.entries[cursor.indexInBucket].hash == hash) {
      cursor.valueptr = &bucket.entries[cursor.indexInBucket].value;
      break;
    }
    cursor.indexInBucket++;
  }
  return cursor;
}

void Hast::cursorNext(B b, Cursor& cursor) const {
  validateB(b);
  jamDebug();
  ndbrequire(isEntryCursor(b, cursor));
  Bucket& bucket = buckets[cursor.bucketNumber];
  cursor.indexInBucket++;
  cursor.valueptr = nullptr;
  while (cursor.indexInBucket < bucket.numberOfEntries) {
    if (bucket.entries[cursor.indexInBucket].hash == cursor.hash) {
      cursor.valueptr = &bucket.entries[cursor.indexInBucket].value;
      break;
    }
    cursor.indexInBucket++;
  }
}

Hast::Value Hast::getValue(B b, Cursor& cursor) const {
  ndbassert(isEntryCursor(b, cursor));
  jamDebug();
  return *cursor.valueptr;
}

void Hast::setValue(B b, Cursor& cursor, Value value) {
  ndbassert(isEntryCursor(b, cursor));
  jamDebug();
  *cursor.valueptr = value;
}

void Hast::insertEntry(B b, Cursor& cursor, Value value) {
  ndbassert(isInsertCursor(b, cursor));
  jamDebug();
  insertEntryIntoBucket(b, buckets[cursor.bucketNumber], cursor.hash, value);
  if(shouldGrow()) {
    jamDebug();
    grow(b);
  }
}

void Hast::deleteEntry(B b, Cursor& cursor) {
  ndbassert(isEntryCursor(b, cursor));
  jamDebug();
  Bucket& bucket = buckets[cursor.bucketNumber];
  Entry* newEntries = nullptr;
  if(bucket.numberOfEntries > 1) {
    newEntries = (Entry*)malloc(b, (bucket.numberOfEntries - 1) * sizeof(Entry));
    if(cursor.indexInBucket > 0) {
      memcpy(newEntries, bucket.entries, cursor.indexInBucket * sizeof(Entry));
    }
    if(cursor.indexInBucket < bucket.numberOfEntries - 1) {
      memcpy(newEntries + cursor.indexInBucket, bucket.entries + cursor.indexInBucket + 1,
             (bucket.numberOfEntries - cursor.indexInBucket - 1) * sizeof(Entry));
    }
  }
  if (bucket.entries != nullptr) {
    free(bucket.entries);
  }
  bucket.entries = newEntries;
  bucket.numberOfEntries--;
  numberOfEntries--;
  if(shouldShrink()) {
    jamDebug();
    shrink(b);
  }
}

/*
 * Internals
 */

void Hast::insertEntryIntoBucket(B b, Bucket& bucket, Uint32 hash, Value value) {
  jamDebug();
  Entry* newEntries = (Entry*)malloc(b, (bucket.numberOfEntries + 1) * sizeof(Entry));
  if (bucket.numberOfEntries > 0) {
    jamDebug();
    memcpy(newEntries, bucket.entries, bucket.numberOfEntries * sizeof(Entry));
    free(bucket.entries);
  }
  newEntries[bucket.numberOfEntries].hash = hash;
  newEntries[bucket.numberOfEntries].value = value;
  bucket.entries = newEntries;
  bucket.numberOfEntries++;
  numberOfEntries++;
}

void* Hast::malloc(B b, size_t size) {
  validateB(b);
  // todoas Do I really need getThreadId() here, or can I use 0?
  void* ret = lc_ndbd_pool_malloc(size, RG_DATAMEM, b->getThreadId(), false);
  ndbrequire(ret != nullptr);
  return ret;
}

void Hast::free(void *ptr) {
  ndbrequire(ptr != nullptr);
  lc_ndbd_pool_free(ptr);
}

bool Hast::shouldGrow() const {
  return numberOfBuckets < Hast::max_number_of_buckets &&
                           numberOfEntries > Uint64(numberOfBuckets) * high_number_of_entries_per_bucket;
}
bool Hast::shouldShrink() const {
  return numberOfBuckets > 1 &&
    numberOfEntries < Uint64(numberOfBuckets) * Hast::low_number_of_entries_per_bucket;
}

void Hast::grow(B b) {
  validateB(b);
  ndbassert(shouldGrow());
  jamDebug();
  Bucket* newBuckets = (Bucket*)malloc(b, (numberOfBuckets + 1) * sizeof(Bucket));
  memcpy(newBuckets, buckets, numberOfBuckets * sizeof(Bucket));
  free(buckets);
  buckets = newBuckets;
  Uint32 newBucketNumber = numberOfBuckets;
  Uint32 oldBucketNumber = computeBucketNumber(newBucketNumber, numberOfBuckets);
  numberOfBuckets++;
  Bucket splitBucket = buckets[oldBucketNumber];
  Uint32 entriesToMove = splitBucket.numberOfEntries;
  buckets[oldBucketNumber].numberOfEntries = 0;
  buckets[oldBucketNumber].entries = nullptr;
  buckets[newBucketNumber].numberOfEntries = 0;
  buckets[newBucketNumber].entries = nullptr;
  for (Uint32 i = 0; i < splitBucket.numberOfEntries; i++) {
    Entry &entry = splitBucket.entries[i];
    Uint32 bucketNumber = computeBucketNumber(entry.hash, numberOfBuckets);
    ndbassert(bucketNumber == oldBucketNumber || bucketNumber == newBucketNumber);
    insertEntryIntoBucket(b, buckets[bucketNumber], entry.hash, entry.value);
  }
  if (splitBucket.entries != nullptr) {
    free(splitBucket.entries);
  }
  splitBucket.numberOfEntries = 0;
  splitBucket.entries = nullptr;
  validateBucket(b, splitBucket, oldBucketNumber);
  validateBucket(b, buckets[oldBucketNumber], oldBucketNumber);
  validateBucket(b, buckets[newBucketNumber], newBucketNumber);
  ndbassert(buckets[oldBucketNumber].numberOfEntries + buckets[newBucketNumber].numberOfEntries == entriesToMove);
}

void Hast::shrink(B b) {
  ndbassert(shouldShrink());
  jamDebug();
  Bucket* newBuckets = (Bucket*)malloc(b, (numberOfBuckets - 1) * sizeof(Bucket));
  memcpy(newBuckets, buckets, (numberOfBuckets - 1) * sizeof(Bucket));
  Uint32 oldBucketNumber = numberOfBuckets - 1;
  Bucket oldBucket = buckets[oldBucketNumber];
  numberOfBuckets--;
  Uint32 newBucketNumber = computeBucketNumber(oldBucketNumber, numberOfBuckets);
  free(buckets);
  buckets = newBuckets;
  Bucket& newBucket = buckets[newBucketNumber];
  if(oldBucket.numberOfEntries > 0) {
    Entry* newEntries = (Entry*)malloc(b, (oldBucket.numberOfEntries + newBucket.numberOfEntries) * sizeof(Entry));
    if(newBucket.numberOfEntries > 0) {
      memcpy(newEntries, newBucket.entries, newBucket.numberOfEntries * sizeof(Entry));
      free(newBucket.entries);
    }
    memcpy(newEntries + newBucket.numberOfEntries, oldBucket.entries, oldBucket.numberOfEntries * sizeof(Entry));
    free(oldBucket.entries);
    oldBucket.entries = nullptr;
    newBucket.numberOfEntries += oldBucket.numberOfEntries;
    oldBucket.numberOfEntries = 0;
  }
  validateBucket(b, oldBucket, oldBucketNumber);
  validateBucket(b, newBucket, newBucketNumber);
}

/*
 * Validation
 */

void Hast::validateAll(B b) const {
  validateHastRoot(b);
  Uint64 totalNumberOfEntries = 0;
  for (Uint32 i = 0; i < numberOfBuckets; i++) {
    validateBucket(b, buckets[i], i);
    totalNumberOfEntries += buckets[i].numberOfEntries;
  }
  ndbassert(totalNumberOfEntries == numberOfEntries);
}

void Hast::validateHastRoot(B b) const {
  validateB(b);
  ndbassert(numberOfBuckets > 0);
  ndbassert(buckets != nullptr);
}

void Hast::validateB(B b) const {
  ndbassert(b == bptr);
  ndbassert(bptr != nullptr);
  ndbassert(threadId == b->getThreadId());
  // todoas: validate bptr->fragrecptr
  //ndbassert(bptr->c_fragment_pool.getPtr(bptr->fragrecptr));
  //ndbassert(bptr->fragrecptr.p != nullptr);
  //ndbassert(Magic::match(bptr->fragrecptr.p->m_magic, Dbacc::Fragmentrec::TYPE_ID));
}

void Hast::validateValue(B b, Value value) const {
  ndbassert(value == (value | 0x000000ffffffffffUL)); // todoas: How many bits are used for the value?
}

void Hast::validateCursor(B b, Cursor& cursor) const {
  ndbassert(cursor.bucketNumber < numberOfBuckets);
  Bucket& bucket = buckets[cursor.bucketNumber];
  // todoas validate hash and bucket number
  if(cursor.valueptr != nullptr) {
    ndbassert(cursor.indexInBucket < bucket.numberOfEntries);
    Entry& entry = bucket.entries[cursor.indexInBucket];
    ndbassert(cursor.hash == entry.hash);
    ndbassert(cursor.valueptr == &entry.value);
    validateValue(b, entry.value);
  }
  else {
    ndbassert(cursor.indexInBucket == bucket.numberOfEntries);
  }
}

void Hast::validateBucket(B b, Bucket& bucket, Uint32 bucketIndex) const {
  if(bucket.numberOfEntries == 0) {
    ndbrequire(bucket.entries == nullptr);
    return;
  }
  ndbrequire(bucket.entries != nullptr);
  #if defined(VM_TRACE) || defined(ERROR_INSERT)
  for (Uint32 i = 0; i < bucket.numberOfEntries; i++) {
    Entry& entry = bucket.entries[i];
    Uint32 hash = entry.hash;
    ndbassert(computeBucketNumber(hash, numberOfBuckets) == bucketIndex);
    validateValue(b, entry.value);
  }
  #endif
}

void Hast::progError(int line, int err_code, const char* extra, const char* check) const {
  if(bptr != nullptr) {
    bptr->progError(line, err_code, extra, check);
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
  ndbassert(bptr != nullptr);
  EmulatedJamBuffer* jamBuffer = bptr->jamBuffer();
  ndbassert(jamBuffer != nullptr);
  return jamBuffer;
}

#undef JAM_FILE_ID
