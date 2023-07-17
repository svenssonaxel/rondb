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

Hast::Hast(Dbacc* dbacc) : numberOfBuckets(1), buckets(new Bucket[1]), numberOfElements(0), dbacc(dbacc) {
  buckets[0].numberOfEntries = 0;
  buckets[0].entries = NULL;
}

void Hast::progError(int line, int err_code, const char* extra, const char* check) const {
  dbacc->progError(line, err_code, extra, check);
}

bool Hast::Cursor::isEntryCursor() const { return elemptr != NULL; }

bool Hast::Cursor::isInsertCursor() const { return elemptr == NULL; }

Uint32 Hast::computeBucketNumber(Uint32 hash) const {
  Uint32 mask = 0xffffffff;
  while ((mask & hash) >= numberOfBuckets) {
    mask >>= 1;
  }
  return hash & mask;
}

Hast::Cursor Hast::getCursor(Uint32 hash) {
  Cursor cursor = Cursor();
  cursor.hash = hash;
  cursor.bucketNumber = computeBucketNumber(hash);
  cursor.indexInBucket = 0;
  cursor.elemptr = NULL;
  Bucket& bucket = buckets[cursor.bucketNumber];
  for (Uint32 i = 0; i < bucket.numberOfEntries; i++) {
    if (bucket.entries[i].hash == hash) {
      cursor.indexInBucket = i;
      cursor.elemptr = &bucket.entries[i].element;
      break;
    }
  }
  return cursor;
}

void Hast::getNextCursor(Cursor& cursor) {
  ndbrequire(cursor.isEntryCursor());
  Bucket& bucket = buckets[cursor.bucketNumber];
  cursor.indexInBucket++;
  cursor.elemptr = NULL;
  for (Uint32 i = cursor.indexInBucket; i < bucket.numberOfEntries; i++) {
    if (bucket.entries[i].hash == cursor.hash) {
      cursor.indexInBucket = i;
      cursor.elemptr = &bucket.entries[i].element;
      break;
    }
  }
}

Uint32 Hast::getElement(Cursor& cursor) {
  ndbrequire(cursor.isEntryCursor());
  return *cursor.elemptr;
}

void Hast::setElement(Cursor& cursor, Uint32 element) {
  ndbrequire(cursor.isEntryCursor());
  *cursor.elemptr = element;
}

void Hast::insertElement(Cursor& cursor, Uint32 element) {
  ndbrequire(cursor.isInsertCursor());
  ndbrequire(cursor.bucketNumber < numberOfBuckets);
  insertElementIntoBucket(buckets[cursor.bucketNumber], cursor.hash, element);
  if(shouldGrow()) {
    grow();
  }
}

void Hast::insertElementIntoBucket(Bucket& bucket, Uint32 hash, Uint32 element) {
  Entry* newEntries = (Entry*)malloc((bucket.numberOfEntries + 1) * sizeof(Entry));
  if (bucket.numberOfEntries > 0) {
    memcpy(newEntries, bucket.entries, bucket.numberOfEntries * sizeof(Entry));
    free(bucket.entries);
  }
  newEntries[bucket.numberOfEntries].hash = hash;
  newEntries[bucket.numberOfEntries].element = element;
  bucket.entries = newEntries;
  bucket.numberOfEntries++;
  numberOfElements++;
}

void Hast::deleteElement(Cursor& cursor) {
  ndbrequire(cursor.isEntryCursor());
  ndbrequire(cursor.bucketNumber < numberOfBuckets)
  Bucket& bucket = buckets[cursor.bucketNumber];
  ndbrequire(cursor.indexInBucket < bucket.numberOfEntries);
  Entry* newEntries = nullptr;
  if(bucket.numberOfEntries > 1) {
    newEntries = (Entry*)malloc((bucket.numberOfEntries - 1) * sizeof(Entry));
    if(cursor.indexInBucket > 0)
      memcpy(newEntries, bucket.entries, cursor.indexInBucket * sizeof(Entry));
    if(cursor.indexInBucket < bucket.numberOfEntries - 1)
      memcpy(newEntries + cursor.indexInBucket, bucket.entries + cursor.indexInBucket + 1,
             (bucket.numberOfEntries - cursor.indexInBucket - 1) * sizeof(Entry));
  }
  free(bucket.entries);
  bucket.entries = newEntries;
  bucket.numberOfEntries--;
  numberOfElements--;
  if(shouldShrink()) {
    shrink();
  }
}

void* Hast::malloc(size_t size) {
  void* ret = lc_ndbd_pool_malloc(size, RG_DATAMEM, dbacc->getThreadId(), false);
  ndbrequire(ret != nullptr);
  return ret;
}

void Hast::free(void *ptr) {
  ndbrequire(ptr != nullptr);
  lc_ndbd_pool_free(ptr);
}

bool Hast::shouldGrow() const { return numberOfBuckets < 0xffffffff && numberOfElements > numberOfBuckets * 18; }

bool Hast::shouldShrink() const { return numberOfBuckets > 1 && numberOfElements < numberOfBuckets * 14; }

void Hast::grow() {
  ndbrequire(shouldGrow());
  Bucket* newBuckets = (Bucket*)malloc((numberOfBuckets + 1) * sizeof(Bucket));
  memcpy(newBuckets, buckets, numberOfBuckets * sizeof(Bucket));
  free(buckets);
  buckets = newBuckets;
  Uint32 newBucketNumber = numberOfBuckets;
  Uint32 oldBucketNumber = computeBucketNumber(newBucketNumber);
  numberOfBuckets++;
  Bucket splitBucket = buckets[oldBucketNumber];
  buckets[oldBucketNumber].numberOfEntries = 0;
  buckets[oldBucketNumber].entries = NULL;
  buckets[newBucketNumber].numberOfEntries = 0;
  buckets[newBucketNumber].entries = NULL;
  for (Uint32 i = 0; i < splitBucket.numberOfEntries; i++) {
    Entry &entry = splitBucket.entries[i];
    insertElementIntoBucket(buckets[computeBucketNumber(entry.hash)], entry.hash, entry.element);
  }
  free(splitBucket.entries);
}

void Hast::shrink() {
  ndbrequire(shouldShrink());
  Bucket* newBuckets = (Bucket*)malloc((numberOfBuckets - 1) * sizeof(Bucket));
  memcpy(newBuckets, buckets, (numberOfBuckets - 1) * sizeof(Bucket));
  Uint32 oldBucketNumber = numberOfBuckets - 1;
  Bucket oldBucket = buckets[oldBucketNumber];
  numberOfBuckets--;
  Uint32 newBucketNumber = computeBucketNumber(oldBucketNumber);
  free(buckets);
  buckets = newBuckets;
  Bucket& newBucket = buckets[newBucketNumber];
  if(oldBucket.numberOfEntries > 0) {
    Entry* newEntries = (Entry*)malloc((oldBucket.numberOfEntries + newBucket.numberOfEntries) * sizeof(Entry));
    if(newBucket.numberOfEntries > 0) {
      memcpy(newEntries, newBucket.entries, newBucket.numberOfEntries * sizeof(Entry));
      free(newBucket.entries);
    }
    memcpy(newEntries + newBucket.numberOfEntries, oldBucket.entries, oldBucket.numberOfEntries * sizeof(Entry));
    free(oldBucket.entries);
    newBucket.numberOfEntries += oldBucket.numberOfEntries;
  }
}

#undef JAM_FILE_ID
