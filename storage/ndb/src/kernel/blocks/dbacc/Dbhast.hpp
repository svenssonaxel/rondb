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
#include "Dbacc.hpp"

class Hast {
public:
  explicit Hast(Dbacc* dbacc);
  class Cursor {
    friend class Hast;
    bool isEntryCursor() const;
    bool isInsertCursor() const;
  private:
    Uint32 hash;
    Uint32 bucketNumber;
    Uint32 indexInBucket;
    Uint32* elemptr;
  };
  // Given a key, return a cursor pointing to the first element for that key if
  // such exists, otherwise an insert cursor for that key.
  Cursor getCursor(Uint32 hash);
  // Given a cursor pointing to an element, make it point to the next element
  // for the same key if such exists, otherwise make it an insert cursor for
  // that key.
  void getNextCursor(Cursor& cursor);
  // Given a cursor pointing to an element, get the element.
  Uint32 getElement(Cursor& cursor);
  // Given a cursor pointing to an element, set the element.
  void setElement(Cursor& cursor, Uint32 element);
  // Given an insert cursor, insert an element.
  void insertElement(Cursor& cursor, Uint32 element);
  // Given a cursor pointing to an element, delete that element.
  void deleteElement(Cursor& cursor);
private:
  class Entry {
    friend class Hast;
  private:
    Uint32 hash;
    Uint32 element;
  };
  class Bucket {
    friend class Hast;
  private:
    Uint32 numberOfEntries;
    Entry* entries;
  };
  void progError(int line, int err_code, const char* extra, const char* check) const;
  Uint32 computeBucketNumber(Uint32 hash) const;
  void insertElementIntoBucket(Bucket& bucket, Uint32 hash, Uint32 element);
  void* malloc(size_t size);
  void free(void* ptr);
  bool shouldGrow() const;
  bool shouldShrink() const;
  void grow();
  void shrink();

  Uint32 numberOfBuckets;
  Bucket* buckets;
  Uint64 numberOfElements;
  Dbacc* dbacc;
};

#endif
