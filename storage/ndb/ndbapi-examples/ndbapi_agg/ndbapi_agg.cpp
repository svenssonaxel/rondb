/*
   Copyright (c) 2005, 2023, Oracle and/or its affiliates.

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


/*
 * ndbapi_scan.cpp: 
 * Illustrates how to use the scan api in the NDBAPI.
 * The example shows how to do scan, scan for update and scan for delete
 * using NdbScanFilter and NdbScanOperation
 *
 * Classes and methods used in this example:
 *
 *  Ndb_cluster_connection
 *       connect()
 *       wait_until_ready()
 *
 *  Ndb
 *       init()
 *       getDictionary()
 *       startTransaction()
 *       closeTransaction()
 *
 *  NdbTransaction
 *       getNdbScanOperation()
 *       execute()
 *
 *  NdbScanOperation
 *       getValue() 
 *       readTuples()
 *       nextResult()
 *       deleteCurrentTuple()
 *       updateCurrentTuple()
 *
 *  const NdbDictionary::Dictionary
 *       getTable()
 *
 *  const NdbDictionary::Table
 *       getColumn()
 *
 *  const NdbDictionary::Column
 *       getLength()
 *
 *  NdbOperation
 *       insertTuple()
 *       equal()
 *       setValue()
 *
 *  NdbScanFilter
 *       begin()
 *	 eq()
 *	 end()
 *
 */

#include "config.h"

#ifdef _WIN32
#include <winsock2.h>
#endif
#include <mysql.h>
#include <mysqld_error.h>
#include <NdbApi.hpp>
// Used for cout
#include<iomanip>
#include <cassert>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <config.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif
#include <random>
// #include <AttributeHeader.hpp>

/**
 * Helper sleep function
 */
static void
milliSleep(int milliseconds){
  struct timeval sleeptime;
  sleeptime.tv_sec = milliseconds / 1000;
  sleeptime.tv_usec = (milliseconds - (sleeptime.tv_sec * 1000)) * 1000000;
  select(0, 0, 0, 0, &sleeptime);
}


/**
 * Helper debugging macros
 */
#define PRINT_ERROR(code,msg) \
  std::cout << "Error in " << __FILE__ << ", line: " << __LINE__ \
  << ", code: " << code \
  << ", msg: " << msg << "." << std::endl
#define MYSQLERROR(mysql) { \
  PRINT_ERROR(mysql_errno(&mysql),mysql_error(&mysql)); \
  exit(-1); }
#define APIERROR(error) { \
  PRINT_ERROR(error.code,error.message); \
  exit(-1); }

struct Row {
  Int32 cint32;
  Int8 cint8;
  Int16 cint16;
  Int32 cint24;
  Int64 cint64;

  Uint8 cuint8;
  Uint16 cuint16;
  Uint32 cuint24;
  Uint32 cuint32;
  Uint64 cuint64;

  float cfloat;
  double cdouble;

  char cchar[20];
};

void drop_table(MYSQL &mysql)
{
  if (mysql_query(&mysql, "DROP TABLE IF EXISTS api_scan"))
    MYSQLERROR(mysql);
}

void create_table(MYSQL &mysql) 
{
  while (mysql_query(&mysql, 
        "CREATE TABLE agg.api_scan ("
        "CINT INT NOT NULL,"
        "CTINYINT TINYINT NOT NULL,"
        "CSMALLINT SMALLINT NOT NULL,"
        "CMEDIUMINT MEDIUMINT NOT NULL,"
        "CBIGINT BIGINT NOT NULL,"
        "CUTINYINT TINYINT UNSIGNED NOT NULL,"
        "CUSMALLINT SMALLINT UNSIGNED NOT NULL,"
        "CUMEDIUMINT MEDIUMINT UNSIGNED NOT NULL,"
        "CUINT INT UNSIGNED NOT NULL,"
        "CUBIGINT BIGINT UNSIGNED NOT NULL,"
        "CFLOAT FLOAT NOT NULL,"
        "CDOUBLE DOUBLE NOT NULL,"
        "CCHAR CHAR(20) NOT NULL,"
        "PRIMARY KEY USING HASH (CINT)) ENGINE=NDB CHARSET=latin1"))
  {
    if (mysql_errno(&mysql) != ER_TABLE_EXISTS_ERROR)
      MYSQLERROR(mysql);
    std::cout << "MySQL Cluster already has example table: api_scan. "
      << "Dropping it..." << std::endl; 
    drop_table(mysql);
  }
}

std::random_device rd;
std::mt19937 gen(rd());

/*
   std::uniform_int_distribution<int64_t> g_bigint(0xFFFFFFFF, 0x7FFFFFFF);
   std::uniform_int_distribution<uint64_t> g_ubigint(0, 0xFFFFFFFF);
   std::uniform_int_distribution<int32_t> g_int(0xFFFF, 0x7FFF);
   std::uniform_int_distribution<uint32_t> g_uint(0, 0xFFFF);
   std::uniform_int_distribution<int32_t> g_mediumint(0x0FFF, 0x7FF);
   std::uniform_int_distribution<uint32_t> g_umediumint(0, 0xFFF);
   std::uniform_int_distribution<int16_t> g_smallint(0xFF, 0x7F);
   std::uniform_int_distribution<uint16_t> g_usmallint(0, 0xFF);
   std::uniform_int_distribution<int8_t> g_tinyint(0xF, 0x7);
   std::uniform_int_distribution<uint8_t> g_utinyint(0, 0xF);
   std::uniform_real_distribution<float> g_float(0xFFFF, 0x7FFF);
   std::uniform_real_distribution<double> g_double(0xFFFFFFFF, 0x7FFFFFFF);
   */

std::uniform_int_distribution<int64_t> g_bigint(-3147483648, 3147483648);
std::uniform_int_distribution<uint64_t> g_ubigint(0, 5294967295);
std::uniform_int_distribution<int32_t> g_int(-2147483648, 2147483647);
std::uniform_int_distribution<uint32_t> g_uint(0, 4294967295);
std::uniform_int_distribution<int32_t> g_mediumint(-8388608, 8388607);
std::uniform_int_distribution<uint32_t> g_umediumint(0, -2147483648);
std::uniform_int_distribution<int16_t> g_smallint(-32768, 32767);
std::uniform_int_distribution<uint16_t> g_usmallint(0, 32768);
std::uniform_int_distribution<int8_t> g_tinyint(-128, 127);
std::uniform_int_distribution<uint8_t> g_utinyint(0, 255);
std::uniform_real_distribution<float> g_float(-32768, 32767);
std::uniform_real_distribution<double> g_double(-8388608, 8388607);

std::uniform_int_distribution<uint8_t> g_zero(0, 19);

#define NUM 10000
int populate(Ndb * myNdb)
{
  int i;
  Row rows[NUM];

  const NdbDictionary::Dictionary* myDict= myNdb->getDictionary();
  const NdbDictionary::Table *myTable= myDict->getTable("api_scan");

  if (myTable == NULL) 
    APIERROR(myDict->getNdbError());

  for (i = 0; i < NUM; i++)
  {
    rows[i].cint32 = g_int(gen);
    rows[i].cint8 = g_tinyint(gen);
    rows[i].cint16 = g_smallint(gen);
    rows[i].cint24 = g_mediumint(gen);
    rows[i].cint64 = g_bigint(gen);

    rows[i].cuint8 = g_utinyint(gen);
    rows[i].cuint16 = g_usmallint(gen);
    if (g_zero(gen) == 6) {
      rows[i].cuint16 = 0;
    }
    rows[i].cuint24 = g_umediumint(gen);
    rows[i].cuint32 = g_uint(gen);
    rows[i].cuint64 = g_ubigint(gen);
    if (g_zero(gen) == 6) {
      rows[i].cuint64 = 0;
    }

    rows[i].cfloat = g_float(gen);
    rows[i].cdouble = g_double(gen);
    if (g_zero(gen) == 6) {
      rows[i].cdouble = 0;
    }

    // Simple for debug
    // rows[i].cint32 = i;
    // rows[i].cint8 = i;
    // rows[i].cint16 = i;
    // rows[i].cint24 = i;
    // rows[i].cint64 = i;
    // rows[i].cuint8 = i * 2;
    // rows[i].cuint16 = i * 2;
    // rows[i].cuint24 = i * 2;
    // rows[i].cuint32 = i * 2;
    // rows[i].cuint64 = i * 2;
    // rows[i].cfloat = i * 1.1;
    // rows[i].cdouble = i * 1.11;


    // Must memset here, otherwise group by this
    // column in aggregation interpreter would be undefined.
    memset(rows[i].cchar, 0, sizeof(rows[i].cchar));

    switch (i % 4) {
      case 0:
        sprintf(rows[i].cchar, "GROUP_1");
        break;
      case 1:
        sprintf(rows[i].cchar, "GROUP_2");
        break;
      case 2:
        sprintf(rows[i].cchar, "GROUP_3");
        break;
      case 3:
        sprintf(rows[i].cchar, "GROUP_4");
        break;
      default:
        assert(0);
    }
  }

  NdbTransaction* myTrans = myNdb->startTransaction();
  if (myTrans == NULL)
    APIERROR(myNdb->getNdbError());

  for (i = 1; i < NUM; i++) 
  {
    NdbOperation* myNdbOperation = myTrans->getNdbOperation(myTable);
    if (myNdbOperation == NULL) 
      APIERROR(myTrans->getNdbError());
    myNdbOperation->insertTuple();
    assert(myNdbOperation->equal("CINT", rows[i].cint32) != -1);
    assert(myNdbOperation->setValue("CTINYINT", rows[i].cint8) != -1);
    assert(myNdbOperation->setValue("CSMALLINT", rows[i].cint16) != -1);
    assert(myNdbOperation->setValue("CMEDIUMINT", rows[i].cint24) != -1);
    assert(myNdbOperation->setValue("CBIGINT", rows[i].cint64) != -1);

    assert(myNdbOperation->setValue("CUTINYINT", rows[i].cuint8) != -1);
    assert(myNdbOperation->setValue("CUSMALLINT", rows[i].cuint16) != -1);
    assert(myNdbOperation->setValue("CUMEDIUMINT", rows[i].cuint24) != -1);
    assert(myNdbOperation->setValue("CUINT", rows[i].cuint32) != -1);
    assert(myNdbOperation->setValue("CUBIGINT", rows[i].cuint64) != -1);

    assert(myNdbOperation->setValue("CFLOAT", rows[i].cfloat) != -1);
    assert(myNdbOperation->setValue("CDOUBLE", rows[i].cdouble) != -1);

    assert(myNdbOperation->setValue("CCHAR", rows[i].cchar) != -1);
  }

  int check = myTrans->execute(NdbTransaction::Commit);
  if (check != 0) {
    std::cout <<  myTrans->getNdbError().message << std::endl;
  }

  myTrans->close();

  return check != -1;
}

int scan_aggregation(Ndb * myNdb)
{
  // Scan all records exclusive and update
  // them one by one
  int                  retryAttempt = 0;
  const int            retryMax = 10;
  NdbError              err;
  NdbTransaction	*myTrans;
  NdbScanOperation	*myScanOp;

  const NdbDictionary::Dictionary* myDict= myNdb->getDictionary();
  const NdbDictionary::Table *myTable= myDict->getTable("api_scan");

  if (myTable == NULL) 
    APIERROR(myDict->getNdbError());

  /**
   * Loop as long as :
   *  retryMax not reached
   *  failed operations due to TEMPORARY errors
   *
   * Exit loop;
   *  retyrMax reached
   *  Permanent error (return -1)
   */
  while (true)
  {

    if (retryAttempt >= retryMax)
    {
      std::cout << "ERROR: has retried this operation " << retryAttempt 
        << " times, failing!" << std::endl;
      return -1;
    }

    myTrans = myNdb->startTransaction();
    if (myTrans == NULL) 
    {
      const NdbError err = myNdb->getNdbError();

      if (err.status == NdbError::TemporaryError)
      {
        milliSleep(50);
        retryAttempt++;
        continue;
      }
      std::cout << err.message << std::endl;
      return -1;
    }
    /*
     * Define a scan operation. 
     * NDBAPI.
     */
    myScanOp = myTrans->getNdbScanOperation(myTable);	
    if (myScanOp == NULL) 
    {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    /*
     * Define an aggregator
     */
    NdbAggregator aggregator(myTable);
    assert(aggregator.GroupBy("CCHAR"));
    assert(aggregator.LoadColumn("CUBIGINT", kReg1));
    assert(aggregator.LoadColumn("CUTINYINT", kReg2));
    assert(aggregator.Add(kReg1, kReg2));
    assert(aggregator.Sum(0, kReg1));
    assert(aggregator.LoadColumn("CDOUBLE", kReg1));
    assert(aggregator.Min(1, kReg1));
    assert(aggregator.LoadColumn("CUMEDIUMINT", kReg1));
    assert(aggregator.Max(2, kReg1));

    /* Example of how to catch an error
    int ret = aggregator.Sum(0, kReg1);
    if (!ret) {
      fprintf(stderr, "Error: %u, %s\n",
                      aggregator.GetError().errno_,
                      aggregator.GetError().err_msg_);
    }
    */

    assert(aggregator.Finalize());
    if (myScanOp->setAggregationCode(&aggregator) == -1) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myScanOp->DoAggregation() == -1) {
      err = myTrans->getNdbError();
      if(err.status == NdbError::TemporaryError) {
        std::cout << myTrans->getNdbError().message << std::endl;
        myNdb->closeTransaction(myTrans);
        milliSleep(50);
        continue;
      }
      std::cout << err.message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    fprintf(stderr, "---FINAL RESULT---\n");
    // aggregator.Print();
    NdbAggregator::ResultRecord record = aggregator.FetchResultRecord();
    while (!record.end()) {
      NdbAggregator::Column column = record.FetchGroupbyColumn();
      while (!column.end()) {
        fprintf(stderr,
            "group [id: %u, type: %u, byte_size: %u, is_null: %u, data: %s]:",
            column.id(), column.type(), column.byte_size(),
            column.is_null(), column.data());
        column = record.FetchGroupbyColumn();
      }

      NdbAggregator::Result result = record.FetchAggregationResult();
      while (!result.end()) {
        switch (result.type()) {
          case NdbDictionary::Column::Bigint:
            fprintf(stderr,
                " (type: %u, is_null: %u, data: %ld)",
                result.type(), result.is_null(), result.data_int64());
            break;
          case NdbDictionary::Column::Bigunsigned:
            fprintf(stderr,
                " (type: %u, is_null: %u, data: %lu)",
                result.type(), result.is_null(), result.data_uint64());
            break;
          case NdbDictionary::Column::Double:
            fprintf(stderr,
                " (type: %u, is_null: %u, data: %lf)",
                result.type(), result.is_null(), result.data_double());
            break;
          default:
            assert(0);
        }
        result = record.FetchAggregationResult();
      }
      fprintf(stderr, "\n");
      record = aggregator.FetchResultRecord();
    }

    myNdb->closeTransaction(myTrans);
    return 1;
  }
  return -1;
}

void mysql_connect_and_create(MYSQL & mysql, const char *socket)
{
  bool ok;

  ok = mysql_real_connect(&mysql, "localhost", "root", "", "", 0, socket, 0);
  if(ok) {
    mysql_query(&mysql, "CREATE DATABASE agg");
    ok = ! mysql_select_db(&mysql, "agg");
  }
  if(ok) {
    create_table(mysql);
  }

  if(! ok) MYSQLERROR(mysql);
}

void ndb_run_scan(const char * connectstring)
{

  /**************************************************************
   * Connect to ndb cluster                                     *
   **************************************************************/

  Ndb_cluster_connection cluster_connection(connectstring);
  if (cluster_connection.connect(4, 5, 1))
  {
    std::cout << "Unable to connect to cluster within 30 secs." << std::endl;
    exit(-1);
  }
  // Optionally connect and wait for the storage nodes (ndbd's)
  if (cluster_connection.wait_until_ready(30,0) < 0)
  {
    std::cout << "Cluster was not ready within 30 secs.\n";
    exit(-1);
  }

  Ndb myNdb(&cluster_connection,"agg");
  if (myNdb.init(1024) == -1) {      // Set max 1024  parallel transactions
    APIERROR(myNdb.getNdbError());
    exit(-1);
  }

  for (int i = 0; i < 1; i++) {
    if (populate(&myNdb) != 1) {
      // std::cout << "populate: Failed!" << std::endl;
    }
  }

  std::cout << "Intialize table and data done!" << std::endl;

  if(scan_aggregation(&myNdb) > 0)
    std::cout << "scan_aggregation: Success!" << std::endl  << std::endl;

}

int main(int argc, char** argv)
{
  if (argc != 3)
  {
    std::cout << "Arguments are <socket mysqld> <connect_string cluster>.\n";
    exit(-1);
  }
  char * mysqld_sock  = argv[1];
  const char *connectstring = argv[2];
  MYSQL mysql;

  mysql_init(& mysql);
  mysql_connect_and_create(mysql, mysqld_sock);

  ndb_init();
  ndb_run_scan(connectstring);
  ndb_end(0);

  mysql_close(&mysql);

  return 0;
}
