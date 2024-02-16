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

int populate(Ndb * myNdb)
{
  int i;
  Row rows[11];

  const NdbDictionary::Dictionary* myDict= myNdb->getDictionary();
  const NdbDictionary::Table *myTable= myDict->getTable("api_scan");

  if (myTable == NULL) 
    APIERROR(myDict->getNdbError());

  for (i = 1; i < 10; i++)
  {
    rows[i].cint32 = -i;
    rows[i].cint8 = -i;
    rows[i].cint16 = -i;
    rows[i].cint24 = -i * 65535;
    rows[i].cint64 = -i;

    rows[i].cuint32 = i;
    rows[i].cuint8 = i;
    rows[i].cuint16 = i;
    rows[i].cuint24 = i * 65535;
    rows[i].cuint64 = i;

    rows[i].cfloat = i * 1.1;
    rows[i].cdouble = i * 1.11;

    // Must memset here, otherwise group by this
    // column in aggregation interpreter would be undefined.
    memset(rows[i].cchar, 0, sizeof(rows[i].cchar));
    if (i % 2 == 0) {
      sprintf(rows[i].cchar, "GROUP_1");
    } else {
      sprintf(rows[i].cchar, "GROUP_2");
    }
  }

  NdbTransaction* myTrans = myNdb->startTransaction();
  if (myTrans == NULL)
    APIERROR(myNdb->getNdbError());

  for (i = 1; i < 10; i++) 
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

int scan_print(Ndb * myNdb)
{
// Scan all records exclusive and update
  // them one by one
  int                  retryAttempt = 0;
  const int            retryMax = 10;
  int fetchedRows = 0;
  int check;
  NdbError              err;
  NdbTransaction	*myTrans;
  NdbScanOperation	*myScanOp;
  /* Result of reading attribute value, three columns:
     REG_NO, BRAND, and COLOR
   */
  NdbRecAttr *    	myRecAttr[14];   

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

    /**
     * Read without locks, without being placed in lock queue
     */
    if( myScanOp->readTuples(NdbOperation::LM_CommittedRead) == -1)
    {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    } 

    /**
     * Define storage for fetched attributes.
     * E.g., the resulting attributes of executing
     * myOp->getValue("REG_NO") is placed in myRecAttr[0].
     * No data exists in myRecAttr until transaction has committed!
     */
    myRecAttr[0] = myScanOp->getValue("CINT");
    myRecAttr[1] = myScanOp->getValue("CTINYINT");
    myRecAttr[2] = myScanOp->getValue("CSMALLINT");
    myRecAttr[3] = myScanOp->getValue("CMEDIUMINT");
    myRecAttr[4] = myScanOp->getValue("CBIGINT");
    myRecAttr[5] = myScanOp->getValue("CUTINYINT");
    myRecAttr[6] = myScanOp->getValue("CUSMALLINT");
    myRecAttr[7] = myScanOp->getValue("CUMEDIUMINT");
    myRecAttr[8] = myScanOp->getValue("CUINT");
    myRecAttr[9] = myScanOp->getValue("CUBIGINT");
    myRecAttr[10] = myScanOp->getValue("CFLOAT");
    myRecAttr[11] = myScanOp->getValue("CDOUBLE");
    myRecAttr[12] = myScanOp->getValue("CCHAR");
    if(myRecAttr[0] ==NULL || myRecAttr[1] == NULL || myRecAttr[2]==NULL ||
       myRecAttr[3] ==NULL || myRecAttr[4] == NULL || myRecAttr[5]==NULL ||
       myRecAttr[6] ==NULL || myRecAttr[7] == NULL || myRecAttr[8]==NULL ||
       myRecAttr[9] ==NULL || myRecAttr[10] == NULL || myRecAttr[11]==NULL ||
       myRecAttr[12] ==NULL)
    {
	std::cout << myTrans->getNdbError().message << std::endl;
	myNdb->closeTransaction(myTrans);
	return -1;
    }
    /**
     * Start scan   (NoCommit since we are only reading at this stage);
     */     
    if(myTrans->execute(NdbTransaction::NoCommit) != 0){      
      err = myTrans->getNdbError();    
      if(err.status == NdbError::TemporaryError){
	std::cout << myTrans->getNdbError().message << std::endl;
	myNdb->closeTransaction(myTrans);
	milliSleep(50);
	continue;
      }
      std::cout << err.code << std::endl;
      std::cout << myTrans->getNdbError().code << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }
    
    /**
     * start of loop: nextResult(true) means that "parallelism" number of
     * rows are fetched from NDB and cached in NDBAPI
     */    
    while((check = myScanOp->nextResult(true)) == 0){
      do {
	
	fetchedRows++;
	/**
	 * print  REG_NO unsigned int
	 */
  fprintf(stdout, "%8d, %8d, %8d, %8d, %8lld, %8u, %8u ,%8u , %8u, %8llu, %8f, %8lf, %s\n",
	myRecAttr[0]->int32_value(),
	myRecAttr[1]->int8_value(),
	myRecAttr[2]->short_value(),
	myRecAttr[3]->medium_value(),
	myRecAttr[4]->int64_value(),
	myRecAttr[5]->u_8_value(),
	myRecAttr[6]->u_short_value(),
	myRecAttr[7]->u_medium_value(),
	myRecAttr[8]->u_32_value(),
	myRecAttr[9]->u_64_value(),
	myRecAttr[10]->float_value(),
	myRecAttr[11]->double_value(),
	myRecAttr[12]->aRef());

	/**
	 * nextResult(false) means that the records 
	 * cached in the NDBAPI are modified before
	 * fetching more rows from NDB.
	 */    
      } while((check = myScanOp->nextResult(false)) == 0);

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

  if(populate(&myNdb) > 0)
     std::cout << "populate: Success!" << std::endl;

  if(scan_print(&myNdb) > 0)
    std::cout << "scan_print: Success!" << std::endl  << std::endl;
  
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
