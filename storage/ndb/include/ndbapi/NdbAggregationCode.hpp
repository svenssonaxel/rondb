/*
 * Copyright [2024] <Copyright Hopsworks AB>
 *
 * Author: Zhao Song
 */
#ifndef NDBAGGREGATIONCODE_H_
#define NDBAGGREGATIONCODE_H_

#include "../../src/kernel/blocks/dbtup/AggCommon.hpp"
#include "include/my_byteorder.h"
class NdbAggregationCode {
 public:
  NdbAggregationCode() {
    memset(buffer_, 0, 1024 * sizeof(uint32));
    instructions_length_ = 25;
    uint32_t curr_pos = 0;
    buffer_[curr_pos++] = (0x0721) << 16 | instructions_length_;
    buffer_[curr_pos++] = 1 << 16 | 4;                // Num of group by cols | Num of agg results
    buffer_[curr_pos++] = 12 << 16;                   // Group by col

    // 1. SUM(UBIGINT + UTINYINT)
    buffer_[curr_pos++] =
                (kOpLoadCol) << 26 |                                      // LOADCOL
                (NDB_TYPE_BIGUNSIGNED & 0x1F) << 21 |                     // "TYPE"
                (kReg1 & 0x0F) << 16 |                                    // Register
                9;                                                        // Column
    buffer_[curr_pos++] =
                (kOpLoadCol) << 26 |                                      // LOADCOL
                (NDB_TYPE_TINYUNSIGNED & 0x1F) << 21 |                    // "TYPE"
                (kReg2 & 0x0F) << 16 |                                    // Register
                5;                                                        // Column
    buffer_[curr_pos++] =
                (kOpPlus) << 26 |                                         // "MATH OP"
                (kReg1 & 0x0F) << 12 |                                    // Register
                (kReg2 & 0x0F) << 8;                                      // Register
    buffer_[curr_pos++] =
                (kOpSum) << 26 |                                          // "AGG OP"
                (kReg1 & 0x0F) << 16 |                                    // Register
                0;                                                        // agg_result 

    // 2. MAX(SMALLINT - MEDIUMINT)
    buffer_[curr_pos++] =
                (kOpLoadCol) << 26 |                                      // LOADCOL
                (NDB_TYPE_SMALLINT & 0x1F) << 21 |                        // "TYPE"
                (kReg1 & 0x0F) << 16 |                                    // Register
                2;                                                        // Column
    buffer_[curr_pos++] =
                (kOpLoadCol) << 26 |                                      // LOADCOL
                (NDB_TYPE_MEDIUMINT & 0x1F) << 21 |                       // "TYPE"
                (kReg2 & 0x0F) << 16 |                                    // Register
                3;                                                        // Column
    buffer_[curr_pos++] =
                (kOpMinus) << 26 |                                        // "MATH OP"
                (kReg1 & 0x0F) << 12 |                                    // Register
                (kReg2 & 0x0F) << 8;                                      // Register
    buffer_[curr_pos++] =
                (kOpMax) << 26 |                                          // "AGG OP"
                (kReg1 & 0x0F) << 16 |                                    // Register
                1;                                                        // agg_result 

    // 3. MIN((DOUBLE / INT) * (FLOAT + 3.5))
    buffer_[curr_pos++] =
                (kOpLoadCol) << 26 |                                      // LOADCOL
                (NDB_TYPE_DOUBLE & 0x1F) << 21 |                          // "TYPE"
                (kReg1 & 0x0F) << 16 |                                    // Register
                11;                                                       // Column
    buffer_[curr_pos++] =
                (kOpLoadCol) << 26 |                                      // LOADCOL
                (NDB_TYPE_INT & 0x1F) << 21 |                             // "TYPE"
                (kReg2 & 0x0F) << 16 |                                    // Register
                0;                                                        // Column
    buffer_[curr_pos++] =
                (kOpDiv) << 26 |                                          // "MATH OP"
                (kReg1 & 0x0F) << 12 |                                    // Register
                (kReg2 & 0x0F) << 8;                                      // Register

    buffer_[curr_pos++] =
                (kOpLoadCol) << 26 |                                      // LOADCOL
                (NDB_TYPE_FLOAT & 0x1F) << 21 |                           // "TYPE"
                (kReg2 & 0x0F) << 16 |                                    // Register
                10;                                                       // Column
    buffer_[curr_pos++] =
                (kOpLoadConst) << 26 |                                    // LOADCONST
                (NDB_TYPE_DOUBLE & 0x1F) << 21 |                          // "TYPE"
                (kReg3 & 0x0F) << 16;                                     // Register
    // longlongstore() for BIGINT
    doublestore(reinterpret_cast<unsigned char*>(
          &(buffer_[curr_pos])), 3.5);
    curr_pos += 2;
    buffer_[curr_pos++] =
                (kOpPlus) << 26 |                                         // "MATH OP"
                (kReg2 & 0x0F) << 12 |                                    // Register
                (kReg3 & 0x0F) << 8;                                      // Register
    buffer_[curr_pos++] =
                (kOpMul) << 26 |                                          // "MATH OP"
                (kReg1 & 0x0F) << 12 |                                    // Register
                (kReg2 & 0x0F) << 8;                                      // Register
    buffer_[curr_pos++] =
                (kOpMin) << 26 |                                          // "AGG OP"
                (kReg1 & 0x0F) << 16 |                                    // Register
                2;                                                        // agg_result 

    // 4. COUNT(UBIGINT % USMALLINT)
    buffer_[curr_pos++] =
                (kOpLoadCol) << 26 |                                      // LOADCOL
                (NDB_TYPE_BIGUNSIGNED & 0x1F) << 21 |                     // "TYPE"
                (kReg1 & 0x0F) << 16 |                                    // Register
                9;                                                        // Column
    buffer_[curr_pos++] =
                (kOpLoadCol) << 26 |                                      // LOADCOL
                (NDB_TYPE_SMALLUNSIGNED & 0x1F) << 21 |                   // "TYPE"
                (kReg2 & 0x0F) << 16 |                                    // Register
                6;                                                        // Column
    buffer_[curr_pos++] =
                (kOpMod) << 26 |                                          // "MATH OP"
                (kReg1 & 0x0F) << 12 |                                    // Register
                (kReg2 & 0x0F) << 8;                                      // Register
    buffer_[curr_pos++] =
                (kOpCount) << 26 |                                        // "AGG OP"
                (kReg1 & 0x0F) << 16 |                                    // Register
                3;                                                        // agg_result 
  }
  const uint32_t* buffer() const {
    return &buffer_[0];
  }
  uint32_t instructions_length() const {
    return instructions_length_;
  }
 private:
  uint32_t buffer_[1024];
  uint32_t instructions_length_;

};
#endif  // NDBAGGREGATIONCODE_H_
