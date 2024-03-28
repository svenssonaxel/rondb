/*
 * Copyright [2024] <Copyright Hopsworks AB>
 *
 * Author: Zhao Song
 */
#ifndef NDBAGGREGATIONCOMMON_H_
#define NDBAGGREGATIONCOMMON_H_
#include <cstring>
#include <cstdint>

#define MAX_AGG_RESULT_BATCH_BYTES 8192
enum InterpreterOp {
  kOpUnknown = 0,
  kOpPlus,
  kOpMinus,
  kOpMul,
  kOpDiv,
  kOpMod,
  kOpLoadCol,
  kOpLoadConst,
  kOpSum,
  kOpMax,
  kOpMin,
  kOpCount,
  kOpTotal
};

enum InterpreterRegisters {
  kReg1 = 0,
  kReg2,
  kReg3,
  kReg4,
  kReg5,
  kReg6,
  kReg7,
  kReg8,
  kRegTotal
};

union DataValue {
  int64_t val_int64;
  uint64_t val_uint64;
  double val_double;
  void* val_ptr;
};

typedef uint32_t DataType;
struct Register {
  DataType type;
  DataValue value;
  bool is_unsigned;
  bool is_null;
};

typedef Register AggResItem;

struct GBColInfo {
  DataType type;
  bool is_unsigned;
};

struct GBHashEntry {
  char *ptr;
  uint32_t len;
};

struct GBHashEntryCmp {
  bool operator() (const GBHashEntry& n1, const GBHashEntry& n2) const {
    uint32_t len = n1.len > n2.len ?
                    n2.len : n1.len;

    int ret = memcmp(n1.ptr, n2.ptr, len);
    if (ret == 0) {
      return n1.len < n2.len;
    } else {
      return ret < 0;
    }
  }
};


#endif  // NDBAGGREGATIONCOMMON_H_
