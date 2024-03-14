/*
 * Copyright [2024] <Copyright Hopsworks AB>
 *
 * Author: Zhao Song
 */
#ifndef AGGCOMMON_H_
#define AGGCOMMON_H_
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

#endif  // AGGCOMMON_H_
