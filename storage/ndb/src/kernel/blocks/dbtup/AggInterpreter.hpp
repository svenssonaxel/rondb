/*
 * Copyright [2024] <Copyright Hopsworks AB>
 *
 * Author: Zhao Song
 */
#ifndef AGGINTERPRETER_H_
#define AGGINTERPRETER_H_

#include <math.h>
#include <map>
#include <mutex>
#include "Dbtup.hpp"

struct GBHashEntry {
  char *ptr;
  uint32_t len;
};

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

class AggInterpreter {
 public:
  AggInterpreter(const uint32_t* prog, uint32_t prog_len, bool print):
    prog_len_(prog_len), cur_pos_(0),
    inited_(false), n_gb_cols_(0), gb_cols_(nullptr),
    n_agg_results_(0),
    agg_results_(nullptr), agg_prog_start_pos_(0),
    gb_map_(nullptr), n_groups_(0),
    buf_pos_(0), print_(print) {
      prog_ = new uint32_t[prog_len];
      memcpy(prog_, prog, prog_len * sizeof(uint32_t));
      memset(buf_, 0, 2048 * sizeof(uint32_t));
  }
  ~AggInterpreter() {
    Print();
    delete[] prog_;
    delete[] gb_cols_;
    delete[] agg_results_;
    if (gb_map_) {
      for (auto iter = gb_map_->begin(); iter != gb_map_->end(); iter++) {
        delete[] iter->first.ptr;
      }
      delete gb_map_;
    }
  }

  bool Init();

  bool ProcessRec(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct);
  void Print();

 private:
  uint32_t* prog_;
  uint32_t prog_len_;
  uint32_t cur_pos_;
  bool inited_;
  Register registers_[kRegTotal];

  uint32_t n_gb_cols_;
  uint32_t* gb_cols_;
  uint32_t n_agg_results_;
  AggResItem* agg_results_;
  uint32_t agg_prog_start_pos_;

  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map_;
  uint32_t n_groups_;
  uint32_t buf_[2048];
  uint32_t buf_pos_;
  static uint32_t g_buf_len_;
  bool print_;
};
#endif  // AGGINTERPRETER_H_
