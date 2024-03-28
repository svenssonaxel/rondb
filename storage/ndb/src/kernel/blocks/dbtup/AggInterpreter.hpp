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
#include "NdbAggregationCommon.hpp"

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
    // Print();
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
  uint32_t PrepareAggResIfNeeded(Signal* signal, bool force);
  static void MergePrint(const AggInterpreter* in1, const AggInterpreter* in2);
  static bool g_debug;

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
