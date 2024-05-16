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
  AggInterpreter(const uint32_t* prog, uint32_t prog_len, bool print, int64_t frag_id):
    prog_len_(prog_len), cur_pos_(0),
    inited_(false), n_gb_cols_(0), gb_cols_(nullptr),
    n_agg_results_(0),
    agg_results_(nullptr), agg_prog_start_pos_(0),
    gb_map_(nullptr), n_groups_(0),
    buf_pos_(0), print_(print), processed_rows_(0),
    result_size_(0), frag_id_(frag_id)/*, pcount_(0)*/ {
      prog_ = new uint32_t[prog_len];
      memcpy(prog_, prog, prog_len * sizeof(uint32_t));
      memset(buf_, 0, 2048 * sizeof(uint32_t));
      // g_mutex.lock();
      // g_count_++;
      // g_mutex.unlock();
      // fprintf(stderr, "construct AggInterpreter on fragment: %ld, count: %u\n", frag_id_, g_count_);
  }
  ~AggInterpreter() {
    // g_mutex.lock();
    // g_count_--;
    // g_pcount_+= pcount_;
    // if (g_count_ == 0) {
    //   fprintf(stderr, "MOZ-FINAL: %lu, processed %u\n", g_res_, g_pcount_);
    //   g_res_ = 0;
    //   g_pcount_ = 0;
    // }
    // g_mutex.unlock();
    delete[] prog_;
    delete[] gb_cols_;
    delete[] agg_results_;
    if (gb_map_) {
      // MOZ debug
      if (!gb_map_->empty()) {
        // TODO (ZHAO)
        // potential crash here if the API closes scan
        // while lqh is processing, double check.
        assert(gb_map_->empty());
      }
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
  uint32_t NumOfResRecords();
  static void MergePrint(const AggInterpreter* in1, const AggInterpreter* in2);
  static bool g_debug;
  const std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map() {
    return gb_map_;
  }
  int64_t frag_id() {
    return frag_id_;
  }

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
  uint64_t processed_rows_;
  uint32_t result_size_;
  static uint32_t g_result_header_size_;
  static uint32_t g_result_header_size_per_group_;

  int64_t frag_id_;
  // uint32_t pcount_;
  // static std::mutex g_mutex;
  // static uint32_t g_count_;
  // static uint64_t g_res_;
  // static uint32_t g_pcount_;
};
#endif  // AGGINTERPRETER_H_
