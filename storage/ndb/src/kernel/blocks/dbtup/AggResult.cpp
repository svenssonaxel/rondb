#include <cstdint>
#include <cstring>
#include "AggResult.hpp"

bool AggResult::Init() {
  if (inited_) {
    return true;
  }

  uint32_t value = 0;

  /*
   * 1. Double check the magic num and  total length of program.
   */
  value = prog_[cur_pos_++];
  assert(((value & 0xFFFF0000) >> 16) == 0x0721);
  assert((value & 0xFFFF) == prog_len_);

  /*
   * 2. Get num of columns for group by and num of aggregation results;
   */
  value = prog_[cur_pos_++];
  n_gb_cols_ = (value >> 16) & 0xFFFF;
  n_agg_results_ = value & 0xFFFF;

  /*
   * 3. Get all the group by columns id.
   */
  if (n_gb_cols_) {
    gb_cols_ = new uint32_t[n_gb_cols_];
    gb_cols_info_ = new GBColInfo[n_gb_cols_];

    uint32_t i = 0;
    while (i < n_gb_cols_ && cur_pos_ < prog_len_) {
      gb_cols_[i++] = prog_[cur_pos_++];
    }

    gb_map_ = new std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>;
  }

  /*
   * 4. Get all aggregation results types
   */
  if (n_agg_results_) {
    agg_results_ = new AggResItem[n_agg_results_];
    uint32_t i = 0;
    while (i < n_agg_results_ && cur_pos_ < prog_len_) {
      agg_results_[i].type = prog_[cur_pos_++];
      agg_results_[i].inited = false;  // used by Min/Max
      agg_results_[i++].value.val_int64 = 0;
    }
  }

  inited_ = true;
  agg_prog_start_pos_ = cur_pos_;
  memset(registers_, 0, sizeof(registers_));

  return true;
}

bool AggResult::ProcessRec(const Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct) {
  assert(inited_ && n_agg_results_ == 1 &&
         n_gb_cols_ == 0 && prog_len_ == 5 && agg_prog_start_pos_ == 3);
  return true;
}
