/*
 * Copyright [2024] <Copyright Hopsworks AB>
 *
 * Author: Zhao Song
 */
#ifndef NDBAGGREGATOR_H_
#define NDBAGGREGATOR_H_

#include "NdbDictionary.hpp"
#include "NdbAggregationCommon.hpp"
#include <map>

class NdbTableImpl;

class NdbAggregator {
 public:
  NdbAggregator(const NdbDictionary::Table* table);
  ~NdbAggregator();
  const uint32_t* buffer() const {
    return &buffer_[0];
  }
  uint32_t instructions_length() const {
    return instructions_length_;
  }
  int32_t ProcessRes(char* buf);
 private:
  const NdbTableImpl *table_impl_;
  uint32_t buffer_[1024];
  uint32_t instructions_length_;

  uint32_t n_gb_cols_;
  uint32_t n_agg_results_;
  AggResItem* agg_results_;
  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map_;
};
#endif  // NDBAGGREGATOR_H_
