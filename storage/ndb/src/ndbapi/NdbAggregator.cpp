/*
 * Copyright [2024] <Copyright Hopsworks AB>
 *
 * Author: Zhao Song
 */
#include "NdbAggregator.hpp"
#include "AttributeHeader.hpp"
#include "../../src/ndbapi/NdbDictionaryImpl.hpp"

NdbAggregator::NdbAggregator(const NdbDictionary::Table* table) :
  table_impl_(nullptr), agg_results_(nullptr), gb_map_(nullptr) {
    if (table != nullptr) {
      table_impl_ = & NdbTableImpl::getImpl(*table);
    }
    n_gb_cols_ = 1;
    n_agg_results_ = 4;
    if (n_gb_cols_) {
      gb_map_ = new std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>;
    }
    if (n_agg_results_) {
      agg_results_ = new AggResItem[n_agg_results_];
      uint32_t i = 0;
      while (i < n_agg_results_) {
        agg_results_[i].type = NDB_TYPE_UNDEFINED;
        agg_results_[i++].value.val_int64 = 0;
        agg_results_[i].is_unsigned = false;
        agg_results_[i].is_null = true;
      }
    }

    // Program
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

NdbAggregator::~NdbAggregator() {
  delete[] agg_results_;
  if (gb_map_) {
    for (auto iter = gb_map_->begin(); iter != gb_map_->end(); iter++) {
      delete[] iter->first.ptr;
    }
    delete gb_map_;
  }
}

int32_t NdbAggregator::ProcessRes(char* buf) {
  if (buf != nullptr) {
  }
  // Moz
  // Aggregation result
  assert(buf != nullptr);
  uint32_t parse_pos = 0;
  const uint32_t* data_buf = (const uint32_t*)buf;

  uint32_t n_gb_cols = data_buf[parse_pos] >> 16;
  uint32_t n_agg_results = data_buf[parse_pos++] & 0xFFFF;
  assert(n_gb_cols == n_gb_cols_);
  assert(n_agg_results == n_agg_results_);
  uint32_t n_res_items = data_buf[parse_pos++];
  fprintf(stderr, "Moz-ProcessRes, GB cols: %u, AGG results: %u, RES items: %u\n",
      n_gb_cols, n_agg_results, n_res_items);

  AggResItem* agg_res_ptr = nullptr;
  if (n_gb_cols) {
    char* agg_rec = nullptr;
    const AttributeHeader* header = nullptr;
    for (uint32_t i = 0; i < n_res_items; i++) {
      uint32_t gb_cols_len = data_buf[parse_pos] >> 16;
      uint32_t agg_res_len = data_buf[parse_pos++] & 0xFFFF;

      GBHashEntry entry{const_cast<char*>(
          reinterpret_cast<const char*>(&data_buf[parse_pos])),
                  gb_cols_len};
      auto iter = gb_map_->find(entry);
      if (iter != gb_map_->end()) {
        header = reinterpret_cast<AttributeHeader*>(iter->first.ptr);
        agg_res_ptr = reinterpret_cast<AggResItem*>(iter->second.ptr);
        fprintf(stderr, "Moz, Found GBHashEntry, id: %u, byte_size: %u, "
            "data_size: %u, is_null: %u\n",
            header->getAttributeId(), header->getByteSize(),
            header->getDataSize(), header->isNULL());
      } else {
        assert(n_agg_results * sizeof(AggResItem) == agg_res_len);
        agg_rec = new char[gb_cols_len + agg_res_len];
        memcpy(agg_rec, reinterpret_cast<const char*>(&data_buf[parse_pos]),
            gb_cols_len + agg_res_len);
        GBHashEntry new_entry{agg_rec, gb_cols_len};

        gb_map_->insert(std::make_pair<GBHashEntry, GBHashEntry>(
              std::move(new_entry), std::move(
                GBHashEntry{agg_rec + gb_cols_len,
                agg_res_len})));
        agg_res_ptr = reinterpret_cast<AggResItem*>(agg_rec + agg_res_len);
      }
      AttributeHeader ah(data_buf[parse_pos]);
      fprintf(stderr,
          "[id: %u, sizeB: %u, sizeW: %u, gb_len: %u, "
          "res_len: %u, value: %p]\n",
          ah.getAttributeId(), ah.getByteSize(),
          ah.getDataSize(), gb_cols_len, agg_res_len,
          agg_res_ptr);
      assert(ah.getDataPtr() != &data_buf[parse_pos]);
      assert(4 + ah.getByteSize() == gb_cols_len);
      parse_pos += ((gb_cols_len + agg_res_len) >> 2);

      /*
       AttributeHeader ah(data_buf[parse_pos++]);
       fprintf(stderr,
       "[id: %u, sizeB: %u, sizeW: %u, gb_len: %u, "
       "res_len: %u, value: ",
       ah.getAttributeId(), ah.getByteSize(),
       ah.getDataSize(), gb_cols_len, agg_res_len);
       assert(ah.getDataPtr() != &data_buf[parse_pos]);
       const char* ptr = (const char*)(&data_buf[parse_pos]);
       for (uint32_t i = 0; i < ah.getByteSize(); i++) {
       fprintf(stderr, " %x", ptr[i]);
       }
       parse_pos += ah.getDataSize();
       fprintf(stderr, "]");
       for (uint32_t i = 0; i < n_agg_results; i++) {
       const AggResItem* ptr = (const AggResItem*)(&data_buf[parse_pos]);
       fprintf(stderr, "(type: %u, is_unsigned: %u, is_null: %u, value: ",
       ptr->type, ptr->is_unsigned, ptr->is_null);
       switch (ptr->type) {
       case NDB_TYPE_BIGINT:
       fprintf(stderr, "%15ld", ptr->value.val_int64);
       break;
       case NDB_TYPE_DOUBLE:
       fprintf(stderr, "%31.16f", ptr->value.val_double);
       break;
       default:
       assert(0);
       }
       fprintf(stderr, ")");
       parse_pos += (sizeof(AggResItem) >> 2);
       }
       fprintf(stderr, "\n");
       */
    }
  } else {
    // TODO(Zhao)
  }
  return parse_pos;
}
