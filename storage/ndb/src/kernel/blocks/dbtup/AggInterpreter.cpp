#include <cstdint>
#include <cstring>
#include "include/my_byteorder.h"
#include "AggInterpreter.hpp"

uint32_t AggInterpreter::g_buf_len_ = 2048;

bool AggInterpreter::Init() {
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

    uint32_t i = 0;
    while (i < n_gb_cols_ && cur_pos_ < prog_len_) {
      gb_cols_[i++] = prog_[cur_pos_++];
    }

    gb_map_ = new std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>;
  }

  /*
   * 4. Reset all aggregation results
   */
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

  inited_ = true;
  agg_prog_start_pos_ = cur_pos_;
  memset(registers_, 0, sizeof(registers_));

  return true;
}

static bool TypeSupported(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYINT:
    case NDB_TYPE_SMALLINT:
    case NDB_TYPE_MEDIUMINT:
    case NDB_TYPE_INT:
    case NDB_TYPE_BIGINT:

    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:

    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:
      return true;
    default:
			return false;
	}
  return false;
}

static bool IsUnsigned(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:
      return true;
		default:
			return false;
  }
	return false;
}

static DataType AlignedType(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYINT:
    case NDB_TYPE_SMALLINT:
    case NDB_TYPE_MEDIUMINT:
    case NDB_TYPE_INT:
    case NDB_TYPE_BIGINT:

    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:

      return NDB_TYPE_BIGINT;
    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:
      return NDB_TYPE_DOUBLE;
    default:
      assert(0);
	}
  return NDB_TYPE_UNDEFINED;
}

static bool TestIfSumOverflowsUint64(uint64_t arg1, uint64_t arg2) {
  return ULLONG_MAX - arg1 < arg2;
}

static void SetRegisterNull(Register* reg) {
  reg->is_null = true;
  reg->value.val_int64 = 0;
  reg->is_unsigned = false;
}

static void ResetRegister(Register* reg) {
  reg->type = NDB_TYPE_UNDEFINED;
  SetRegisterNull(reg);
}

static int32_t Sum(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    *res = a;
    if (print) {
      fprintf(stderr, "Moz, Init AggRes to [%ld, %d, %d, %d]\n",
          res->value.val_int64, res->type, res->is_unsigned, res->is_null);
    }
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has null value
    return 1;
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    int64_t val0 = a.value.val_int64;
    int64_t val1 = res->value.val_int64;
    int64_t res_val = static_cast<uint64_t>(val0) + static_cast<uint64_t>(val1);
    bool res_unsigned = false;

    if (a.is_unsigned) {
      if (res->is_unsigned || val1 >= 0) {
        if (TestIfSumOverflowsUint64((uint64_t)val0, (uint64_t)val1)) {
          // overflows;
          return -1;
        } else {
          res_unsigned = true;
        }
      } else {
        if ((uint64_t)val0 > (uint64_t)(LLONG_MAX)) {
          res_unsigned = true;
        }
      }
    } else {
      if (res->is_unsigned) {
        if (val0 >= 0) {
          if (TestIfSumOverflowsUint64((uint64_t)val0, (uint64_t)val1)) {
            // overflows;
            return -1;
          } else {
            res_unsigned = true;
          }
        } else {
          if ((uint64_t)val1 > (uint64_t)(LLONG_MAX)) {
            res_unsigned = true;
          }
        }
      } else {
        if (val0 >= 0 && val1 >= 0) {
          res_unsigned = true;
        } else if (val0 < 0 && val1 < 0 && res_val >= 0) {
          // overflow
          return -1;
        }
      }
    }

    // Check if res_val is overflow
    bool unsigned_flag = false;
    if (res_type == NDB_TYPE_BIGINT) {
      unsigned_flag = (a.is_unsigned | res->is_unsigned);
    } else {
      assert(res_type == NDB_TYPE_DOUBLE);
      unsigned_flag = (a.is_unsigned & res->is_unsigned);
    }
    if ((unsigned_flag && !res_unsigned && res_val < 0) ||
        (!unsigned_flag && res_unsigned &&
         (uint64_t)res_val > (uint64_t)LLONG_MAX)) {
      return -1;
    } else {
      if (unsigned_flag) {
        res->value.val_uint64 = res_val;
      } else {
        res->value.val_int64 = res_val;
      }
    }
    res->is_unsigned = unsigned_flag;
  } else {
    double val0 = (a.type == NDB_TYPE_DOUBLE) ?
                     a.value.val_double :
                     ((a.is_unsigned == true) ?
                       static_cast<double>(a.value.val_uint64) :
                       static_cast<double>(a.value.val_int64));
    double val1 = (res->type == NDB_TYPE_DOUBLE) ?
                     res->value.val_double :
                     ((res->is_unsigned == true) ?
                       static_cast<double>(res->value.val_uint64) :
                       static_cast<double>(res->value.val_int64));
    double res_val = val0 + val1;
    if (std::isfinite(res_val)) {
      res->value.val_double = res_val;
    } else {
      // overflow
      return -1;
    }
    res->is_unsigned = false;
  }

  res->type = res_type;

  if (print) {
    fprintf(stderr, "Moz, Update AggRes to [%ld, %d, %d, %d]\n",
        res->value.val_int64, res->type, res->is_unsigned, res->is_null);
  }
  return 0;
}

bool AggInterpreter::ProcessRec(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct) {
  assert(inited_ && n_agg_results_ == 1 &&
         n_gb_cols_ == 1 && prog_len_ == 5 && agg_prog_start_pos_ == 3);

	AggResItem* agg_res_ptr = nullptr;
  if (n_gb_cols_) {
		char* agg_rec = nullptr;

		// DataType type = NDB_TYPE_UNDEFINED;
		AttributeHeader* header = nullptr;
		buf_pos_ = 0;
    for (uint32_t i = 0; i < n_gb_cols_; i++) {
			int ret = block_tup->readAttributes(req_struct, &(gb_cols_[i]), 1,
																		buf_ + buf_pos_, g_buf_len_ - buf_pos_);
			assert(ret >= 0);
			header = reinterpret_cast<AttributeHeader*>(buf_ + buf_pos_);
			buf_pos_ += (1 + header->getDataSize());
    }

		uint32_t len_in_char = (buf_pos_ + 1) * sizeof(uint32_t);
    GBHashEntry entry{(char*)buf_, len_in_char};
    auto iter = gb_map_->find(entry);
    if (iter != gb_map_->end()) {
			header = reinterpret_cast<AttributeHeader*>(iter->first.ptr);
      agg_res_ptr = reinterpret_cast<AggResItem*>(iter->second.ptr);
			fprintf(stderr, "Moz, Found GBHashEntry, id: %u, byte_size: %u, "
                      "data_size: %u, is_null: %u\n",
											header->getAttributeId(), header->getByteSize(),
											header->getDataSize(), header->isNULL());
    } else {
			agg_rec = new char[len_in_char +
												 n_agg_results_ * sizeof(AggResItem)];
			memcpy(agg_rec, (char*)buf_, len_in_char);
			GBHashEntry new_entry{agg_rec, len_in_char};

      gb_map_->insert(std::make_pair<GBHashEntry, GBHashEntry>(
											std::move(new_entry), std::move(
						GBHashEntry{agg_rec + len_in_char,
            static_cast<uint32_t>(n_agg_results_ * sizeof(AggResItem))})));
      n_groups_ = gb_map_->size();
      agg_res_ptr = reinterpret_cast<AggResItem*>(agg_rec + len_in_char);

      for (uint32_t i = 0; i < n_agg_results_; i++) {
        agg_res_ptr[i].type = agg_results_[i].type;
      }
    }
  } else {
    agg_res_ptr = agg_results_;
  }

  uint32_t value;
  DataType type;
  bool is_unsigned;
  uint32_t reg_index;

  // DataType type2;
  // bool is_unsigned2;
  // uint32_t reg_index2;
  uint32_t agg_index;
  uint32_t col_index;

  const uint32_t* attrDescriptor = nullptr;

  uint32_t exec_pos = agg_prog_start_pos_;
  while (exec_pos < prog_len_) {
    value = prog_[exec_pos++];
    uint8_t op = (value & 0xFC000000) >> 26;
    int ret = 0;
    buf_pos_ = 0;
		AttributeHeader* header = nullptr;
    
		switch (op) {
			case kOpLoadCol:
				type = (value & 0x03E00000) >> 21;
				is_unsigned = IsUnsigned(type);
				reg_index = (value & 0x000F0000) >> 16;
				col_index = (value & 0x0000FFFF) << 16;

        ret = block_tup->readAttributes(req_struct, &(col_index), 1,
                                      buf_ + buf_pos_, g_buf_len_ - buf_pos_);
        assert(ret >= 0);
        header = reinterpret_cast<AttributeHeader*>(buf_ + buf_pos_);
        attrDescriptor = req_struct->tablePtrP->tabDescriptor +
                          (((col_index) >> 16) * ZAD_SIZE);
        assert(header->getAttributeId() == (col_index >> 16));

        assert(type == AttributeDescriptor::getType(attrDescriptor[0]));
				assert(TypeSupported(type));

				ResetRegister(&registers_[reg_index]);
				registers_[reg_index].type = AlignedType(type);
				registers_[reg_index].is_unsigned = is_unsigned;
				registers_[reg_index].is_null = header->isNULL();
				switch (type) {
					case NDB_TYPE_TINYINT:
            registers_[reg_index].value.val_int64 = 
                  *reinterpret_cast<int8_t*>(&buf_[buf_pos_ + 1]);
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_TINYINT %ld\n", registers_[reg_index].value.val_int64);
						break;
					case NDB_TYPE_SMALLINT:
            registers_[reg_index].value.val_int64 =
                  sint2korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_SMALLINT %ld\n", registers_[reg_index].value.val_int64);
						break;
          case NDB_TYPE_MEDIUMINT:
            registers_[reg_index].value.val_int64 =
                  sint3korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_MEDIUM %ld\n", registers_[reg_index].value.val_int64);
            break;
					case NDB_TYPE_INT:
            registers_[reg_index].value.val_int64 = 
                  sint4korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_INT %ld\n", registers_[reg_index].value.val_int64);
						break;
					case NDB_TYPE_BIGINT:
            registers_[reg_index].value.val_int64 =
                  sint8korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_BIGINT %ld\n", registers_[reg_index].value.val_int64);
						break;
					case NDB_TYPE_TINYUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                  *reinterpret_cast<uint8_t*>(&buf_[buf_pos_ + 1]);
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_TINYUNSIGNED %lu\n", registers_[reg_index].value.val_uint64);
						break;
					case NDB_TYPE_SMALLUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                  uint2korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_SMALLUNSIGNED %lu\n", registers_[reg_index].value.val_uint64);
						break;
          case NDB_TYPE_MEDIUMUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                  uint3korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_MEDIUMUNSIGNED %lu\n", registers_[reg_index].value.val_uint64);
            break;
					case NDB_TYPE_UNSIGNED:
            registers_[reg_index].value.val_uint64 =
                  uint4korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_UNSIGNED %lu\n", registers_[reg_index].value.val_uint64);
						break;
					case NDB_TYPE_BIGUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                  uint8korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_BIGUNSIGNED %lu\n", registers_[reg_index].value.val_uint64);
            break;

          case NDB_TYPE_FLOAT:
            registers_[reg_index].value.val_double =
                  floatget(reinterpret_cast<unsigned char*>(&buf_[buf_pos_ + 1]));
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_FLOAT %lf\n", registers_[reg_index].value.val_double);
            break;
          case NDB_TYPE_DOUBLE:
            registers_[reg_index].value.val_double =
                  doubleget(reinterpret_cast<unsigned char*>(&buf_[buf_pos_ + 1]));
            fprintf(stderr, "Moz-Intp: Load NDB_TYPE_DOUBLE %lf\n", registers_[reg_index].value.val_double);
            break;

					default:
						assert(0);
				}
				break;
      case kOpSum:
				type = (value & 0x03E00000) >> 21;
				is_unsigned = IsUnsigned(type);
				reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Sum(registers_[reg_index], &agg_res_ptr[agg_index], print_);
        if (ret < 0) {
          fprintf(stderr, "Overflow, value is out of range\n");
        }
        assert(ret >= 0);
        break;
      default:
        assert(0);
		}
  }
  return true;
}

void AggInterpreter::Print() {
  if (!print_) {
    return;
  }
  if (n_gb_cols_) {
    if (gb_map_) {
      fprintf(stderr, "Group by columns: [");
      for (uint32_t i = 0; i < n_gb_cols_; i++) {
        if (i != n_gb_cols_ - 1) {
          fprintf(stderr, "%u ", gb_cols_[i] >> 16);
        } else {
          fprintf(stderr, "%u", gb_cols_[i] >> 16);
        }
      }
      fprintf(stderr, "]\n");
      fprintf(stderr, "Num of groups: %lu\n", gb_map_->size());
      fprintf(stderr, "Aggregation results:\n");

      for (auto iter = gb_map_->begin(); iter != gb_map_->end(); iter++) {
        int pos = 0;
        fprintf(stderr, "(");
        for (uint32_t i = 0; i < n_gb_cols_; i++) {
          if (i != n_gb_cols_ - 1) {
            fprintf(stderr, "%u: %p, ", i, iter->first.ptr + pos);
          } else {
            fprintf(stderr, "%u: %p): ", i, iter->first.ptr + pos);
          }
        }

        AggResItem* item = reinterpret_cast<AggResItem*>(iter->second.ptr);
        for (uint32_t i = 0; i < n_agg_results_; i++) {
          switch (item[i].type) {
            case NDB_TYPE_BIGINT:
              // fprintf(stderr, "    (NDB_TYPE_BIGINT: %ld)\n", item[i].value.val_int64);
              fprintf(stderr, "[%15ld]", item[i].value.val_int64);
              break;

            case NDB_TYPE_DOUBLE:
              // fprintf(stderr, "    (NDB_TYPE_DOUBLE: %.16f)\n", item[i].value.val_double);
              fprintf(stderr, "[%31.16f]", item[i].value.val_double);
              break;
            default:
              assert(0);
          }
        }
        fprintf(stderr, "\n");
      }
    }
  } else {
    AggResItem* item = agg_results_;
    for (uint32_t i = 0; i < n_agg_results_; i++) {
      switch (item[i].type) {
        case NDB_TYPE_BIGINT:
          // fprintf(stderr, "    (NDB_TYPE_BIGINT: %ld)\n", item[i].value.val_int64);
          fprintf(stderr, "[%15ld]", item[i].value.val_int64);
          break;

        case NDB_TYPE_DOUBLE:
          // fprintf(stderr, "    (NDB_TYPE_DOUBLE: %.16f)\n", item[i].value.val_double);
          fprintf(stderr, "[%31.16f]", item[i].value.val_double);
          break;
        default:
          assert(0);
      }
    }
    fprintf(stderr, "\n");
  }
}

