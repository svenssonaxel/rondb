/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include <iomanip>
#include "ResultPrinter.hpp"
#include "RonDBSQLPreparer.hpp"
using std::endl;
using std::max;
using std::runtime_error;

static void print_json_string_from_utf8(std::ostream* output_stream, LexString ls, bool utf8_output);

ResultPrinter::ResultPrinter(ArenaAllocator* aalloc,
                             struct SelectStatement* query,
                             DynamicArray<LexCString>* column_names,
                             ExecutionParameters::QueryOutputFormat output_format,
                             std::basic_ostream<char>* err):
  m_aalloc(aalloc),
  m_query(query),
  m_column_names(column_names),
  m_output_format(output_format),
  m_err(err),
  m_program(aalloc),
  m_groupby_cols(aalloc),
  m_outputs(aalloc),
  m_col_idx_groupby_map(aalloc)
{
  assert(query != NULL);
  assert(aalloc != NULL);
  switch (output_format)
  {
  case ExecutionParameters::QueryOutputFormat::CSV:
    assert(false); // Not implemented
    break;
  case ExecutionParameters::QueryOutputFormat::JSON_UTF8:
    break;
  case ExecutionParameters::QueryOutputFormat::JSON_ASCII:
    break;
  default:
    assert(false);
  }
  assert(err != NULL);
  compile();
  optimize();
}

void
ResultPrinter::compile()
{
  std::basic_ostream<char>& err = *m_err;
  DynamicArray<LexCString>& column_names = *m_column_names;
  // Populate m_groupby_columns, an array of the column idxs listed in GROUP BY.
  {
    struct GroupbyColumns* g = m_query->groupby_columns;
    while(g != NULL)
    {
      m_groupby_cols.push(g->col_idx);
      g = g->next;
    }
  }
  // Populate and validate m_outputs, an array of the SELECT expressions.
  // Calculate number_of_aggregates.
  // Populate m_col_idx_groupby_map.
  uint number_of_aggregates = 0;
  {
    struct Outputs* o = m_query->outputs;
    while(o != NULL)
    {
      m_outputs.push(o);
      switch (o->type)
      {
      case Outputs::Type::COLUMN:
        for (uint i = 0; ; i++)
        {
          // Validate that the column appears in the GROUP BY clause
          uint col_idx = o->column.col_idx;
          if (i >= m_groupby_cols.size())
          {
            assert(m_column_names->size() > col_idx);
            err << "Syntax error: Column " << column_names[col_idx].c_str()
                << " appears in a SELECT expression but not in the GROUP BY clause." << endl;
            throw runtime_error("Column in SELECT expression must appear in GROUP BY expression.");
            // todo We should detect much earlier if we are given a non-aggregation query, i.e. with no aggregates and no group by columns. Also, we should test for aggregates without groups and groups without aggregates.
          }
          if (m_groupby_cols[i] == col_idx)
          {
            while (m_col_idx_groupby_map.size() < col_idx + 1)
            {
              m_col_idx_groupby_map.push(0);
            }
            m_col_idx_groupby_map[col_idx] = i;
            break;
          }
        }
        break;
      case Outputs::Type::AGGREGATE:
        number_of_aggregates =
          max(number_of_aggregates, o->aggregate.agg_index + 1);
        break;
      case Outputs::Type::AVG:
        number_of_aggregates =
          max(number_of_aggregates, o->avg.agg_index_sum + 1);
        number_of_aggregates =
          max(number_of_aggregates, o->avg.agg_index_count + 1);
        break;
      default:
        assert(false);
      }
      o = o->next;
    }
  }
  // Allocate registers. Even if some of them won't be used in an optimized
  // program, the memory waste is minimal.
  m_regs_g = m_aalloc->alloc<NdbAggregator::Column>(m_groupby_cols.size());
  m_regs_a = m_aalloc->alloc<NdbAggregator::Result>(number_of_aggregates);
  // Create a correct but non-optimized program
  for (uint i = 0; i < m_groupby_cols.size(); i++)
  {
    Cmd cmd;
    cmd.type = Cmd::Type::STORE_GROUP_BY_COLUMN;
    cmd.store_group_by_column.group_by_idx = i;
    cmd.store_group_by_column.reg_g = i;
    m_program.push(cmd);
  }
  {
    Cmd cmd;
    cmd.type = Cmd::Type::END_OF_GROUP_BY_COLUMNS;
    m_program.push(cmd);
  }
  for (uint i = 0; i < number_of_aggregates; i++)
  {
    Cmd cmd;
    cmd.type = Cmd::Type::STORE_AGGREGATE;
    cmd.store_aggregate.agg_index = i;
    cmd.store_aggregate.reg_a = i;
    m_program.push(cmd);
  }
  {
    Cmd cmd;
    cmd.type = Cmd::Type::END_OF_AGGREGATES;
    m_program.push(cmd);
  }
  switch (m_output_format)
  {
  case ExecutionParameters::QueryOutputFormat::CSV:
    assert(false); // Not implemented
    break;
  case ExecutionParameters::QueryOutputFormat::JSON_UTF8:
    m_utf8_output = true;
    break;
  case ExecutionParameters::QueryOutputFormat::JSON_ASCII:
    m_utf8_output = false;
    break;
  default:
    assert(false);
  }
  for (uint i = 0; i < m_outputs.size(); i++)
  {
    {
      Cmd cmd;
      cmd.type = Cmd::Type::PRINT_STR;
      cmd.print_str.content = LexString{ i == 0 ? "{" : ",", 1 };
      m_program.push(cmd);
    }
    Outputs* o = m_outputs[i];
    {
      Cmd cmd;
      cmd.type = Cmd::Type::PRINT_STR_JSON;
      cmd.print_str.content = o->output_name;
      m_program.push(cmd);
    }
    {
      Cmd cmd;
      cmd.type = Cmd::Type::PRINT_STR;
      cmd.print_str.content = LexString{ ":", 1 };
      m_program.push(cmd);
    }
    switch (o->type)
    {
    case Outputs::Type::COLUMN:
      {
        Cmd cmd;
        cmd.type = Cmd::Type::PRINT_GROUP_BY_COLUMN;
        cmd.print_group_by_column.reg_g = m_col_idx_groupby_map[o->column.col_idx];
        m_program.push(cmd);
      }
      break;
    case Outputs::Type::AGGREGATE:
      {
        Cmd cmd;
        cmd.type = Cmd::Type::PRINT_AGGREGATE;
        cmd.print_aggregate.reg_a = o->aggregate.agg_index;
        m_program.push(cmd);
      }
      break;
    case Outputs::Type::AVG:
      {
        Cmd cmd;
        cmd.type = Cmd::Type::PRINT_AVG;
        cmd.print_avg.reg_a_sum = o->avg.agg_index_sum;
        cmd.print_avg.reg_a_count = o->avg.agg_index_count;
        m_program.push(cmd);
      }
      break;
    default:
      assert(false);
    }
  }
  {
    Cmd cmd;
    cmd.type = Cmd::Type::PRINT_STR;
    cmd.print_str.content = LexString{ "}\n", 2 };
    m_program.push(cmd);
  }
}

void
ResultPrinter::optimize()
{
  // todo
}

void
ResultPrinter::print_result(NdbAggregator* aggregator,
                            std::basic_ostream<char>* query_output_stream)
{
  assert(query_output_stream != NULL);
  std::ostream& out = *query_output_stream;
  out << '[';
  bool first_record = true;
  for (NdbAggregator::ResultRecord record = aggregator->FetchResultRecord();
       !record.end();
       record = aggregator->FetchResultRecord())
  {
    if (first_record) first_record = false; else out << ',';
    for (uint cmd_index = 0; cmd_index < m_program.size(); cmd_index++)
    {
      Cmd& cmd = m_program[cmd_index];
      switch (cmd.type)
      {
      case Cmd::Type::STORE_GROUP_BY_COLUMN:
        {
          NdbAggregator::Column column = record.FetchGroupbyColumn();
          if (column.end())
          {
            throw std::runtime_error("Got record with fewer GROUP BY columns than expected.");
          }
          m_regs_g[cmd.store_group_by_column.reg_g] = column;
        }
        break;
      case Cmd::Type::END_OF_GROUP_BY_COLUMNS:
        {
          NdbAggregator::Column column = record.FetchGroupbyColumn();
          if (!column.end())
          {
            throw std::runtime_error("Got record with more GROUP BY columns than expected.");
          }
        }
        break;
      case Cmd::Type::STORE_AGGREGATE:
        {
          NdbAggregator::Result result = record.FetchAggregationResult();
          if (result.end())
          {
            throw std::runtime_error("Got record with fewer aggregates than expected.");
          }
          m_regs_a[cmd.store_aggregate.reg_a] = result;
        }
        break;
      case Cmd::Type::END_OF_AGGREGATES:
        {
          NdbAggregator::Result result = record.FetchAggregationResult();
          if (!result.end())
          {
            throw std::runtime_error("Got record with more aggregates than expected.");
          }
        }
        break;
      case Cmd::Type::PRINT_GROUP_BY_COLUMN:
        {
          NdbAggregator::Column column = m_regs_g[cmd.print_group_by_column.reg_g];
          if (column.type() == 15)
          {
            print_json_string_from_utf8(query_output_stream,
                                        LexString{ &column.data()[1],
                                                   (size_t)column.data()[0] },
                                        m_utf8_output);
          }
          else
          {
            out << column.data_medium();
          }
        }
        break;
      case Cmd::Type::PRINT_AGGREGATE:
        {
          NdbAggregator::Result result = m_regs_a[cmd.print_aggregate.reg_a];
          switch (result.type())
          {
          case NdbDictionary::Column::Bigint:
            out << result.data_int64();
            break;
          case NdbDictionary::Column::Bigunsigned:
            out << result.data_uint64();
            break;
          case NdbDictionary::Column::Double:
            out << std::fixed << std::setprecision(6) << result.data_double();
            break;
          case NdbDictionary::Column::Undefined:
            // Already handled above
            assert(0);
            break;
          default:
            assert(false);
          }
        }
        break;
      case Cmd::Type::PRINT_AVG:
        {
          NdbAggregator::Result result_sum = m_regs_a[cmd.print_avg.reg_a_sum];
          NdbAggregator::Result result_count = m_regs_a[cmd.print_avg.reg_a_count];
          // todo this must be tested thoroughly against MySQL.
          double numerator;
          unsigned long denominator;
          switch (result_sum.type())
          {
          case NdbDictionary::Column::Bigint:
            numerator = result_sum.data_int64();
            break;
          default:
            assert(false);
          }
          switch (result_count.type())
          {
          case NdbDictionary::Column::Bigint:
            denominator = result_count.data_uint64();
            break;
          default:
            assert(false);
          }
          double result = numerator / denominator;
          out << std::fixed << std::setprecision(6) << result;
        }
        break;
      case Cmd::Type::PRINT_STR:
        out.write(cmd.print_str.content.str, cmd.print_str.content.len);
        break;
      case Cmd::Type::PRINT_STR_JSON:
        print_json_string_from_utf8(query_output_stream,
                                    cmd.print_str.content,
                                    m_utf8_output);
        break;
      default:
        assert(false);
      }
    }
  }
  out << "]\n";
}

// Print a JSON representation of ls to output_stream, assuming ls is correctly
// UTF-8 encoded. utf8_output determines the output encoding:
// utf8_output == true:  If ls contains invalid UTF-8, the output will likewise
//                       be invalid.
// utf8_output == false: Use \u escape for characters with code point U+0080 and
//                       above. Crash if ls contains invalid UTF-8.
static void
print_json_string_from_utf8(std::ostream* output_stream,
                            LexString ls,
                            bool utf8_output)
{
  assert(output_stream != NULL);
  std::ostream& out = *output_stream;
  const char* str = ls.str;
  const char* end = &ls.str[ls.len];
  out << '"';
  while (str < end)
  {
    static const char* hex = "0123456789abcdef";
    char ch = *str;
    if (utf8_output || (ch & 0x80) == 0x00)
    {
      // 1-byte encoding for values 0-7 bits in length if utf8_output == true,
      // or all bytes if utf8_output == false.
      switch (ch)
      {
      case '"':  out << "\\\""; break;
      case '\\': out << "\\\\"; break;
      case '/':  out << "\\/";  break;
      case '\b': out << "\\b";  break;
      case '\f': out << "\\f";  break;
      case '\n': out << "\\n";  break;
      case '\r': out << "\\r";  break;
      case '\t': out << "\\t";  break;
      default:   out << ch;     break;
      }
      str++;
      continue;
    }
    if ((ch & 0xe0) == 0xc0)
    {
      // 2-byte encoding for values 8-11 bits in length
      char ch2 = str[1];
      assert((ch  & 0x3e) != 0x00 &&
             (ch2 & 0xc0) == 0x80);
      Uint32 codepoint = ((ch  & 0x1f) << 6) |
                          (ch2 & 0x3f);
      out << "\\u0"
          << hex[codepoint >> 8]
          << hex[(codepoint >> 4) & 0x0f]
          << hex[codepoint & 0x0f];
      str += 2;
      continue;
    }
    if ((ch & 0xf0) == 0xe0)
    {
      // 3-byte encoding for values 12-16 bits in length
      char ch2 = str[1];
      char ch3 = str[2];
      assert((ch2 & 0xc0) == 0x80 &&
             (ch3 & 0xc0) == 0x80);
      Uint32 codepoint = ((ch  & 0x0f) << 12) |
                         ((ch2 & 0x3f) <<  6) |
                          (ch3 & 0x3f);
      assert((codepoint & 0xf800) != 0xd800);
      out << "\\u"
          << hex[codepoint >> 12]
          << hex[(codepoint >> 8) & 0xf]
          << hex[(codepoint >> 4) & 0xf]
          << hex[codepoint & 0xf];
      str += 3;
      continue;
    }
    if ((ch & 0xf8) == 0xf0)
    {
      // 4-byte encoding for values 17-21 bits in length
      char ch2 = str[1];
      char ch3 = str[2];
      char ch4 = str[3];
      assert((ch2 & 0xc0) == 0x80 &&
             (ch3 & 0xc0) == 0x80 &&
             (ch4 & 0xc0) == 0x80);
      Uint32 codepoint = ((ch  & 0x07) << 18) |
                         ((ch2 & 0x3f) << 12) |
                         ((ch3 & 0x3f) <<  6) |
                          (ch4 & 0x3f);
      assert((codepoint & 0x1f0000) != 0x000000);
      Uint32 sp = codepoint - 0x10000;
      assert((sp & 0x100000) == 0);
      out << "\\ud"
          << hex[(sp >> 18) | 0x8]
          << hex[(sp >> 14) & 0xf]
          << hex[(sp >> 10) & 0xf]
          << "\\ud"
          << hex[((sp >> 8) & 0x3) | 0xc]
          << hex[(sp >> 4) & 0xf]
          << hex[sp & 0xf];
      str += 4;
      continue;
    }
    assert(false);
  }
  out << '"';
}
