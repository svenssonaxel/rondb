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

#ifndef RonDBSQLPreparer_hpp_included
#define RonDBSQLPreparer_hpp_included 1

#include <cstddef>
#include <cstdint>
#include "AggregationAPICompiler.hpp"
#include "LexString.hpp"
#include "ArenaAllocator.hpp"
#include "DynamicArray.hpp"
#include <NdbApi.hpp>

// Definitions from RonDBSQLLexer.l.hpp that are needed here. We can't include
// the whole file because it would create a circular dependency.
typedef void* yyscan_t;
typedef struct yy_buffer_state *YY_BUFFER_STATE;
struct yy_buffer_state;

struct LexLocation
{
  char* begin = NULL;
  char* end = NULL;
};

struct Outputs
{
  bool is_agg;
  LexString output_name;
  union
  {
    uint col_idx;
    struct
    {
      int fun;
      AggregationAPICompiler::Expr* arg;
    } aggregate;
  };
  struct Outputs* next;
};

struct ConditionalExpression
{
  int op;
  union
  {
    struct
    {
      struct ConditionalExpression* left;
      struct ConditionalExpression* right;
    } args;
    uint col_idx;
    long int constant_integer;
    struct
    {
      struct ConditionalExpression* arg;
      bool null;
    } is;
    struct
    {
      struct ConditionalExpression* arg;
      int interval_type;
    } interval;
    struct
    {
      int interval_type;
      struct ConditionalExpression* arg;
    } extract;
    LexString string;
  };
};

struct GroupbyColumns
{
  uint col_idx;
  struct GroupbyColumns* next;
};

struct OrderbyColumns
{
  uint col_idx;
  bool ascending;
  struct OrderbyColumns* next;
};

struct SelectStatement
{
  bool do_explain = false;
  Outputs* outputs = NULL;
  LexCString table = { NULL, 0};
  struct ConditionalExpression* where_expression = NULL;
  struct GroupbyColumns* groupby_columns = NULL;
  struct OrderbyColumns* orderby_columns = NULL;
};

struct ExecutionParameters
{
  char* sql_buffer = NULL;
  size_t sql_len = 0;
  ArenaAllocator* aalloc;
  Ndb* ndb = NULL;
  enum class ExecutionMode
  {
    ALLOW_BOTH_QUERY_AND_EXPLAIN, // Explain if EXPLAIN keyword is present in
                                  // SQL code, otherwise query.
    ALLOW_QUERY_ONLY,             // Throw an exception if EXPLAIN keyword is
                                  // present in SQL code, otherwise query.
    ALLOW_EXPLAIN_ONLY,           // Explain if EXPLAIN keyword is present in
                                  // SQL code, otherwise throw an exception.
    QUERY_OVERRIDE,               // Query regardless of whether EXPLAIN keyword
                                  // is present in SQL code.
    EXPLAIN_OVERRIDE,             // Explain regardless of whether EXPLAIN
                                  // keyword is present in SQL code.
  };
  ExecutionMode mode = ExecutionMode::ALLOW_BOTH_QUERY_AND_EXPLAIN;
  std::basic_ostream<char>* query_output_stream = NULL;
  enum class QueryOutputFormat
  {
    CSV,
    JSON_UTF8,
    JSON_ASCII,
  };
  QueryOutputFormat query_output_format = QueryOutputFormat::JSON_UTF8;
  std::basic_ostream<char>* explain_output_stream = NULL;
  enum class ExplainOutputFormat
  {
    TEXT,
    JSON,
  };
  ExplainOutputFormat explain_output_format = ExplainOutputFormat::TEXT;
  std::basic_ostream<char>* err_output_stream = NULL;
};

struct Column
{
  LexCString name = LexCString{ NULL, 0};
  uint col_id_in_table;
};

class RonDBSQLPreparer
{
public:
  enum class ErrState
  {
    NONE,
    LEX_NUL,
    LEX_U_ILLEGAL_BYTE,
    LEX_U_ENC_ERR,
    LEX_U_OVERLONG,
    LEX_U_TOOHIGH,
    LEX_U_SURROGATE,
    LEX_NONBMP_IDENTIFIER,
    LEX_UNIMPLEMENTED_KEYWORD,
    LEX_TOO_LONG_IDENTIFIER,
    LEX_INCOMPLETE_ESCAPE_SEQUENCE_IN_SINGLE_QUOTED_STRING,
    LEX_UNEXPECTED_EOI_IN_SINGLE_QUOTED_STRING,
    LEX_ILLEGAL_TOKEN,
    LEX_UNEXPECTED_EOI_IN_QUOTED_IDENTIFIER,
    TOO_LONG_UNALIASED_OUTPUT,
    PARSER_ERROR,
  };
  class TemporaryError : public std::exception {};
  /*
   * The context class is used to expose parser internals to flex and bison code
   * without making them public.
   */
  class Context
  {
    friend class RonDBSQLPreparer;
  private:
    RonDBSQLPreparer& m_parser;
    ErrState m_err_state = ErrState::NONE;
    const char* m_err_pos = NULL;
    uint m_err_len = 0;
  public:
    Context(RonDBSQLPreparer& parser):
      m_parser(parser)
    {}
    void set_err_state(ErrState state, char* err_pos, uint err_len);
    AggregationAPICompiler* get_agg();
    ArenaAllocator* get_allocator();
    uint column_name_to_idx(LexCString);
    SelectStatement ast_root;
  };
private:
  // m_conf is a value rather than a pointer to prevent the caller from altering
  // it during the lifetime of RonDBSQLPreparer.
  ExecutionParameters m_conf;
  enum class Status
  {
    BEGIN,
    PREPARED,
    FAILED,
  };
  Status m_status = Status::BEGIN;
  LexString m_sql = {NULL, 0};
  ArenaAllocator* m_aalloc;
  Context m_context;
  DynamicArray<Column> m_columns;
  yyscan_t m_scanner;
  YY_BUFFER_STATE m_buf;
  AggregationAPICompiler* m_agg = NULL;
  LexCString column_idx_to_name(uint);
  void (*m_print_json_string)(std::basic_ostream<char>& out, const char* str) = NULL;

  // Functions used in preparation phase
public:
  RonDBSQLPreparer(ExecutionParameters conf);
private:
  void configure();
  void parse();
  bool has_width(uint pos);
  void load();
  void compile();

  // Functions used in execution phase
public:
  void execute();
private:
  void applyFilter(NdbScanFilter* filter, const NdbDictionary::Table* myTable);
  bool applyFilter(NdbScanFilter* filter,
                   struct ConditionalExpression* ce,
                   const NdbDictionary::Table* myTable);
  void programAggregator(NdbAggregator* aggregator);
  void print_result_json(NdbAggregator* aggregator);
  void print();
  void print(struct ConditionalExpression* ce,
             LexString prefix);

public:
  ~RonDBSQLPreparer();
};

#endif
