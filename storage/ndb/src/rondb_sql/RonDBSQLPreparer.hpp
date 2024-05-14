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
    LexString col_name;
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
    LexString identifier;
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
  LexString col_name;
  struct GroupbyColumns* next;
};

struct OrderbyColumns
{
  LexString col_name;
  bool ascending;
  struct OrderbyColumns* next;
};

struct SelectStatement
{
  Outputs* outputs = NULL;
  LexString table = {NULL, 0};
  struct ConditionalExpression* where_expression = NULL;
  struct GroupbyColumns* groupby_columns = NULL;
  struct OrderbyColumns* orderby_columns = NULL;
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
    SelectStatement ast_root;
  };
private:
  enum class Status
  {
    INITIALIZED,
    PARSING,
    PARSED,
    LOADING,
    LOADED,
    COMPILING,
    COMPILED,
    FAILED,
  };
  Status m_status = Status::INITIALIZED;
  LexString m_sql = {NULL, 0};
  ArenaAllocator* m_aalloc;
  Context m_context;
  DynamicArray<LexString> m_identifiers;
  yyscan_t m_scanner;
  YY_BUFFER_STATE m_buf;
  AggregationAPICompiler* m_agg = NULL;
  uint column_name_to_idx(LexString);
  LexString column_idx_to_name(uint);
  bool has_width(uint pos);

public:
  RonDBSQLPreparer(char* sql_buffer, size_t sql_len, ArenaAllocator* aalloc);
  bool parse();
  bool load();
  bool compile();
  const NdbDictionary::Table* get_table(const NdbDictionary::Dictionary* myDict);
  bool programAggregator(NdbAggregator* aggregator);
  bool print();
  void print(struct ConditionalExpression* ce, LexString prefix);
  ~RonDBSQLPreparer();
};

#endif
