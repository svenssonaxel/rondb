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

#ifndef RonSQLCommon_hpp_included
#define RonSQLCommon_hpp_included 1

#include "Ndb.hpp"
#include "NdbOperation.hpp"

#include "ArenaAllocator.hpp"
#include "LexString.hpp"

struct ExecutionParameters
{
  char* sql_buffer = NULL;
  size_t sql_len = 0;
  ArenaAllocator* aalloc = NULL;
  Ndb* ndb = NULL;
  enum class ExplainMode
  {
             // SELECT causes: | EXPLAIN SELECT causes: | Ndb connection required:
    ALLOW,   // SELECT         | EXPLAIN SELECT         | Yes
    FORBID,  // SELECT         | Exception              | Yes
    REQUIRE, // Exception      | EXPLAIN SELECT         | No
    REMOVE,  // SELECT         | SELECT                 | Yes
    FORCE,   // EXPLAIN SELECT | EXPLAIN SELECT         | No
  };
  ExplainMode explain_mode = ExplainMode::ALLOW;
  std::basic_ostream<char>* out_stream = NULL;
  enum class OutputFormat
  {
    JSON,          // Output a JSON representation of the result set or EXPLAIN
                   // output. Characters with code point U+0080 and above are
                   // encoded as UTF-8.
    JSON_ASCII,    // Output a JSON representation of the result set or EXPLAIN
                   // output. Characters with code point U+007f and above are
                   // encoded using \u escape sequences, meaning the output stream
                   // will only contain ASCII characters 0x0a, 0x20 -- 0x7e.
    TEXT,          // For query output, mimic mysql tab-separated output with
                   // headers. For EXPLAIN output, use a plain text format.
    TEXT_NOHEADER, // Same as TEXT, except suppress the header line for query
                   // output.
  };
  OutputFormat output_format = OutputFormat::JSON;
  std::basic_ostream<char>* err_stream = NULL;
  const char* operation_id = NULL; // Only used with RDRS
  bool* do_explain = NULL; // If not NULL, use this to inform the caller whether
                           // we EXPLAIN. This is needed by RDRS to determine
                           // content type.
};

// Forward declaration used in struct Outputs below
class AggregationAPICompiler_Expr;

// structs for parse tree

struct Outputs
{
  enum class Type
  {
    COLUMN,
    AGGREGATE,
    AVG,
  };
  Type type;
  LexString output_name;
  union
  {
    struct
    {
      uint col_idx;
    } column;
    struct
    {
      int fun;
      AggregationAPICompiler_Expr* arg;
      uint agg_index;
    } aggregate;
    struct
    {
      AggregationAPICompiler_Expr* arg;
      uint agg_index_sum;
      uint agg_index_count;
    } avg;
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
    Int64 constant_integer;
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
  LexCString table = LexCString{ NULL, 0};
  struct ConditionalExpression* where_expression = NULL;
  struct GroupbyColumns* groupby_columns = NULL;
  struct OrderbyColumns* orderby_columns = NULL;
};

#endif
