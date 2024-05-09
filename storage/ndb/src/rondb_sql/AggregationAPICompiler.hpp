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

#ifndef AggregationAPICompiler_hpp
#define AggregationAPICompiler_hpp 1

#include <stdlib.h>
#include <stdexcept>
#include <functional>
#include "LexString.hpp"
#include "ArenaAllocator.hpp"
#include "DynamicArray.hpp"
// todo order and remove superfluous includes
using std::string;

#define REGS 16

#define FORALL_ARITHMETIC_OPS(X) \
  X(Add) \
  X(Minus) \
  X(Mul) \
  X(Div) \
  X(Rem)
#define FORALL_AGGS(X) \
  X(Sum) \
  X(Min) \
  X(Max) \
  X(Count)
#define FORALL_INSTRUCTIONS(X) \
  X(Load) \
  X(LoadConstantInteger) \
  X(Mov) \
  FORALL_ARITHMETIC_OPS(X) \
  FORALL_AGGS(X)

class AggregationAPICompiler
{
public:
  AggregationAPICompiler(std::function<int(LexString)> column_name_to_idx,
             std::function<LexString(int)> column_idx_to_name,
             ArenaAllocator* aalloc);
  enum class Status
  {
    PROGRAMMING, // High-level API available only in this state
    COMPILING,
    COMPILED,
    FAILED,
  };
  Status getStatus();
private:
  Status m_status = Status::PROGRAMMING;
  ArenaAllocator* m_aalloc;

  // High-level API:
public:
#define ARITHMETIC_ENUM(Name) Name,
  enum class ExprOp
  {
    Load,
    LoadConstantInt,
    FORALL_ARITHMETIC_OPS(ARITHMETIC_ENUM)
  };
#undef ARITHMETIC_ENUM
  struct Expr
  {
    friend class AggregationAPICompiler;
  private:
    ExprOp op; // Binary operation or Load
    Expr* left = NULL; // Left argument to binary operation
    Expr* right = NULL; // Right argument to binary operation
    uint idx = 0; // Column number for load operation, or index in constant list
                  // for loadconstant operations
    int usage = 0; // Reference count from Expr and AggExpr.
                   // Only used for asserts.
    uint est_regs = 0; // Estimated number of registers necessary to calculate
                       // the expression.
    bool eval_left_first = false; // True if we should evaluate left before
                                  // right.
    // The following values belong to compiler (below). They are placed in this
    // struct for convenience.
    int program_usage = 0; // Reference count in program, including uses so far
                           // in calculation but excluding uses in
                           // re-calculation. Only used for asserts.
    bool has_been_compiled = false; // Only used to determine program_usage.
  };
  union Constant
  {
    long int long_int;
  };
private:
  std::function<int(LexString)> m_column_name_to_idx;
  std::function<LexString(int)> m_column_idx_to_name;
  DynamicArray<Expr> m_exprs;
  Expr* new_expr(ExprOp op, Expr* left, Expr* right, uint idx);
#define AGG_ENUM(Name) Name,
  enum class AggType
  {
    FORALL_AGGS(AGG_ENUM)
  };
#undef AGG_ENUM
  struct AggExpr
  {
    AggType agg_type;
    Expr* expr = NULL;
  };
  DynamicArray<AggExpr> m_aggs;
  int new_agg(AggType agg_type, Expr* expr);
  DynamicArray<Constant> m_constants;
public:
  // Load operations
  Expr* Load(LexString col_name);
  Expr* Load(const char* col_name);
  Expr* ConstantInteger(long int long_int);
  // Arithmetic and aggregation operations could easily have been defined using
  // templates, but we prefer doing it without templates and with better
  // argument names.
  // Arithmetic operations
#define DEFINE_ARITH_FUNC(OP, OP_ARG1, OP_ARG2, EXPR_ARG1, EXPR_ARG2) \
  Expr* OP(OP_ARG1, OP_ARG2) \
  { \
    return public_arithmetic_expression_helper(ExprOp::OP, \
                                               EXPR_ARG1, \
                                               EXPR_ARG2); \
  }
#define DEFINE_ARITH_FUNCS(OP) \
  DEFINE_ARITH_FUNC(OP, Expr* expr_x,           Expr* expr_y,           \
                        expr_x,                 expr_y)                 \
  DEFINE_ARITH_FUNC(OP, Expr* expr_x,           LexString col_name_y,   \
                        expr_x,                 Load(col_name_y))       \
  DEFINE_ARITH_FUNC(OP, Expr* expr_x,           const char* col_name_y, \
                        expr_x,                 Load(col_name_y))       \
  DEFINE_ARITH_FUNC(OP, LexString col_name_x,   Expr* expr_y,           \
                        Load(col_name_x),       expr_y)                 \
  DEFINE_ARITH_FUNC(OP, LexString col_name_x,   LexString col_name_y,   \
                        Load(col_name_x),       Load(col_name_y))       \
  DEFINE_ARITH_FUNC(OP, LexString col_name_x,   const char* col_name_y, \
                        Load(col_name_x),       Load(col_name_y))       \
  DEFINE_ARITH_FUNC(OP, const char* col_name_x, Expr* expr_y,           \
                        Load(col_name_x),       expr_y)                 \
  DEFINE_ARITH_FUNC(OP, const char* col_name_x, LexString col_name_y,   \
                        Load(col_name_x),       Load(col_name_y))       \
  DEFINE_ARITH_FUNC(OP, const char* col_name_x, const char* col_name_y, \
                        Load(col_name_x),       Load(col_name_y))
  FORALL_ARITHMETIC_OPS(DEFINE_ARITH_FUNCS)
#undef DEFINE_ARITH_FUNCS
#undef DEFINE_ARITH_FUNC
  // Aggregation operations
#define DEFINE_AGG_FUNC(OP, OP_ARG, EXPR_ARG) \
  int OP(OP_ARG) \
  { \
    return public_aggregate_function_helper(AggType::OP, EXPR_ARG); \
  }
#define DEFINE_AGG_FUNCS(OP) \
  DEFINE_AGG_FUNC(OP, Expr*       expr,     expr) \
  DEFINE_AGG_FUNC(OP, LexString  col_name, Load(col_name)) \
  DEFINE_AGG_FUNC(OP, const char* col_idx,  Load(col_idx))
  FORALL_AGGS(DEFINE_AGG_FUNCS)
#undef DEFINE_AGG_FUNCS
#undef DEFINE_AGG_FUNC
private:
  Expr* public_arithmetic_expression_helper(ExprOp op, Expr* x, Expr* y);
  int public_aggregate_function_helper(AggType agg_type, Expr* x);

  // Symbolic Virtual Machine:
private:
  Expr* r[REGS];
  void svm_init();
#define INSTR_ENUM(Name) Name,
  enum class SVMInstrType
  {
    FORALL_INSTRUCTIONS(INSTR_ENUM)
  };
#undef INSTR_ENUM
  struct Instr
  {
    SVMInstrType type;
    uint dest;
    uint src;
  };
  void svm_execute(Instr* instr, bool is_first_compilation);
  void svm_use(uint reg, bool is_first_compilation);

  // Aggregation Compiler:
private:
  DynamicArray<Instr> m_program;
  int m_locked[REGS];
public:
  bool compile();
private:
  bool compile(AggExpr* agg, int idx);
  bool compile(Expr* expr, uint* reg);
  bool seize_register(uint* reg, uint max_cost);
  uint estimated_cost_of_recalculating(Expr* expr, uint without_using_reg);
  void pushInstr(SVMInstrType type,
                 uint dest,
                 uint src,
                 bool is_first_compilation);
  void pushInstr(AggType type, uint dest, uint src, bool is_first_compilation);
  void pushInstr(ExprOp op, uint dest, uint src, bool is_first_compilation);
  void dead_code_elimination();

  // Aggregation Program Printer
public:
  void print_aggregates();
  void print_aggregate(int idx);
  void print(Expr* expr);
  void print_program();
  void print(Instr* instr);
  // Wrapper class for printing quoted identifiers
  class QuotedIdentifier {
  public:
    const LexString m_id;
    QuotedIdentifier(const LexString identifier) : m_id(identifier) {}
    friend std::ostream& operator<< (std::ostream& out,
                                     const QuotedIdentifier& identifier);
  };
  QuotedIdentifier quoted_identifier(LexString id) const
  {
    return QuotedIdentifier(id);
  }

}; // End of class AggregationAPICompiler

#endif
