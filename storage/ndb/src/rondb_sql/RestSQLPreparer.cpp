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

#include <assert.h>
#include "AggregationAPICompiler.hpp"
#include "RestSQLParser.y.hpp"
#include "RestSQLLexer.l.hpp"
#include "RestSQLPreparer.hpp"
using std::cout;
using std::cerr;
using std::endl;

RestSQLPreparer::RestSQLPreparer(char* sql_buffer,
                                 size_t sql_len,
                                 ArenaAllocator* aalloc):
  m_aalloc(aalloc),
  m_context(*this),
  m_identifiers(aalloc)
{
  /*
   * Both `yy_scan_string' and `yy_scan_bytes' create and scan a copy of the
   * input. This may be desirable, since `yylex()' modifies the contents of the
   * buffer it is scanning. In order to avoid copying, we use `yy_scan_buffer'.
   * It requires the last two bytes of the buffer to be NUL. These last two
   * bytes are not scanned.
   * See https://ftp.gnu.org/old-gnu/Manuals/flex-2.5.4/html_node/flex_12.html
   */
  assert(sql_len >= 2);
  assert(sql_buffer[sql_len-1] == '\0');
  assert(sql_buffer[sql_len-2] == '\0');
  rsqlp_lex_init_extra(&m_context, &m_scanner);
  // The non-const sql_buffer is only used to initialize the flex scanner. The
  // flex scanner shouldn't modify it either, but only because we have removed
  // the buffer-modifying code from the generated output (see Makefile rule
  // RestSQLLexer.l.cpp: RestSQLLexer.l.with-hold_char.cpp). For this reason,
  // the lexer still declares the buffer as non-const.
  m_buf = rsqlp__scan_buffer(sql_buffer, sql_len, m_scanner);
  // We don't want the NUL bytes that flex requires.
  uint our_buffer_len = sql_len - 2;
  m_sql = { (const char*)(sql_buffer), our_buffer_len };
}

#define assert_status(name) assert(m_status == Status::name)

bool
RestSQLPreparer::parse()
{
  if (m_status == Status::FAILED)
  {
    return false;
  }
  assert_status(INITIALIZED);
  m_status = Status::PARSING;
  int parse_result = rsqlp_parse(m_scanner);
  if (parse_result == 0)
  {
    assert(m_context.m_err_state == ErrState::NONE);
    m_status = Status::PARSED;
    return true;
  }
  m_status = Status::FAILED;
  // The rest is error handling.
  if (parse_result == 2)
  {
    /*
     * Bison parser reports OOM. Generally, this can happen in three situations:
     * 1) Stack depth would exceed YYINITDEPTH but bison doesn't know how to
     *    expand the stack. Since RSQLP_LTYPE_IS_TRIVIAL and
     *    RSQLP_STYPE_IS_TRIVIAL are defined in RestSQLParser.y, this case does
     *    not apply to us.
     * 2) Stack depth would exceed YYMAXDEPTH.
     * 3) The allocator used by the parser returns NULL, indicating OOM. Since
     *    our ArenaAllocator does not return NULL but rather throws an exception
     *    on OOM, this case does not apply to us.
     * Therefore, we know that if we end up here, we are in case 2).
     */
    cerr << "Parser stack exceeded its maximum depth." << endl;
    m_status = Status::FAILED;
    return false;
  }
  assert(parse_result == 1);
  assert(m_context.m_err_state != ErrState::NONE);
  assert(m_sql.str <= m_context.m_err_pos);
  uint err_pos = m_context.m_err_pos - m_sql.str;
  uint err_stop = err_pos + m_context.m_err_len;
  assert(err_pos <= m_sql.len);
  assert(err_stop <= m_sql.len + 1); // "Unexpected end of input" marks the
                                     // character directly after the end.
  const char* msg = NULL;
  bool print_statement = true;
  switch (m_context.m_err_state)
  {
  case ErrState::LEX_NUL:
    msg = "Unexpected null byte.";
    break;
  case ErrState::LEX_U_ILLEGAL_BYTE:
    msg = "Bytes 0xf8-0xff are illegal in UTF-8.";
    break;
  case ErrState::LEX_U_OVERLONG:
    msg = "Overlong UTF-8 encoding.";
    break;
  case ErrState::LEX_U_TOOHIGH:
    msg = "Unicode code points above U+10FFFF are invalid.";
    break;
  case ErrState::LEX_U_SURROGATE:
    msg = "Unicode code points U+D800 -- U+DFFF are invalid, as they correspond to UTF-16 surrogate pairs.";
    break;
  case ErrState::LEX_NONBMP_IDENTIFIER:
    msg = "Unicode code points above U+FFFF are not allowed in MySQL identifiers.";
    break;
  case ErrState::LEX_UNIMPLEMENTED_KEYWORD:
    msg = "Unimplemented keyword. If this was intended as an identifier, use backtick quotation.";
    break;
  case ErrState::LEX_TOO_LONG_IDENTIFIER:
    /*
     * MySQL will happily truncate an identifier that is too long, but does not
     * check that truncation happens at character boundaries. For identifiers
     * containing multi-byte UTF-8 sequences, such truncation can result in an
     * identifier with a name that is illegal UTF-8. We cannot allow such
     * identifiers since the REST server may need to return legal UTF-8. We also
     * cannot truncate in a "better" way than MySQL since we promise to either
     * produce a result equivalent with that produced by MySQL, or fail.
     * Therefore we have to fail, at least in some cases. We could check whether
     * truncation would result in legal UTF-8, but it is simpler both from
     * the implementer's and user's perspective to disallow all identifiers that
     * are too long.
     *
     * Note that MySQL allows for 256-byte aliases, but we restrict them to 64
     * bytes. It is simpler that way, as we allow only identifiers, not strings,
     * as aliases.
     */
    msg = "This identifier is too long. The limit is 64 bytes encoded as UTF-8.";
    break;
  case ErrState::LEX_INCOMPLETE_ESCAPE_SEQUENCE_IN_SINGLE_QUOTED_STRING:
    msg = "Incomplete escape sequence in single-quoted string";
    break;
  case ErrState::LEX_UNEXPECTED_EOI_IN_SINGLE_QUOTED_STRING:
    msg = "Unexpected end of input inside single-quoted string";
    break;
  case ErrState::LEX_ILLEGAL_TOKEN:
    msg = "Illegal token";
    break;
  case ErrState::LEX_UNEXPECTED_EOI_IN_QUOTED_IDENTIFIER:
    msg = "Unexpected end of input inside quoted identifier";
    break;
  case ErrState::LEX_U_ENC_ERR:
    msg = "Invalid UTF-8 encoding.";
    break;
  case ErrState::TOO_LONG_UNALIASED_OUTPUT:
    msg = "Unaliased select expression too long. Use `AS` to add an alias no more than 64 bytes long.";
    break;
  case ErrState::PARSER_ERROR:
    if (m_sql.len == 0)
    {
      fprintf(stderr, "Syntax error in SQL statement: Empty input\n");
      print_statement = false;
    }
    else if (err_pos == m_sql.len)
    {
      msg = "Unexpected end of input";
    }
    else
    {
      msg = "Unexpected at this point";
    }
    break;
  default:
    assert(false);
  }
  if (print_statement)
  {
    /*
     * Explain the syntax error by showing the message followed by a print of
     * the SQL statement with the problematic section underlined with carets.
     */
    cerr << "Syntax error in SQL statement: " << msg << endl;
    uint line_started_at = 0;
    for (uint pos = 0; pos <= m_sql.len; pos++)
    {
      if (line_started_at == pos)
      {
        cerr << "> ";
      }
      char c = m_sql.str[pos];
      bool is_eol = c == '\n';
      if (pos == m_sql.len)
      {
        if (m_sql.str[pos-1] != '\n')
        {
          cerr << '\n';
          is_eol = true;
        }
      }
      else if ( c != '\r')
      {
        cerr << c;
      }
      if (is_eol &&
         err_pos <= pos &&
         line_started_at <= err_stop)
      {
        cerr << "! ";
        uint err_marker_pos = line_started_at;
        // We use has_width to find the number of code points in the string
        // before and inside the error. This is a quite crude approximation of
        // the number of graphemes[†]. Thus, the error marker will be misaligned
        // whenever the number of graphemes do not match the number of code
        // points, e.g. when the string contains combining, zero-width or
        // control characters that are often used with emojis or with diacritics
        // that are unusual or NFD/NDKD normalized. This approximation is used
        // for the sake of simplicity and stability, as correctness is less
        // important in this case.
        // [†] https://unicode.org/glossary/#grapheme
        while (err_marker_pos < err_pos)
        {
          if (has_width(err_marker_pos))
          {
            cerr << " ";
          }
          err_marker_pos++;
        }
        while (err_marker_pos < err_stop &&
              (pos == err_pos
               ? err_marker_pos <= pos
               : err_marker_pos < pos))
        {
          if (has_width(err_marker_pos))
          {
            cerr << "^";
          }
          err_marker_pos++;
        }
        cerr << endl;
      }
      if (is_eol)
      {
        line_started_at = pos + 1;
      }
    }
  }
  return false;
}

/*
 * Return false if the position is a UTF-8 continuation byte and part of a
 * prefix of a correct UTF-8 multi-byte sequence, otherwise true.
 */
bool
RestSQLPreparer::has_width(uint pos)
{
  const char* s = m_sql.str;
  char c = s[pos];
  if ((c & 0xc0) != 0x80) return true;
  if (pos < 1) return true;
  c = s[pos - 1];
  if ((c & 0xe0) == 0xc0) return false;
  if ((c & 0xf0) == 0xe0) return false;
  if ((c & 0xf8) == 0xf0) return false;
  if ((c & 0xc0) != 0x80) return true;
  if (pos < 2) return true;
  c = s[pos - 2];
  if ((c & 0xf0) == 0xe0) return false;
  if ((c & 0xf8) == 0xf0) return false;
  if ((c & 0xc0) != 0x80) return true;
  if (pos < 3) return true;
  c = s[pos - 3];
  if ((c & 0xf8) == 0xf0) return false;
  return true;
}

bool
RestSQLPreparer::load()
{
  if (m_status == Status::FAILED)
  {
    return false;
  }
  assert_status(PARSED);
  m_status = Status::LOADING;
  /*
   * todo: During parsing, strings that are claimed to be column names were
   * assigned consecutive indexes as they were found. These indexes have already
   * been used to construct expressions in m_agg. Now that parsing is done and
   * we know the table name, we should look up the real column indexes in the
   * schema, check that the table and columns exist, and remap the indexes
   * inside both m_ast_root and m_agg.
   */

  // Load schema information and check that the table and columns exist.

  // Remap column indexes in m_ast_root and m_agg.

  // Load aggregates
  Outputs* outputs = m_context.ast_root.outputs;
  while (outputs != NULL)
  {
    if (outputs->is_agg)
    {
      assert(m_agg != NULL);
      int fun = outputs->aggregate.fun;
      AggregationAPICompiler::Expr* expr = outputs->aggregate.arg;
      switch (fun)
      {
      case T_AVG:
        m_agg->Sum(expr);
        m_agg->Count(expr);
        break;
      case T_COUNT:
        m_agg->Count(expr);
        break;
      case T_MAX:
        m_agg->Max(expr);
        break;
      case T_MIN:
        m_agg->Min(expr);
        break;
      case T_SUM:
        m_agg->Sum(expr);
        break;
      default:
        assert(false);
      }
    }
    outputs = outputs->next;
  }
  if (m_agg != NULL)
  {
    if (m_agg->getStatus() != AggregationAPICompiler::Status::PROGRAMMING)
    {
      m_status = Status::FAILED;
      return false;
    }
  }
  m_status = Status::LOADED;
  return true;
}

bool
RestSQLPreparer::compile()
{
  if (m_status == Status::FAILED)
  {
    return false;
  }
  assert_status(LOADED);
  m_status = Status::COMPILING;
  if (m_agg != NULL)
  {
    if (m_agg->compile())
    {
      assert(m_agg->getStatus() == AggregationAPICompiler::Status::COMPILED);
      m_status = Status::COMPILED;
      return true;
    }
    else
    {
      assert(m_agg->getStatus() == AggregationAPICompiler::Status::FAILED);
      m_status = Status::FAILED;
      return false;
    }
  }
  m_status = Status::COMPILED;
  return true;
}

bool
RestSQLPreparer::print()
{
  if (m_status == Status::FAILED)
  {
    return false;
  }
  assert_status(COMPILED);
  SelectStatement& ast_root = m_context.ast_root;
  cout << "SELECT\n";
  Outputs* outputs = ast_root.outputs;
  int out_count = 0;
  while (outputs != NULL)
  {
    cout << "  Out_" << out_count << ":" <<
      m_agg->quoted_identifier(outputs->output_name) << endl <<
      "   = ";
    if (outputs->is_agg)
    {
      int pr;
      switch (outputs->aggregate.fun)
      {
      case T_AVG:
        cout << "CLIENT-SIDE CALCULATION: ";
        pr = m_agg->Sum(outputs->aggregate.arg);
        cout << "A" << pr << ":";
        m_agg->print_aggregate(pr);
        cout << " / ";
        pr = m_agg->Count(outputs->aggregate.arg);
        break;
      case T_COUNT:
        pr = m_agg->Count(outputs->aggregate.arg);
        break;
      case T_MAX:
        pr = m_agg->Max(outputs->aggregate.arg);
        break;
      case T_MIN:
        pr = m_agg->Min(outputs->aggregate.arg);
        break;
      case T_SUM:
        pr = m_agg->Sum(outputs->aggregate.arg);
        break;
      default:
        // Unknown aggregate function
        assert(false);
      }
      cout << "A" << pr << ":";
      m_agg->print_aggregate(pr);
      cout << endl;
    }
    else
    {
      LexString col_name = outputs->col_name;
      int col_idx = column_name_to_idx(col_name);
      cout << "C" << col_idx << ":" <<
        m_agg->quoted_identifier(col_name) << endl;
    }
    out_count++;
    outputs = outputs->next;
  }
  cout << "FROM " << ast_root.table << endl;
  struct ConditionalExpression* where = ast_root.where_expression;
  if (where != NULL)
  {
    cout << "WHERE" << endl;
    print(where, LexString{NULL, 0});
  }
  struct GroupbyColumns* groupby = ast_root.groupby_columns;
  if (groupby != NULL)
  {
    cout << "GROUP BY" << endl;
    while (groupby != NULL)
    {
      auto col_name = groupby->col_name;
      auto col_idx = column_name_to_idx(col_name);
      cout << "  C" << col_idx << ":" << m_agg->quoted_identifier(col_name) <<
        endl;
      groupby = groupby->next;
    }
  }
  struct OrderbyColumns* orderby = ast_root.orderby_columns;
  if (orderby != NULL)
  {
    cout << "ORDER BY" << endl;
    while (orderby != NULL)
    {
      LexString col_name = orderby->col_name;
      int col_idx = column_name_to_idx(col_name);
      bool ascending = orderby->ascending;
      cout << "  C" << col_idx << ":" << m_agg->quoted_identifier(col_name) <<
        (ascending ? " ASC" : " DESC") << endl;
      orderby = orderby->next;
    }
  }
  cout << endl;
  if (m_agg != NULL)
  {
    m_agg->print_program();
  }
  else
  {
    printf("No aggregation program.\n\n");
  }
  return true;
}

void
RestSQLPreparer::print(struct ConditionalExpression* ce, LexString prefix)
{
  const char* opstr = NULL;
  bool prefix_op = false;
  switch (ce->op)
  {
  case T_IDENTIFIER:
    cout << ce->identifier << endl;
    return;
  case T_STRING:
    {
      cout << "STRING: ";
      for (uint i = 0; i < ce->string.len; i++)
      {
        char c = ce->string.str[i];
        if ( 0x21 <= c && c <= 0x7E && c != '<' && c != '>')
        {
          cout << c;
        }
        else
        {
          static const char* hex = "0123456789ABCDEF";
          cout << "<" << hex[(c >> 4) & 0xF] << hex[c & 0xF] << ">";
        }
      }
      cout << endl;
      return;
    }
  case T_INT:
    cout << ce->constant_integer << endl;
    return;
  case T_OR:
    opstr = "OR";
    break;
  case T_XOR:
    opstr = "XOR";
    break;
  case T_AND:
    opstr = "AND";
    break;
  case T_NOT:
    opstr = "NOT";
    prefix_op = true;
    break;
  case T_EQUALS:
    opstr = "=";
    break;
  case T_GE:
    opstr = ">=";
    break;
  case T_GT:
    opstr = ">";
    break;
  case T_LE:
    opstr = "<=";
    break;
  case T_LT:
    opstr = "<";
    break;
  case T_NOT_EQUALS:
    opstr = "!=";
    break;
  case T_IS:
    {
      cout << "IS" << endl <<
        prefix << "+- ";
      LexString prefix_arg = prefix.concat(LexString{"|  ", 3}, m_aalloc);
      print(ce->is.arg, prefix_arg);
      cout << prefix << "\\- ";
      if (ce->is.null == true)
      {
        cout << "NULL" << endl;
        return;
      }
      if (ce->is.null == false)
      {
        cout << "NOT NULL" << endl;
        return;
      }
      assert(false);
    }
  case T_BITWISE_OR:
    opstr = "BITWISE-OR (|)";
    break;
  case T_BITWISE_AND:
    opstr = "&";
    break;
  case T_BITSHIFT_LEFT:
    opstr = "<<";
    break;
  case T_BITSHIFT_RIGHT:
    opstr = ">>";
    break;
  case T_PLUS:
    opstr = "+";
    break;
  case T_MINUS:
    if (ce->args.left == NULL)
    {
      cout << "NEGATION" << endl <<
        prefix << "\\- ";
      LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_aalloc);
      print(ce->args.right, prefix_arg);
      return;
    }
    opstr = "-";
    break;
  case T_MULTIPLY:
    opstr = "*";
    break;
  case T_DIVIDE:
    opstr = "/";
    break;
  case T_MODULO:
    opstr = "%";
    break;
  case T_BITWISE_XOR:
    opstr = "^";
    break;
  case T_EXCLAMATION:
    opstr = "!";
    prefix_op = true;
    break;
  case T_INTERVAL:
    {
      cout << "INTERVAL" << endl <<
        prefix << "+- ";
      LexString prefix_arg = prefix.concat(LexString{"|  ", 3}, m_aalloc);
      print(ce->interval.arg, prefix_arg);
      cout << prefix << "\\- " <<
        interval_type_name(ce->interval.interval_type) << endl;
      return;
    }
  case T_DATE_ADD:
    opstr = "DATE_ADD";
    break;
  case T_DATE_SUB:
    opstr = "DATE_SUB";
    break;
  case T_EXTRACT:
    {
      cout << "EXTRACT" << endl <<
        prefix << "+- " <<
        interval_type_name(ce->extract.interval_type) << endl <<
        prefix << "\\- ";
      LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_aalloc);
      print(ce->extract.arg, prefix_arg);
      return;
    }
  default:
    // Unknown operator
    assert(false);
  }
  if (prefix_op)
  {
    cout << opstr << endl <<
         prefix << "\\- ";
    LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_aalloc);
    print(ce->args.left, prefix_arg);
  }
  else
  {
    cout << opstr << endl <<
      prefix << "+- ";
    LexString prefix_left = prefix.concat(LexString{"|  ", 3}, m_aalloc);
    print(ce->args.left, prefix_left);
    cout << prefix << "\\- ";
    LexString prefix_right = prefix.concat(LexString{"   ", 3}, m_aalloc);
    print(ce->args.right, prefix_right);
  }
}

const char* interval_type_name(int interval_type)
{
  switch (interval_type)
  {
  case T_MICROSECOND: return "MICROSECOND";
  case T_SECOND: return "SECOND";
  case T_MINUTE: return "MINUTE";
  case T_HOUR: return "HOUR";
  case T_DAY: return "DAY";
  case T_WEEK: return "WEEK";
  case T_MONTH: return "MONTH";
  case T_QUARTER: return "QUARTER";
  case T_YEAR: return "YEAR";
  case T_SECOND_MICROSECOND: return "SECOND_MICROSECOND";
  case T_MINUTE_MICROSECOND: return "MINUTE_MICROSECOND";
  case T_MINUTE_SECOND: return "MINUTE_SECOND";
  case T_HOUR_MICROSECOND: return "HOUR_MICROSECOND";
  case T_HOUR_SECOND: return "HOUR_SECOND";
  case T_HOUR_MINUTE: return "HOUR_MINUTE";
  case T_DAY_MICROSECOND: return "DAY_MICROSECOND";
  case T_DAY_SECOND: return "DAY_SECOND";
  case T_DAY_MINUTE: return "DAY_MINUTE";
  case T_DAY_HOUR: return "DAY_HOUR";
  case T_YEAR_MONTH: return "YEAR_MONTH";
  default: assert(false);
  }
}

uint
RestSQLPreparer::column_name_to_idx(LexString col_name)
{
  for (uint i=0; i < m_identifiers.size(); i++)
  {
    if (m_identifiers[i] == col_name)
    {
      return i;
    }
  }
  m_identifiers.push(col_name);
  return m_identifiers.size()-1;
}

LexString
RestSQLPreparer::column_idx_to_name(uint col_idx)
{
  assert(col_idx < m_identifiers.size());
  return m_identifiers[col_idx];
}

RestSQLPreparer::~RestSQLPreparer()
{
  rsqlp__delete_buffer(m_buf, m_scanner);
  rsqlp_lex_destroy(m_scanner);
}

void
RestSQLPreparer::Context::set_err_state(ErrState state,
                                  char* err_pos,
                                  uint err_len)
{
  if (m_err_state == ErrState::NONE)
  {
    m_err_state = state;
    m_err_pos = err_pos;
    m_err_len = err_len;
  }
  else
  {
    /*
     * We want to save the error with the left-most position or, if two errors
     * have the same position, the shorter (more low-level) error. However,
     * above we actually save the error detected first. Presumably, that's the
     * same thing, but here we assert so.
     */
    assert((m_err_pos < err_pos) ||
           (m_err_pos == err_pos &&
            m_err_len <= err_len));
  }
}

AggregationAPICompiler*
RestSQLPreparer::Context::get_agg()
{
  if (m_parser.m_agg)
  {
    return m_parser.m_agg;
  }
  RestSQLPreparer* _this = &m_parser;
  std::function<int(LexString)> column_name_to_idx =
    [_this](LexString ls) -> uint
    {
      for (uint i=0; i < _this->m_identifiers.size(); i++)
      {
        if (ls == LexString(_this->m_identifiers[i]))
        {
          return i;
        }
      }
      _this->m_identifiers.push(ls);
      return _this->m_identifiers.size()-1;
    };
  std::function<LexString(uint)> column_idx_to_name =
    [_this](uint idx) -> LexString
    {
      assert(idx < _this->m_identifiers.size());
      return _this->m_identifiers[idx];
    };

  /*
   * The aggregator uses the same arena allocator as the RestSQLPreparer object
   * because they are both working in the prepare phase. After loading and
   * compilation, a new object will be crafted that holds the information
   * necessary for execution and post-processing.
   */
  m_parser.m_agg = new AggregationAPICompiler(column_name_to_idx,
                                  column_idx_to_name,
                                  m_parser.m_aalloc);
  return m_parser.m_agg;
}

ArenaAllocator*
RestSQLPreparer::Context::get_allocator()
{
  return m_parser.m_aalloc;
}
