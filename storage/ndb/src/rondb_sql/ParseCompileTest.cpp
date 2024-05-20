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

#include <stdio.h>
#include <stdlib.h>
#include <stdexcept>
#include <assert.h>
#include "RonDBSQLPreparer.hpp"
#include "ArenaAllocator.hpp"
using std::cout;
using std::endl;

int
main(int argc, char** argv)
{

  ArenaAllocator aalloc;

  assert(argc==2 || argc==3);

  // bison requires two NUL bytes at end
  char* cmdline_arg = argv[1];
  int cmdline_arg_len = strlen(cmdline_arg);
  char* parse_str = aalloc.alloc<char>(cmdline_arg_len + 2);
  size_t parse_len = (cmdline_arg_len+2) * sizeof(char);
  memcpy(parse_str, cmdline_arg, cmdline_arg_len);
  parse_str[cmdline_arg_len] = '\0';
  parse_str[cmdline_arg_len+1] = '\0';
  if (argc==3)
  {
    // Test a string containing a null byte at a certain position
    parse_str[atoi(argv[2])] = '\0';
  }

  try
  {
    struct ExecutionParameters params;
    params.sql_buffer = parse_str;
    params.sql_len = parse_len;
    params.aalloc = &aalloc;
    params.mode = ExecutionParameters::ExecutionMode::EXPLAIN_OVERRIDE;
    params.explain_output_stream = &std::cout;
    params.err_output_stream = &cout;
    RonDBSQLPreparer preparer(params);
    preparer.execute();
  }
  catch (std::runtime_error& e)
  {
    cout << "Error: " << e.what() << endl;
    return 1;
  }

  if (argc == 1)
  {
    cout << "Usage: " << argv[0] << " SQL_QUERY_1 [ SQL_QUERY_2 ... ]" << endl;
  }

  return 0;
}
