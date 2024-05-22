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
#include "AggregationAPICompiler.hpp"
#include "ArenaAllocator.hpp"
using std::cout;
using std::cerr;
using std::endl;

int
main()
{

  ArenaAllocator aalloc;

  cout << "Compilation example 1:" << endl << endl;
  AggregationAPICompiler a(
    [](uint col_idx) -> const char*
    {
      static const char* col_names[3] = {"apples", "oranges", "kiwis"};
      assert(col_idx<3);
      return col_names[col_idx];
    },
    cout,
    cerr,
    &aalloc);
  AggregationAPICompiler::Expr* apples = a.Load(0);
  AggregationAPICompiler::Expr* oranges = a.Load(1);
  AggregationAPICompiler::Expr* kiwis = a.Load(2);
  a.Max(apples);
  a.Sum(a.Add(apples,a.Mul(oranges, kiwis)));
  a.Min(a.Minus(apples,a.Mul(oranges, kiwis)));
  AggregationAPICompiler::Expr* orki = a.Mul(oranges, kiwis);
  a.Sum(a.Add(apples,orki));
  a.Min(a.Minus(apples,orki));
  a.Count(a.Add(apples,apples));
  a.Count(a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,apples)))))))))))))))));
  a.Min(a.Add(apples,a.Add(apples,a.Add(apples,apples))));
  a.Max(a.Add(a.Add(a.Add(apples,apples),apples),apples));
  a.Count(a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,a.Add(apples,apples)))))));
  a.Count(a.Add(apples,a.Add(apples,apples)));
  if (!a.compile())
  {
    cout << "Failed to compile." << endl;
    return 1;
  }
  a.print_aggregates();
  cout << endl;
  a.print_program();

  cout << endl << endl
       << "Compilation example 2:" << endl << endl;
  AggregationAPICompiler b(
    [](uint col_idx) -> const char*
    {
      assert(col_idx == 0);
      return "c";
    },
    cout,
    cerr,
    &aalloc);
  auto c = b.Load(0);
  b.Sum(b.Add(c,c));
  b.Sum(b.Minus(c,c));
  b.Sum(b.Add(b.Add(c,c),b.Add(c,c)));
  b.Sum(b.Minus(b.Add(c,c),b.Add(c,c)));
  b.Sum(b.Add(b.Add(c,c),c));
  b.Sum(b.Add(c,b.Add(c,b.Add(c,b.Add(c,b.Add(c,b.Add(c,b.Add(c,b.Add(c,b.Add(c,b.Add(c,c)))))))))));
  b.Sum(b.Add(b.Add(b.Add(b.Add(b.Add(b.Add(b.Add(b.Add(b.Add(b.Add(c,c),c),c),c),c),c),c),c),c),c));
  if (!b.compile())
  {
    cout << "Failed to compile." << endl;
    return 1;
  }
  b.print_aggregates();
  cout << endl;
  b.print_program();

  cout << endl;

  return 0;
}
