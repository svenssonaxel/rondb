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

int
main()
{

  ArenaAllocator aalloc;

  printf("Compilation example 1:\n\n");
  char apples[10]; strcpy(apples, "apples");
  char oranges[10]; strcpy(oranges, "oranges");
  char kiwis[10]; strcpy(kiwis, "kiwis");
  LexString col_names_a[] =
  {
    LexString{apples, strlen(apples)},
    LexString{oranges, strlen(oranges)},
    LexString{kiwis, strlen(kiwis)}
  };
  AggregationAPICompiler a(
    [col_names_a](LexString ls) -> int
    {
      for (int i = 0; i < 3; i++)
      {
        if (ls == col_names_a[i])
        {
          return i;
        }
      }
      return -1;
    },
    [col_names_a](int idx) -> LexString
    {
      assert(idx>=0 && idx<3);
      return col_names_a[idx];
    },
    &aalloc);
  a.Max(apples);
  a.Sum(a.Add(apples,a.Mul(oranges, kiwis)));
  a.Min(a.Minus(apples,a.Mul(oranges, kiwis)));
  auto orki = a.Mul(oranges, kiwis);
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
    printf("Failed to compile.\n");
    return 1;
  }
  a.print_aggregates();
  printf("\n");
  a.print_program();

  printf("\nCompilation example 2:\n\n");
  char c_cstr[10]; strcpy(c_cstr, "c");
  LexString c{c_cstr, strlen(c_cstr)};
  AggregationAPICompiler b(
    [c](LexString ls) -> int
    {
      if (ls == c)
      {
        return 0;
      }
      return -1;
    },
    [c](int idx) -> LexString
    {
      assert(idx==0);
      return c;
    },
    &aalloc);
  b.Sum(b.Add("c","c"));
  b.Sum(b.Minus("c","c"));
  b.Sum(b.Add(b.Add("c","c"),b.Add("c","c")));
  b.Sum(b.Minus(b.Add("c","c"),b.Add("c","c")));
  b.Sum(b.Add(b.Add("c","c"),"c"));
  b.Sum(b.Add("c",b.Add("c",b.Add("c",b.Add("c",b.Add("c",b.Add("c",b.Add("c",b.Add("c",b.Add("c",b.Add("c","c")))))))))));
  b.Sum(b.Add(b.Add(b.Add(b.Add(b.Add(b.Add(b.Add(b.Add(b.Add(b.Add("c","c"),"c"),"c"),"c"),"c"),"c"),"c"),"c"),"c"),"c"));
  if (!b.compile())
  {
    printf("Failed to compile.\n");
    return 1;
  }
  b.print_aggregates();
  printf("\n");
  b.print_program();

  return 0;
}
