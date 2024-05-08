# Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

all: \
 ParseCompileTest \
 APICompileTest \
 KeywordsUnitTest \


RestSQLParser.y.raw.cpp RestSQLParser.y.hpp: \
 DynamicArray.hpp \
 Makefile \
 RestSQLParser.y \
 RestSQLPreparer.hpp \

	bison -Wall -Wcounterexamples -Wdangling-alias -Werror RestSQLParser.y

RestSQLParser.y.cpp: RestSQLParser.y.raw.cpp
# We use `%define api.location.type` to declare a custom location type. However,
# bison does not have any option for the default value of that type. So,
# unfortunately we have to edit the generated file the hacky way.
	[ "$$(grep -Ec '  = \{ 1, 1, 1, 1 \}' RestSQLParser.y.raw.cpp)" = 1 ]
	sed -r "s/  = \{ 1, 1, 1, 1 \}/  = { NULL, 0 }/" $< > $@
	if diff -q $< $@; then false; else true; fi

RestSQLLexer.l.with-hold_char.cpp RestSQLLexer.l.hpp: \
 DynamicArray.hpp \
 Makefile \
 RestSQLLexer.l \
 RestSQLParser.y.hpp \
 RestSQLPreparer.hpp \

# flex has no option to treat warnings as errors, so we use a temporary file for
# that.
	if flex RestSQLLexer.l 2> RestSQLLexer.l.err; then \
	  true; \
	else \
	  echo "flex exit code=$$?" >> RestSQLLexer.l.err; \
	fi
	if grep -F '' RestSQLLexer.l.err; then \
	  rm -f RestSQLLexer.l.err \
	        RestSQLLexer.l.with-hold_char.cpp \
	        RestSQLLexer.l.hpp; \
	  exit 1; \
	else \
	  rm -f RestSQLLexer.l.err; \
	fi

RestSQLLexer.l.cpp: RestSQLLexer.l.with-hold_char.cpp

# flex temporarily inserts a null byte after each token it scans so that the
# user can comfortably use the token as a null-terminated C string. We don't
# need that, and we also don't want it, for two reasons:
# 1) In case of a parse error, we can't use the underlying buffer to print the
#    entire SQL statement since it may have a null byte somewhere in the middle.
# 2) Unnecessarily saving a character, writing a null byte and restoring it, for
#    every token, could have a slight performance impact.
# Unfortunately, flex does not provide an option to turn off this feature.
# Fortunately, removing this unwanted feature from the flex output in an
# incredibly hacky way is both possible and easy, so that's what we'll do. First
# though, we need to make sure no such patterns are in the source file.
	if grep -qE "yy_hold_char| = '.0';" RestSQLLexer.l; then false; else true; fi
	sed -r "/yy_hold_char/d; / = '.0';/d;" $< > $@

RestSQLParser.y.o: RestSQLParser.y.cpp \
 AggregationAPICompiler.hpp \
 DynamicArray.hpp \
 LexString.hpp \
 RestSQLLexer.l.hpp \
 RestSQLParser.y.hpp \
 RestSQLPreparer.hpp \

	g++ -c -o $@ $<

RestSQLLexer.l.o: RestSQLLexer.l.cpp \
 DynamicArray.hpp \
 RestSQLParser.y.hpp \
 RestSQLPreparer.hpp \

	g++ -c -o $@ $<

RestSQLPreparer.o: RestSQLPreparer.cpp \
 AggregationAPICompiler.hpp \
 ArenaAllocator.hpp \
 DynamicArray.hpp \
 LexString.hpp \
 RestSQLLexer.l.hpp \
 RestSQLParser.y.hpp \
 RestSQLPreparer.hpp \

	g++ -c -o $@ $<

AggregationAPICompiler.o: AggregationAPICompiler.cpp \
 AggregationAPICompiler.hpp \
 ArenaAllocator.hpp \
 DynamicArray.hpp \
 LexString.hpp \

	g++ -c -o $@ $<

LexString.o: LexString.cpp \
 LexString.hpp \

	g++ -c -o $@ $<

ArenaAllocator.o: ArenaAllocator.cpp \
 ArenaAllocator.hpp \

	g++ -c -o $@ $<

ParseCompileTest: ParseCompileTest.cpp \
 AggregationAPICompiler.hpp \
 AggregationAPICompiler.o \
 ArenaAllocator.hpp \
 ArenaAllocator.o \
 DynamicArray.hpp \
 LexString.hpp \
 LexString.o \
 RestSQLLexer.l.o \
 RestSQLParser.y.o \
 RestSQLPreparer.hpp \
 RestSQLPreparer.o \

	g++ -o $@ $< \
	 AggregationAPICompiler.o \
	 RestSQLPreparer.o \
	 RestSQLParser.y.o \
	 RestSQLLexer.l.o \
	 LexString.o \
	 ArenaAllocator.o

APICompileTest: APICompileTest.cpp \
 AggregationAPICompiler.hpp \
 AggregationAPICompiler.o \
 ArenaAllocator.hpp \
 ArenaAllocator.o \
 DynamicArray.hpp \
 LexString.hpp \
 LexString.o \
 RestSQLPreparer.hpp \

	g++ -o $@ $< AggregationAPICompiler.o LexString.o ArenaAllocator.o

KeywordsUnitTest: KeywordsUnitTest.cpp \
 Keywords.hpp \
 RestSQLParser.y.hpp \

	g++ -o $@ $<

clean:
	rm -f ParseCompileTest APICompileTest KeywordsUnitTest *.o *.y.* *.l.* RestSQLLexer.l.err
