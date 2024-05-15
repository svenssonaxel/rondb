#!/usr/bin/env bash

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

# Set up paths
runtime_output="$1"
[ -n "$runtime_output" ] || {
    echo "Usage: $0 <runtime_output_directory>"
    exit 1
}
[ -d "$runtime_output" ] || {
    echo "Runtime output directory not found at $runtime_output"
    exit 1
}
ParseCompileTest () { "$runtime_output/ParseCompileTest" "$@"; }
[ -x "$runtime_output/ParseCompileTest" ] || {
    echo "ParseCompileTest not found at $runtime_output/ParseCompileTest"
    exit 1
}
KeywordsUnitTest () { "$runtime_output/KeywordsUnitTest" "$@"; }
[ -x "$runtime_output/KeywordsUnitTest" ] || {
    echo "KeywordsUnitTest not found at $runtime_output/KeywordsUnitTest"
    exit 1
}
APICompileTest () { "$runtime_output/APICompileTest" "$@"; }
[ -x "$runtime_output/APICompileTest" ] || {
    echo "APICompileTest not found at $runtime_output/APICompileTest"
    exit 1
}

runtest()
{
    local title ec
    title="$1"
    shift;
    echo -e "\n===== $title =====";
    "$@" 2>&1
    ec="$?"
    echo -e "=> Exit code: $ec"
}

# It's hard to make git work with files containing null bytes, so let's convert
# them to uppercase N in such test cases.
explainNUL()
{
    local ec tf
    tf="$(mktemp)"
    "$@" >"$tf" 2>&1
    ec=$?
    tr '\0' N < "$tf"
    rm "$tf"
    return $ec
}

tmpfile=$(mktemp)
trap 'rm -f $tmpfile' EXIT

# Unit tests

runtest "Keywords unit test" KeywordsUnitTest

# Test errors

runtest "Null byte at beginning" explainNUL ParseCompileTest 'select a from tbl;' 0
runtest "Null byte in identifier" explainNUL ParseCompileTest 'select a  from tbl;' 8
runtest "Null byte at end" explainNUL ParseCompileTest 'select a from tbl;' 17
runtest "Illegal UTF-8 byte" ParseCompileTest $'select a\xfa from tbl;'
runtest "Illegal 0xc0 UTF-8 byte" ParseCompileTest $'select a\xc0 from tbl;'
runtest "Illegal 0xc1 UTF-8 byte" ParseCompileTest $'select a\xc1 from tbl;'
# a = U+0061 = 01100001 ≈ 11000001 10100001 = c1 a1
runtest "UTF-8 overlong 2-byte sequence" ParseCompileTest $'select a\xc1\xa1 from tbl;'
# ö = U+00f6 = 11000011 10110110 ≈ 11100000 10000011 10110110 = e0 83 b6
runtest "UTF-8 overlong 3-byte sequence" ParseCompileTest $'select a\xe0\x83\xb6 from tbl;'
# U+123456 = 11110100 10100011 10010001 10010110 = f4 a3 91 96
runtest "Too high code point (U+123456)" ParseCompileTest $'select `a\xf4\xa3\x91\x96` from tbl;'
# U+dead = 11101101 10111010 10101101 = ed ba ad
runtest "Surrogate (U+dead)" ParseCompileTest $'select `a\xed\xba\xad` from tbl;'
# U+ffff < U+204d7 = 𠓗 = 11110000 10100000 10010011 10010111 = f0 a0 93 97
runtest "Non-BMP UTF-8 in identifier" ParseCompileTest $'select `a\xf0\xa0\x93\x97` from tbl;'
runtest "Unimplemented keyword used as unquoted identifier" ParseCompileTest $'select zone from tbl;'
runtest "Incomplete escape sequence in single-quoted string" ParseCompileTest $'select a from tbl where \x27hello\x5c'
runtest "Unexpected EOI in single-quoted string" ParseCompileTest $'select a from tbl where \x27hello'
runtest "Illegal token" ParseCompileTest 'select #a from tbl;'
runtest "EOI inside quoted identifier" ParseCompileTest 'select `a'
runtest "EOI inside escaped identifier" ParseCompileTest 'select `bc``de'
# å = U+00e5 = 11000011 10100101 = c3 a5
runtest "UTF-8 2-byte sequence with illegal 2nd byte" ParseCompileTest $'select `a\xc3` from tbl;'
runtest "UTF-8 2-byte sequence at EOI with 2nd byte missing" ParseCompileTest $'select `a` from `table\xc3'
# ᚱ = U+16b1 = 11100001 10011010 10110001 = e1 9a b1
runtest "UTF-8 3-byte sequence with illegal 2nd byte" ParseCompileTest $'select `a\xe1` from tbl;'
runtest "UTF-8 3-byte sequence with illegal 3rd byte" ParseCompileTest $'select `a\xe1\x9a` from tbl;'
runtest "UTF-8 3-byte sequence at EOI with 2nd byte missing" ParseCompileTest $'select `a` from `table\xe1'
runtest "UTF-8 3-byte sequence at EOI with 3rd byte missing" ParseCompileTest $'select `a` from `table\xe1\x9a'
# 𠓗 = U+204d7 = 11110000 10100000 10010011 10010111 = f0 a0 93 97
runtest "UTF-8 4-byte sequence with illegal 2nd byte" ParseCompileTest $'select `a\xf0` from tbl;'
runtest "UTF-8 4-byte sequence with illegal 3rd byte" ParseCompileTest $'select `a\xf0\xa0` from tbl;'
runtest "UTF-8 4-byte sequence with illegal 4th byte" ParseCompileTest $'select `a\xf0\xa0\x93` from tbl;'
runtest "UTF-8 4-byte sequence at EOI with 2nd byte missing" ParseCompileTest $'select `a` from `table\xf0'
runtest "UTF-8 4-byte sequence at EOI with 3rd byte missing" ParseCompileTest $'select `a` from `table\xf0\xa0'
runtest "UTF-8 4-byte sequence at EOI with 4th byte missing" ParseCompileTest $'select `a` from `table\xf0\xa0\x93'
runtest "Rogue UTF-8 continuation byte" ParseCompileTest $'select a\x89 from tbl;'
runtest "Empty input" ParseCompileTest ''
runtest "Invalid token at beginning" ParseCompileTest $'\033select a from tbl;'
runtest "Control character in unquoted identifier" ParseCompileTest $'select a\005 from tbl;'
runtest "Invalid token at end" ParseCompileTest $'select a from tbl;\177'
runtest "Unexpected end of input" ParseCompileTest 'select a from tbl'
runtest "Unexpected at this point" ParseCompileTest $'select a `bcde` from tbl;'
runtest "Unexpected before newline" ParseCompileTest $'select a `bcde`\n from tbl;'
runtest "Unexpected after newline" ParseCompileTest $'select a \n`bcde` from tbl;'
runtest "Unexpected with ending newline" ParseCompileTest $'select a `bcde` from tbl;\n'
runtest "Unexpected with newline at start" ParseCompileTest $'\nselect a `bcde` from tbl;'
runtest "Unexpected containing newline" ParseCompileTest $'select a `bc\nde` from tbl;'
runtest "Unexpected containing two newlines" ParseCompileTest $'select a `b\ncd\ne` from tbl;'
runtest "Unexpected escaped identifier" ParseCompileTest $'select a `bc``de` from tbl;'
runtest "Two escaped identifiers, 1st unexpected" ParseCompileTest $'select a `bc``de` from `fg``h``i`;'
runtest "Two escaped identifiers, 2nd unexpected" ParseCompileTest $'select a, `bc``de` from tbl `fg``h``i`;'
runtest "Error marker alignment after 2-byte UTF-8 characters" ParseCompileTest $'select a\n      ,räksmörgås räksmörgås\nfrom tbl;'
runtest "Error marker alignment after 3-byte UTF-8 character" ParseCompileTest $'select a\n      ,ᚱab ᚱab\nfrom tbl;'
runtest "Incomplete escape sequence in single-quoted string" ParseCompileTest $"select a from tbl where 'word\\"
runtest "Unexpected end of input inside single-quoted string" ParseCompileTest $"select a from tbl where 'word"
runtest "date_add not supported in output" ParseCompileTest $'select date_add(col1, interval 1 day) from tbl;'
runtest "Parser stack exhausted" ParseCompileTest $'
select col from tbl where
((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((
'
runtest "Too long unquoted identifier in column name" ParseCompileTest $'
select aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa from tbl;'
runtest "Too long quoted identifier in column name" ParseCompileTest $'
select `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` from tbl;'
runtest "Too long quoted identifier in column name, although characters < 64" ParseCompileTest $'
select `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaöööööööööö` from tbl;'
runtest "Too long unquoted alias" ParseCompileTest $'
select a as aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa from tbl;'
runtest "Too long quoted alias" ParseCompileTest $'
select a as `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` from tbl;'
runtest "Too long unquoted identifier in WHERE" ParseCompileTest $'
select a from tbl where aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;'
runtest "Too long quoted identifier in WHERE" ParseCompileTest $'
select a from tbl where `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`;'
runtest "Too long unquoted identifier in GROUP BY" ParseCompileTest $'
select a from tbl group by aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;'
runtest "Too long quoted identifier in GROUP BY" ParseCompileTest $'
select a from tbl group by `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`;'
runtest "Too long unquoted identifier in ORDER BY" ParseCompileTest $'
select a from tbl ORDER BY aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;'
runtest "Too long quoted identifier in ORDER BY" ParseCompileTest $'
select a from tbl ORDER BY `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`;'
runtest "Too long unaliased select expression" ParseCompileTest $'
select max ((((((((((((((((((((((((((((((a)))))))))))))))))))))))))))))) from tbl;'

# Test successes

runtest "APICompileTest" APICompileTest
runtest "Simple" ParseCompileTest 'select a from tbl;'
runtest "Arithmetics" ParseCompileTest 'select a, count(b), min((b+c)/(d-e)), max(d*e/f-b/c/f), count(b/c/f+d*e/f*(b+c)) from tbl;'
runtest "Quoted ID" ParseCompileTest $'select a, `b`, `c``c`, count(`d`), min((`e``e`+`f`)/(g-`h`)) from tbl;'
# å = U+00e5 = c3 a5
runtest "UTF-8 2-byte character in unquoted identifier" ParseCompileTest $'select a\xc3\xa5 from tbl;'
runtest "UTF-8 2-byte character in quoted identifier" ParseCompileTest $'select `a\xc3\xa5` from tbl;'
# ᚱ = U+16b1 = 11100001 10011010 10110001 = e1 9a b1
runtest "UTF-8 3-byte character in unquoted identifier" ParseCompileTest $'select a\xe1\x9a\xb1 from tbl;'
runtest "UTF-8 3-byte character in quoted identifier" ParseCompileTest $'select `a\xe1\x9a\xb1` from tbl;'
runtest "Control character in quoted identifier" ParseCompileTest $'select `a\005` from tbl;'
runtest "has_item regression" ParseCompileTest '
select count(a+a+a+a+a+a+a+a+a+a+a+a+a+a+a+a+a)
      ,max(d*e/f-b/c/f)
      ,min((ee+f)/(g-h))
from tbl;'
runtest "Alias" ParseCompileTest $'select a, b as c, `d` as `e``e`, `f``f` as g, count(h+i/`j``j`) as k from tbl;'
runtest "Integer constants" ParseCompileTest $'
select col1
      ,sum(col2+543)
      ,max(col3-792) as subtraction
from tbl;'
runtest "Avg" ParseCompileTest $'
select col1
      ,sum(col2)
      ,max(col2)
      ,avg(col2)
      ,min(col2)
      ,count(col2)
      ,col3 as lastcol
from tbl
group by col1, col3;'
runtest "Count all" ParseCompileTest $'
select col1
      ,sum(col2)
      ,count(*)
      ,max(col2)
      ,count(col2)
      ,count(*) as count_all
      ,min(col2)
      ,col3 as lastcol
from tbl
group by col1, col3;'
runtest "Condition" ParseCompileTest $'
select col1
      ,sum(col2)
      ,max(col3)
      ,col4 as lastcol
from tbl
where col2=col3+5 xor
  col2 <> col4 and
  !(col2 >= 57)
group by col1, col3;'
runtest "Complex operator precedence" ParseCompileTest $'
select a
from tbl
where (
a or a || a xor a and a && not a = a >= a > a <= a < a != a <> a is null | a & a << a >> a + a - a * a / a % a ^ ! a
) AND (
! a ^ a % a / a * a - a + a >> a << a & a | a is not null <> a != a < a <= a > a >= a = not a && a and a xor a || a or a
);'
cat > "$tmpfile" <<"EOF"
select col from tbl where
'0x00=\0,0x27=\',0x08=\b,0x0a=\n,0x0d=\r,0x09=\t,0x1a=\Z,bs=\\,bs_perc=\%,bs_ul=\_,Q=\Q,7=\7'
is not null;
EOF
runtest "Single quoted strings" ParseCompileTest "$(cat "$tmpfile")"
runtest "Compound strings" ParseCompileTest "
select col from tbl where 'hello'
  ' world';"
runtest "date_add" ParseCompileTest $'select col from tbl where date_add(col1, interval 1 day) is not null;'
runtest "date_sub" ParseCompileTest $'select col from tbl where date_add(\x272024-05-06\x27, interval 23 microsecond) > col2;'
runtest "extract" ParseCompileTest $'select col from tbl where extract(year from \x272024-05-06\x27) <= col2;'
runtest "order by" ParseCompileTest $'select col1 from tbl order by col1;'
runtest "order by 2 columns" ParseCompileTest $'select col1 from tbl order by col1, col2;'
runtest "group and order by" ParseCompileTest $'select col1, `col #2`, max(col3) from tbl group by col1, `col #2` order by col1, `col #2`;'
runtest "order by ASC/DESC" ParseCompileTest $'select col1 from tbl order by col1, col2 ASC, col3 DESC, col4;'
runtest "Unimplemented keyword used as quoted identifier" ParseCompileTest $'select `zone` from tbl;'
runtest "Negation" ParseCompileTest $'
select col1
      ,min(-543)
      ,sum(col2+-543)
      ,max(col3--792) as subtraction
      ,count(a---(b---1))
from tbl
where col1 < -45
and col2 > -----col3
and col3 > ----7;'
runtest "Almost too long unaliased select expression" ParseCompileTest $'
select max((((((((((((((((((((((((((((((a)))))))))))))))))))))))))))))) from tbl;'
runtest "Aliased expression that would be too long without alias" ParseCompileTest $'
select max ((((((((((((((((((((((((((((((a)))))))))))))))))))))))))))))) as `max(a)` from tbl;'

# Complex queries

cat > "$tmpfile" <<"EOF"
select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        lineitem
where
        l_shipdate <= date_sub('1998-12-01', interval '90' day)
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;
EOF
runtest "dbt3-1.10/queries/mysql/1_2.sql" ParseCompileTest "$(cat "$tmpfile")"
