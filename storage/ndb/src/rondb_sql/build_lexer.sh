#!/usr/bin/env bash
set -euo pipefail

FLEX="$1"
SOURCE_RONDBSQLLEXER_L="$2"
TARGET_RONDBSQLLEXER_L_HPP="$3"
TARGET_RONDBSQLLEXER_L_CPP="$4"

[ -x "$FLEX" ]
[ -f "$SOURCE_RONDBSQLLEXER_L" ]

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# As we are not quite satisfied with the flex output, we let flex output to
# temporary files, allowing us to edit them before they are saved to the target
# location.

"$FLEX" \
    --header-file="$TMPDIR/RonDBSQLLexer.l.hpp" \
    --outfile="$TMPDIR/RonDBSQLLexer.l.cpp" \
    "$SOURCE_RONDBSQLLEXER_L" \
    2>&1 | \
    tee "$TMPDIR/RonDBSQLLexer.l.err"

# flex has no option to treat warnings as errors, so we use a temporary file for
# that.
if [ -s "$TMPDIR/RonDBSQLLexer.l.err" ]; then
    echo "flex produced non-empty output on stderr:"
    cat "$TMPDIR/RonDBSQLLexer.l.err"
    exit 1
fi

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

if grep -qE "yy_hold_char| = '.0';" "$SOURCE_RONDBSQLLEXER_L"; then
    echo "Source file must not contain patterns that need to be removed from the generated file:"
    grep -HEn "yy_hold_char| = '.0';" "$SOURCE_RONDBSQLLEXER_L"
    exit 1
fi

sed -r "/yy_hold_char/d; / = '.0';/d;" "$TMPDIR/RonDBSQLLexer.l.cpp" > "$TMPDIR/RonDBSQLLexer.l.fix_1.cpp"

if diff -q "$TMPDIR/RonDBSQLLexer.l.cpp" "$TMPDIR/RonDBSQLLexer.l.fix_1.cpp"; then
    echo "Editing to remove hold_char ineffective."
    exit 1
else
    echo "Confirmed that attempt to remove hold_char made some change."
fi

if grep -qE "yy_hold_char| = '.0';" "$TMPDIR/RonDBSQLLexer.l.fix_1.cpp"; then
    echo "Editing to remove hold_char ineffective."
    grep -HEn "yy_hold_char| = '.0';" "$TMPDIR/RonDBSQLLexer.l.fix_1.cpp"
    exit 1
fi

# Flex uses #line directives in the output file to aid error messages. Some of
# them refer to the source file $SOURCE_RONDBSQLLEXER_L and others to the output
# file. The latter are incorrect in two ways: We have removed some lines, making
# the line number incorrect, and we will move the file to the target below,
# making the file name incorrect. We use `awk` to fix those #line directives.

AWK_SCRIPT='
  # Handle #line directives specially
  /^#line/ {
    if ($3 == "\"" change_from_file "\"")
    {
      correct_line = NR + 1
      # Correct both file file name and line number.
      print "#line " correct_line " \"" change_to_file "\""
      next
    }
    else if ($3 == "\"" no_touch_file "\"")
    {
      # This file name is expected, and should remain. The line number can be
      # anything since it refers to another file, and should also remain.
      print
      next
    }
    else
    {
      # Unexpected file name
      print "Expected file name " change_from_file " or " no_touch_file " but got: " $0 | "cat >&2"
      exit 1
    }
  }
  # Default action, applied to lines other than #line directives is to change
  # nothing
  { print }
'

awk \
    -v change_from_file="$TMPDIR/RonDBSQLLexer.l.hpp" \
    -v change_to_file="$TARGET_RONDBSQLLEXER_L_HPP" \
    -v no_touch_file="$SOURCE_RONDBSQLLEXER_L" \
    "$AWK_SCRIPT" \
    "$TMPDIR/RonDBSQLLexer.l.hpp" \
    > "$TMPDIR/RonDBSQLLexer.l.fix_1.hpp"
awk \
    -v change_from_file="$TMPDIR/RonDBSQLLexer.l.cpp" \
    -v change_to_file="$TARGET_RONDBSQLLEXER_L_CPP" \
    -v no_touch_file="$SOURCE_RONDBSQLLEXER_L" \
    "$AWK_SCRIPT" \
    "$TMPDIR/RonDBSQLLexer.l.fix_1.cpp" \
    > "$TMPDIR/RonDBSQLLexer.l.fix_2.cpp"

# Success

mv "$TMPDIR/RonDBSQLLexer.l.fix_1.hpp" "$TARGET_RONDBSQLLEXER_L_HPP"
mv "$TMPDIR/RonDBSQLLexer.l.fix_2.cpp" "$TARGET_RONDBSQLLEXER_L_CPP"

echo "Done building RonDB SQL lexer."
