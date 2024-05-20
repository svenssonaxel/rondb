#!/usr/bin/env bash
set -euo pipefail

BISON="$1"
SOURCE_RONDBSQLPARSER_Y="$2"
TARGET_RONDBSQLPARSER_Y_HPP="$3"
TARGET_RONDBSQLPARSER_Y_CPP="$4"

[ -x "$BISON" ]
[ -f "$SOURCE_RONDBSQLPARSER_Y" ]

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# As we are not quite satisfied with the bison output, we let bison output to
# temporary files, allowing us to edit them before they are saved to the target
# location.

"$BISON" \
    -Wall \
    -Wcounterexamples \
    -Wdangling-alias \
    -Werror \
    --header="$TMPDIR/RonDBSQLParser.y.hpp" \
    --output="$TMPDIR/RonDBSQLParser.y.cpp" \
    "$SOURCE_RONDBSQLPARSER_Y"

# We use `%define api.location.type` to declare a custom location type. However,
# bison does not have any option for the default value of that type. So,
# unfortunately we have to edit the generated file the hacky way.

if [ "$(grep -Ec '  = \{ 1, 1, 1, 1 \}' "$TMPDIR/RonDBSQLParser.y.cpp")" = 1 ]; then
    echo "Confirmed that generated RonDBSQLParser.y.cpp has exactly 1 occurrence of default location value."
else
    echo "Could not confirm that generated RonDBSQLParser.y.cpp has exactly 1 occurrence of default location value."
    exit 1
fi

sed -r "s/  = \{ 1, 1, 1, 1 \}/  = { NULL, 0 }/" "$TMPDIR/RonDBSQLParser.y.cpp" > "$TMPDIR/RonDBSQLParser.y.fix_1.cpp"

if diff -q "$TMPDIR/RonDBSQLParser.y.cpp" "$TMPDIR/RonDBSQLParser.y.fix_1.cpp"; then
    echo "Editing of default location value ineffective."
    exit 1
else
    echo "Confirmed that editing of default location made some change."
fi

# bison uses #line directives in the output file to aid error messages. Some of
# them refer to the source file $SOURCE_RONDBSQLPARSER_Y and others to the
# output file. Since we let bison output to temporary files, the latter would
# become incorrect when we move the file to the target below. We use `awk` to
# fix these #line directives.

AWK_SCRIPT='
  # Handle #line directives specially
  /^#line/ {
    if ($3 == "\"" change_from_file "\"") {
      correct_line = NR + 1
      # Since we have not added or removed any lines, we expect the line number to already be correct.
      if ($2 != correct_line) {
        print "Expected line number " correct_line " but got: " $0 | "cat >&2"
        exit 1
      }
      # This is the file name we are supposed to change.
      print "#line " correct_line " \"" change_to_file "\""
      next
    }
    else if ($3 == "\"" no_touch_file "\"") {
      # This file name is expected, and should remain. The line number can be
      # anything since it refers to another file.
      print
      next
    } else {
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
    -v change_from_file="$TMPDIR/RonDBSQLParser.y.hpp" \
    -v change_to_file="$TARGET_RONDBSQLPARSER_Y_HPP" \
    -v no_touch_file="$SOURCE_RONDBSQLPARSER_Y" \
    "$AWK_SCRIPT" \
    "$TMPDIR/RonDBSQLParser.y.hpp" \
    > "$TMPDIR/RonDBSQLParser.y.fix_1.hpp"
awk \
    -v change_from_file="$TMPDIR/RonDBSQLParser.y.cpp" \
    -v change_to_file="$TARGET_RONDBSQLPARSER_Y_CPP" \
    -v no_touch_file="$SOURCE_RONDBSQLPARSER_Y" \
    "$AWK_SCRIPT" \
    "$TMPDIR/RonDBSQLParser.y.fix_1.cpp" \
    > "$TMPDIR/RonDBSQLParser.y.fix_2.cpp"

# Success

mv "$TMPDIR/RonDBSQLParser.y.fix_1.hpp" "$TARGET_RONDBSQLPARSER_Y_HPP"
mv "$TMPDIR/RonDBSQLParser.y.fix_2.cpp" "$TARGET_RONDBSQLPARSER_Y_CPP"

echo "Done building RonDB SQL parser."
