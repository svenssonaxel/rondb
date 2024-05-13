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

"$BISON" \
    -Wall \
    -Wcounterexamples \
    -Wdangling-alias \
    -Werror \
    --header="$TMPDIR/RonDBSQLParser.y.hpp" \
    --output="$TMPDIR/RonDBSQLParser.y.raw.cpp" \
    "$SOURCE_RONDBSQLPARSER_Y"

# We use `%define api.location.type` to declare a custom location type. However,
# bison does not have any option for the default value of that type. So,
# unfortunately we have to edit the generated file the hacky way.

if [ "$(grep -Ec '  = \{ 1, 1, 1, 1 \}' "$TMPDIR/RonDBSQLParser.y.raw.cpp")" = 1 ]; then
    echo "Confirmed that generated RonDBSQLParser.y.raw.cpp has exactly 1 occurrence of default location value."
else
    echo "Could not confirm that generated RonDBSQLParser.y.raw.cpp has exactly 1 occurrence of default location value."
    exit 1
fi

sed -r "s/  = \{ 1, 1, 1, 1 \}/  = { NULL, 0 }/" "$TMPDIR/RonDBSQLParser.y.raw.cpp" > "$TMPDIR/RonDBSQLParser.y.cpp"

if diff -q "$TMPDIR/RonDBSQLParser.y.raw.cpp" "$TMPDIR/RonDBSQLParser.y.cpp"; then
    echo "Editing of default location value ineffective."
    exit 1
else
    echo "Confirmed that editing of default location made some change."
fi

# Success

mv "$TMPDIR/RonDBSQLParser.y.hpp" "$TARGET_RONDBSQLPARSER_Y_HPP"
mv "$TMPDIR/RonDBSQLParser.y.cpp" "$TARGET_RONDBSQLPARSER_Y_CPP"

echo "Done building RonDB SQL parser."
