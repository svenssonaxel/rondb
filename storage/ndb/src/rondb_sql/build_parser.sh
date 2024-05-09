#!/usr/bin/env bash
set -euo pipefail

BISON="$1"
SOURCE_RESTSQLPARSER_Y="$2"
TARGET_RESTSQLPARSER_Y_HPP="$3"
TARGET_RESTSQLPARSER_Y_CPP="$4"

[ -x "$BISON" ]
[ -f "$SOURCE_RESTSQLPARSER_Y" ]

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

"$BISON" \
    -Wall \
    -Wcounterexamples \
    -Wdangling-alias \
    -Werror \
    --header="$TMPDIR/RestSQLParser.y.hpp" \
    --output="$TMPDIR/RestSQLParser.y.raw.cpp" \
    "$SOURCE_RESTSQLPARSER_Y"

# We use `%define api.location.type` to declare a custom location type. However,
# bison does not have any option for the default value of that type. So,
# unfortunately we have to edit the generated file the hacky way.

if [ "$(grep -Ec '  = \{ 1, 1, 1, 1 \}' "$TMPDIR/RestSQLParser.y.raw.cpp")" = 1 ]; then
    echo "Confirmed that generated RestSQLParser.y.raw.cpp has exactly 1 occurrence of default location value."
else
    echo "Could not confirm that generated RestSQLParser.y.raw.cpp has exactly 1 occurrence of default location value."
    exit 1
fi

sed -r "s/  = \{ 1, 1, 1, 1 \}/  = { NULL, 0 }/" "$TMPDIR/RestSQLParser.y.raw.cpp" > "$TMPDIR/RestSQLParser.y.cpp"

if diff -q "$TMPDIR/RestSQLParser.y.raw.cpp" "$TMPDIR/RestSQLParser.y.cpp"; then
    echo "Editing of default location value ineffective."
    exit 1
else
    echo "Confirmed that editing of default location made some change."
fi

# Success

mv "$TMPDIR/RestSQLParser.y.hpp" "$TARGET_RESTSQLPARSER_Y_HPP"
mv "$TMPDIR/RestSQLParser.y.cpp" "$TARGET_RESTSQLPARSER_Y_CPP"

echo "Done building RonDB SQL parser."
