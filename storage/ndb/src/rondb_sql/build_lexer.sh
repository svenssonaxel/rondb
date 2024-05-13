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

"$FLEX" \
    --header-file="$TMPDIR/RonDBSQLLexer.l.hpp" \
    --outfile="$TMPDIR/RonDBSQLLexer.l.with-hold_char.cpp" \
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

sed -r "/yy_hold_char/d; / = '.0';/d;" "$TMPDIR/RonDBSQLLexer.l.with-hold_char.cpp" > "$TMPDIR/RonDBSQLLexer.l.cpp"

if diff -q "$TMPDIR/RonDBSQLLexer.l.with-hold_char.cpp" "$TMPDIR/RonDBSQLLexer.l.cpp"; then
    echo "Editing to remove hold_char ineffective."
    exit 1
else
    echo "Confirmed that attempt to remove hold_char made some change."
fi

if grep -qE "yy_hold_char| = '.0';" "$TMPDIR/RonDBSQLLexer.l.cpp"; then
    echo "Editing to remove hold_char ineffective."
    grep -HEn "yy_hold_char| = '.0';" "$TMPDIR/RonDBSQLLexer.l.cpp"
    exit 1
fi

# Success

mv "$TMPDIR/RonDBSQLLexer.l.hpp" "$TARGET_RONDBSQLLEXER_L_HPP"
mv "$TMPDIR/RonDBSQLLexer.l.cpp" "$TARGET_RONDBSQLLEXER_L_CPP"

echo "Done building RonDB SQL lexer."
