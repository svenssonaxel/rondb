# RonDB REST SQL

RonDB REST SQL is a subset of MySQL.
A query that is accepted by RonDB REST SQL should hopefully execute faster than via MySQL, but always produce the same result as MySQL.
This document only details the supported subset of MySQL.
For the meaning of functions, operators and other keywords, refer to the MySQL documentation.

## Functionality

- `SELECT` is the only statement supported. A select expression can only be
  - A column name.
  - An aggregate function `AVG`, `COUNT`, `MAX`, `MIN` or `SUM`, of an arithmetic expression. Such an arithmetic expression can only contain
    - Column names
    - Positive integer literals
    - Operators `+`, `-`, `*`, `/`, `%`.
    - Parentheses
  - `COUNT(*)`.
  - One of the above, aliased using `AS`.
- `FROM` is required and can only refer to one table. No joins or subqueries.
- `WHERE` is supported. The condition is restricted to the following:
  - Column names
  - String literals
  - Positive integer literals
  - Parentheses
  - Operators `OR`, `||`, `XOR`, `AND`, `&&`, `NOT`, `=`, `>=`, `>`, `<=`, `<`, `!=`, `<>`, `IS NULL`, `IS NOT NULL`, `|`, `&`, `<<`, `>>`, `+`, `-`, `*`, `/`, `%`, `^`, `!`
  - Functions `DATE_ADD`, `DATE_SUB` and `EXTRACT` with constant-only arguments, e.g. `DATE_ADD('2024-05-07', INTERVAL '75' MICROSECOND)`.
- `GROUP BY`: Supported, but only for column names, no expressions.
- `ORDER BY`, `ASC`, `DESC`: Supported, but only for column names, no expressions.

## Data types

RonDB REST SQL does not support all data types.

The data types supported depend on the context:
- **SELECT-col**: columns in select expressions without aggregation.
- **SELECT-agg**: Columns in an aggregate function argument.
- **WHERE**: Columns in the `WHERE` condition.
- **GROUP BY**: Columns in the `GROUP BY` column list.
- **ORDER BY**: Columns in the `ORDER BY` column list.

Refer to the following table for what data types are supported in each context.

| Data type                   | **SELECT-col** | **SELECT-agg** | **WHERE** | **GROUP BY** | **ORDER BY** |
| --------------------------- | -------------- | -------------- | --------- | ------------ | ------------ |
| `TINYINT`                   | Yes            | Yes            | Yes       | Yes          | Yes          |
| `SMALLINT`                  | Yes            | Yes            | Yes       | Yes          | Yes          |
| `MEDIUMINT`                 | Yes            | Yes            | Yes       | Yes          | Yes          |
| `INT`/`INTEGER`             | Yes            | Yes            | Yes       | Yes          | Yes          |
| `BIGINT`                    | Yes            | Yes            | Yes       | Yes          | Yes          |
| `FLOAT`/`REAL`              | Yes            | Yes            | Yes       | Yes          | Yes          |
| `DOUBLE`/`DOUBLE PRECISION` | Yes            | Yes            | Yes       | Yes          | Yes          |
| `DECIMAL`                   | No             | No             | Yes       | Yes          | No           |
| `VARCHAR`                   | Yes            | No             | Yes       | Yes          | No           |

## Syntax elements

- Single-quoted strings are supported in the `WHERE` condition, but not as the alias after `AS`.
  Therefore, aliases cannot contain characters with code points higher than U+FFFF.
  Character set introducer and `COLLATE` clause are not supported.
- Column names, table names and aliases can be unquoted or use backtick quotes.
  However, unquoted identifiers may not coincide with a MySQL keyword, even if
  such unquoted identifier is allowed by MySQL, and even if the keyword is not
  implemented by RonDB REST SQL.
- Aliases after `AS` are limited to 64 bytes rather than 256. Similar to MySQL,
  the length limits for other identifiers are 64 bytes. However, providing a
  longer identifier produces an error rather than truncation. Even without an
  alias, the limit on the output column name is 64 bytes and will not be
  truncated. For very complex expressions, this means that a shorter alias
  provided with `AS` is required. Note that the MySQL documentation incorrectly
  claims that the identifier length limits are a certain number of *characters*,
  while the actual limit is in *bytes*. When using UTF-8, the number of
  characters is less than the number of bytes whenever an identifier contains a
  character with a code point greater than `U+007f`.
- Double quotes are not supported, neither for identifiers nor strings.
  This makes the `ANSI_QUOTES` SQL mode irrelevant.

## Encoding

- RonDB REST SQL supports and requires UTF-8 encoding.
- NUL characters are not allowed anywhere, but can be represented in strings by means of escape sequence.
- No Unicode normalization will be performed by the server.
