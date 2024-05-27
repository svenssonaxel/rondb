# This include-file, or subroutine, executes a query with both mysql client and
# rondb_sql_cli, then compares the results.
--echo == Query ==
--echo $QUERY
--echo
--echo == MySQL result ==
--exec $MYSQL -D test -e '$QUERY' > $MYSQL_TMP_DIR/mysql.out
--exec (head -n 1 $MYSQL_TMP_DIR/mysql.out && tail -n +2 $MYSQL_TMP_DIR/mysql.out | sort) > $MYSQL_TMP_DIR/mysql.out.sorted
--cat_file $MYSQL_TMP_DIR/mysql.out.sorted
--echo
--echo == RonDBSQL result ==
--exec $RONDB_SQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test --query-output-format TSV -e '$QUERY' > $MYSQL_TMP_DIR/rondb_sql.out
--exec (head -n 1 $MYSQL_TMP_DIR/rondb_sql.out && tail -n +2 $MYSQL_TMP_DIR/rondb_sql.out | sort) > $MYSQL_TMP_DIR/rondb_sql.out.sorted
--cat_file $MYSQL_TMP_DIR/rondb_sql.out.sorted
--echo
--echo == Diff ==
# todo The "|| true" at the end makes this test succeed no matter what the
# results are. Remove it when possible.
--exec diff -u --label mysql.out $MYSQL_TMP_DIR/mysql.out.sorted --label rondb_sql.out $MYSQL_TMP_DIR/rondb_sql.out.sorted || true
--remove_file $MYSQL_TMP_DIR/mysql.out
--remove_file $MYSQL_TMP_DIR/mysql.out.sorted
--remove_file $MYSQL_TMP_DIR/rondb_sql.out
--remove_file $MYSQL_TMP_DIR/rondb_sql.out.sorted
--echo ================================================================================
--echo
--echo
--echo
