let $replica_io= query_get_value(SHOW REPLICA STATUS, Replica_IO_State, 1);
let $replica_sql= query_get_value(SHOW REPLICA STATUS, Replica_SQL_Running_State, 1);
--echo Replica_IO_State=$replica_io
--echo Replica_SQL_Running_State=$replica_sql
