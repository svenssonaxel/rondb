/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include <RonDBSQLPreparer.hpp>
#include <my_sys.h> // Needed for MY_GIVE_INFO

using std::cerr;
using std::cout;
using std::endl;

struct Config
{
  bool help = false;
  bool connect_ndb = true;
  ExecutionParameters params;
  int infoflag = 0;
  int retry_max = 0;
  const char* connectstring = "localhost::1186";
  char* sql_query = NULL;
};

static void print_help(const char* argv0);
static int parse_cmdline_arguments(int argc, char** argv, Config& config);
static int run_rondb_sql(ExecutionParameters& params, const int retryMax);
static void milliSleep(int milliseconds);

int
main(int argc, char** argv)
{
  Config config;
  ExecutionParameters& params = config.params;
  ArenaAllocator aalloc;
  params.aalloc = &aalloc;
  params.query_output_stream = &cout;
  params.explain_output_stream = &cout;
  params.err_output_stream = &cerr;
  int exit_code = 0;

  exit_code = parse_cmdline_arguments(argc, argv, config);
  if (config.help)
  {
    print_help(argv[0]);
  }
  if (config.help || exit_code > 0)
  {
    return exit_code;
  }

  if (!config.connect_ndb)
  {
    return run_rondb_sql(params, config.retry_max);
  }

  ndb_init();
  // Block scope for ndb cluster connection
  {
    // Ndb connection
    Ndb_cluster_connection cluster_connection(config.connectstring);
    if (cluster_connection.connect(4, 5, 1))
    {
      cerr << "Unable to connect to cluster within 30 secs." << endl;
      return 1;
    }
    // Connect and wait for the storage nodes (ndbd's)
    if (cluster_connection.wait_until_ready(30,0) < 0)
    {
      cerr << "Cluster was not ready within 30 secs." << endl;
      return 1;
    }
    Ndb myNdb(&cluster_connection,"agg");
    // Set max 1024  parallel transactions
    if (myNdb.init(1024) == -1) {
      auto error = myNdb.getNdbError();
      cerr << "ndbapi error " << error.code << ": " << error.message << endl;
      return 1;
    }
    params.ndb = &myNdb;
    // Execute
    exit_code = run_rondb_sql(params, config.retry_max);
  }
  // End of nested scope. This is necessary to clean up the cluster connection
  // before calling ndb_end.
  ndb_end(config.infoflag);

  return exit_code;
}

static void
print_help(const char* argv0)
{
  cout <<
    "Usage: " << argv0 << " [OPTIONS] SQL_QUERY\n"
    "\n"
    "Options:\n"
    "  -h    Display this help message\n"
    "  -C <CONNECTION-STRING>\n"
    "        Connection string (default: localhost:1186)\n"
    "  -i    Give time info about process\n"
    "  -I    Do not give time info about process (default)\n"
    "  -r <RETRY-MAX>\n"
    "        Maximum number of retries (default: 0)\n"
    "  -N    No Ndb cluster connection (supports no queries and only limited EXPLAIN.\n"
    "                                   Use together with -e or -E)\n"
    "  -n    Connect to Ndb cluster (default)\n"
    "    Execution mode:\n"
    "  -b    Allow both query and explain (default)\n"
    "  -q    Allow query only\n"
    "  -e    Allow explain only\n"
    "  -Q    Query override\n"
    "  -E    Explain override\n"
    "    Lock mode:\n"
    "  -R    LM_Read:          Read with shared lock\n"
    "  -X    LM_Exclusive:     Read with exclusive lock\n"
    "  -D    LM_CommittedRead: Ignore locks, read last committed value, a.k.a.\n"
    "                          Dirty (default)\n"
    "  -S    LM_SimpleRead:    Read with shared lock, but release lock directly\n"
    "    Query output format:\n"
    "  -a    JSON ASCII\n"
    "  -u    JSON UTF8 (default)\n"
    "  -c    CSV\n"
    "    Explain output format:\n"
    "  -j    JSON\n"
    "  -t    TEXT (default)\n";
}

static int
parse_cmdline_arguments(int argc, char** argv, Config& config)
{
  ExecutionParameters& params = config.params;
  int opt;
  while ((opt = getopt(argc, argv, "hC:iIr:NnbqeQERXDSaucjt")) != -1)
  {
    switch (opt)
    {
    case 'h': config.help = true; return 0;
    case 'C': config.connectstring = optarg; break;
    case 'i': config.infoflag = MY_GIVE_INFO; break;
    case 'I': config.infoflag = 0; break;
    case 'r': config.retry_max = atoi(optarg); break;
    case 'N': config.connect_ndb = false; break;
    case 'n': config.connect_ndb = true; break;
    case 'b': params.mode = ExecutionParameters::ExecutionMode::ALLOW_BOTH_QUERY_AND_EXPLAIN; break;
    case 'q': params.mode = ExecutionParameters::ExecutionMode::ALLOW_QUERY_ONLY; break;
    case 'e': params.mode = ExecutionParameters::ExecutionMode::ALLOW_EXPLAIN_ONLY; break;
    case 'Q': params.mode = ExecutionParameters::ExecutionMode::QUERY_OVERRIDE; break;
    case 'E': params.mode = ExecutionParameters::ExecutionMode::EXPLAIN_OVERRIDE; break;
    case 'R': params.lock_mode = NdbOperation::LockMode::LM_Read; break;
    case 'X': params.lock_mode = NdbOperation::LockMode::LM_Exclusive; break;
    case 'D': params.lock_mode = NdbOperation::LockMode::LM_CommittedRead; break;
    case 'S': params.lock_mode = NdbOperation::LockMode::LM_SimpleRead; break;
    case 'a': params.query_output_format = ExecutionParameters::QueryOutputFormat::JSON_ASCII; break;
    case 'u': params.query_output_format = ExecutionParameters::QueryOutputFormat::JSON_UTF8; break;
    case 'c': params.query_output_format = ExecutionParameters::QueryOutputFormat::CSV; break;
    case 'j': params.explain_output_format = ExecutionParameters::ExplainOutputFormat::JSON; break;
    case 't': params.explain_output_format = ExecutionParameters::ExplainOutputFormat::TEXT; break;
    default:
      config.help = true;
      return 1;
    }
  }
  // Make sure exactly 1 positional argument
  if (optind + 1 != argc)
  {
    config.help = true;
    return 1;
  }
  // SQL query
  char* sql_query = argv[optind];
  uint sql_query_len = strlen(sql_query);
  char* parse_str = params.aalloc->alloc<char>(sql_query_len + 2);
  size_t parse_len = (sql_query_len + 2) * sizeof(char);
  memcpy(parse_str, sql_query, sql_query_len);
  parse_str[sql_query_len] = '\0';
  parse_str[sql_query_len+1] = '\0';
  params.sql_buffer = parse_str;
  params.sql_len = parse_len;
  return 0;
}

static int
run_rondb_sql(ExecutionParameters& params, const int retryMax)
{
  int retryAttempt = 0;
  while (true)
  {
    try
    {
      RonDBSQLPreparer executor(params);
      executor.execute();
      return 0;
    }
    catch (RonDBSQLPreparer::TemporaryError& e)
    {
      cerr << "Caught temporary error: " << e.what() << endl;
      milliSleep(50);
      if (retryAttempt >= retryMax)
      {
        cerr << "ERROR: has retried this operation " << retryAttempt
             << " times, failing!" << endl;
        // Use exit code 3 to distinguish temporary errors.
        // (Avoid exit code 2 as it is used by e.g. bash.)
        return 3;
      }
      else
      {
        retryAttempt++;
        continue;
      }
    }
    catch (std::runtime_error& e)
    {
      cerr << "Caught exception: " << e.what() << endl;
      return 1;
    }
  }
  assert(false);
}

static void
milliSleep(int milliseconds)
{
  struct timeval sleeptime;
  sleeptime.tv_sec = milliseconds / 1000;
  sleeptime.tv_usec = (milliseconds - (sleeptime.tv_sec * 1000)) * 1000000;
  select(0, 0, 0, 0, &sleeptime);
}
