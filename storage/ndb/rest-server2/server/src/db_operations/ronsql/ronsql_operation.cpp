/*
 * Copyright (C) 2023 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#include "ronsql_operation.hpp"
#include "NdbBlob.hpp"
#include "NdbOperation.hpp"
#include "NdbRecAttr.hpp"
#include "NdbTransaction.hpp"
#include "src/error_strings.h"
#include "src/logger.hpp"
#include "src/rdrs_const.h"
#include "src/status.hpp"
#include "src/mystring.hpp"

#include <memory>
#include <mysql_time.h>
#include <algorithm>
#include <utility>
#include <my_base.h>
#include <storage/ndb/include/ndbapi/NdbDictionary.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/beast/core/detail/base64.hpp>

#include "storage/ndb/src/ronsql/RonSQLCommon.hpp"
#include "storage/ndb/src/ronsql/RonSQLPreparer.hpp"


RS_Status ronsql_op(ExecutionParameters& params) {
  std::basic_ostream<char>& err = *params.err_output_stream;
  try
  {
    RonSQLPreparer executor(params);
    executor.execute();
    return RS_OK;
  }
  catch (RonSQLPreparer::TemporaryError& e)
  {
    err << "Caught temporary error: " << e.what() << std::endl;
    return RS_SERVER_ERROR(ERROR_065);
  }
  catch (std::runtime_error& e)
  {
    err << "Caught exception: " << e.what() << std::endl;
    return RS_SERVER_ERROR(ERROR_066);
  }
  // Should be unreachable
  abort();
}
