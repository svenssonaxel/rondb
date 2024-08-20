/*
 * Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_CTRL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_CTRL_HPP_

#include "rdrs_dal.h"
#include "constants.hpp"
#include "base_ctrl.hpp"

#include <drogon/drogon.h>
#include <drogon/HttpSimpleController.h>

#include "storage/ndb/src/ronsql/RonSQLCommon.hpp"
#include "ronsql_data_structs.hpp"

class RonSQLCtrl : public drogon::HttpController<RonSQLCtrl> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(RonSQLCtrl::ronsql, RONSQL_PATH, drogon::Post);
  METHOD_LIST_END

  static void ronsql(const drogon::HttpRequestPtr &req,
                     std::function<void(const drogon::HttpResponsePtr &)> &&callback);
};

RS_Status ronsql_validate_database_name(std::string& database);

RS_Status ronsql_validate_and_init_params(RonSQLParams& input,
                                          ExecutionParameters& ep,
                                          std::ostringstream* query_output,
                                          std::ostringstream* explain_output,
                                          std::ostringstream* err_output,
                                          ArenaAllocator* aalloc);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_CTRL_HPP_
