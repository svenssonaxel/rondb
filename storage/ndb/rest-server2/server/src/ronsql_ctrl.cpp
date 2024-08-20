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

#include "ronsql_ctrl.hpp"
#include "ronsql_structs.hpp"
#include "json_parser.hpp"
#include "encoding.hpp"
#include "buffer_manager.hpp"
#include "config_structs.hpp"
#include "constants.hpp"

#include <cstring>
#include <drogon/HttpTypes.h>
#include <memory>
#include <simdjson.h>

#include "storage/ndb/src/ronsql/RonSQLPreparer.hpp"
#include "storage/ndb/src/ronsql/RonSQLCommon.hpp"

using std::endl;

void RonSQLCtrl::ronsql(const drogon::HttpRequestPtr &req,
                        std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (currentThreadIndex >= globalConfigs.rest.numThreads) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
  size_t length        = req->getBody().length();
  if (length > REQ_BUFFER_SIZE) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Request too large");
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }
  memcpy(jsonParser.get_buffer(currentThreadIndex).get(), json_str, length);

  RonSQLParams reqStruct;

  RS_Status status = jsonParser.ronsql_parse(
      currentThreadIndex,
      simdjson::padded_string_view(jsonParser.get_buffer(currentThreadIndex).get(), length,
                                   REQ_BUFFER_SIZE + simdjson::SIMDJSON_PADDING),
      reqStruct);
  auto resp = drogon::HttpResponse::newHttpResponse();

  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }


  ArenaAllocator aalloc;
  ExecutionParameters params;
  // todo validate database, seize ndb object, save old database from ndb, set new

  status = ronsql_validate_and_init_params(reqStruct, params, &aalloc, myNdb);
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  if (ronsql_run(params)) {
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
    //resp->setContentTypeCode(drogon::CT_TEXT_PLAIN); // todo let ronsql return content type
    //todo perhaps explain and output stream should be the same, since only one is ever used? At least return a flag for which was used.
    resp->setStatusCode(drogon::HttpStatusCode::k200OK);
    resp->setBody(params.query_output_stream);
  }
  else {
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    resp->setBody(params.err_output_stream);
  }
  callback(resp);
}

RS_Status ronsql_validate_and_init_params(RonSQLParams& input,
                                          ExecutionParameters& ep,
                                          ArenaAllocator* aalloc,
                                          Ndb* myNdb) {
  assert(aalloc != NULL);
  ep.sql_len = input.query.length(); // todo what if length is not in bytes?
  assert(ep.sql_buffer == NULL);
  ep.sql_buffer = aalloc->alloc<char>(ep.sql_len + 2); // todo catch OOM exception
  memcpy(ep.sql_buffer, input.query.c_str(), input.query.length());
  ep.sql_buffer[ep.sql_len++] = '\0';
  ep.sql_buffer[ep.sql_len++] = '\0';
  assert(myNdb != NULL);
  ep.ndb = myNdb;
  if (input.explainMode == "ALLOW") {
    ep.explain_mode = ExecutionParameters::ExplainMode::ALLOW;
  }
  else if (input.explainMode == "FORBID") {
    ep.explain_mode = ExecutionParameters::ExplainMode::FORBID;
  }
  else if (input.explainMode == "REQUIRE") {
    ep.explain_mode = ExecutionParameters::ExplainMode::REQUIRE;
  }
  else if (input.explainMode == "REMOVE") {
    ep.explain_mode = ExecutionParameters::ExplainMode::REMOVE;
  }
  else if (input.explainMode == "FORCE") {
    ep.explain_mode = ExecutionParameters::ExplainMode::FORCE;
  }
  else {
    return RS_Status{400, "Invalid explainMode"};
  }
  assert(ep.query_output_stream == NULL);
  ep.query_output_stream = new StringStream(); // todo
  if (input.queryOutputFormat == "JSON_UTF8") {
    ep.query_output_format = ExecutionParameters::QueryOutputFormat::JSON_UTF8;
  }
  else if (input.queryOutputFormat == "JSON_ASCII") {
    ep.query_output_format = ExecutionParameters::QueryOutputFormat::JSON_ASCII;
  }
  else if (input.queryOutputFormat == "TSV") {
    ep.query_output_format = ExecutionParameters::QueryOutputFormat::TSV;
  }
  else if (input.queryOutputFormat == "TSV_DATA") {
    ep.query_output_format = ExecutionParameters::QueryOutputFormat::TSV_DATA;
  }
  else {
    return RS_Status{400, "Invalid queryOutputFormat"};
  }
  assert(ep.explain_output_stream == NULL);
  ep.explain_output_stream = new StringStream(); // todo
  if (input.explainOutputFormat == "TEXT") {
    ep.explain_output_format = ExecutionParameters::ExplainOutputFormat::TEXT;
  }
  else if (input.explainOutputFormat == "JSON_UTF8") {
    ep.explain_output_format = ExecutionParameters::ExplainOutputFormat::JSON_UTF8;
  }
  else {
    return RS_Status{400, "Invalid explainOutputFormat"};
  }
  assert(ep.err_output_stream == NULL);
  ep.err_output_stream = new StringStream(); // todo
  // todo validate input.database and apply to ndb object.
  return RS_Status{200, "OK"};
}

bool ronsql_run(ExecutionParameters params) {
  try
  {
    RonSQLPreparer executor(params);
    executor.execute();
    return true;
  }
  catch (RonSQLPreparer::TemporaryError& e)
  {
    params.err_output_stream << "Caught temporary error: " << e.what() << endl;
    return false;
  }
  catch (std::runtime_error& e)
  {
    params.err_output_stream << "Caught exception: " << e.what() << endl;
    return false;
  }
}
