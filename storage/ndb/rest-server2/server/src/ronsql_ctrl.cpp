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
#include "error_strings.h"
#include "rdrs_dal.hpp"
#include "json_parser.hpp"
#include <drogon/HttpTypes.h>
#include "storage/ndb/src/ronsql/RonSQLPreparer.hpp"

using std::endl;

void RonSQLCtrl::ronsql(const drogon::HttpRequestPtr &req,
                        std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  auto resp = drogon::HttpResponse::newHttpResponse();

  if (currentThreadIndex >= globalConfigs.rest.numThreads) {
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
  size_t length        = req->getBody().length();
  if (length > REQ_BUFFER_SIZE) {
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

  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  ArenaAllocator aalloc;
  ExecutionParameters params;

  std::string& database = reqStruct.database;
  status = ronsql_validate_database_name(database);
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  std::ostringstream query_output;
  std::ostringstream explain_output;
  std::ostringstream err_output;

  status = ronsql_validate_and_init_params(reqStruct,
                                           params,
                                           &query_output,
                                           &explain_output,
                                           &err_output,
                                           &aalloc);
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  status = ronsql_dal(database.c_str(), params);
  std::string query_output_str = query_output.str();
  std::string explain_output_str = explain_output.str();
  std::string err_output_str = err_output.str();
  bool hasQueryOutput = !query_output_str.empty();
  bool hasExplainOutput = !explain_output_str.empty();
  bool hasErrOutput = !err_output_str.empty();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
    assert(!hasErrOutput);
    if (hasQueryOutput) {
      assert(!hasExplainOutput);
      switch (params.query_output_format) {
      case ExecutionParameters::QueryOutputFormat::JSON_UTF8:
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        break;
      case ExecutionParameters::QueryOutputFormat::JSON_ASCII:
        /*
         * Although JSON is described as a text-based format, it requires the
         * UTF-8 encoding [1]. Since one cannot choose a text encoding, it may
         * be considered a binary format. Indeed, the application/json media
         * type is registerd as a binary format [2], and therefore has no
         * charset parameter. Despite this, many servers supply a
         * `charset=utf-8` parameter, including drogon [3]. The intention behind
         * ExecutionParameters::QueryOutputFormat::JSON_ASCII is to provide a
         * format that only uses ASCII characters, so we communicate this using
         * a `charset=US-ASCII` parameter. This may help a RonSQL aware client
         * to confirm ASCII-only content. It is not expected to cause any
         * trouble for unaware clients, as a compliant client should ignore the
         * charset parameter altogether and use utf-8 to decode the JSON object,
         * which will work since UTF-8 is backwards compatible with ASCII.
         *
         * [1] https://www.rfc-editor.org/rfc/rfc8259#section-8.1
         * [2] https://www.iana.org/assignments/media-types/application/json
         * [3] ../../extra/drogon/drogon-1.8.7/lib/src/HttpUtils.cc:565
         */
        resp->setContentTypeCodeAndCustomString(drogon::CT_APPLICATION_JSON, "content-type: application/json; charset=US-ASCII\r\n");
        break;
      case ExecutionParameters::QueryOutputFormat::TSV:
        /*
         * The text/tab-separated-values media type [1] is unfortunately a
         * little lack-luster. It has no mechanism to specify whether a header
         * row is present, and also requires an "Encoding type" parameters
         * without describing it. Here we do the best we can by using the
         * parameter definitions from the specification for the text/csv media
         * type [2] instead.
         *
         * [1] https://www.iana.org/assignments/media-types/text/tab-separated-values
         * [2] https://www.iana.org/assignments/media-types/text/csv
         */
        resp->setContentTypeCodeAndCustomString(drogon::CT_CUSTOM, "content-type: text/tab-separated-values; charset=utf-8; header=present\r\n");
        break;
      case ExecutionParameters::QueryOutputFormat::TSV_DATA:
        // See comment above.
        resp->setContentTypeCodeAndCustomString(drogon::CT_CUSTOM, "content-type: text/tab-separated-values; charset=utf-8; header=absent\r\n");
        break;
      default:
        // Should be unreachable
        abort();
      }
      resp->setBody(query_output_str);
    }
    else {
      assert(hasExplainOutput);
      switch (params.explain_output_format) {
      case ExecutionParameters::ExplainOutputFormat::JSON_UTF8:
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        break;
      case ExecutionParameters::ExplainOutputFormat::TEXT:
        /*
         * The text/plain media type technically requires CRLF line endings.
         * This output instead uses Unix line endings (LF). There seems to be no
         * way to specify this in the content type.
         */
        resp->setContentTypeCodeAndCustomString(drogon::CT_TEXT_PLAIN, "content-type: text/plain; charset=utf-8; \r\n");
        break;
      default:
        // Should be unreachable
        abort();
      }
      resp->setBody(explain_output_str);
    }
    //
    //todo perhaps explain and output stream should be the same, since only one is ever used? At least return a flag for which was used.
    resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  }
  else {
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    resp->setContentTypeCodeAndCustomString(drogon::CT_TEXT_PLAIN, "content-type: text/plain; charset=utf-8; \r\n");
    resp->setBody(err_output_str);
  }
  callback(resp);
}

RS_Status ronsql_validate_database_name(std::string& database) {
  RS_Status status = validate_db_identifier(database);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    if (status.code == ERROR_CODE_EMPTY_IDENTIFIER) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        ERROR_CODE_EMPTY_IDENTIFIER,
                        ERROR_049)
          .status;
    }
    if (status.code == ERROR_CODE_IDENTIFIER_TOO_LONG) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        ERROR_CODE_MAX_DB, ERROR_050)
          .status;
    }
    if (status.code == ERROR_CODE_INVALID_IDENTIFIER) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        ERROR_CODE_INVALID_IDENTIFIER, ERROR_051)
          .status;
    }
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_DB_NAME,
                      (std::string(ERROR_051) + "; error: " + status.message).c_str())
        .status;
  }
  return RS_OK;
}

RS_Status ronsql_validate_and_init_params(RonSQLParams& req,
                                          ExecutionParameters& ep,
                                          std::ostringstream* query_output,
                                          std::ostringstream* explain_output,
                                          std::ostringstream* err_output,
                                          ArenaAllocator* aalloc) {
  // req.query -> ep.sql_buffer and ep.sql_len
  assert(aalloc != NULL);
  ep.sql_len = req.query.length(); // todo what if length is not in bytes?
  assert(ep.sql_buffer == NULL);
  ep.sql_buffer = aalloc->alloc<char>(ep.sql_len + 2); // todo catch OOM exception
  memcpy(ep.sql_buffer, req.query.c_str(), req.query.length());
  ep.sql_buffer[ep.sql_len++] = '\0';
  ep.sql_buffer[ep.sql_len++] = '\0';
  // aalloc -> ep.aalloc
  assert(ep.aalloc == NULL);
  ep.aalloc = aalloc;
  // req.explainMode -> ep.explain_mode
  if (req.explainMode == "ALLOW") {
    ep.explain_mode = ExecutionParameters::ExplainMode::ALLOW;
  }
  else if (req.explainMode == "FORBID") {
    ep.explain_mode = ExecutionParameters::ExplainMode::FORBID;
  }
  else if (req.explainMode == "REQUIRE") {
    ep.explain_mode = ExecutionParameters::ExplainMode::REQUIRE;
  }
  else if (req.explainMode == "REMOVE") {
    ep.explain_mode = ExecutionParameters::ExplainMode::REMOVE;
  }
  else if (req.explainMode == "FORCE") {
    ep.explain_mode = ExecutionParameters::ExplainMode::FORCE;
  }
  else {
    return RS_CLIENT_ERROR("Invalid explainMode");
  }
  // query_output -> ep.query_output_stream
  assert(ep.query_output_stream == NULL);
  assert(query_output != NULL);
  ep.query_output_stream = query_output;
  // req.queryOutputFormat -> ep.query_output_format
  if (req.queryOutputFormat == "JSON_UTF8") {
    ep.query_output_format = ExecutionParameters::QueryOutputFormat::JSON_UTF8;
  }
  else if (req.queryOutputFormat == "JSON_ASCII") {
    ep.query_output_format = ExecutionParameters::QueryOutputFormat::JSON_ASCII;
  }
  else if (req.queryOutputFormat == "TSV") {
    ep.query_output_format = ExecutionParameters::QueryOutputFormat::TSV;
  }
  else if (req.queryOutputFormat == "TSV_DATA") {
    ep.query_output_format = ExecutionParameters::QueryOutputFormat::TSV_DATA;
  }
  else {
    return RS_CLIENT_ERROR("Invalid queryOutputFormat");
  }
  // explain_output -> ep.explain_output_stream
  assert(ep.explain_output_stream == NULL);
  assert(explain_output != NULL);
  ep.explain_output_stream = explain_output;
  // req.explainOutputFormat -> ep.explain_output_format
  if (req.explainOutputFormat == "TEXT") {
    ep.explain_output_format = ExecutionParameters::ExplainOutputFormat::TEXT;
  }
  else if (req.explainOutputFormat == "JSON_UTF8") {
    ep.explain_output_format = ExecutionParameters::ExplainOutputFormat::JSON_UTF8;
  }
  else {
    return RS_CLIENT_ERROR("Invalid explainOutputFormat");
  }
  // err_output -> ep.err_output_stream
  assert(ep.err_output_stream == NULL);
  assert(err_output != NULL);
  ep.err_output_stream = err_output;
  // req.operationId -> ep.operation_id
  RS_Status status = validate_operation_id(req.operationId);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_OPERATION_ID,
                      (std::string(ERROR_055) + "; error: " + status.message).c_str())
        .status;
  if (!req.operationId.empty()) {
    ep.operation_id = req.operationId.c_str();
  }
  // Everything ok
  return RS_OK;
}
