#include <arpa/inet.h>

#include <stdint.h>

#include <cstring>
#include <deque>
#include <iostream>
#include <string>

#include <process/future.hpp>
#include <process/http.hpp>
#include <process/io.hpp>

#include <stout/lambda.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>

#include "decoder.hpp"
#include "event_manager.hpp"

using std::deque;
using std::string;

using process::http::Request;
using process::http::Response;

namespace process {

namespace http {

hashmap<uint16_t, string> statuses;

namespace internal {

Future<Response> decode(const string& buffer)
{
  ResponseDecoder decoder;
  deque<Response*> responses = decoder.decode(buffer.c_str(), buffer.length());

  if (decoder.failed() || responses.empty()) {
    for (size_t i = 0; i < responses.size(); ++i) {
      delete responses[i];
    }
    return Failure("Failed to decode HTTP response:\n" + buffer + "\n");
  } else if (responses.size() > 1) {
    PLOG(ERROR) << "Received more than 1 HTTP Response";
  }

  Response response = *responses[0];
  for (size_t i = 0; i < responses.size(); ++i) {
    delete responses[i];
  }

  return response;
}


Future<Response> request(
    const UPID& upid,
    const string& method,
    const Option<string>& path,
    const Option<string>& query,
    const Option<hashmap<string, string> >& _headers,
    const Option<string>& body,
    const Option<string>& contentType)
{
  // We use inet_ntop since inet_ntoa is not thread-safe!
  char ip[INET_ADDRSTRLEN];
  if (inet_ntop(AF_INET, (in_addr*) &upid.ip, ip, INET_ADDRSTRLEN) == NULL) {
    return Failure(ErrnoError("Failed to get human-readable IP address for '" +
                              stringify(upid.ip) + "'"));
  }

  const string host = string(ip) + ":" + stringify(upid.port);

  Try<ConnectionHandle> conn_handle = EventManager::new_connection(
      upid.ip,
      upid.port,
      IPPROTO_IP);
  if (conn_handle.isError()) {
    return Failure("Failed to create Http connection: " + conn_handle.error());
  }
  ConnectionHandle connection_handle = conn_handle.get();

  std::ostringstream out;

  out << method << " /" << upid.id;

  if (path.isSome()) {
    out << "/" << path.get();
  }

  if (query.isSome()) {
    out << "?" << query.get();
  }

  out << " HTTP/1.1\r\n";

  // Set up the headers as necessary.
  hashmap<string, string> headers;

  if (_headers.isSome()) {
    headers = _headers.get();
  }

  // Need to specify the 'Host' header.
  headers["Host"] = host;

  // Tell the server to close the connection when it's done.
  headers["Connection"] = "close";

  // Overwrite Content-Type if necessary.
  if (contentType.isSome()) {
    headers["Content-Type"] = contentType.get();
  }

  // Make sure the Content-Length is set correctly if necessary.
  if (body.isSome()) {
    headers["Content-Length"] = stringify(body.get().length());
  }

  // Emit the headers.
  foreachpair (const string& key, const string& value, headers) {
    out << key << ": " << value << "\r\n";
  }

  out << "\r\n";

  if (body.isSome()) {
    out << body.get();
  }

  return EventManager::conn_write(connection_handle, out.str())
    .then(lambda::bind(&EventManager::conn_read, connection_handle))
    .then(lambda::bind(&internal::decode, lambda::_1));
}


} // namespace internal {


Future<Response> get(
    const UPID& upid,
    const Option<string>& path,
    const Option<string>& query,
    const Option<hashmap<string, string> >& headers)
{
  return internal::request(
      upid, "GET", path, query, headers, None(), None());
}


Future<Response> post(
    const UPID& upid,
    const Option<string>& path,
    const Option<hashmap<string, string> >& headers,
    const Option<string>& body,
    const Option<string>& contentType)
{
  if (body.isNone() && contentType.isSome()) {
    return Failure("Attempted to do a POST with a Content-Type but no body");
  }

  return internal::request(
      upid, "POST", path, None(), headers, body, contentType);
}


} // namespace http {
} // namespace process {
