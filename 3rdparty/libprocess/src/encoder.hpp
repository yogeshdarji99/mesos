#ifndef __ENCODER_HPP__
#define __ENCODER_HPP__

#include <stdint.h>

#include <map>
#include <sstream>

#include <process/http.hpp>
#include <process/process.hpp>

#include <stout/foreach.hpp>
#include <stout/gzip.hpp>
#include <stout/hashmap.hpp>
#include <stout/numify.hpp>
#include <stout/os.hpp>

namespace process {

const uint32_t GZIP_MINIMUM_BODY_LENGTH = 1024;


class Encoder
{
public:
  enum IOKind
  {
    send_data,
    send_file
  };

  explicit Encoder(const ConnectionHandle& _connection_handle)
      : conn_handle(_connection_handle) {}
  virtual ~Encoder() {}

  virtual IOKind io_kind() = 0;

  const ConnectionHandle& connection_handle() const
  {
    return conn_handle;
  }

private:

  // The connection handle this encoder is associated with.
  const ConnectionHandle conn_handle;
};


class DataEncoder : public Encoder
{
public:
  DataEncoder(
      const ConnectionHandle& _connection_handle,
      const std::string& _data)
      : Encoder(_connection_handle), data(_data), index(0) {}

  virtual ~DataEncoder() {}

  virtual IOKind io_kind()
  {
    return send_data;
  }

  virtual const char* next(size_t* length)
  {
    size_t temp = index;
    index = data.size();
    *length = data.size() - temp;
    return data.data() + temp;
  }

  virtual void backup(size_t length)
  {
    if (index >= length) {
      index -= length;
    }
  }

  virtual size_t remaining() const
  {
    return data.size() - index;
  }

private:
  const std::string data;
  size_t index;
};


class MessageEncoder : public DataEncoder
{
public:
  MessageEncoder(const ConnectionHandle& _connection_handle, Message* _message)
    : DataEncoder(_connection_handle, encode(_message)), message(_message) {}

  virtual ~MessageEncoder()
  {
    if (message != NULL) {
      delete message;
    }
  }

  static std::string encode(Message* message)
  {
    std::ostringstream out;

    if (message != NULL) {
      out << "POST ";
      // Nothing keeps the 'id' component of a PID from being an empty
      // string which would create a malformed path that has two
      // '//' unless we check for it explicitly.
      // TODO(benh): Make the 'id' part of a PID optional so when it's
      // missing it's clear that we're simply addressing an ip:port.
      if (message->to.id != "") {
        out << "/" << message->to.id;
      }
      out << "/" << message->name << " HTTP/1.0\r\n"
          << "User-Agent: libprocess/" << message->from << "\r\n"
          << "Connection: Keep-Alive\r\n";

      if (message->body.size() > 0) {
        out << "Transfer-Encoding: chunked\r\n\r\n"
            << std::hex << message->body.size() << "\r\n";
        out.write(message->body.data(), message->body.size());
        out << "\r\n"
            << "0\r\n"
            << "\r\n";
      } else {
        out << "\r\n";
      }
    }

    return out.str();
  }

private:
  Message* message;
};


class HttpResponseEncoder : public DataEncoder
{
public:
  HttpResponseEncoder(
      const ConnectionHandle& _connection_handle,
      const http::Response& response,
      const http::Request& request)
    : DataEncoder(_connection_handle, encode(response, request)) {}

  static std::string encode(
      const http::Response& response,
      const http::Request& request)
  {
    std::ostringstream out;

    // TODO(benh): Check version?

    out << "HTTP/1.1 " << response.status << "\r\n";

    hashmap<std::string, std::string> headers = response.headers;

    // HTTP 1.1 requires the "Date" header. In the future once we
    // start checking the version (above) then we can conditionally
    // add this header, but for now, we always do.
    time_t rawtime;
    time(&rawtime);

    char date[256];

    // TODO(benh): Check return code of strftime!
    strftime(date, 256, "%a, %d %b %Y %H:%M:%S GMT", gmtime(&rawtime));

    headers["Date"] = date;

    // Should we compress this response?
    std::string body = response.body;

    if (response.type == http::Response::BODY &&
        response.body.length() >= GZIP_MINIMUM_BODY_LENGTH &&
        !headers.contains("Content-Encoding") &&
        request.accepts("gzip")) {
      Try<std::string> compressed = gzip::compress(body);
      if (compressed.isError()) {
        LOG(WARNING) << "Failed to gzip response body: " << compressed.error();
      } else {
        body = compressed.get();
        headers["Content-Length"] = stringify(body.length());
        headers["Content-Encoding"] = "gzip";
      }
    }

    foreachpair (const std::string& key, const std::string& value, headers) {
      out << key << ": " << value << "\r\n";
    }

    // Add a Content-Length header if the response is of type "none"
    // or "body" and no Content-Length header has been supplied.
    if (response.type == http::Response::NONE &&
        !headers.contains("Content-Length")) {
      out << "Content-Length: 0\r\n";
    } else if (response.type == http::Response::BODY &&
               !headers.contains("Content-Length")) {
      out << "Content-Length: " << body.size() << "\r\n";
    }

    // Use a CRLF to mark end of headers.
    out << "\r\n";

    // Add the body if necessary.
    if (response.type == http::Response::BODY) {
      // If the Content-Length header was supplied, only write as much data
      // as the length specifies.
      Result<uint32_t> length = numify<uint32_t>(headers.get("Content-Length"));
      if (length.isSome() && length.get() <= body.length()) {
        out.write(body.data(), length.get());
      } else {
        out.write(body.data(), body.size());
      }
    }

    return out.str();
  }
};


class FileEncoder : public Encoder
{
public:
  FileEncoder(const ConnectionHandle& _connection_handle, int _fd, size_t _size)
    : Encoder(_connection_handle), fd(_fd), size(_size), index(0) {}

  virtual ~FileEncoder()
  {
    os::close(fd);
  }

  virtual IOKind io_kind()
  {
    return send_file;
  }

  virtual int next(off_t* offset, size_t* length)
  {
    off_t temp = index;
    index = size;
    *offset = temp;
    *length = size - temp;
    return fd;
  }

  virtual void backup(size_t length)
  {
    if (index >= length) {
      index -= length;
    }
  }

  virtual size_t remaining() const
  {
    return size - index;
  }

private:
  int fd;
  size_t size;
  off_t index;
};

}  // namespace process {

#endif // __ENCODER_HPP__
