#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>

#include <libaio.h>

#include <netinet/tcp.h>
#include <sys/eventfd.h>

#include <mutex>

#include <process/io.hpp>
#include <process/node.hpp>

#include "decoder.hpp"
#include "libevent_event_manager.hpp"

using process::http::Request;
using process::http::Response;

using std::deque;
using std::list;
using std::map;
using std::queue;
using std::set;
using std::string;

namespace process {

class LibeventConnection : public Connection {
public:
  class Entry {
  public:
    enum kind_t {
      string_entry,
      size_entry
    };
    virtual ~Entry() {}
    virtual kind_t get_kind() const = 0;
  protected:
    Entry() : finished(0UL) {}
    size_t finished;
  };
  class StringEntry : public Entry {
  public:
    StringEntry(size_t _limit) : limit(_limit) {}
    virtual ~StringEntry() {}
    virtual kind_t get_kind() const
    {
      return string_entry;
    }
  private:
    Promise<std::string> promise;
    size_t limit;
    std::ostringstream ss;
    friend class LibeventConnection;
  };
  class SizeEntry : public Entry {
  public:
    SizeEntry(size_t _limit, void* _cbarg = nullptr)
      : limit(_limit), cbarg(_cbarg) {}
    virtual ~SizeEntry() {}
    virtual kind_t get_kind() const
    {
      return size_entry;
    }
  private:
    Promise<size_t> promise;
    size_t limit;
    void* cbarg;
    friend class LibeventConnection;
  };
  LibeventConnection(const Connection::id_t& fd, struct bufferevent* _bev, bool _thread_safe) : Connection(fd), bev(_bev), thread_safe(_thread_safe), persist(true), decoder(nullptr) {}
  virtual ~LibeventConnection() {
    bufferevent_free(bev);
  }
  Future<std::string> read_till(const ConnectionHandle& _handle, size_t limit) {
    StringEntry* entry = new StringEntry(limit);
    {
      std::lock_guard<std::mutex> lock(_mutex);
      handle = _handle;
      read_queue.push_back(entry);
    }
    bufferevent_enable(bev, EV_READ);
    return entry->promise.future();
  }
  Future<size_t> write(const ConnectionHandle& _handle, const std::string& data) {
    SizeEntry* entry = new SizeEntry(data.size());
    std::lock_guard<std::mutex> write_lock(write_mutex);
    {
      std::lock_guard<std::mutex> lock(_mutex);
      handle = _handle;
      write_queue.push_back(entry);
    }
    bufferevent_lock(bev);
    bufferevent_enable(bev, EV_WRITE);
    evbuffer_add(bufferevent_get_output(bev), data.data(), data.size());
    bufferevent_unlock(bev);
    return entry->promise.future();
  }
  void write(Encoder* encoder, bool _persist) {
    const Encoder::IOKind io_kind = encoder->io_kind();
    switch (io_kind) {
      case Encoder::send_data: {
        DataEncoder* data_encoder = reinterpret_cast<DataEncoder*>(encoder);
        size_t size;
        const void* const data = data_encoder->next(&size);
        CHECK(size > 0);
        SizeEntry* entry = new SizeEntry(size, encoder);
        std::lock_guard<std::mutex> write_lock(write_mutex);
        {
          std::lock_guard<std::mutex> lock(_mutex);
          persist = _persist;
          handle = encoder->connection_handle();
          write_queue.push_back(entry);
        }
        evbuffer_add(bufferevent_get_output(bev), data, size);
        break;
      }
      case Encoder::send_file: {
        FileEncoder* file_encoder = reinterpret_cast<FileEncoder*>(encoder);
        size_t size;
        off_t offset;
        int fd = file_encoder->next(&offset, &size);
        SizeEntry* entry = new SizeEntry(size, encoder);
        std::lock_guard<std::mutex> write_lock(write_mutex);
        {
          std::lock_guard<std::mutex> lock(_mutex);
          persist = _persist;
          handle = encoder->connection_handle();
          write_queue.push_back(entry);
        }
        evbuffer_add_file(bufferevent_get_output(bev), fd, offset, size);
        break;
      }
    }
  }
  void finish_writes(size_t bytes) {
    std::list<SizeEntry *> finished;
    bool should_close;
    {
      std::lock_guard<std::mutex> lock(_mutex);
      for (;bytes;) {
        CHECK(!write_queue.empty());
        SizeEntry *entry = reinterpret_cast<SizeEntry*>(write_queue.front());
        size_t left = std::min(bytes, entry->limit - entry->finished);
        bytes -= left;
        entry->finished += left;
        if (entry->finished == entry->limit) {
          finished.push_back(entry);
          write_queue.pop_front();
        }
      }
      should_close = write_queue.empty() && !persist;
    }
    foreach(SizeEntry *entry, finished) {
      entry->promise.set(entry->finished);
      if (entry->cbarg) {
        DataEncoder *encoder = reinterpret_cast<DataEncoder*>(entry->cbarg);
        delete encoder;
      }
      delete entry;
    }
    if (should_close) {
      delete decoder;
      decoder = nullptr;
      handle.reset();
    }
  }
  void finish_reads() {
    struct evbuffer *input = bufferevent_get_input(bev);
    size_t length = evbuffer_get_length(input);
    const char* data = reinterpret_cast<const char *>(evbuffer_pullup(input, -1));
    size_t avail = length;
    {
      // TODO: we need to deal with the scenario where data is available before we request a read!
      std::lock_guard<std::mutex> lock(_mutex);
      for (;avail;) {
        CHECK(!read_queue.empty());
        Entry *entry_base = read_queue.front();
        switch (entry_base->get_kind()) {
          case Entry::string_entry: {
            StringEntry* entry = reinterpret_cast<StringEntry*>(entry_base);
            entry->ss.write(data, avail);
            avail = 0;
            break;
          }
          case Entry::size_entry: {
            throw std::logic_error("TODO: implement read of specific bytes into buffer");
            break;
          }
        }
      }
    }
    evbuffer_drain(input, length - avail);
  }
  void handle_conn_close() {
    std::list<Entry *> finished;
    // TODO: make a variable that we switch under the lock to prevent anyone new from registering requests.
    {
      std::lock_guard<std::mutex> lock(_mutex);
      std::swap(finished, read_queue);
    }
    for (;!finished.empty();) {
      Entry *entry_base = finished.front();
      switch (entry_base->get_kind()) {
        case Entry::string_entry: {
          StringEntry* entry = reinterpret_cast<StringEntry*>(entry_base);
          entry->promise.set(entry->ss.str());
          break;
        }
        case Entry::size_entry: {
          printf("TODO: implement size_entry read connection close\n");
          throw std::logic_error("TODO: implement size_entry read connection close");
          break;
        }
      }
      delete entry_base;
      finished.pop_front();
    }
    {
      std::lock_guard<std::mutex> lock(_mutex);
      std::swap(finished, write_queue);
    }
    foreach(Entry *base_entry, finished) {
      SizeEntry* entry = reinterpret_cast<SizeEntry*>(base_entry);
      entry->promise.set(entry->finished);
      if (entry->cbarg) {
        DataEncoder *encoder = reinterpret_cast<DataEncoder*>(entry->cbarg);
        delete encoder;
      }
      delete entry;
    }
    handle.reset();
  }
  void set_decoder(DataDecoder* _decoder)
  {
    CHECK(!decoder) << "Can not assign multiple decoders";
    decoder = _decoder;
  }
  DataDecoder* get_decoder() const {
    return decoder;
  }
private:
  struct bufferevent* bev;
  const bool thread_safe;
  std::list<Entry *> read_queue;
  std::list<Entry *> write_queue;
  ConnectionHandle handle; // TODO maybe these should be part of the entries?
  bool persist;
  DataDecoder* decoder;
  std::mutex _mutex;
  std::mutex write_mutex;
  friend class LibeventEventManager;
};

class LibeventEventManager : public EventManager
{
public:
  LibeventEventManager(EventManager::ProcessManager* _proc_man);

  virtual ~LibeventEventManager() {}

  void accepted(const ConnectionHandle& conn_handle);

  void closed(const Connection::id_t& conn_id);

  virtual void initialize() override;

  virtual Try<ConnectionHandle> make_new_connection(
      uint32_t ip,
      uint16_t port,
      int protocol) override;

  virtual Future<short> do_poll(
      const ConnectionHandle& conn_handle,
      short events) override;

  virtual Future<size_t> do_read(
      const ConnectionHandle& conn_handle,
      void* data,
      size_t size) override;

  virtual Future<std::string> do_read(
      const ConnectionHandle& conn_handle) override;

  virtual Future<Nothing> do_write(
      const ConnectionHandle& conn_handle,
      const std::string& data) override;

  virtual Future<size_t> do_write(
      const ConnectionHandle& conn_handle,
      const void* data,
      size_t size) override;

  virtual double get_time() const override;

  virtual void link(ProcessBase* process, const UPID& to) override;

  virtual PID<HttpProxy> proxy(
      const ConnectionHandle& connection_handle) override;

  virtual void send(Encoder* encoder, bool persist) override;
  virtual void send(const Response& response,
            const Request& request,
            const ConnectionHandle& connection_handle) override;
  virtual void send(Message* message) override;

  virtual void exited(ProcessBase* process) override;

  virtual bool has_pending_timers() const override;

  virtual void update_timer() override;

  virtual void try_update_timer() override;

  virtual Future<short> poll(int fd, short events) override;

  virtual Future<size_t> read(int fd, void* data, size_t size) override;

  virtual Future<std::string> read(int fd) override;

  virtual Future<size_t> write(int fd, const void* data, size_t size) override;

  virtual Future<Nothing> write(int fd, const std::string& data) override;

  virtual Future<Nothing> redirect(
      int from,
      Option<int> to,
      size_t chunk) override;

  /* A pointer to the dependency injection base class of ProcessManager. This
   can be publicly accessible since we're in a PIMPL pattern and using this
   pointer in libev function callbacks. */
  EventManager::ProcessManager* proc_man;

private:

  Try<ConnectionHandle> make_connection(
      uint32_t ip,
      uint16_t port,
      int protocol,
      bool thread_safe);

  struct event_base* ev_base;
  struct evconnlistener *listener;

  // Collection of all active connection handles.
  map<Connection::id_t, ConnectionHandle> connection_handles;

  // Map from socket to node (ip, port).
  map<Connection::id_t, Node> nodes;

  // Map from UPID (local/remote) to process.
  map<UPID, set<ProcessBase*> > links;

  map<Node, Connection::id_t> persists;

  // HTTP proxies.
  map<Connection::id_t, HttpProxy*> proxies;

  synchronizable(this);
};

static LibeventEventManager* LibeventMan;

EventManager* GetLibeventEventManager(
    EventManager::ProcessManager* proc_man)
{
  return LibeventMan ? LibeventMan
    : (LibeventMan = new LibeventEventManager(proc_man));
}

namespace internal {

struct event* timer_ev;
struct timeval tv {0, 10};
bool update_timer_flag = false;
bool pending_timers = false;

void timercb(int sock, short which, void *arg) {
  if (!evtimer_pending(timer_ev, NULL)) {
    event_del(timer_ev);
    evtimer_add(timer_ev, &tv);
  }
  list<Timer> timedout;

  synchronized (timeouts) {
    Time now = Clock::now();

    VLOG(3) << "Handling timeouts up to " << now;

    foreachkey (const Time& timeout, *timeouts) {
      if (timeout > now) {
        break;
      }

      VLOG(3) << "Have timeout(s) at " << timeout;

      // Record that we have pending timers to execute so the
      // Clock::settle() operation can wait until we're done.
      pending_timers = true;

      foreach (const Timer& timer, (*timeouts)[timeout]) {
        timedout.push_back(timer);
      }
    }

    // Now erase the range of timeouts that timed out.
    timeouts->erase(timeouts->begin(), timeouts->upper_bound(now));

    // Okay, so the timeout for the next timer should not have fired.
    CHECK(timeouts->empty() || (timeouts->begin()->first > now));

    // TODO(jmlvanre): set the timer correctly
#if 0
    // Update the timer as necessary.
    if (!timeouts->empty()) {
      // Determine when the next timer should fire.
      timeouts_watcher.repeat =
        (timeouts->begin()->first - Clock::now()).secs();

      if (timeouts_watcher.repeat <= 0) {
        // Feed the event now!
        timeouts_watcher.repeat = 0;
        ev_timer_again(loop, &timeouts_watcher);
        ev_feed_event(loop, &timeouts_watcher, EV_TIMEOUT);
      } else {
        // Don't fire the timer if the clock is paused since we don't
        // want time to advance (instead a call to Clock::advance()
        // will handle the timer).
        if (Clock::paused() && timeouts_watcher.repeat > 0) {
          timeouts_watcher.repeat = 0;
        }

        ev_timer_again(loop, &timeouts_watcher);
      }
    }
#endif

    update_timer_flag = false; // Since we might have a queued update_timer_flag.
  }

  // Update current time of process (if it's present/valid). It might
  // be necessary to actually add some more synchronization around
  // this so that, for example, pausing and resuming the clock doesn't
  // cause some processes to get thier current times updated and
  // others not. Since ProcessManager::use acquires the 'processes'
  // lock we had to move this out of the synchronized (timeouts) above
  // since there was a deadlock with acquring 'processes' then
  // 'timeouts' (reverse order) in ProcessManager::cleanup. Note that
  // current time may be greater than the timeout if a local message
  // was received (and happens-before kicks in).
  if (Clock::paused()) {
    foreach (const Timer& timer, timedout) {
      if (ProcessReference process = LibeventMan->proc_man->use(timer.creator())) {
        Clock::update(process, timer.timeout().time());
      }
    }
  }

  // Invoke the timers that timed out (TODO(benh): Do this
  // asynchronously so that we don't tie up the event thread!).
  foreach (const Timer& timer, timedout) {
    timer();
  }

  // Mark ourselves as done executing the timers since it's now safe
  // for a call to Clock::settle() to check if there will be any
  // future timeouts reached.
  synchronized (timeouts) {
    pending_timers = false;
  }
}

void *serve(void* _ev_base) {
  struct event_base* ev_base = reinterpret_cast<struct event_base*>(_ev_base);
  int r = event_base_loop(ev_base, 0);
  return nullptr;
}

namespace inbound {

void buffercb(struct evbuffer *buffer, const struct evbuffer_cb_info *info, void *arg)
{
  LibeventConnection* conn = reinterpret_cast<LibeventConnection*>(arg);
  if (info->n_deleted > 0) {
    conn->finish_writes(info->n_deleted);
  }
}

void readcb(struct bufferevent *bev, void *user_data)
{
  struct evbuffer *input = bufferevent_get_input(bev);
  size_t length = evbuffer_get_length(input);
  LibeventConnection* conn = reinterpret_cast<LibeventConnection*>(user_data);
  DataDecoder* decoder = conn->get_decoder();
  CHECK(length > 0);
  // TODO(jmlvanre) once we have a more intelligent parser we can user
  // evbuffer_get_contiguous_space
  const char *data = reinterpret_cast<const char *>(evbuffer_pullup(input, -1));
  const deque<Request*>& requests = decoder->decode(data, length);

  if (!requests.empty()) {
    evbuffer_drain(input, length);
    bufferevent_unlock(bev);
    foreach (Request* request, requests) {
      LibeventMan->proc_man->handle(decoder->connection_handle(), request);
    }
    bufferevent_lock(bev);
  } else if (requests.empty() && decoder->failed()) {
    printf("execption path [inbound::readcb] length=[%ld] data===\n%s\n===\n", length, data);
    VLOG(1) << "Decoder error while receiving";
    throw std::logic_error("TODO: implement exception path");
  }
}

void writecb(struct bufferevent *bev, void *user_data) {}

void eventcb(struct bufferevent *bev, short events, void *user_data)
{
  LibeventConnection* conn = reinterpret_cast<LibeventConnection*>(user_data);
  if (events & BEV_EVENT_EOF) {
    conn->handle_conn_close();
    bufferevent_unlock(bev);
    LibeventMan->closed(*conn);
    bufferevent_lock(bev);
  } else if (events & BEV_EVENT_ERROR) {
    conn->handle_conn_close();
    bufferevent_unlock(bev);
    LibeventMan->closed(*conn);
    bufferevent_lock(bev);
  }
}

} // inbound {

void acceptcb(struct evconnlistener *listener, int sock, struct sockaddr */*sa*/, int /*sa_len*/, void *arg)
{
  struct event_base *ev_base = evconnlistener_get_base(listener);
  struct bufferevent *bev = bufferevent_socket_new(ev_base, sock, BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS | BEV_OPT_THREADSAFE);
  std::shared_ptr<LibeventConnection> conn_handle = std::make_shared<LibeventConnection>(sock, bev, true);
  evbuffer_add_cb(bufferevent_get_output(bev), inbound::buffercb, conn_handle.get());
  DataDecoder* decoder = new DataDecoder(conn_handle);
  conn_handle->set_decoder(decoder);
  LibeventMan->accepted(conn_handle);
  int on = 1;
  if (setsockopt(sock, SOL_TCP, TCP_NODELAY, &on, sizeof(on)) < 0) {
    const char* error = strerror(errno);
    VLOG(1) << "Failed to turn off the Nagle algorithm: " << error;
    bufferevent_free(bev);
  } else {
    bufferevent_setcb(bev, inbound::readcb, inbound::writecb, inbound::eventcb, conn_handle.get());
    bufferevent_enable(bev, EV_READ | EV_WRITE);
  }
}

void readcb(struct bufferevent *bev, void *user_data)
{
  LibeventConnection* conn = reinterpret_cast<LibeventConnection*>(user_data);
  conn->finish_reads();
}

void writecb(struct bufferevent *bev, void *user_data) {}

void eventcb(struct bufferevent *bev, short events, void *user_data)
{
  LibeventConnection* conn = reinterpret_cast<LibeventConnection*>(user_data);
  if (events & BEV_EVENT_EOF) {
    conn->handle_conn_close();
  } else if (events & BEV_EVENT_ERROR) {
    conn->handle_conn_close();
  }
}

} // namespace internal {

LibeventEventManager::LibeventEventManager(ProcessManager* _proc_man)
  : proc_man(_proc_man)
{
  synchronizer(this) = SYNCHRONIZED_INITIALIZER_RECURSIVE;
}

void LibeventEventManager::initialize()
{
  int r = evthread_use_pthreads();
  if (r < 0) {
    LOG(FATAL) << "Failed to initialize, evthread_use_pthreads";
    return;
  }
  event_enable_debug_mode(); // TODO: remove

  ev_base = event_base_new(); // TODO: cleanup
  if (!ev_base) {
    LOG(FATAL) << "Failed to initialize, event_base";
    return;
  }
  internal::timer_ev = evtimer_new(ev_base, internal::timercb, nullptr);
  evtimer_add(internal::timer_ev, &internal::tv);
  listener = evconnlistener_new(ev_base, internal::acceptcb, nullptr, LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE /*| LEV_OPT_THREADSAFE*/, 1024, __s__); // TODO: cleanup
  pthread_t thread; // For now, not saving handles on our threads.
  if (pthread_create(&thread, NULL, internal::serve, ev_base) != 0) {
    LOG(FATAL) << "Failed to initialize, pthread_create";
  }
}

void LibeventEventManager::accepted(const ConnectionHandle& conn_handle)
{
  synchronized (this) {
    connection_handles[*conn_handle] = conn_handle;
  }
}

void LibeventEventManager::closed(const Connection::id_t& conn_id)
{
  synchronized (this) {
    connection_handles.erase(conn_id);
  }
}

Try<ConnectionHandle> LibeventEventManager::make_new_connection(
    uint32_t ip,
    uint16_t port,
    int protocol)
{
  return make_connection(ip, port, protocol, true);
}

Future<short> LibeventEventManager::do_poll(
    const ConnectionHandle& conn_handle,
    short events)
{
  throw std::logic_error("TODO: implement");
}

Future<size_t> LibeventEventManager::do_read(
    const ConnectionHandle& conn_handle,
    void* data,
    size_t size)
{
  printf("TODO: implement do_read\n");
  throw std::logic_error("TODO: implement");
}

Future<std::string> LibeventEventManager::do_read(
    const ConnectionHandle& conn_handle)
{
  LibeventConnection* conn = reinterpret_cast<LibeventConnection*>(conn_handle.get());
  // TODO: conn must hold a copy of itself to stay alive
  return conn->read_till(conn_handle, io::BUFFERED_READ_SIZE);
}

Future<Nothing> LibeventEventManager::do_write(
    const ConnectionHandle& conn_handle,
    const std::string& data)
{
  LibeventConnection* conn = reinterpret_cast<LibeventConnection*>(conn_handle.get());
  return conn->write(conn_handle, data)
    .then([]() {return Nothing();}); // TODO: non-lambda version
}

Future<size_t> LibeventEventManager::do_write(
    const ConnectionHandle& conn_handle,
    const void* data,
    size_t size)
{
  printf("TODO: implement do_write\n");
  throw std::logic_error("TODO: implement");
}

double LibeventEventManager::get_time() const
{
  struct timeval tval;
  event_base_gettimeofday_cached(ev_base, &tval);
  return tval.tv_sec + tval.tv_usec * 1e-6;
}

void LibeventEventManager::link(ProcessBase* process, const UPID& to)
{
  CHECK(process != NULL);

  Node node(to.ip, to.port);
  synchronized(this) {
    if ((node.ip != __ip__ || node.port != __port__)
      && persists.count(node) == 0) {
      // Okay, no link, let's create a socket.
      Try<ConnectionHandle> connection_handle = make_connection(
          to.ip,
          to.port,
          0,
          false);
      if (connection_handle.isError()) {
        LOG(FATAL) << "Fail to link, connection: " << connection_handle.error();
        return;
      }
      ConnectionHandle conn_handle = connection_handle.get();
      const Connection::id_t &conn_id = *conn_handle;
      connection_handles[conn_id] = conn_handle;

      nodes[conn_id] = node;

      persists[node] = conn_id;
    }
    links[to].insert(process);
  }
}

PID<HttpProxy> LibeventEventManager::proxy(
    const ConnectionHandle& connection_handle)
{
  HttpProxy* proxy = NULL;

  const Connection::id_t& conn_id = *connection_handle;
  synchronized (this) {
    if (connection_handles.count(conn_id) > 0) {
      if (proxies.count(conn_id) > 0) {
        return proxies[conn_id]->self();
      } else {
        proxy = new HttpProxy(connection_handle, this);
        proxies[conn_id] = proxy;
      }
    }
  }

  if (proxy != NULL) {
    return spawn(proxy, true);
  }

  return PID<HttpProxy>();
}

void LibeventEventManager::send(Encoder* encoder, bool persist)
{
  CHECK(encoder != NULL);
  const ConnectionHandle& conn_handle = encoder->connection_handle();
  LibeventConnection* conn = reinterpret_cast<LibeventConnection*>(conn_handle.get());
  const Connection::id_t &conn_id = *conn;
  if (!persist) {
    HttpProxy* proxy = nullptr;
    synchronized (this) {
      assert(connection_handles.find(conn_id) != connection_handles.end());
      connection_handles.erase(conn_id);
      auto pos = proxies.find(conn_id);
      if (pos != proxies.end()) {
        proxy = pos->second;
        proxies.erase(pos);
      }
    }
    if (proxy) {
      terminate(proxy);
    }
  }
  conn->write(encoder, persist);
}

void LibeventEventManager::send(const Response& response,
          const Request& request,
          const ConnectionHandle& connection_handle)
{
  bool persist = request.keepAlive;

  // Don't persist the connection if the headers include
  // 'Connection: close'.
  if (response.headers.contains("Connection")) {
    if (response.headers.get("Connection").get() == "close") {
      persist = false;
    }
  }

  send(new HttpResponseEncoder(connection_handle, response, request), persist);
}
void LibeventEventManager::send(Message* message)
{
  CHECK(message != NULL);
  Node node(message->to.ip, message->to.port);
  ConnectionHandle send_to_conn;
  bool persist = false;
  synchronized (this) {
    auto persist_pos = persists.find(node);
    // Check if there is already a socket.
    persist = persist_pos != persists.end();
    if (persist) {
      send_to_conn = connection_handles[persist_pos->second];
    } else {
      Try<ConnectionHandle> connection_handle = make_connection(
          message->to.ip,
          message->to.port,
          0,
          false);
      if (connection_handle.isError()) {
        LOG(FATAL) << "Fail to link, connection: " << connection_handle.error();
        return;
      }
      ConnectionHandle conn_handle = connection_handle.get();
      const Connection::id_t& conn_id = *conn_handle;

      connection_handles[conn_id] = conn_handle;
      nodes[conn_id] = node;
      send_to_conn = conn_handle;
    }
  }
  send(new MessageEncoder(send_to_conn, message), persist);
}

void LibeventEventManager::exited(ProcessBase* process)
{
  const UPID pid = get_pid(process);
  const Time time = Clock::now(process);
  synchronized (this) {
    // Iterate through the links, removing any links the process might
    // have had and creating exited events for any linked processes.
    foreachpair (const UPID& linkee, set<ProcessBase*>& processes, links) {
      processes.erase(process);
      if (linkee == pid) {
        foreach (ProcessBase* linker, processes) {
          CHECK(linker != process) << "Process linked with itself";
          synchronized (timeouts) {
            if (Clock::paused()) {
              Clock::update(linker, time);
            }
          }
          enqueue(linker, new ExitedEvent(linkee));
        }
      }
    }
    links.erase(pid);
  }
}

bool LibeventEventManager::has_pending_timers() const
{
  return internal::pending_timers;
}

void LibeventEventManager::update_timer()
{
  internal::update_timer_flag = true;
}

void LibeventEventManager::try_update_timer()
{
  if (!internal::update_timer_flag) {
    internal::update_timer_flag = true;
  }
}

class PipePollRequest {
public:
  PipePollRequest(short _interest) : interest(_interest) {}
  short interest;
  Promise<short> promise;
};

//TODO(jmlvanre): we should deal with this state and discard under a lock to prevent collisions?
void pipepollcb(evutil_socket_t fd, short what, void *arg) {
  PipePollRequest* request = reinterpret_cast<PipePollRequest*>(arg);
  short avail = ((what & EV_READ) ? io::READ : 0) | ((what & EV_WRITE) ? io::WRITE : 0);
  request->promise.set(avail);
  delete request;
}

Future<short> LibeventEventManager::poll(int fd, short events)
{
  bool is_file = true;
  off_t offset = lseek(fd, 0, SEEK_CUR);
  if (offset < 0 && errno == ESPIPE) {
    is_file = false;
  }
  if (is_file) {
    return events;
  } else {
    PipePollRequest* request = new PipePollRequest(events);
    short io_interest = ((events & io::READ) ? EV_READ : 0) | ((events & io::WRITE) ? EV_WRITE : 0);
    struct event* pipe_ev = event_new(ev_base, fd, io_interest, pipepollcb, request);
    event_add(pipe_ev, nullptr);
    return request->promise.future().onDiscard([request, pipe_ev](){
      event_del(pipe_ev);
      event_free(pipe_ev);
      request->promise.discard(); delete request;
    }); // old lambda version
  }
}

class IORequest {
  public:
  IORequest(int _fd, size_t _size) : ctxp(0), fd(_fd), size(_size) {
    memset(&_iocb, 0, sizeof(_iocb));
    // TODO(jmlvanre): configure max_aio_num.
    int ret = 0;
    if ((ret = io_setup(1, &ctxp)) < 0) {
      LOG(FATAL) << "Error setting up aio ctx: " << ret;
      return;
    }
  }
  ~IORequest() {
    io_destroy(ctxp);
  }

  io_context_t ctxp;
  int fd;
  size_t size;
  struct iocb _iocb;
  Promise<size_t> promise;
};

void tmpreadcb(evutil_socket_t fd, short what, void *arg) {
  IORequest* request = reinterpret_cast<IORequest*>(arg);
  // Ignore this function if the read operation has been discarded.
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
    delete request;
    return;
  }
  struct io_event io_ev[1];
  memset(io_ev, 0, sizeof(struct io_event));
  int r = io_getevents(request->ctxp, 1, 1, io_ev, nullptr);
  CHECK(r == 1);
  const io_event &ev = io_ev[0];
  if (ev.res != request->_iocb.u.c.nbytes) {
    request->promise.fail("Failed read io");
  } else {
    lseek(request->fd, request->size, SEEK_CUR);
    request->promise.set(ev.res);
  }
  delete request;
}

class PipeRequest {
  public:
  PipeRequest(void* _data, size_t _size) : ev(nullptr), data(_data), size(_size) {}
  ~PipeRequest() {
    if (ev) {
      event_free(ev);
    }
  }
  struct event* ev;
  void *data;
  size_t size;
  Promise<size_t> promise;
};

void pipereadcb(evutil_socket_t fd, short what, void *arg) {
  PipeRequest* request = reinterpret_cast<PipeRequest*>(arg);
  // Ignore this function if the read operation has been discarded.
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
    delete request;
    return;
  }
  ssize_t r = read(fd, request->data, request->size);
  if (r < 0) {
    printf("error reading from pipe [%d]\n", static_cast<int>(r));
    throw std::logic_error("Error");
  } else {
    request->promise.set(r);
    delete request;
  }
}

Future<size_t> LibeventEventManager::read(int fd, void* data, size_t size)
{
  if (size == 0) {
    return 0;
  }
  // Check the file descriptor.
  Try<bool> nonblock = os::isNonblock(fd);
  if (nonblock.isError()) {
    memory::shared_ptr<Promise<size_t> > promise(new Promise<size_t>());
    // The file descriptor is not valid (e.g., has been closed).
    promise->fail(
        "Failed to check if file descriptor was non-blocking: " +
        nonblock.error());
    return promise->future();
  } else if (!nonblock.get()) {
    memory::shared_ptr<Promise<size_t> > promise(new Promise<size_t>());
    // The file descriptor is not non-blocking.
    promise->fail("Expected a non-blocking file descriptor");
    return promise->future();
  }
  bool is_file = true;
  off_t offset = lseek(fd, 0, SEEK_CUR);
  if (offset < 0 && errno == ESPIPE) {
    is_file = false;
  }
  if (is_file) {
    IORequest *request = new IORequest(fd, size);
    int event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    struct event* ev = event_new(ev_base, event_fd, EV_READ, tmpreadcb, request);
    event_add(ev, nullptr);
    io_prep_pread(&request->_iocb, fd, data, size, offset);
    io_set_eventfd(&request->_iocb, event_fd);
    struct iocb *ioq[1];
    ioq[0] = &(request->_iocb);
    int r = io_submit(request->ctxp, 1, ioq);
    return request->promise.future();
  } else {
    PipeRequest* request = new PipeRequest(data, size);
    struct event* pipe_ev = event_new(ev_base, fd, EV_READ, pipereadcb, request);
    request->ev = pipe_ev;
    event_add(pipe_ev, nullptr);
    return request->promise.future().onDiscard([request, pipe_ev]() {
      event_active(pipe_ev, EV_READ, 0);
    }); // old lambda version
  }
}

class BufferedIORequest {
public:
  BufferedIORequest(size_t size) : ctxp(0) {
    data.resize(size);
    memset(&_iocb, 0, sizeof(iocb));
    int ret = 0;
    if ((ret = io_setup(1, &ctxp)) < 0) {
      LOG(FATAL) << "Error setting up aio ctx: " << ret;
      return;
    }
  }
  Promise<std::string> promise;
  std::string data;
  io_context_t ctxp;
  struct iocb _iocb;
};

void stringreadcb(evutil_socket_t fd, short what, void *arg) {
  BufferedIORequest* request = reinterpret_cast<BufferedIORequest*>(arg);
  // Ignore this function if the read operation has been discarded.
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
    delete request;
    return;
  }
  struct io_event io_ev[1];
  memset(io_ev, 0, sizeof(struct io_event));
  int r = io_getevents(request->ctxp, 1, 1, io_ev, nullptr);
  CHECK(r == 1);
  const io_event &ev = io_ev[0];
  if (ev.res != request->_iocb.u.c.nbytes) {
    request->promise.fail("Failed read io: " + stringify(strerror(-ev.res)));
  } else {
    request->promise.set(request->data);
  }
  delete request;
}

class BufferedPipeRequest {
  public:
  BufferedPipeRequest() : ev(nullptr), buf(nullptr) {
    buf = new char[io::BUFFERED_READ_SIZE];
  }
  ~BufferedPipeRequest() {
    if (ev) {
      event_free(ev);
    }
    delete buf;
  }
  struct event* ev;
  char *buf;
  std::ostringstream ss;
  Promise<std::string> promise;
};

void bufferedpipereadcb(evutil_socket_t fd, short what, void *arg) {
  BufferedPipeRequest* request = reinterpret_cast<BufferedPipeRequest*>(arg);
  // Ignore this function if the read operation has been discarded.
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
    delete request;
    return;
  }
  ssize_t r = read(fd, request->buf, io::BUFFERED_READ_SIZE);
  if (r < 0) {
    request->promise.fail("Failed reading from fd: " + stringify(strerror(errno)));
    delete request;
  } else if (r == 0) {
    request->promise.set(request->ss.str());
    delete request;
  } else {
    request->ss.write(request->buf, r);
  }
}

Future<std::string> LibeventEventManager::read(int fd)
{
  bool is_file = true;
  off_t offset = lseek(fd, 0, SEEK_CUR);
  if (offset < 0) {
    if (errno == ESPIPE) {
      is_file = false;
    } else {
      memory::shared_ptr<Promise<std::string> > promise(new Promise<std::string>());
      // The file descriptor is not valid (e.g., has been closed).
      promise->fail(
          "Failed reading from fd: " +
          stringify(strerror(errno)));
      return promise->future();
    }
  }
  if (is_file) {
    off_t end_offset = lseek(fd, 0, SEEK_END);
    size_t total_bytes_to_read = end_offset - offset;
    if (total_bytes_to_read == 0) {
      return std::string();
    }
    BufferedIORequest *request = new BufferedIORequest(total_bytes_to_read);
    int event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    struct event* ev = event_new(ev_base, event_fd, EV_READ, stringreadcb, request);
    event_add(ev, nullptr);
    io_prep_pread(&request->_iocb, fd, const_cast<char *>(request->data.data()), total_bytes_to_read, offset);
    io_set_eventfd(&request->_iocb, event_fd);
    struct iocb *ioq[1];
    ioq[0] = &(request->_iocb);
    int r = io_submit(request->ctxp, 1, ioq);
    return request->promise.future();
  } else {
    BufferedPipeRequest* request = new BufferedPipeRequest();
    struct event* pipe_ev = event_new(ev_base, fd, EV_READ | EV_PERSIST, bufferedpipereadcb, request);
    request->ev = pipe_ev;
    event_add(pipe_ev, nullptr);
    return request->promise.future().onDiscard([request, pipe_ev]() {
      event_active(pipe_ev, EV_READ, 0);
    }); // old lambda version
  }
}

void pipewritecb(evutil_socket_t fd, short what, void *arg) {
  PipeRequest* request = reinterpret_cast<PipeRequest*>(arg);
  // Ignore this function if the read operation has been discarded.
  if (request->promise.future().hasDiscard()) {
    request->promise.discard();
    delete request;
    return;
  }
  bool pending = os::signals::pending(SIGPIPE);
  bool unblock = !pending ? os::signals::block(SIGPIPE) : false;
  ssize_t length = write(fd, request->data, request->size);

  // Save the errno so we can restore it after doing sig* functions
  // below.
  int errno_ = errno;

  if (length < 0 && errno == EPIPE && !pending) {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGPIPE);

    int result;
    do {
      int ignored;
      result = sigwait(&mask, &ignored);
    } while (result == -1 && errno == EINTR);
  }

  if (unblock) {
    os::signals::unblock(SIGPIPE);
  }

  errno = errno_;

  if (length < 0) {
    if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
      throw std::logic_error("We should not fail writing if we were able to write");
    } else {
      // Error occurred.
      request->promise.fail(strerror(errno));
      delete request;
    }
  } else {
    request->promise.set(length);
    delete request;
  }
}

Future<size_t> LibeventEventManager::write(int fd, const void* data, size_t size)
{
  if (size == 0) {
    return 0;
  }
  // Check the file descriptor.
  Try<bool> nonblock = os::isNonblock(fd);
  if (nonblock.isError()) {
    memory::shared_ptr<Promise<size_t> > promise(new Promise<size_t>());
    // The file descriptor is not valid (e.g., has been closed).
    promise->fail(
        "Failed to check if file descriptor was non-blocking: " +
        nonblock.error());
    return promise->future();
  } else if (!nonblock.get()) {
    memory::shared_ptr<Promise<size_t> > promise(new Promise<size_t>());
    // The file descriptor is not non-blocking.
    promise->fail("Expected a non-blocking file descriptor");
    return promise->future();
  }
  bool is_file = true;
  off_t offset = lseek(fd, 0, SEEK_CUR);
  if (offset < 0 && errno == ESPIPE) {
    if (errno == ESPIPE) {
      is_file = false;
    } else {
      memory::shared_ptr<Promise<size_t> > promise(new Promise<size_t>());
      // The file descriptor is not valid (e.g., has been closed).
      promise->fail(
          "Failed write to fd: " +
          stringify(strerror(errno)));
      return promise->future();
    }
  }
  if (is_file) {
    IORequest *request = new IORequest(fd, size);
    int event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    struct event* ev = event_new(ev_base, event_fd, EV_READ, tmpreadcb, request);
    event_add(ev, nullptr);
    io_prep_pwrite(&request->_iocb, fd, const_cast<void *>(data), size, offset);
    io_set_eventfd(&request->_iocb, event_fd);
    struct iocb *ioq[1];
    ioq[0] = &(request->_iocb);
    int r = io_submit(request->ctxp, 1, ioq);
    return request->promise.future();
  } else {
    PipeRequest* request = new PipeRequest(const_cast<void *>(data), size);
    struct event* pipe_ev = event_new(ev_base, fd, EV_WRITE, pipewritecb, request);
    request->ev = pipe_ev;
    event_add(pipe_ev, nullptr);
    return request->promise.future().onDiscard([request, pipe_ev]() {
      event_active(pipe_ev, EV_WRITE, 0);
    }); // old lambda version
  }
}

class BufferedWriteRequest {
public:
  BufferedWriteRequest(LibeventEventManager*_ev_man, int _fd, const std::string& _data) : ev_man(_ev_man), bytes_written(0), fd(_fd), data(_data) {}
  LibeventEventManager* ev_man;
  size_t bytes_written;
  int fd;
  std::string data;
  Future<Nothing> complete(size_t bytes) {
    bytes_written += bytes;
    size_t bytes_left = data.size() - bytes_written;
    if (bytes_left > 0) {
      return ev_man->write(fd, data.data() + bytes_written, bytes_left).then(lambda::bind(&BufferedWriteRequest::complete, this, lambda::_1));
    }
    delete this;
    return Nothing();
  }
};

Future<Nothing> LibeventEventManager::write(int fd, const std::string& data)
{
  BufferedWriteRequest* request = new BufferedWriteRequest(this, fd, data);
  return write(fd, request->data.data(), request->data.size()).then(lambda::bind(&BufferedWriteRequest::complete, request, lambda::_1))
    .then([](){return Nothing();});
}

namespace internal {

class RedirectRequest {
public:
  RedirectRequest(LibeventEventManager* _ev_man, int _from, int _to, size_t _chunk) : from(_from), to(_to), chunk(_chunk), ev_man(_ev_man), buf(nullptr), data_ready(0), read_ev(nullptr), write_ev(nullptr) {
    buf = new char[chunk];
  }
  ~RedirectRequest() {
    delete []buf;
    if (read_ev) {
      event_free(read_ev);
    }
    if (write_ev) {
      event_free(write_ev);
    }
  }
  Promise<Nothing> promise;
  int from;
  int to;
  size_t chunk;
  LibeventEventManager* ev_man;
  char *buf;
  size_t data_ready;
  struct event* read_ev;
  struct event* write_ev;
};

void redirectreadcb(evutil_socket_t fd, short what, void *arg) {
  RedirectRequest* request = reinterpret_cast<RedirectRequest*>(arg);
  request->ev_man->read(request->from, request->buf, request->chunk)
    .then([request](size_t bytes) {
      if (bytes == 0) {
        auto f = request->promise.future();
        request->promise.set(Nothing());
        delete request;
        return f;
      } else {
        request->data_ready = bytes;
        event_active(request->write_ev, EV_WRITE, 0);
      }
      return request->promise.future();
    });
}

void redirectwritecb(evutil_socket_t fd, short what, void *arg) {
  RedirectRequest* request = reinterpret_cast<RedirectRequest*>(arg);
  request->ev_man->write(request->to, request->buf, request->data_ready)
    .then([request](size_t bytes) {
      event_active(request->read_ev, EV_READ, 0);
      return request->promise.future();
    });
}

Future<Nothing> redirect(LibeventEventManager* event_manager, struct event_base* ev_base, int from, int to, size_t chunk) {
  RedirectRequest* request = new RedirectRequest(event_manager, from, to, chunk);
  struct event* read_ev = event_new(ev_base, -1, 0, redirectreadcb, request);
  struct event* write_ev = event_new(ev_base, -1, 0, redirectwritecb, request);
  request->read_ev = read_ev;
  request->write_ev = write_ev;
  event_add(read_ev, nullptr);
  event_add(write_ev, nullptr);
  return event_manager->read(from, request->buf, chunk)
    .then([request, read_ev, write_ev](size_t bytes) {
      request->data_ready = bytes;
      event_active(request->write_ev, EV_WRITE, 0);
      return request->promise.future();
    });
}

} // namespace internal {

Future<Nothing> LibeventEventManager::redirect(
    int from,
    Option<int> to,
    size_t chunk)
{
  // Make sure we've got "valid" file descriptors.
  if (from < 0 || (to.isSome() && to.get() < 0)) {
    return Failure(strerror(EBADF));
  }

  if (to.isNone()) {
    // Open up /dev/null that we can splice into.
    Try<int> open = os::open("/dev/null", O_WRONLY);

    if (open.isError()) {
      return Failure("Failed to open /dev/null for writing: " + open.error());
    }

    to = open.get();
  } else {
    // Duplicate 'to' so that we're in control of its lifetime.
    int fd = dup(to.get());
    if (fd == -1) {
      return Failure(ErrnoError("Failed to duplicate 'to' file descriptor"));
    }

    to = fd;
  }

  CHECK_SOME(to);

  // Duplicate 'from' so that we're in control of its lifetime.
  from = dup(from);
  if (from == -1) {
    return Failure(ErrnoError("Failed to duplicate 'from' file descriptor"));
  }

  // Set the close-on-exec flag (no-op if already set).
  Try<Nothing> cloexec = os::cloexec(from);
  if (cloexec.isError()) {
    os::close(from);
    os::close(to.get());
    return Failure("Failed to set close-on-exec on 'from': " + cloexec.error());
  }

  cloexec = os::cloexec(to.get());
  if (cloexec.isError()) {
    os::close(from);
    os::close(to.get());
    return Failure("Failed to set close-on-exec on 'to': " + cloexec.error());
  }

  // Make the file descriptors non-blocking (no-op if already set).
  Try<Nothing> nonblock = os::nonblock(from);
  if (nonblock.isError()) {
    os::close(from);
    os::close(to.get());
    return Failure("Failed to make 'from' non-blocking: " + nonblock.error());
  }

  nonblock = os::nonblock(to.get());
  if (nonblock.isError()) {
    os::close(from);
    os::close(to.get());
    return Failure("Failed to make 'to' non-blocking: " + nonblock.error());
  }
  int to_fd = to.get();
  return internal::redirect(this, ev_base, from, to.get(), chunk)
    .onAny(lambda::bind(&os::close, from))
    .onAny(lambda::bind(&os::close, to.get()));
}

Try<ConnectionHandle> LibeventEventManager::make_connection(
      uint32_t ip,
      uint16_t port,
      int protocol,
      bool thread_safe) {
  Try<int> socket = process::socket(AF_INET, SOCK_STREAM, 0);
  int s = socket.get();

  Try<Nothing> nonblock = os::nonblock(s);
  if (nonblock.isError()) {
    return Error("Failed to create connection nonblock: " + nonblock.error());
  }

  Try<Nothing> cloexec = os::cloexec(s);
  if (cloexec.isError()) {
    return Error("Failed to create connection: cloexec: " + cloexec.error());
  }
  struct bufferevent* bev = bufferevent_socket_new(ev_base, s, BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS | (thread_safe ? BEV_OPT_THREADSAFE : BEV_OPT_THREADSAFE));
  if (!bev) {
    return Error("Failed to create connection: bufferevent_socket_new");
  }
  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = ip;
  ConnectionHandle conn_handle = std::make_shared<LibeventConnection>(s, bev, thread_safe);
  evbuffer_add_cb(bufferevent_get_output(bev), internal::inbound::buffercb, conn_handle.get());
  bufferevent_setcb(bev, internal::readcb, internal::writecb, internal::eventcb, conn_handle.get());
  if (bufferevent_socket_connect(bev, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
    bufferevent_free(bev);
    return Error("Failed to create connection: bufferevent_socket_connect");
  }
  return conn_handle;
}

} // namespace process {