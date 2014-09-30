#include <ev.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <list>
#include <map>
#include <queue>
#include <set>

#include <boost/shared_array.hpp>

#include <process/http.hpp>
#include <process/io.hpp>
#include <process/node.hpp>

#include "decoder.hpp"
#include "libev_event_manager.hpp"

using process::http::Request;
using process::http::Response;

using std::deque;
using std::list;
using std::map;
using std::queue;
using std::set;
using std::string;

namespace process {

class LibevEventManager : public EventManager
{
public:
  LibevEventManager(EventManager::ProcessManager* _proc_man);

  virtual ~LibevEventManager() {}

  virtual void initialize() override;

  virtual double get_time() const override;

  Socket accepted(int s);

  virtual void link(ProcessBase* process, const UPID& to) override;

  virtual PID<HttpProxy> proxy(const Socket& socket) override;

  virtual void send(Encoder* encoder, bool persist) override;
  virtual void send(const Response& response,
            const Request& request,
            const Socket& socket) override;
  virtual void send(Message* message) override;

  Encoder* next(int s);

  void close(int s);

  void exited(const Node& node);
  virtual void exited(ProcessBase* process) override;

  virtual bool has_pending_timers() const override;

  virtual void update_timer() override;

  virtual void try_update_timer() override;

  virtual Future<short> poll(int fd, short events) override;

  virtual Future<size_t> read(int fd, void* data, size_t size) override;

  virtual Future<std::string> read(int fd) override;

  virtual Future<size_t> write(int fd, void* data, size_t size) override;

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
  // Map from UPID (local/remote) to process.
  map<UPID, set<ProcessBase*> > links;

  // Collection of all actice sockets.
  map<int, Socket> sockets;

  // Collection of sockets that should be disposed when they are
  // finished being used (e.g., when there is no more data to send on
  // them).
  set<int> dispose;

  // Map from socket to node (ip, port).
  map<int, Node> nodes;

  // Maps from node (ip, port) to temporary sockets (i.e., they will
  // get closed once there is no more data to send on them).
  map<Node, int> temps;

  // Maps from node (ip, port) to persistent sockets (i.e., they will
  // remain open even if there is no more data to send on them).  We
  // distinguish these from the 'temps' collection so we can tell when
  // a persistant socket has been lost (and thus generate
  // ExitedEvents).
  map<Node, int> persists;

  // Map from socket to outgoing queue.
  map<int, queue<Encoder*> > outgoing;

  // HTTP proxies.
  map<int, HttpProxy*> proxies;

  // Protects instance variables.
  synchronizable(this);

};

static LibevEventManager* LibevMan;

EventManager* GetLibevEventManager(
    EventManager::ProcessManager* proc_man)
{
  return LibevMan ? LibevMan : (LibevMan = new LibevEventManager(proc_man));
}

// Event loop.
static struct ev_loop* loop = NULL;

// Asynchronous watcher for interrupting loop.
static ev_async async_watcher;

// Watcher for timeouts.
static ev_timer timeouts_watcher;

// Server watcher for accepting connections.
static ev_io server_watcher;

// Queue of functions to be invoked asynchronously within the vent
// loop (protected by 'watchers' below).
static queue<lambda::function<void(void)> >* functions =
  new queue<lambda::function<void(void)> >();

// Queue of I/O watchers.
// (protected by 'watchers' below).
// TODO(benh): Replace this queue with functions that we put in
// 'functions' below that perform the ev_io_start themselves.
static queue<ev_io*>* watchers = new queue<ev_io*>();
static synchronizable(watchers) = SYNCHRONIZED_INITIALIZER;

// Flag to indicate whether or to update the timer on async interrupt.
static bool update_timer_flag = false;

// For supporting Clock::settle(), true if timers have been removed
// from 'timeouts' but may not have been executed yet. Protected by
// the timeouts lock. This is only used when the clock is paused.
static bool pending_timers = false;

typedef void (*Sender)(struct ev_loop*, ev_io*, int);

void send_data(struct ev_loop*, ev_io*, int);
void send_file(struct ev_loop*, ev_io*, int);

Sender get_send_function(Encoder::IOKind kind) {
  switch (kind) {
    case Encoder::send_data: {
      return send_data;
    }
    case Encoder::send_file: {
      return send_file;
    }
    default: {
      std::cerr << "Unhandled Encoder IOKind" << std::endl;
      abort();
    }
  }
}

// Wrapper around function we want to run in the event loop.
template <typename T>
void _run_in_event_loop(
    const lambda::function<Future<T>(void)>& f,
    const Owned<Promise<T> >& promise)
{
  // Don't bother running the function if the future has been discarded.
  if (promise->future().hasDiscard()) {
    promise->discard();
  } else {
    promise->set(f());
  }
}


// Helper for running a function in the event loop.
template <typename T>
Future<T> run_in_event_loop(const lambda::function<Future<T>(void)>& f)
{
  Owned<Promise<T> > promise(new Promise<T>());

  Future<T> future = promise->future();

  // Enqueue the function.
  synchronized (watchers) {
    functions->push(lambda::bind(&_run_in_event_loop<T>, f, promise));
  }

  // Interrupt the loop.
  ev_async_send(loop, &async_watcher);

  return future;
}

void* serve(void* arg)
{
  ev_loop(((struct ev_loop*) arg), 0);

  return NULL;
}

void handle_async(struct ev_loop* loop, ev_async* _, int revents)
{
  synchronized (watchers) {
    // Start all the new I/O watchers.
    while (!watchers->empty()) {
      ev_io* watcher = watchers->front();
      watchers->pop();
      ev_io_start(loop, watcher);
    }

    while (!functions->empty()) {
      (functions->front())();
      functions->pop();
    }
  }

  synchronized (timeouts) {
    if (update_timer_flag) {
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
          // Don't fire the timer if the clock is paused since we
          // don't want time to advance (instead a call to
          // clock::advance() will handle the timer).
          if (Clock::paused() && timeouts_watcher.repeat > 0) {
            timeouts_watcher.repeat = 0;
          }

          ev_timer_again(loop, &timeouts_watcher);
        }
      }

      update_timer_flag = false;
    }
  }
}


void handle_timeouts(struct ev_loop* loop, ev_timer* _, int revents)
{
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
      if (ProcessReference process = LibevMan->proc_man->use(timer.creator())) {
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


void recv_data(struct ev_loop* loop, ev_io* watcher, int revents)
{
  DataDecoder* decoder = (DataDecoder*) watcher->data;

  int s = watcher->fd;

  while (true) {
    const ssize_t size = 80 * 1024;
    ssize_t length = 0;

    char data[size];

    length = recv(s, data, size, 0);

    if (length < 0 && (errno == EINTR)) {
      // Interrupted, try again now.
      continue;
    } else if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Might block, try again later.
      break;
    } else if (length <= 0) {
      // Socket error or closed.
      if (length < 0) {
        const char* error = strerror(errno);
        VLOG(1) << "Socket error while receiving: " << error;
      } else {
        VLOG(2) << "Socket closed while receiving";
      }
      LibevMan->close(s);
      delete decoder;
      ev_io_stop(loop, watcher);
      delete watcher;
      break;
    } else {
      CHECK(length > 0);

      // Decode as much of the data as possible into HTTP requests.
      const deque<Request*>& requests = decoder->decode(data, length);

      if (!requests.empty()) {
        foreach (Request* request, requests) {
          LibevMan->proc_man->handle(decoder->socket(), request);
        }
      } else if (requests.empty() && decoder->failed()) {
        VLOG(1) << "Decoder error while receiving";
        LibevMan->close(s);
        delete decoder;
        ev_io_stop(loop, watcher);
        delete watcher;
        break;
      }
    }
  }
}


// A variant of 'recv_data' that doesn't do anything with the
// data. Used by sockets created via SocketManager::link as well as
// SocketManager::send(Message) where we don't care about the data
// received we mostly just want to know when the socket has been
// closed.
void ignore_data(struct ev_loop* loop, ev_io* watcher, int revents)
{
  Socket* socket = (Socket*) watcher->data;

  int s = watcher->fd;

  while (true) {
    const ssize_t size = 80 * 1024;
    ssize_t length = 0;

    char data[size];

    length = recv(s, data, size, 0);

    if (length < 0 && (errno == EINTR)) {
      // Interrupted, try again now.
      continue;
    } else if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Might block, try again later.
      break;
    } else if (length <= 0) {
      // Socket error or closed.
      if (length < 0) {
        const char* error = strerror(errno);
        VLOG(1) << "Socket error while receiving: " << error;
      } else {
        VLOG(2) << "Socket closed while receiving";
      }
      LibevMan->close(s);
      ev_io_stop(loop, watcher);
      delete socket;
      delete watcher;
      break;
    } else {
      VLOG(2) << "Ignoring " << length << " bytes of data received "
              << "on socket used only for sending";
    }
  }
}


void send_data(struct ev_loop* loop, ev_io* watcher, int revents)
{
  DataEncoder* encoder = (DataEncoder*) watcher->data;

  int s = watcher->fd;

  while (true) {
    const void* data;
    size_t size;

    data = encoder->next(&size);
    CHECK(size > 0);

    ssize_t length = send(s, data, size, MSG_NOSIGNAL);

    if (length < 0 && (errno == EINTR)) {
      // Interrupted, try again now.
      encoder->backup(size);
      continue;
    } else if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Might block, try again later.
      encoder->backup(size);
      break;
    } else if (length <= 0) {
      // Socket error or closed.
      if (length < 0) {
        const char* error = strerror(errno);
        VLOG(1) << "Socket error while sending: " << error;
      } else {
        VLOG(1) << "Socket closed while sending";
      }
      LibevMan->close(s);
      delete encoder;
      ev_io_stop(loop, watcher);
      delete watcher;
      break;
    } else {
      CHECK(length > 0);

      // Update the encoder with the amount sent.
      encoder->backup(size - length);

      // See if there is any more of the message to send.
      if (encoder->remaining() == 0) {
        delete encoder;

        // Stop this watcher for now.
        ev_io_stop(loop, watcher);

        // Check for more stuff to send on socket.
        Encoder* next = LibevMan->next(s);
        if (next != NULL) {
          watcher->data = next;
          ev_io_init(watcher, get_send_function(next->io_kind()), s, EV_WRITE);
          ev_io_start(loop, watcher);
        } else {
          // Nothing more to send right now, clean up.
          delete watcher;
        }
        break;
      }
    }
  }
}


void send_file(struct ev_loop* loop, ev_io* watcher, int revents)
{
  FileEncoder* encoder = (FileEncoder*) watcher->data;

  int s = watcher->fd;

  while (true) {
    int fd;
    off_t offset;
    size_t size;

    fd = encoder->next(&offset, &size);
    CHECK(size > 0);

    ssize_t length = os::sendfile(s, fd, offset, size);

    if (length < 0 && (errno == EINTR)) {
      // Interrupted, try again now.
      encoder->backup(size);
      continue;
    } else if (length < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Might block, try again later.
      encoder->backup(size);
      break;
    } else if (length <= 0) {
      // Socket error or closed.
      if (length < 0) {
        const char* error = strerror(errno);
        VLOG(1) << "Socket error while sending: " << error;
      } else {
        VLOG(1) << "Socket closed while sending";
      }
      LibevMan->close(s);
      delete encoder;
      ev_io_stop(loop, watcher);
      delete watcher;
      break;
    } else {
      CHECK(length > 0);

      // Update the encoder with the amount sent.
      encoder->backup(size - length);

      // See if there is any more of the message to send.
      if (encoder->remaining() == 0) {
        delete encoder;

        // Stop this watcher for now.
        ev_io_stop(loop, watcher);

        // Check for more stuff to send on socket.
        Encoder* next = LibevMan->next(s);
        if (next != NULL) {
          watcher->data = next;
          ev_io_init(watcher, get_send_function(next->io_kind()), s, EV_WRITE);
          ev_io_start(loop, watcher);
        } else {
          // Nothing more to send right now, clean up.
          delete watcher;
        }
        break;
      }
    }
  }
}


void sending_connect(struct ev_loop* loop, ev_io* watcher, int revents)
{
  int s = watcher->fd;

  // Now check that a successful connection was made.
  int opt;
  socklen_t optlen = sizeof(opt);

  if (getsockopt(s, SOL_SOCKET, SO_ERROR, &opt, &optlen) < 0 || opt != 0) {
    // Connect failure.
    VLOG(1) << "Socket error while connecting";
    LibevMan->close(s);
    MessageEncoder* encoder = (MessageEncoder*) watcher->data;
    delete encoder;
    ev_io_stop(loop, watcher);
    delete watcher;
  } else {
    // We're connected! Now let's do some sending.
    ev_io_stop(loop, watcher);
    ev_io_init(watcher, send_data, s, EV_WRITE);
    ev_io_start(loop, watcher);
  }
}


void receiving_connect(struct ev_loop* loop, ev_io* watcher, int revents)
{
  int s = watcher->fd;

  // Now check that a successful connection was made.
  int opt;
  socklen_t optlen = sizeof(opt);

  if (getsockopt(s, SOL_SOCKET, SO_ERROR, &opt, &optlen) < 0 || opt != 0) {
    // Connect failure.
    VLOG(1) << "Socket error while connecting";
    LibevMan->close(s);
    Socket* socket = (Socket*) watcher->data;
    delete socket;
    ev_io_stop(loop, watcher);
    delete watcher;
  } else {
    // We're connected! Now let's do some receiving.
    ev_io_stop(loop, watcher);
    ev_io_init(watcher, ignore_data, s, EV_READ);
    ev_io_start(loop, watcher);
  }
}


void accept(struct ev_loop* loop, ev_io* watcher, int revents)
{
  CHECK_EQ(__s__, watcher->fd);

  sockaddr_in addr;
  socklen_t addrlen = sizeof(addr);

  int s = ::accept(__s__, (sockaddr*) &addr, &addrlen);

  if (s < 0) {
    return;
  }

  Try<Nothing> nonblock = os::nonblock(s);
  if (nonblock.isError()) {
    LOG_IF(INFO, VLOG_IS_ON(1)) << "Failed to accept, nonblock: "
                                << nonblock.error();
    os::close(s);
    return;
  }

  Try<Nothing> cloexec = os::cloexec(s);
  if (cloexec.isError()) {
    LOG_IF(INFO, VLOG_IS_ON(1)) << "Failed to accept, cloexec: "
                                << cloexec.error();
    os::close(s);
    return;
  }

  // Turn off Nagle (TCP_NODELAY) so pipelined requests don't wait.
  int on = 1;
  if (setsockopt(s, SOL_TCP, TCP_NODELAY, &on, sizeof(on)) < 0) {
    const char* error = strerror(errno);
    VLOG(1) << "Failed to turn off the Nagle algorithm: " << error;
    os::close(s);
  } else {
    // Inform the socket manager for proper bookkeeping.
    const Socket& socket = LibevMan->accepted(s);

    // Allocate and initialize the decoder and watcher.
    DataDecoder* decoder = new DataDecoder(socket);

    ev_io* watcher = new ev_io();
    watcher->data = decoder;

    ev_io_init(watcher, recv_data, s, EV_READ);
    ev_io_start(loop, watcher);
  }
}

LibevEventManager::LibevEventManager(EventManager::ProcessManager* _proc_man)
    : proc_man(_proc_man)
{
  synchronizer(this) = SYNCHRONIZED_INITIALIZER_RECURSIVE;
}

void LibevEventManager::initialize()
{
    // Setup event loop.
#ifdef __sun__
  loop = ev_default_loop(EVBACKEND_POLL | EVBACKEND_SELECT);
#else
  loop = ev_default_loop(EVFLAG_AUTO);
#endif // __sun__

  ev_async_init(&async_watcher, handle_async);
  ev_async_start(loop, &async_watcher);

  ev_timer_init(&timeouts_watcher, handle_timeouts, 0., 2100000.0);
  ev_timer_again(loop, &timeouts_watcher);

  ev_io_init(&server_watcher, accept, __s__, EV_READ);
  ev_io_start(loop, &server_watcher);

//   ev_child_init(&child_watcher, child_exited, pid, 0);
//   ev_child_start(loop, &cw);

//   /* Install signal handler. */
//   struct sigaction sa;

//   sa.sa_handler = ev_sighandler;
//   sigfillset (&sa.sa_mask);
//   sa.sa_flags = SA_RESTART; /* if restarting works we save one iteration */
//   sigaction (w->signum, &sa, 0);

//   sigemptyset (&sa.sa_mask);
//   sigaddset (&sa.sa_mask, w->signum);
//   sigprocmask (SIG_UNBLOCK, &sa.sa_mask, 0);

  pthread_t thread; // For now, not saving handles on our threads.
  if (pthread_create(&thread, NULL, serve, loop) != 0) {
    LOG(FATAL) << "Failed to initialize, pthread_create";
  }
}

double LibevEventManager::get_time() const
{
  return ev_time();
}

Socket LibevEventManager::accepted(int s)
{
  synchronized (this) {
    return sockets[s] = Socket(s);
  }

  return UNREACHABLE(); // Quiet the compiler.
}


void LibevEventManager::link(ProcessBase* process, const UPID& to)
{
  // TODO(benh): The semantics we want to support for link are such
  // that if there is nobody to link to (local or remote) then an
  // ExitedEvent gets generated. This functionality has only been
  // implemented when the link is local, not remote. Of course, if
  // there is nobody listening on the remote side, then this should
  // work remotely ... but if there is someone listening remotely just
  // not at that id, then it will silently continue executing.

  CHECK(process != NULL);

  Node node(to.ip, to.port);

  synchronized (this) {
    // Check if node is remote and there isn't a persistant link.
    if ((node.ip != __ip__ || node.port != __port__)
        && persists.count(node) == 0) {
      // Okay, no link, let's create a socket.
      Try<int> socket = process::socket(AF_INET, SOCK_STREAM, 0);
      if (socket.isError()) {
        LOG(FATAL) << "Failed to link, socket: " << socket.error();
      }

      int s = socket.get();

      Try<Nothing> nonblock = os::nonblock(s);
      if (nonblock.isError()) {
        LOG(FATAL) << "Failed to link, nonblock: " << nonblock.error();
      }

      Try<Nothing> cloexec = os::cloexec(s);
      if (cloexec.isError()) {
        LOG(FATAL) << "Failed to link, cloexec: " << cloexec.error();
      }

      sockets[s] = Socket(s);
      nodes[s] = node;

      persists[node] = s;

      // Allocate and initialize a watcher for reading data from this
      // socket. Note that we don't expect to receive anything other
      // than HTTP '202 Accepted' responses which we anyway ignore.
      // We do, however, want to react when it gets closed so we can
      // generate appropriate lost events (since this is a 'link').
      ev_io* watcher = new ev_io();
      watcher->data = new Socket(sockets[s]);

      // Try and connect to the node using this socket.
      sockaddr_in addr;
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = PF_INET;
      addr.sin_port = htons(to.port);
      addr.sin_addr.s_addr = to.ip;

      if (connect(s, (sockaddr*) &addr, sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) {
          PLOG(FATAL) << "Failed to link, connect";
        }

        // Wait for socket to be connected.
        ev_io_init(watcher, receiving_connect, s, EV_WRITE);
      } else {
        ev_io_init(watcher, ignore_data, s, EV_READ);
      }

      // Enqueue the watcher.
      synchronized (watchers) {
        watchers->push(watcher);
      }

      // Interrupt the loop.
      ev_async_send(loop, &async_watcher);
    }

    links[to].insert(process);
  }
}


PID<HttpProxy> LibevEventManager::proxy(const Socket& socket)
{
  HttpProxy* proxy = NULL;

  synchronized (this) {
    // This socket might have been asked to get closed (e.g., remote
    // side hang up) while a process is attempting to handle an HTTP
    // request. Thus, if there is no more socket, return an empty PID.
    if (sockets.count(socket) > 0) {
      if (proxies.count(socket) > 0) {
        return proxies[socket]->self();
      } else {
        proxy = new HttpProxy(sockets[socket], this);
        proxies[socket] = proxy;
      }
    }
  }

  // Now check if we need to spawn a newly created proxy. Note that we
  // need to do this outside of the synchronized block above to avoid
  // a possible deadlock (because spawn eventually synchronizes on
  // ProcessManager and ProcessManager::cleanup synchronizes on
  // ProcessManager and then SocketManager, so a deadlock results if
  // we do spawn within the synchronized block above).
  if (proxy != NULL) {
    return spawn(proxy, true);
  }

  return PID<HttpProxy>();
}


void LibevEventManager::send(Encoder* encoder, bool persist)
{
  CHECK(encoder != NULL);

  synchronized (this) {
    if (sockets.count(encoder->socket()) > 0) {
      // Update whether or not this socket should get disposed after
      // there is no more data to send.
      if (!persist) {
        dispose.insert(encoder->socket());
      }

      if (outgoing.count(encoder->socket()) > 0) {
        outgoing[encoder->socket()].push(encoder);
      } else {
        // Initialize the outgoing queue.
        outgoing[encoder->socket()];

        // Allocate and initialize the watcher.
        ev_io* watcher = new ev_io();
        watcher->data = encoder;

        ev_io_init(
            watcher,
            get_send_function(encoder->io_kind()),
            encoder->socket(),
            EV_WRITE);

        synchronized (watchers) {
          watchers->push(watcher);
        }

        ev_async_send(loop, &async_watcher);
      }
    } else {
      VLOG(1) << "Attempting to send on a no longer valid socket!";
      delete encoder;
    }
  }
}


void LibevEventManager::send(
    const Response& response,
    const Request& request,
    const Socket& socket)
{
  bool persist = request.keepAlive;

  // Don't persist the connection if the headers include
  // 'Connection: close'.
  if (response.headers.contains("Connection")) {
    if (response.headers.get("Connection").get() == "close") {
      persist = false;
    }
  }

  send(new HttpResponseEncoder(socket, response, request), persist);
}


void LibevEventManager::send(Message* message)
{
  CHECK(message != NULL);

  Node node(message->to.ip, message->to.port);

  synchronized (this) {
    // Check if there is already a socket.
    bool persist = persists.count(node) > 0;
    bool temp = temps.count(node) > 0;
    if (persist || temp) {
      int s = persist ? persists[node] : temps[node];
      CHECK(sockets.count(s) > 0);
      send(new MessageEncoder(sockets[s], message), persist);
    } else {
      // No peristent or temporary socket to the node currently
      // exists, so we create a temporary one.
      Try<int> socket = process::socket(AF_INET, SOCK_STREAM, 0);
      if (socket.isError()) {
        LOG(FATAL) << "Failed to send, socket: " << socket.error();
      }

      int s = socket.get();

      Try<Nothing> nonblock = os::nonblock(s);
      if (nonblock.isError()) {
        LOG(FATAL) << "Failed to send, nonblock: " << nonblock.error();
      }

      Try<Nothing> cloexec = os::cloexec(s);
      if (cloexec.isError()) {
        LOG(FATAL) << "Failed to send, cloexec: " << cloexec.error();
      }

      sockets[s] = Socket(s);
      nodes[s] = node;
      temps[node] = s;

      dispose.insert(s);

      // Initialize the outgoing queue.
      outgoing[s];

      // Allocate and initialize a watcher for reading data from this
      // socket. Note that we don't expect to receive anything other
      // than HTTP '202 Accepted' responses which we anyway ignore.
      ev_io* watcher = new ev_io();
      watcher->data = new Socket(sockets[s]);

      ev_io_init(watcher, ignore_data, s, EV_READ);

      // Enqueue the watcher.
      synchronized (watchers) {
        watchers->push(watcher);
      }

      // Allocate and initialize a watcher for sending the message.
      watcher = new ev_io();
      watcher->data = new MessageEncoder(sockets[s], message);

      // Try and connect to the node using this socket.
      sockaddr_in addr;
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = PF_INET;
      addr.sin_port = htons(message->to.port);
      addr.sin_addr.s_addr = message->to.ip;

      if (connect(s, (sockaddr*) &addr, sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) {
          PLOG(FATAL) << "Failed to send, connect";
        }

        // Initialize watcher for connecting.
        ev_io_init(watcher, sending_connect, s, EV_WRITE);
      } else {
        // Initialize watcher for sending.
        ev_io_init(watcher, send_data, s, EV_WRITE);
      }

      // Enqueue the watcher.
      synchronized (watchers) {
        watchers->push(watcher);
      }

      ev_async_send(loop, &async_watcher);
    }
  }
}


Encoder* LibevEventManager::next(int s)
{
  HttpProxy* proxy = NULL; // Non-null if needs to be terminated.

  synchronized (this) {
    // We cannot assume 'sockets.count(s) > 0' here because it's
    // possible that 's' has been removed with a a call to
    // SocketManager::close. For example, it could be the case that a
    // socket has gone to CLOSE_WAIT and the call to 'recv' in
    // recv_data returned 0 causing SocketManager::close to get
    // invoked. Later a call to 'send' or 'sendfile' (e.g., in
    // send_data or send_file) can "succeed" (because the socket is
    // not "closed" yet because there are still some Socket
    // references, namely the reference being used in send_data or
    // send_file!). However, when SocketManger::next is actually
    // invoked we find out there there is no more data and thus stop
    // sending.
    // TODO(benh): Should we actually finish sending the data!?
    if (sockets.count(s) > 0) {
      CHECK(outgoing.count(s) > 0);

      if (!outgoing[s].empty()) {
        // More messages!
        Encoder* encoder = outgoing[s].front();
        outgoing[s].pop();
        return encoder;
      } else {
        // No more messages ... erase the outgoing queue.
        outgoing.erase(s);

        if (dispose.count(s) > 0) {
          // This is either a temporary socket we created or it's a
          // socket that we were receiving data from and possibly
          // sending HTTP responses back on. Clean up either way.
          if (nodes.count(s) > 0) {
            const Node& node = nodes[s];
            CHECK(temps.count(node) > 0 && temps[node] == s);
            temps.erase(node);
            nodes.erase(s);
          }

          if (proxies.count(s) > 0) {
            proxy = proxies[s];
            proxies.erase(s);
          }

          dispose.erase(s);
          sockets.erase(s);

          // We don't actually close the socket (we wait for the Socket
          // abstraction to close it once there are no more references),
          // but we do shutdown the receiving end so any DataDecoder
          // will get cleaned up (which might have the last reference).
          shutdown(s, SHUT_RD);
        }
      }
    }
  }

  // We terminate the proxy outside the synchronized block to avoid
  // possible deadlock between the ProcessManager and SocketManager
  // (see comment in SocketManager::proxy for more information).
  if (proxy != NULL) {
    terminate(proxy);
  }

  return NULL;
}


void LibevEventManager::close(int s)
{
  HttpProxy* proxy = NULL; // Non-null if needs to be terminated.

  synchronized (this) {
    // This socket might not be active if it was already asked to get
    // closed (e.g., a write on the socket failed so we try and close
    // it and then later the read side of the socket gets closed so we
    // try and close it again). Thus, ignore the request if we don't
    // know about the socket.
    if (sockets.count(s) > 0) {
      // Clean up any remaining encoders for this socket.
      if (outgoing.count(s) > 0) {
        while (!outgoing[s].empty()) {
          Encoder* encoder = outgoing[s].front();
          delete encoder;
          outgoing[s].pop();
        }

        outgoing.erase(s);
      }

      // Clean up after sockets used for node communication.
      if (nodes.count(s) > 0) {
        const Node& node = nodes[s];

        // Don't bother invoking exited unless socket was persistant.
        if (persists.count(node) > 0 && persists[node] == s) {
          persists.erase(node);
          exited(node); // Generate ExitedEvent(s)!
        } else if (temps.count(node) > 0 && temps[node] == s) {
          temps.erase(node);
        }

        nodes.erase(s);
      }

      // Clean up any proxy associated with this socket.
      if (proxies.count(s) > 0) {
        proxy = proxies[s];
        proxies.erase(s);
      }

      // We need to stop any 'ignore_data' readers as they may have
      // the last Socket reference so we shutdown reads but don't do a
      // full close (since that will be taken care of by ~Socket, see
      // comment below). Calling 'shutdown' will trigger 'ignore_data'
      // which will get back a 0 (i.e., EOF) when it tries to read
      // from the socket. Note we need to do this before we call
      // 'sockets.erase(s)' to avoid the potential race with the last
      // reference being in 'sockets'.
      shutdown(s, SHUT_RD);

      dispose.erase(s);
      sockets.erase(s);
    }
  }

  // We terminate the proxy outside the synchronized block to avoid
  // possible deadlock between the ProcessManager and SocketManager.
  if (proxy != NULL) {
    terminate(proxy);
  }

  // Note that we don't actually:
  //
  //   close(s);
  //
  // Because, for example, there could be a race between an HttpProxy
  // trying to do send a response with SocketManager::send() or a
  // process might be responding to another Request (e.g., trying
  // to do a sendfile) since these things may be happening
  // asynchronously we can't close the socket yet, because it might
  // get reused before any of the above things have finished, and then
  // we'll end up sending data on the wrong socket! Instead, we rely
  // on the last reference of our Socket object to close the
  // socket. Note, however, that since socket is no longer in
  // 'sockets' any attempt to send with it will just get ignored.
  // TODO(benh): Always do a 'shutdown(s, SHUT_RDWR)' since that
  // should keep the file descriptor valid until the last Socket
  // reference does a close but force all libev watchers to stop?
}


void LibevEventManager::exited(const Node& node)
{
  // TODO(benh): It would be cleaner if this routine could call back
  // into ProcessManager ... then we wouldn't have to convince
  // ourselves that the accesses to each Process object will always be
  // valid.
  synchronized (this) {
    list<UPID> removed;
    // Look up all linked processes.
    foreachpair (const UPID& linkee, set<ProcessBase*>& processes, links) {
      if (linkee.ip == node.ip && linkee.port == node.port) {
        foreach (ProcessBase* linker, processes) {
          LibevMan->enqueue(linker, new ExitedEvent(linkee));
        }
        removed.push_back(linkee);
      }
    }

    foreach (const UPID& pid, removed) {
      links.erase(pid);
    }
  }
}


void LibevEventManager::exited(ProcessBase* process)
{
  // An exited event is enough to cause the process to get deleted
  // (e.g., by the garbage collector), which means we can't
  // dereference process (or even use the address) after we enqueue at
  // least one exited event. Thus, we save the process pid.
  const UPID pid = LibevMan->get_pid(process);

  // Likewise, we need to save the current time of the process so we
  // can update the clocks of linked processes as appropriate.
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
          LibevMan->enqueue(linker, new ExitedEvent(linkee));
        }
      }
    }

    links.erase(pid);
  }
}

void LibevEventManager::update_timer()
{
  update_timer_flag = true;
  ev_async_send(loop, &async_watcher);
}


void LibevEventManager::try_update_timer()
{
  if (!update_timer_flag) {
    update_timer_flag = true;
    ev_async_send(loop, &async_watcher);
  }
}

bool LibevEventManager::has_pending_timers() const {
  return pending_timers;
}

// Data necessary for polling so we can discard polling and actually
// stop it in the event loop.
struct Poll
{
  Poll()
  {
    // Need to explicitly instantiate the watchers.
    watcher.io.reset(new ev_io());
    watcher.async.reset(new ev_async());
  }

  // An I/O watcher for checking for readability or writeability and
  // an async watcher for being able to discard the polling.
  struct {
    memory::shared_ptr<ev_io> io;
    memory::shared_ptr<ev_async> async;
  } watcher;

  Promise<short> promise;
};

// Event loop callback when I/O is ready on polling file descriptor.
void polled(struct ev_loop* loop, ev_io* watcher, int revents)
{
  Poll* poll = (Poll*) watcher->data;

  ev_io_stop(loop, poll->watcher.io.get());

  // Stop the async watcher (also clears if pending so 'discard_poll'
  // will not get invoked and we can delete 'poll' here).
  ev_async_stop(loop, poll->watcher.async.get());

  poll->promise.set(revents);

  delete poll;
}


// Event loop callback when future associated with polling file
// descriptor has been discarded.
void discard_poll(struct ev_loop* loop, ev_async* watcher, int revents)
{
  Poll* poll = (Poll*) watcher->data;

  // Check and see if we have a pending 'polled' callback and if so
  // let it "win".
  if (ev_is_pending(poll->watcher.io.get())) {
    return;
  }

  ev_async_stop(loop, poll->watcher.async.get());

  // Stop the I/O watcher (but note we check if pending above) so it
  // won't get invoked and we can delete 'poll' here.
  ev_io_stop(loop, poll->watcher.io.get());

  poll->promise.discard();

  delete poll;
}

namespace io {

namespace internal {

// Helper/continuation of 'poll' on future discard.
void _poll(const memory::shared_ptr<ev_async>& async)
{
  ev_async_send(loop, async.get());
}


Future<short> poll(int fd, short events)
{
  Poll* poll = new Poll();

  // Have the watchers data point back to the struct.
  poll->watcher.async->data = poll;
  poll->watcher.io->data = poll;

  // Get a copy of the future to avoid any races with the event loop.
  Future<short> future = poll->promise.future();

  // Initialize and start the async watcher.
  ev_async_init(poll->watcher.async.get(), discard_poll);
  ev_async_start(loop, poll->watcher.async.get());

  // Make sure we stop polling if a discard occurs on our future.
  // Note that it's possible that we'll invoke '_poll' when someone
  // does a discard even after the polling has already completed, but
  // in this case while we will interrupt the event loop since the
  // async watcher has already been stopped we won't cause
  // 'discard_poll' to get invoked.
  future.onDiscard(lambda::bind(&_poll, poll->watcher.async));

  // Initialize and start the I/O watcher.
  ev_io_init(poll->watcher.io.get(), polled, fd, events);
  ev_io_start(loop, poll->watcher.io.get());

  return future;
}


void read(
    int fd,
    void* data,
    size_t size,
    const memory::shared_ptr<Promise<size_t> >& promise,
    const Future<short>& future)
{
  // Ignore this function if the read operation has been discarded.
  if (promise->future().hasDiscard()) {
    CHECK(!future.isPending());
    promise->discard();
    return;
  }

  if (size == 0) {
    promise->set(0);
    return;
  }

  if (future.isDiscarded()) {
    promise->fail("Failed to poll: discarded future");
  } else if (future.isFailed()) {
    promise->fail(future.failure());
  } else {
    ssize_t length = ::read(fd, data, size);
    if (length < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        // Restart the read operation.
        Future<short> future =
          io::poll(fd, process::io::READ).onAny(
              lambda::bind(&internal::read,
                           fd,
                           data,
                           size,
                           promise,
                           lambda::_1));

        // Stop polling if a discard occurs on our future.
        promise->future().onDiscard(
            lambda::bind(&process::internal::discard<short>,
                         WeakFuture<short>(future)));
      } else {
        // Error occurred.
        promise->fail(strerror(errno));
      }
    } else {
      promise->set(length);
    }
  }
}


void write(
    int fd,
    void* data,
    size_t size,
    const memory::shared_ptr<Promise<size_t> >& promise,
    const Future<short>& future)
{
  // Ignore this function if the write operation has been discarded.
  if (promise->future().hasDiscard()) {
    promise->discard();
    return;
  }

  if (size == 0) {
    promise->set(0);
    return;
  }

  if (future.isDiscarded()) {
    promise->fail("Failed to poll: discarded future");
  } else if (future.isFailed()) {
    promise->fail(future.failure());
  } else {
    // Do a write but ignore SIGPIPE so we can return an error when
    // writing to a pipe or socket where the reading end is closed.
    // TODO(benh): The 'suppress' macro failed to work on OS X as it
    // appears that signal delivery was happening asynchronously.
    // That is, the signal would not appear to be pending when the
    // 'suppress' block was closed thus the destructor for
    // 'Suppressor' was not waiting/removing the signal via 'sigwait'.
    // It also appeared that the signal would be delivered to another
    // thread even if it remained blocked in this thiread. The
    // workaround here is to check explicitly for EPIPE and then do
    // 'sigwait' regardless of what 'os::signals::pending' returns. We
    // don't have that luxury with 'Suppressor' and arbitrary signals
    // because we don't always have something like EPIPE to tell us
    // that a signal is (or will soon be) pending.
    bool pending = os::signals::pending(SIGPIPE);
    bool unblock = !pending ? os::signals::block(SIGPIPE) : false;

    ssize_t length = ::write(fd, data, size);

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
        // Restart the write operation.
        Future<short> future =
          io::poll(fd, process::io::WRITE).onAny(
              lambda::bind(&internal::write,
                           fd,
                           data,
                           size,
                           promise,
                           lambda::_1));

        // Stop polling if a discard occurs on our future.
        promise->future().onDiscard(
            lambda::bind(&process::internal::discard<short>,
                         WeakFuture<short>(future)));
      } else {
        // Error occurred.
        promise->fail(strerror(errno));
      }
    } else {
      // TODO(benh): Retry if 'length' is 0?
      promise->set(length);
    }
  }
}


} // namespace internal {


namespace internal {

#if __cplusplus >= 201103L
Future<string> _read(
    int fd,
    const memory::shared_ptr<string>& buffer,
    const boost::shared_array<char>& data,
    size_t length)
{
  return io::read(fd, data.get(), length)
    .then([=] (size_t size) -> Future<string> {
      if (size == 0) { // EOF.
        return string(*buffer);
      }
      buffer->append(data.get(), size);
      return _read(fd, buffer, data, length);
    });
}
#else
// Forward declataion.
Future<string> _read(
    int fd,
    const memory::shared_ptr<string>& buffer,
    const boost::shared_array<char>& data,
    size_t length);


Future<string> __read(
    size_t size,
    int fd,
    const memory::shared_ptr<string>& buffer,
    const boost::shared_array<char>& data,
    size_t length)
{
  if (size == 0) { // EOF.
    return string(*buffer);
  }

  buffer->append(data.get(), size);

  return _read(fd, buffer, data, length);
}


Future<string> _read(
    int fd,
    const memory::shared_ptr<string>& buffer,
    const boost::shared_array<char>& data,
    size_t length)
{
  return io::read(fd, data.get(), length)
    .then(lambda::bind(&__read, lambda::_1, fd, buffer, data, length));
}
#endif // __cplusplus >= 201103L


#if __cplusplus >= 201103L
Future<Nothing> _write(
    int fd,
    Owned<string> data,
    size_t index)
{
  return io::write(fd, (void*) (data->data() + index), data->size() - index)
    .then([=] (size_t length) -> Future<Nothing> {
      if (index + length == data->size()) {
        return Nothing();
      }
      return _write(fd, data, index + length);
    });
}
#else
// Forward declaration.
Future<Nothing> _write(
    int fd,
    Owned<string> data,
    size_t index);


Future<Nothing> __write(
    int fd,
    Owned<string> data,
    size_t index,
    size_t length)
{
  if (index + length == data->size()) {
    return Nothing();
  }
  return _write(fd, data, index + length);
}


Future<Nothing> _write(
    int fd,
    Owned<string> data,
    size_t index)
{
  return io::write(fd, (void*) (data->data() + index), data->size() - index)
    .then(lambda::bind(&__write, fd, data, index, lambda::_1));
}
#endif // __cplusplus >= 201103L


#if __cplusplus >= 201103L
void _splice(
    int from,
    int to,
    size_t chunk,
    boost::shared_array<char> data,
    memory::shared_ptr<Promise<Nothing>> promise)
{
  // Stop splicing if a discard occured on our future.
  if (promise->future().hasDiscard()) {
    // TODO(benh): Consider returning the number of bytes already
    // spliced on discarded, or a failure. Same for the 'onDiscarded'
    // callbacks below.
    promise->discard();
    return;
  }

  // Note that only one of io::read or io::write is outstanding at any
  // one point in time thus the reuse of 'data' for both operations.

  Future<size_t> read = io::read(from, data.get(), chunk);

  // Stop reading (or potentially indefinitely polling) if a discard
  // occcurs on our future.
  promise->future().onDiscard(
      lambda::bind(&process::internal::discard<size_t>,
                   WeakFuture<size_t>(read)));

  read
    .onReady([=] (size_t size) {
      if (size == 0) { // EOF.
        promise->set(Nothing());
      } else {
        // Note that we always try and complete the write, even if a
        // discard has occured on our future, in order to provide
        // semantics where everything read is written. The promise
        // will eventually be discarded in the next read.
        io::write(to, string(data.get(), size))
          .onReady([=] () { _splice(from, to, chunk, data, promise); })
          .onFailed([=] (const string& message) { promise->fail(message); })
          .onDiscarded([=] () { promise->discard(); });
      }
    })
    .onFailed([=] (const string& message) { promise->fail(message); })
    .onDiscarded([=] () { promise->discard(); });
}
#else
// Forward declarations.
void __splice(
    int from,
    int to,
    size_t chunk,
    boost::shared_array<char> data,
    memory::shared_ptr<Promise<Nothing> > promise,
    size_t size);

void ___splice(
    memory::shared_ptr<Promise<Nothing> > promise,
    const string& message);

void ____splice(
    memory::shared_ptr<Promise<Nothing> > promise);


void _splice(
    int from,
    int to,
    size_t chunk,
    boost::shared_array<char> data,
    memory::shared_ptr<Promise<Nothing> > promise)
{
  // Stop splicing if a discard occured on our future.
  if (promise->future().hasDiscard()) {
    // TODO(benh): Consider returning the number of bytes already
    // spliced on discarded, or a failure. Same for the 'onDiscarded'
    // callbacks below.
    promise->discard();
    return;
  }

  Future<size_t> read = io::read(from, data.get(), chunk);

  // Stop reading (or potentially indefinitely polling) if a discard
  // occurs on our future.
  promise->future().onDiscard(
      lambda::bind(&process::internal::discard<size_t>,
                   WeakFuture<size_t>(read)));

  read
    .onReady(
        lambda::bind(&__splice, from, to, chunk, data, promise, lambda::_1))
    .onFailed(lambda::bind(&___splice, promise, lambda::_1))
    .onDiscarded(lambda::bind(&____splice, promise));
}


void __splice(
    int from,
    int to,
    size_t chunk,
    boost::shared_array<char> data,
    memory::shared_ptr<Promise<Nothing> > promise,
    size_t size)
{
  if (size == 0) { // EOF.
    promise->set(Nothing());
  } else {
    // Note that we always try and complete the write, even if a
    // discard has occured on our future, in order to provide
    // semantics where everything read is written. The promise will
    // eventually be discarded in the next read.
    io::write(to, string(data.get(), size))
      .onReady(lambda::bind(&_splice, from, to, chunk, data, promise))
      .onFailed(lambda::bind(&___splice, promise, lambda::_1))
      .onDiscarded(lambda::bind(&____splice, promise));
  }
}


void ___splice(
    memory::shared_ptr<Promise<Nothing> > promise,
    const string& message)
{
  promise->fail(message);
}


void ____splice(
    memory::shared_ptr<Promise<Nothing> > promise)
{
  promise->discard();
}
#endif // __cplusplus >= 201103L


Future<Nothing> splice(int from, int to, size_t chunk)
{
  boost::shared_array<char> data(new char[chunk]);

  // Rather than having internal::_splice return a future and
  // implementing internal::_splice as a chain of io::read and
  // io::write calls, we use an explicit promise that we pass around
  // so that we don't increase memory usage the longer that we splice.
  memory::shared_ptr<Promise<Nothing> > promise(new Promise<Nothing>());

  Future<Nothing> future = promise->future();

  _splice(from, to, chunk, data, promise);

  return future;
}

} // namespace internal {


} // namespace io {

Future<short> LibevEventManager::poll(int fd, short events)
{
  process::initialize();

  // TODO(benh): Check if the file descriptor is non-blocking?

  return run_in_event_loop<short>(lambda::bind(&io::internal::poll, fd, events));
}

Future<size_t> LibevEventManager::read(int fd, void* data, size_t size)
{
  process::initialize();

  memory::shared_ptr<Promise<size_t> > promise(new Promise<size_t>());

  // Check the file descriptor.
  Try<bool> nonblock = os::isNonblock(fd);
  if (nonblock.isError()) {
    // The file descriptor is not valid (e.g., has been closed).
    promise->fail(
        "Failed to check if file descriptor was non-blocking: " +
        nonblock.error());
    return promise->future();
  } else if (!nonblock.get()) {
    // The file descriptor is not non-blocking.
    promise->fail("Expected a non-blocking file descriptor");
    return promise->future();
  }

  // Because the file descriptor is non-blocking, we call read()
  // immediately. The read may in turn call poll if necessary,
  // avoiding unnecessary polling. We also observed that for some
  // combination of libev and Linux kernel versions, the poll would
  // block for non-deterministically long periods of time. This may be
  // fixed in a newer version of libev (we use 3.8 at the time of
  // writing this comment).
  io::internal::read(fd, data, size, promise, io::READ);

  return promise->future();
}


Future<size_t> LibevEventManager::write(int fd, void* data, size_t size)
{
  process::initialize();

  memory::shared_ptr<Promise<size_t> > promise(new Promise<size_t>());

  // Check the file descriptor.
  Try<bool> nonblock = os::isNonblock(fd);
  if (nonblock.isError()) {
    // The file descriptor is not valid (e.g., has been closed).
    promise->fail(
        "Failed to check if file descriptor was non-blocking: " +
        nonblock.error());
    return promise->future();
  } else if (!nonblock.get()) {
    // The file descriptor is not non-blocking.
    promise->fail("Expected a non-blocking file descriptor");
    return promise->future();
  }

  // Because the file descriptor is non-blocking, we call write()
  // immediately. The write may in turn call poll if necessary,
  // avoiding unnecessary polling. We also observed that for some
  // combination of libev and Linux kernel versions, the poll would
  // block for non-deterministically long periods of time. This may be
  // fixed in a newer version of libev (we use 3.8 at the time of
  // writing this comment).
  io::internal::write(fd, data, size, promise, io::WRITE);

  return promise->future();
}

Future<string> LibevEventManager::read(int fd)
{
  process::initialize();

  // Get our own copy of the file descriptor so that we're in control
  // of the lifetime and don't crash if/when someone by accidently
  // closes the file descriptor before discarding this future. We can
  // also make sure it's non-blocking and will close-on-exec. Start by
  // checking we've got a "valid" file descriptor before dup'ing.
  if (fd < 0) {
    return Failure(strerror(EBADF));
  }

  fd = dup(fd);
  if (fd == -1) {
    return Failure(ErrnoError("Failed to duplicate file descriptor"));
  }

  // Set the close-on-exec flag.
  Try<Nothing> cloexec = os::cloexec(fd);
  if (cloexec.isError()) {
    os::close(fd);
    return Failure(
        "Failed to set close-on-exec on duplicated file descriptor: " +
        cloexec.error());
  }

  // Make the file descriptor is non-blocking.
  Try<Nothing> nonblock = os::nonblock(fd);
  if (nonblock.isError()) {
    os::close(fd);
    return Failure(
        "Failed to make duplicated file descriptor non-blocking: " +
        nonblock.error());
  }

  // TODO(benh): Wrap up this data as a struct, use 'Owner'.
  // TODO(bmahler): For efficiency, use a rope for the buffer.
  memory::shared_ptr<string> buffer(new string());
  boost::shared_array<char> data(new char[io::BUFFERED_READ_SIZE]);

  return io::internal::_read(fd, buffer, data, io::BUFFERED_READ_SIZE)
    .onAny(lambda::bind(&os::close, fd));
}


Future<Nothing> LibevEventManager::write(int fd, const std::string& data)
{
  process::initialize();

  // Get our own copy of the file descriptor so that we're in control
  // of the lifetime and don't crash if/when someone by accidently
  // closes the file descriptor before discarding this future. We can
  // also make sure it's non-blocking and will close-on-exec. Start by
  // checking we've got a "valid" file descriptor before dup'ing.
  if (fd < 0) {
    return Failure(strerror(EBADF));
  }

  fd = dup(fd);
  if (fd == -1) {
    return Failure(ErrnoError("Failed to duplicate file descriptor"));
  }

  // Set the close-on-exec flag.
  Try<Nothing> cloexec = os::cloexec(fd);
  if (cloexec.isError()) {
    os::close(fd);
    return Failure(
        "Failed to set close-on-exec on duplicated file descriptor: " +
        cloexec.error());
  }

  // Make the file descriptor is non-blocking.
  Try<Nothing> nonblock = os::nonblock(fd);
  if (nonblock.isError()) {
    os::close(fd);
    return Failure(
        "Failed to make duplicated file descriptor non-blocking: " +
        nonblock.error());
  }

  return io::internal::_write(fd, Owned<string>(new string(data)), 0)
    .onAny(lambda::bind(&os::close, fd));
}


Future<Nothing> LibevEventManager::redirect(
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

  return io::internal::splice(from, to.get(), chunk)
    .onAny(lambda::bind(&os::close, from))
    .onAny(lambda::bind(&os::close, to.get()));
}

} // namespace process {