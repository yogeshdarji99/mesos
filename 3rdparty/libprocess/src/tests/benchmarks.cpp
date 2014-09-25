#include <gtest/gtest.h>

#include <gmock/gmock.h>

#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_set>

#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/process.hpp>

int main(int argc, char** argv)
{
  // Initialize Google Mock/Test.
  testing::InitGoogleMock(&argc, argv);

  // Add the libprocess test event listeners.
  ::testing::TestEventListeners& listeners =
    ::testing::UnitTest::GetInstance()->listeners();

  listeners.Append(process::ClockTestEventListener::instance());
  listeners.Append(process::FilterTestEventListener::instance());

  return RUN_ALL_TESTS();
}

using namespace process;
using std::string;

class BenchmarkProcess : public Process<BenchmarkProcess>
{
public:
  BenchmarkProcess(
      size_t _num_iter = 1,
      size_t _max_outstanding = 1,
      const Option<UPID>& _other = Option<UPID>())
      : other(_other),
      counter(0UL),
      done(false),
      num_iter(_num_iter),
      max_outstanding(_max_outstanding),
      outstanding(0),
      sent(0)
  {
    if (other.isSome()) {
      setlink(other.get());
    }
  }

  virtual void initialize()
  {
    install("ping", &BenchmarkProcess::ping);
    install("pong", &BenchmarkProcess::pong);
  }

  void setlink(const UPID&that) {
    link(that);
  }

  void start() {
    const char *msg = "hi";
    for (;outstanding < max_outstanding &&
        sent < num_iter; ++outstanding, ++sent) {
      send(other.get(), "ping", msg, strlen(msg));
    }
    std::unique_lock<std::mutex> lock(mutex);
    while (!done) {
      cond.wait(lock);
    }
  }

private:

  void ping(const UPID& from, const string& body) {
    if (linked_ports.find(from.port) != linked_ports.end()) {
      link(from);
      linked_ports.emplace(from.port);
    }
    const char *msg = "hi";
    send(from, "pong", msg, strlen(msg));
  }

  void pong(const UPID& from, const string& body) {
    ++counter;
    --outstanding;
    if (counter >= num_iter) {
      std::lock_guard<std::mutex> lock(mutex);
      done = true;
      cond.notify_one();
    }
    const char *msg = "hi";
    for (;outstanding < max_outstanding &&
        sent < num_iter; ++outstanding, ++sent) {
      send(other.get(), "ping", msg, strlen(msg));
    }
  }

  Option<UPID> other;

  size_t counter;

  bool done;
  std::mutex mutex;
  std::condition_variable cond;

  const size_t num_iter;
  const size_t max_outstanding;
  size_t outstanding;
  size_t sent;
  std::unordered_set<int> linked_ports;

};

TEST(Process, Process_BENCHMARK_Test)
{
  const size_t num_iter = 7500;
  const size_t queue_depth = 1000;
  const size_t num_threads = 16;
  // fork in order to get 2 seperate ProcessManagers. This avoids the
  // short-circuit built into ProcessManager for processes
  // communicating in the same manager.
  int pipes[2];
  pid_t pid = -1;
  if(pipe(pipes) < 0) {
    perror("pipe failed");
  }
  pid = fork();

  if (pid < 0) {
    perror("fork() failed");
  } else if (pid == 0) {
    // child
    close(pipes[1]);
    int32_t strsize = 0;
    size_t r = read(pipes[0], &strsize, sizeof(strsize));
    char buf[strsize];
    memset(&buf, 0, strsize);
    r = read(pipes[0], &buf, strsize);
    std::istringstream iss(buf);
    UPID other;
    iss >> other;
    auto launcher = [&]() {
      BenchmarkProcess process(num_iter, queue_depth, other);
      UPID pid = spawn(&process);
      process.start();
      terminate(process);
      wait(process);
    };
    sleep(1);
    Stopwatch watch;
    watch.start();
    std::vector<std::thread> tvec;
    for (size_t i = 0; i < num_threads; ++i) {
      tvec.emplace_back(launcher);
    }
    for (auto &t : tvec) {
      t.join();
    }
    double elapsed = watch.elapsed().secs();
    size_t total_iter = num_threads * num_iter;
    LOG(INFO) << "Processed " << total_iter << " rpcs with queue depth ["
              << queue_depth << "] in " << elapsed << " = ["
              << static_cast<size_t>(total_iter / elapsed) << "] / s";
    close(pipes[0]);
  } else {
    // parent
    close(pipes[0]);
    BenchmarkProcess process(num_iter, queue_depth);
    UPID pid = spawn(&process);
    std::ostringstream ss;
    ss << pid;
    int32_t strsize = ss.str().size();
    size_t w = write(pipes[1], &strsize, sizeof(strsize));
    w = write(pipes[1], ss.str().c_str(), strsize);
    close(pipes[1]);
    ::wait(nullptr);
    terminate(process);
    wait(process);
  }
}