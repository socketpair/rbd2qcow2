#include <algorithm>
#include <future>
#include <iostream>
#include <rbd/librbd.hpp>
#include <vector>

#include "limitedq.h"
#include "qed.h"

using namespace ::librados;
using namespace ::librbd;
using namespace ::std;
/*
using namespace ::chrono;

template <class T> static double dur2sec(const T &dur) { return duration_cast<duration<double>>(dur).count(); }

template <class T> static double dur2msec(const T &dur) { return duration_cast<duration<double, milli>>(dur).count(); }
*/

class diffchunk {
public:
  explicit diffchunk(uint64_t offset_, size_t len_, int exists_) : offset(offset_), len(len_), exists(exists_) {}
  // TODO diffchunk(const diffchunk &) = delete;

  uint64_t offset;
  size_t len;
  int exists;
  ceph::bufferlist bl;
};

template <typename Func> static auto make_jobs(size_t count, Func fun) -> unique_ptr<vector<future<decltype(fun())>>> {
  using VectorType = vector<future<decltype(fun())>>;

  unique_ptr<VectorType> x(new VectorType);
  x->reserve(count);
  for (size_t i = 0; i < count; i++)
    x->emplace_back(async(launch::async, fun));
  return x;
}

static void transfer(Image &img, QEDImage &qed, const vector<diffchunk> &chunks) {
  const int rbd_streams = 3;

  // TODO: *2 is arbitrary chosen
  LimitedQ<diffchunk> q1(rbd_streams * 4);
  LimitedQ<diffchunk> q2(rbd_streams * 4);

  auto exists_chunks_pusher = async(launch::async, [&q1, &chunks]() {
    decltype(q1)::Terminator t(q1);
    for (const auto &chunk : chunks)
      if (chunk.exists)
        q1.push_front(chunk);
  });

  auto zero_chunks_pusher = async(launch::async, [&q2, &chunks]() {
    for (const auto &chunk : chunks)
      if (!chunk.exists)
        q2.push_front(chunk);
  });

  auto rbd_reader = async(launch::async, [&q1, &q2, &img]() {
    decltype(q2)::Terminator t(q2);
    auto handles2 = make_jobs(rbd_streams, [&q1, &q2, &img]() {
      for (;;) {
        auto chunk = q1.pop_back();
        if (!chunk)
          break; // end of stream

        if (!chunk->exists)
          throw "Should never happen";

        // TODO: read_iterate ? if so, push chunks to q2, instead of ceph_bufferlist...
        if (img.read(chunk->offset, chunk->len, chunk->bl) != (ssize_t)chunk->len) {
          throw "Fail to read from Ceph";
        }

        q2.push_front(move(chunk));
      }
    });
    for (auto &h : *handles2)
      h.wait();
    for (auto &h : *handles2)
      h.get();
  });

  auto qed_writer = async(launch::async, [&q2, &qed]() {
    for (;;) {
      auto chunk = q2.pop_back();
      if (!chunk)
        break;
      if (chunk->exists) {
        cout << "writing data chunk at " << chunk->offset << endl;
        qed.write(chunk->offset, chunk->len, chunk->bl.get_contiguous(0, chunk->len));
      } else {
        cout << "writing zero chunk at " << chunk->offset << endl;
        qed.write_zeroes(chunk->offset, chunk->len);
      }
    }
  });

  cout << "Start waiting 1" << endl;
  exists_chunks_pusher.wait();
  cout << "Start waiting 2" << endl;
  zero_chunks_pusher.wait();
  cout << "Start waiting 3" << endl;
  rbd_reader.wait();
  cout << "Start waiting 4" << endl;
  qed_writer.wait();
  cout << "Start waiting 5" << endl;

  exists_chunks_pusher.get();
  zero_chunks_pusher.get();
  rbd_reader.get();
  qed_writer.get();
}

static void _main(int argc, const char *argv[]) {
  Rados rados;

  int err;
  if ((err = rados.init("admin")) < 0) {
    cerr << "Failed to init: " << strerror(-err) << endl;
    throw "Failed to init";
  }

  if ((err = rados.conf_read_file("/etc/ceph/ceph.conf")) < 0) {
    cerr << "Failed to read conf file: " << strerror(-err) << endl;
    throw "Failed to read conf file";
  }

  if ((err = rados.conf_parse_argv(argc, argv)) < 0) {
    cerr << "Failed to parse argv: " << strerror(-err) << endl;
    throw "Failed to parse argv";
  }

  if ((err = rados.connect()) < 0) {
    cerr << "Failed to connect: " << strerror(-err) << endl;
    throw "Failed to connect";
  }

  // https://tracker.ceph.com/issues/24114
  // this_thread::sleep_for(milliseconds(100));

  struct {
    string pool;
  } settings;

  settings.pool = "prod";

  IoCtx ioctx;
  // TODO: cleanup
  /*
   * NOTE: be sure to call watch_flush() prior to destroying any IoCtx
   * that is used for watch events to ensure that racing callbacks
   * have completed.
   */

  if (rados.ioctx_create(settings.pool.c_str(), ioctx) < 0)
    throw "Failed to create ioctx";

  RBD rbd;

  Image img;

  static const char *to_snap = "1543508293";
  static const char *from_snap = "test";
  bool whole_object = false;

  if (rbd.open_read_only(ioctx, img, "bitrix", from_snap) < 0)
    throw "Failed to open image readonly";

  uint64_t size;
  if (img.size(&size) < 0)
    throw "Failed to get image size";
  // TODO: list ? no, we sort it.
  vector<diffchunk> chunks;

  auto callback = [](uint64_t offset, size_t len, int exists, void *arg) {
    static_cast<decltype(&chunks)>(arg)->emplace_back(offset, len, exists);
    return 0;
  };

  cout << "Iterating over chunk list. Image size is " << size << endl;
  int qwe;
  if ((qwe = img.diff_iterate2(to_snap, 0, size, false /* include parent */, whole_object, callback, &chunks)) < 0)
    throw string("Iteration failed ") + to_string(qwe);

  uint64_t data_bytes = 0;
  uint64_t zero_bytes = 0;
  for (const auto &chunk : chunks) {
    if (chunk.exists)
      data_bytes += chunk.len;
    else
      zero_bytes += chunk.len;
  }

  cout << "Need to transfer " << chunks.size() << " chunks: " << data_bytes << " of data and " << zero_bytes
       << " of zeroes" << endl;

  QEDImage qed("xxx.qed", size);

  sort(chunks.begin(), chunks.end(),
       [](const diffchunk &a, const diffchunk &b) -> bool { return a.offset < b.offset; });

  // just an optimisation. in order to make QED more linear for MULTITHREADED WRITE!.
  cout << "Preallocating data structures in QED" << endl;
  for (const auto &chunk : chunks) {
    qed.alloc_data(chunk.offset, chunk.len);
  }

  cout << "Transferring diff from Ceph to QED" << endl;

  transfer(img, qed, chunks);

  qed.fdatasync();
}

/*
static void onterminate() {
  try {
    auto unknown = std::current_exception();
    if (unknown) {
      std::rethrow_exception(unknown);
    } else {
      std::cerr << "normal termination" << std::endl;
    }
  } catch (const std::exception &e) { // for proper `std::` exceptions
    std::cerr << "unexpected exception: " << e.what() << std::endl;
  } catch (const char *xxx) { // last resort for things like `throw 1;`
    std::cerr << "unknown exception: " << xxx << std::endl;
  }
}
*/

int main(int argc, const char *argv[]) {

  //  std::set_terminate(onterminate);

  try {
    _main(argc, argv);
  } catch (string s) {
    cerr << "Unhandled exception: " << s << endl;
    return 1;
  } catch (const char *msg) {
    cerr << "Unhandled exception: " << msg << endl;
    return 1;
  }

  cout << "Exiting successfully." << endl;
  return 0;
}
