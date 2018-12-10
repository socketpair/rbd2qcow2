#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring> // memset
#include <fcntl.h> // posix_fallocate
#include <functional>
#include <iostream>
#include <map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "qed.h"

using namespace std;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__

static uint32_t le32_to_cpu(uint32_t x) { return x; }

static uint32_t cpu_to_le32(uint32_t x) { return x; }

static uint64_t le64_to_cpu(uint64_t x) { return x; }

static uint64_t cpu_to_le64(uint64_t x) { return x; }

#else
static uint32_t le32_to_cpu(uint32_t x) { return __builtin_bswap32(x); }

static uint32_t cpu_to_le32(uint32_t x) { return __builtin_bswap32(x); }

static uint64_t le64_to_cpu(uint64_t x) { return __builtin_bswap64(x); }

static uint64_t cpu_to_le64(uint64_t x) { return __builtin_bswap64(x); }
#endif

template <class T> static unsigned int mylog2(T x) {
  if (!x)
    throw invalid_argument("Zero and log");
  if (x & (x - 1))
    throw invalid_argument("Is not a power of 2");

  if (sizeof x <= sizeof(unsigned int))
    return __builtin_ctz(x);
  if (sizeof x == sizeof(unsigned long))
    return __builtin_ctzl(x);
  if (sizeof x == sizeof(unsigned long long))
    return __builtin_ctzll(x);

  throw runtime_error("Should never happend");
}

typedef struct __attribute__((packed)) {
  uint32_t magic; /* QED\0 */

  uint32_t cluster_size; /* in bytes */
  uint32_t table_size;   /* for L1 and L2 tables, in clusters */
  uint32_t header_size;  /* in clusters */

  uint64_t features;           /* format feature bits */
  uint64_t compat_features;    /* compat feature bits */
  uint64_t autoclear_features; /* self-resetting feature bits */

  uint64_t l1_table_offset; /* in bytes */
  uint64_t image_size;      /* total logical image size, in bytes */

  /* if (features & QED_F_BACKING_FILE) */
  uint32_t backing_filename_offset; /* in bytes from start of header */
  uint32_t backing_filename_size;   /* in bytes */
} qed_header;

#define QED_F_BACKING_FILE 1u
#define QED_F_NEED_CHECK 2u

// i.e. RAW ?
#define QED_F_BACKING_FORMAT_NO_PROBE 4u

class L2Map_mmap {
public:
  L2Map_mmap(int fd, uint64_t l2_table_file_offset, uint64_t cluster_mask_, uint64_t table_size_in_bytes_,
             bool readonly_)
      : table_size_in_bytes(table_size_in_bytes_), l2_table(nullptr), cluster_mask(~cluster_mask_),
        readonly(readonly_) {
    l2_table = (uint64_t *)::mmap(nullptr, table_size_in_bytes, readonly ? PROT_READ : (PROT_READ | PROT_WRITE),
                                  MAP_SHARED, fd, l2_table_file_offset);
    if (!l2_table)
      throw "Fail to mmap L2 table";
  }

  ~L2Map_mmap() {
    if (!readonly && ::msync(l2_table, table_size_in_bytes, MS_SYNC) == -1)
      terminate();
    if (::munmap(l2_table, table_size_in_bytes) == -1)
      terminate();
  }

  uint64_t get_offset(size_t l2_index) const { return le64_to_cpu(l2_table[l2_index]) & cluster_mask; }

  void set_offset(size_t l2_index, uint64_t offset) {
    if (readonly)
      throw "L2map is readonly!"; // have to check, otherwise, segfault.
    l2_table[l2_index] = cpu_to_le64(offset & cluster_mask);
  }

private:
  const uint64_t table_size_in_bytes;
  uint64_t *l2_table;
  const uint64_t cluster_mask;
  const bool readonly;
};

template <class T> class Layercache {
public:
  Layercache(int fd_, uint64_t cluster_mask_, uint64_t table_size_, bool readonly_)
      : fd(fd_), cluster_mask(cluster_mask_), table_size(table_size_), readonly(readonly_){};

  T *getmap(uint64_t offset) {
    auto &v = cache[offset];
    if (!v) {
      // Nothing bad if empty unique_ptr stay in the map when exception occurrs here...
      v.reset(new T(fd, offset, cluster_mask, table_size, readonly));
    }
    return v.get();
  }

private:
  map<uint64_t, unique_ptr<T>> cache;
  const int fd;
  const uint64_t cluster_mask;
  const uint64_t table_size;
  const bool readonly;
};

static void repeatable_pread(int fd, void *buf_, size_t count, size_t offset) {
  uint8_t *buf = static_cast<uint8_t *>(buf_);
  while (count) {
    ssize_t r = ::pread(fd, buf, count, offset);
    if (r == -1)
      throw "pread error";
    buf += r;
    count -= r;
    offset += r;
  }
}

static void repeatable_pwrite(int fd, const void *buf_, size_t count, size_t offset) {
  const uint8_t *buf = static_cast<const uint8_t *>(buf_);
  while (count) {
    ssize_t r = ::pwrite(fd, buf, count, offset);
    if (r == -1)
      throw "pwrite error";
    buf += r;
    count -= r;
    offset += r;
  }
}

void QEDImage::print() const {
  qed_header h;

  struct stat s;

  repeatable_pread(fd, &h, sizeof(h), 0);

  uint32_t clustersize = le32_to_cpu(h.cluster_size);

  cout << "h.magic = " << le32_to_cpu(h.magic) << endl;
  cout << "h.cluster_size = " << clustersize << endl;
  cout << "h.image_size = " << le64_to_cpu(h.image_size) << endl;
  cout << "h.table_size = " << le32_to_cpu(h.table_size) << endl;
  cout << "h.header_size = " << le32_to_cpu(h.header_size) << endl;
  cout << "h.l1_table_offset = " << le64_to_cpu(h.l1_table_offset) << " ("
       << le64_to_cpu(h.l1_table_offset) / clustersize << ")" << endl;

  if (::fstat(fd, &s) == -1)
    throw "fstat error";

  uint64_t i;

  cout << "file size = " << s.st_size << " (" << s.st_size / clustersize << ")" << endl;

  if (le64_to_cpu(h.features) & QED_F_BACKING_FILE) {
    uint32_t fs = le32_to_cpu(h.backing_filename_size);
    unique_ptr<char[]> fn(new char[fs + 1]);
    repeatable_pread(fd, fn.get(), fs, le32_to_cpu(h.backing_filename_offset));
    fn[fs] = 0;
    cout << "backing file: " << fn.get() << endl;
  } else {
    cout << "No backing store file." << endl;
  }

  vector<bool> usedc(file_size / clustersize, false);

  for (i = 0; i < le32_to_cpu(h.header_size); i++) {
    usedc[i] = true;
  }

  L2Map_mmap myl1(fd, le64_to_cpu(h.l1_table_offset), cluster_mask, table_size_in_bytes, true);
  for (i = le64_to_cpu(h.l1_table_offset) / clustersize;
       i < le64_to_cpu(h.l1_table_offset) / clustersize + le32_to_cpu(h.table_size); i++) {
    if (usedc.at(i))
      throw "cluster bitmap error! L1 table uses used clusters!";
    usedc[i] = true;
  }

  for (i = 0; i < table_size_in_bytes / sizeof(uint64_t); i++) {
    uint64_t offset = myl1.get_offset(i);
    if (!offset)
      continue;

    cout << "L1 idx=" << i << " L2 is at " << offset << " (" << offset / clustersize << ")" << endl;

    for (uint64_t k = offset / clustersize; k < offset / clustersize + le32_to_cpu(h.table_size); k++) {
      if (usedc.at(k))
        throw "cluster bitmap error! L2 table uses used clusters!";
      usedc[k] = true;
    }
    L2Map_mmap myl2(fd, offset, cluster_mask, table_size_in_bytes, true);

    uint64_t j;
    for (j = 0; j < table_size_in_bytes / sizeof(uint64_t); j++) {
      uint64_t data_offset = myl2.get_offset(j);
      if (!data_offset)
        continue;
      cout << "L2 idx=" << j << " data_offset=" << data_offset << " (" << data_offset / clustersize << ")" << endl;

      if (usedc.at(data_offset / clustersize))
        throw "cluster bitmap error! data uses used clusters!";

      usedc[data_offset / clustersize] = true;
    }
  }

  for (i = 0; i < usedc.size(); i++) {
    if (!usedc.at(i))
      cout << "unused cluster: " << i << endl;
  }
}

void QEDImage::create(const char *filename, uint64_t size, const char *backing_stor) {
  const uint32_t table_size = 4;          // in clusters
  const uint32_t cluster_size = 1u << 16; // in bytes

  const uint64_t upperlimit = uint64_t(table_size * cluster_size / sizeof(uint64_t)) *
                              uint64_t(table_size * cluster_size / sizeof(uint64_t)) * cluster_size;

  if (!size || size % 512 || size > upperlimit)
    throw "Wrong size";

  // TODO: open temporary file + fsync + atomic rename
  fd = ::open(filename, O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL, 0644);
  if (fd == -1)
    throw "Fail to open image file";

  try {
    const uint32_t header_size = 1; // in clusters

    file_size = 0;
    allocate((header_size + table_size) * cluster_size);

    qed_header h;
    memset(&h, 0, sizeof(h));
    h.magic = cpu_to_le32(0x00444551);
    h.image_size = cpu_to_le64(size);
    h.cluster_size = cpu_to_le32(cluster_size);
    h.table_size = cpu_to_le32(table_size);
    h.header_size = cpu_to_le32(header_size);
    h.l1_table_offset = cpu_to_le64(header_size * cluster_size);

    uint64_t features = 0;
    if (backing_stor) {
      size_t l = strlen(backing_stor);
      if (!l)
        throw "Empty string as backing stor filename ?";

      // We support only header size=1 cluster for now
      if (l > cluster_size - sizeof(h))
        throw "Too big backing_stor filename";

      features |= QED_F_BACKING_FILE;
      h.backing_filename_offset = cpu_to_le32(sizeof(h));
      h.backing_filename_size = cpu_to_le32(l);

      repeatable_pwrite(fd, backing_stor, l, sizeof(h));
    }

    h.features = cpu_to_le64(features);
    repeatable_pwrite(fd, &h, sizeof(h), 0);

    fdatasync();

  } catch (...) {
    ::close(fd);
    throw;
  }
  ::close(fd);
}

void QEDImage::initialize(const char *filename) {
  fd = ::open(filename, (readonly ? O_RDONLY : O_RDWR) | O_CLOEXEC);
  if (fd == -1)
    throw "Fail to open image file";
  try {

    qed_header h;

    repeatable_pread(fd, &h, sizeof(h), 0);

    if (h.magic != cpu_to_le32(0x00444551))
      throw "Wrong magic";

    if (h.features & ~cpu_to_le64(QED_F_BACKING_FILE))
      throw "Some features present";

    if (h.autoclear_features)
      throw "Some autoclear bits";

    cluster_size = le32_to_cpu(h.cluster_size);
    cluster_bits = mylog2(cluster_size);
    if (cluster_bits < 12 || cluster_bits > 26) {
      // cerr << cluster_bits << endl;
      throw "Wrong cluster size";
    }
    cluster_mask = cluster_size - 1; //(((uint64_t)1)<<cluster_bits);

    size_t table_size_bits = mylog2(le32_to_cpu(h.table_size));
    if (!table_size_bits || table_size_bits > 4)
      throw "Wrong table size";

    table_bits = table_size_bits + cluster_bits - mylog2(sizeof(uint64_t));
    table_mask = (1u << table_bits) - 1;
    table_size_in_bytes = 1u << (table_size_bits + cluster_bits);

    const uint64_t upperlimit = uint64_t(table_size_in_bytes / sizeof(uint64_t)) *
                                uint64_t(table_size_in_bytes / sizeof(uint64_t)) * cluster_size;

    logical_image_size = le64_to_cpu(h.image_size);

    if (logical_image_size % 512 || !logical_image_size || logical_image_size > upperlimit)
      throw "Wrong image_size";

    uint64_t l1_table_offset = le64_to_cpu(h.l1_table_offset); // in bytes
    if (l1_table_offset % cluster_size)
      throw "Wrong l1_table_offset";
    if (l1_table_offset < le32_to_cpu(h.header_size) * cluster_size)
      throw "Too small l1_table_offset";

    struct stat s;
    if (::fstat(fd, &s) == -1)
      throw "fstat error";

    file_size = ((uint64_t)s.st_size) & (~(uint64_t)cluster_mask);
    if (l1_table_offset + table_size_in_bytes > file_size)
      throw "File too short";

    l1.reset(new L2Map_mmap(fd, l1_table_offset, cluster_mask, table_size_in_bytes, readonly));
    layer_cache.reset(new Layercache<L2Map_mmap>(fd, cluster_mask, table_size_in_bytes, readonly));

    if (le64_to_cpu(h.features) & QED_F_BACKING_FILE) {
      uint32_t fs = le32_to_cpu(h.backing_filename_size);

      unique_ptr<char[]> fn(new char[fs + 1]);
      repeatable_pread(fd, fn.get(), fs, le32_to_cpu(h.backing_filename_offset));
      fn[fs] = 0;
      bstore.reset(new QEDImage(fn.get(), true));
    }

  } catch (...) {
    ::close(fd);
    throw;
  }
}

void QEDImage::fdatasync() const {
  if (readonly)
    return;
  if (::fdatasync(fd) == -1)
    throw "fdatasync error";
}

QEDImage::QEDImage(const char *filename, uint64_t size, const char *backing_store) : readonly(false), myname(filename) {
  create(filename, size, backing_store);
  initialize(filename);
}

QEDImage::QEDImage(const char *filename, bool readonly_) : readonly(readonly_), myname(filename) {
  initialize(filename);
}

QEDImage::~QEDImage() {
  fdatasync();
  ::close(fd);
}

// retuns beginning of allocated region (i.e. offset of previous file end for now)
uint64_t QEDImage::allocate(size_t count) {
  if (readonly)
    throw "Mutate on readonly image";
  const uint64_t ret = file_size;
#if 1
  if (posix_fallocate(fd, file_size, count))
    throw("fallocate error");
#else
  vector<uint8_t> x(count, 0);
  repeatable_pwrite(fd, x.data(), count, file_size);
#endif
  file_size += count;
  return ret;
}

void QEDImage::read_bstore(uint64_t offset, size_t count, void *dst_) {

  uint8_t *dst = static_cast<uint8_t *>(dst_);

  if (bstore && count) {
    const uint64_t lll = bstore->logical_image_size;
    if (offset < lll) {
      const uint64_t cnt = min(lll - offset, count);
      bstore->read(offset, cnt, dst);
      count -= cnt;
      // offset+=cnt;
      dst += cnt;
    }
  }
  if (count)
    memset(dst, 0, count);
}

#if 0
    return any_of(/*execution::par,*/ raw_buf, raw_buf + items, [](size_t v) { return bool(v); });
  //__assume_aligned(raw_buf, 64);
#endif

static bool mem_is_zero(const void *data, size_t count) {

  if (!count)
    throw "zerocount";

  const uint8_t *data8 = static_cast<const uint8_t *>(data);

  for (; (size_t(data8) % sizeof(size_t)) && count; data8++, count--)
    if (*data8)
      return false;

  const size_t *raw_buf = reinterpret_cast<const size_t *>(data8);

  // https://en.cppreference.com/w/cpp/thread/hardware_destructive_interference_size
  const size_t align = 128;

  for (; count >= sizeof(size_t) && (size_t(raw_buf) % align); count -= sizeof(size_t), raw_buf++)
    if (*raw_buf)
      return false;

  for (; count >= align; count -= align, raw_buf += align / sizeof(size_t)) {
    size_t x = 0;

    // TODO: prefetch
    // TODO: OR even and odd cells ?
    // Now, vectorize that (!)
    for (const size_t *j = raw_buf; j < raw_buf + align / sizeof(size_t); j++)
      x |= *j;

    if (x)
      return false;
  }

  for (; count >= sizeof(size_t); count -= sizeof(size_t), raw_buf++)
    if (*raw_buf)
      return false;

  data8 = reinterpret_cast<const uint8_t *>(raw_buf);
  for (; count; data8++, count--)
    if (*data8)
      return false;

  return true;
}

void QEDImage::prefill(const uint64_t logical_offset, const size_t incluster_len, const uint64_t phys_cluster_offset) {
  if (!bstore)
    return;

  // Whole cluster have to be written. Prefilling will be completely overwritten.
  if (incluster_len == cluster_size)
    return;

  const uint64_t logical_cluster_start = logical_offset & ~cluster_mask;

  assert(incluster_len <= cluster_size);
  assert((phys_cluster_offset & cluster_mask) == 0);
  assert(logical_offset + incluster_len <= logical_cluster_start + cluster_size);

  if (logical_cluster_start >= bstore->logical_image_size)
    return;

  struct {
    size_t datalen;
    uint64_t logical_offset;
    uint64_t phys_offset;
  } ops[2];

  ops[0].logical_offset = logical_cluster_start;
  ops[0].datalen = logical_offset - logical_cluster_start;
  ops[0].phys_offset = phys_cluster_offset;

  ops[1].logical_offset = logical_offset + incluster_len;
  ops[1].datalen = logical_cluster_start + cluster_size - ops[1].logical_offset;
  ops[1].phys_offset = phys_cluster_offset + cluster_size - ops[1].datalen;

  // 8192 is arbitrary chosen constant.
  // if parts are close enough, read (and write) them in one IO.
  if (ops[0].datalen && ops[1].datalen && incluster_len <= 8192) {
    ops[0].datalen = cluster_size;
    ops[1].datalen = 0;
  }

  // alignas(128) char cacheline[128];
  static const size_t alignment = 128;

  // Having static buffer prevents multithread access...
  unique_ptr<void, function<void(void *)>> buf(::aligned_alloc(alignment, cluster_size), free);

  if (!buf)
    throw "aligned memory allocation error";

  void *databuf = buf.get();

  for (const auto &i : ops) {
    if (!i.datalen)
      continue;

    read_bstore(i.logical_offset, i.datalen, databuf);

    if (mem_is_zero(databuf, i.datalen))
      continue;

    repeatable_pwrite(fd, databuf, i.datalen, i.phys_offset);
  }
}

void QEDImage::io(uint64_t logical_offset, size_t count, void *dst_, const void *src_, Opmode mode) {
  cout << "io: " << mode << " from " << myname << endl;

  if (readonly && mode != Read)
    throw "Mutate operation on readonly image";

  if (count > logical_image_size || logical_offset > logical_image_size - count)
    throw "Attempt to do IO beyond the end.";

  uint8_t *dst = static_cast<uint8_t *>(dst_);
  const uint8_t *src = static_cast<const uint8_t *>(src_);

  size_t incluster_len;
  for (; count; dst += incluster_len, src += incluster_len, logical_offset += incluster_len, count -= incluster_len) {

    uint64_t off = logical_offset;

    const size_t incluster_offset = off & cluster_mask;
    off >>= cluster_bits;

    const size_t l2_index = off & table_mask;
    off >>= table_bits;

    const size_t l1_index = off & table_mask;

    incluster_len = min(cluster_size - incluster_offset, count);

    uint64_t l2_table_file_offset = l1->get_offset(l1_index);

    if (!l2_table_file_offset) {
      if (mode == Read) {
        // cerr << "No L2 table, so reading backing store" << endl;
        read_bstore(logical_offset, incluster_len, dst);
        continue;
      }
      // cerr << "Allocating a new L2 table" << endl;
      l2_table_file_offset = allocate(table_size_in_bytes);
      l1->set_offset(l1_index, l2_table_file_offset);
    }
    if (l2_table_file_offset + table_size_in_bytes > file_size)
      throw "Wrong L2 table offset!";

    if (mode == AllocMaps) {
      // cerr << "Only map alloc mode. going next.";
      continue;
    }

    auto l2 = layer_cache->getmap(l2_table_file_offset);
    uint64_t data_cluster_offset = l2->get_offset(l2_index);

    if (!data_cluster_offset) {
      if (mode == Read) {
        // cerr << "No cluster in L2 table, so reading ZEROES" << endl;
        read_bstore(logical_offset, incluster_len, dst);
        continue;
      }
      // cerr << "Allocating a new data cluster (!)" << endl;
      data_cluster_offset = allocate(cluster_size);
      l2->set_offset(l2_index, data_cluster_offset);

      if (mode == WriteZeroes)
        continue;

      if (mode == Write && mem_is_zero(src, incluster_len))
        continue;

      // Yes, we have to prefill if AllocData requested.
      prefill(logical_offset, incluster_len, data_cluster_offset);
    }

    if (mode == AllocData) {
      // cerr << "Skipping actual write, since only allocadata" << endl;
      continue;
    }

    const uint64_t data_offset = data_cluster_offset + incluster_offset;

    if (mode == Read) {
      // cerr << "Reading user data at abs. offset " << data_offset << " " << incluster_len << " bytes" << endl;
      repeatable_pread(fd, dst, incluster_len, data_offset);
      continue;
    }
    if (mode == WriteZeroes) {
      if (::fallocate(fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE, data_offset, incluster_len) == -1)
        throw "Fail to zero range";
      continue;
    }
    if (mode == Write) {
      // cerr << "Writing user data at abs. offset " << data_offset << " " << incluster_len << " bytes" << endl;
      repeatable_pwrite(fd, src, incluster_len, data_offset);
      continue;
    }
    throw "Unknown mode";
  }
}
