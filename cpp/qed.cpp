#include <algorithm>
#include <cstring> // memset
#include <fcntl.h> // posix_fallocate
#include <iostream>
#include <map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
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
__builtin_bswap32
#endif

template <class T> static unsigned int mylog2(T x) {
  if (!x)
    throw "Zero and log";
  if (x & (x - 1))
    throw "Is not a power of 2";

  if (sizeof x <= sizeof(unsigned int))
    return __builtin_ctz(x);
  if (sizeof x == sizeof(unsigned long))
    return __builtin_ctzl(x);
  if (sizeof x == sizeof(unsigned long long))
    return __builtin_ctzll(x);
  throw("should never happend");
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

#define QED_F_BACKING_FILE 1
#define QED_F_NEED_CHECK 2

// i.e. RAW ?
#define QED_F_BACKING_FORMAT_NO_PROBE 4

// TODO: validate indexes (upper value) as table_size_in_bytes / sizeof(uint64_t)
// TODO: index -> size_t ?
class L2Map_fio {
public:
  L2Map_fio(int fd_, uint64_t offset_, uint64_t cluster_mask_, uint64_t table_size_in_bytes, bool readonly_)
      : fd(fd_), l2_table_file_offset(offset_), cluster_mask(~cluster_mask_), readonly(readonly_) {

    (void)table_size_in_bytes;
  }

  uint64_t get_offset(uint64_t l2_index) const {

    uint64_t abs_offset;
    if ((size_t)::pread(fd, &abs_offset, sizeof(abs_offset), l2_table_file_offset + l2_index * sizeof(uint64_t)) !=
        sizeof(abs_offset))
      throw "Fail to read L2 entry";

    return le64_to_cpu(abs_offset) & cluster_mask;
  }

  void set_offset(uint64_t l2_index, uint64_t xxx) {
    if (readonly)
      throw "This map is readonly";
    xxx = cpu_to_le64(xxx & cluster_mask);
    if ((size_t)::pwrite(fd, &xxx, sizeof(xxx), l2_table_file_offset + l2_index * sizeof(uint64_t)) != sizeof(xxx))
      throw "Error writing L2 entry";
  }

private:
  const int fd;
  const uint64_t l2_table_file_offset;
  const uint64_t cluster_mask;
  const bool readonly;
};

class L2Map_cached {
public:
  L2Map_cached(int fd_, uint64_t offset_, uint64_t cluster_mask_, uint64_t table_size_in_bytes, bool readonly_)
      : fd(fd_), l2_table_file_offset(offset_), cluster_mask(~cluster_mask_), readonly(readonly_) {

    // Take advantage of transparent hugepages?
    cache.reset(new uint64_t[table_size_in_bytes / sizeof(uint64_t)]);

    if ((size_t)::pread(fd, cache.get(), table_size_in_bytes, l2_table_file_offset) != table_size_in_bytes)
      throw "Fail to read L2 entry";
  }

  uint64_t get_offset(uint64_t l2_index) const {
    // TODO: bswap in constructor (!)
    return le64_to_cpu(cache[l2_index]) & cluster_mask;
  }

  void set_offset(uint64_t l2_index, uint64_t xxx) {
    if (readonly)
      throw "This map is readonly";
    xxx = cpu_to_le64(xxx & cluster_mask);
    if ((size_t)::pwrite(fd, &xxx, sizeof(xxx), l2_table_file_offset + l2_index * sizeof(uint64_t)) != sizeof(xxx))
      throw "Error writing L2 entry";
    cache[l2_index] = cpu_to_le64(xxx & cluster_mask);
  }

private:
  const int fd;
  unique_ptr<uint64_t[]> cache;
  const uint64_t l2_table_file_offset;
  const uint64_t cluster_mask;
  const bool readonly;
};

class L2Map_mmap {
public:
  L2Map_mmap(int fd, uint64_t l2_table_file_offset, uint64_t cluster_mask_, uint64_t table_size_in_bytes_,
             bool readonly_)
      : table_size_in_bytes(table_size_in_bytes_), l2_table(NULL), cluster_mask(~cluster_mask_), readonly(readonly_) {
    l2_table = (uint64_t *)::mmap(NULL, table_size_in_bytes, readonly ? PROT_READ : (PROT_READ | PROT_WRITE),
                                  MAP_SHARED, fd, l2_table_file_offset);
    if (!l2_table)
      throw "Fail to mmap L2 table";
  }

  ~L2Map_mmap() {
    if (::munmap(l2_table, table_size_in_bytes) == -1)
      terminate();
    // throw "Fail to unmap L2 table";
  }

  uint64_t get_offset(uint64_t l2_index) const { return le64_to_cpu(l2_table[l2_index]) & cluster_mask; }

  void set_offset(uint64_t l2_index, uint64_t xxx) {
    if (readonly)
      throw "L2map is readonly!"; // have to check, otherwise, segfault.
    l2_table[l2_index] = cpu_to_le64(xxx & cluster_mask);
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

void QEDImage::print() const {
  qed_header h;

  struct stat s;

  if (::pread(fd, &h, sizeof(h), 0) != sizeof(h))
    throw "Fail to read header";

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
    if (::pread(fd, fn.get(), fs, le32_to_cpu(h.backing_filename_offset)) != (ssize_t)fs)
      throw "Erro reading backing_filename";
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

  // TODO: check upper limit
  if (!size || size % 512)
    throw "Wrong size";

  // TODO: open temporary file + fsync + atomic rename
  fd = ::open(filename, O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL, 0644);
  if (fd == -1)
    throw "Fail to open image file";

  try {
    const uint32_t header_size = 1;        // in clusters
    const uint32_t table_size = 4;         // inclusters
    const uint32_t cluster_size = 1 << 16; // in bytes

    file_size = 0;
    allocate((header_size + table_size) * cluster_size);

    qed_header h;
    memset(&h, 0, sizeof(h));
    h.magic = cpu_to_le32(0x00444551);
    h.image_size = cpu_to_le64(size);
    h.cluster_size = cpu_to_le32(65536);
    h.table_size = cpu_to_le32(table_size);
    h.header_size = cpu_to_le32(header_size);
    h.l1_table_offset = cpu_to_le64(header_size * cluster_size);

    uint64_t features = 0;
    if (backing_stor) {
      size_t l = strlen(backing_stor);
      if (!l)
        throw "Empty string as backing stor filename ?";

      // We support only header size=1 cluster for now
      if (l > 0xFFFFFFFF || l > cluster_size - sizeof(h))
        throw "Too big backing_stor filename";

      features |= QED_F_BACKING_FILE;
      h.backing_filename_offset = cpu_to_le32(sizeof(h));
      h.backing_filename_size = cpu_to_le32(l);

      if ((uint64_t)::pwrite(fd, backing_stor, l, sizeof(h)) != l)
        throw "Failed to write backing_stor filename";
    }

    h.features = cpu_to_le64(features);
    if ((uint64_t)::pwrite(fd, &h, sizeof(h), 0) != sizeof(h))
      throw "Failed to write header file";

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

    if (::pread(fd, &h, sizeof(h), 0) != sizeof(h))
      throw "Fail to read header";

    if (h.magic != cpu_to_le32(0x00444551))
      throw "Wrong magic";

    if (h.features & ~cpu_to_le64(QED_F_BACKING_FILE))
      throw "Some features present";

    if (h.autoclear_features)
      throw "Some autoclear bits";

    logical_image_size = le64_to_cpu(h.image_size);

    cluster_size = le32_to_cpu(h.cluster_size);
    cluster_bits = mylog2(cluster_size);
    if (cluster_bits < 12 || cluster_bits > 26) {
      // cerr << cluster_bits << endl;
      throw "Wrong cluster size";
    }
    cluster_mask = cluster_size - 1; //(((uint64_t)1)<<cluster_bits);

    unsigned int table_size_bits = mylog2(le32_to_cpu(h.table_size));
    if (!table_size_bits || table_size_bits > 4)
      throw "Wrong table size";

    table_bits = table_size_bits + cluster_bits - mylog2(sizeof(uint64_t));
    table_mask = (1 << table_bits) - 1;
    table_size_in_bytes = 1 << (table_size_bits + cluster_bits);

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
    layer_cache.reset(
        new remove_reference<decltype(*layer_cache)>::type(fd, cluster_mask, table_size_in_bytes, readonly));

    if (le64_to_cpu(h.features) & QED_F_BACKING_FILE) {
      uint32_t fs = le32_to_cpu(h.backing_filename_size);
      unique_ptr<char[]> fn(new char[fs + 1]);
      if (::pread(fd, fn.get(), fs, le32_to_cpu(h.backing_filename_offset)) != (ssize_t)fs)
        throw "Erro reading backing_filename";
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

QEDImage::QEDImage(const char *filename, uint64_t size, const char *backing_store) : readonly(false) {
  create(filename, size, backing_store);
  initialize(filename);
}
QEDImage::QEDImage(const char *filename, bool readonly_) : readonly(readonly_) { initialize(filename); }

QEDImage::~QEDImage() {
  fdatasync();
  ::close(fd);
}

uint64_t QEDImage::allocate(uint64_t count) {
  if (readonly)
    throw "Mutate on readonly image";
  const uint64_t ret = file_size;
#if 1
  if (posix_fallocate(fd, file_size, count))
    throw("fallocate error");
#else
  unique_ptr<uint8_t[]> x(new uint8_t[count]);
  memset(x.get(), 0, count);
  if ((uint64_t)::pwrite(fd, x.get(), count, file_size) != count)
    throw "Failed to enlarge file";
#endif
  file_size += count;
  return ret;
}

void QEDImage::read_bstore(uint64_t offset, size_t count, uint8_t *dst) {
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

void QEDImage::io(uint64_t logical_offset, size_t count, uint8_t *dst, const uint8_t *src, Opmode mode) {
  if (readonly && mode != Read)
    throw "Mutate operation on readonly image";

  if (count > logical_image_size || logical_offset > logical_image_size - count)
    throw "Attempt to do IO beyond the end.";

  uint64_t incluster_len;
  for (; count; dst += incluster_len, src += incluster_len, logical_offset += incluster_len, count -= incluster_len) {

    uint64_t off = logical_offset;

    const uint64_t incluster_offset = off & cluster_mask;
    off >>= cluster_bits;

    const size_t l2_index = off & table_mask;
    off >>= table_bits;

    const size_t l1_index = off & table_mask;

    incluster_len = min(cluster_size - incluster_offset, count);

    uint64_t l2_table_file_offset = l1->get_offset(l1_index);

    if (!l2_table_file_offset) {
      if (mode == Read) {
        // cerr << "No L2 table, so reading ZEROES" << endl;
        read_bstore(logical_offset, incluster_len, dst);
        continue;
      }
      if (mode == WriteZeroes)
        continue;

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
      if (mode == WriteZeroes)
        continue;
      // cerr << "Allocating a new data cluster (!)" << endl;
      data_cluster_offset = allocate(cluster_size);
      l2->set_offset(l2_index, data_cluster_offset);

      // read backing store if not full rewrite
      if (bstore && incluster_len != cluster_size) {
        uint64_t cluster_start = logical_offset & ~cluster_mask;
        if (cluster_start < bstore->logical_image_size) {
          uint8_t *raw_buf = new uint8_t[cluster_size];
          unique_ptr<uint8_t[]> buf(raw_buf);
          // TODO: optimize: read less the cluster!
          read_bstore(cluster_start, cluster_size, raw_buf);
          ::close(0x1234);
          ::close(1234);
          if (any_of(/*execution::par,*/ raw_buf, raw_buf + cluster_size, [](uint8_t v) { return bool(v); })) {
            if ((uint64_t)::pwrite(fd, buf.get(), cluster_size, data_cluster_offset) != cluster_size)
              throw "Write error";
          }
        }
      }
    }

    if (mode == AllocData) {
      // cerr << "Skipping actual write, since only allocadata" << endl;
      continue;
    }

    const uint64_t data_offset = data_cluster_offset + incluster_offset;

    if (mode == Read) {
      // cerr << "Reading user data at abs. offset " << data_offset << " " << incluster_len << " bytes" << endl;
      if ((uint64_t)::pread(fd, dst, incluster_len, data_offset) != incluster_len)
        throw "Read error";
      continue;
    }
    if (mode == WriteZeroes) {
      if (::fallocate(fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE, data_offset, incluster_len) == -1)
        throw "Fail to zero range";
      continue;
    }
    if (mode == Write) {
      // cerr << "Writing user data at abs. offset " << data_offset << " " << incluster_len << " bytes" << endl;
      if ((uint64_t)::pwrite(fd, src, incluster_len, data_offset) != incluster_len)
        throw "Write error";
      continue;
    }
    throw "Unknown mode";
  }
}
