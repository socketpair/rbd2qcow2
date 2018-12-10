#include <memory>

template <class T> class Layercache;

class L2Map_mmap;

class QEDImage {
public:
  explicit QEDImage(const char *filename, uint64_t size, const char *backing_store = nullptr);

  explicit QEDImage(const char *filename, bool readonly = false);

  ~QEDImage();

  void print() const;

  void read(uint64_t offset, size_t count, void *dst) { io(offset, count, dst, nullptr, Read); }

  void write(uint64_t offset, size_t count, const void *src) { io(offset, count, nullptr, src, Write); }

  void write_zeroes(uint64_t offset, size_t count) { io(offset, count, nullptr, nullptr, WriteZeroes); }

  void alloc_maps(uint64_t offset, size_t count) { io(offset, count, nullptr, nullptr, AllocMaps); }

  void alloc_data(uint64_t offset, size_t count) { io(offset, count, nullptr, nullptr, AllocData); }

  uint64_t get_logical_image_size() const { return logical_image_size; }

  void fdatasync() const;

private:
  void prefill(uint64_t logical_offset, size_t incluster_len, uint64_t phys_cluster_offset);

  void initialize(const char *filename);

  void create(const char *filename, uint64_t size, const char *backing_store);

  enum Opmode { Read, Write, AllocMaps, AllocData, WriteZeroes };

  uint64_t allocate(size_t count);

  void io(uint64_t logical_offset, size_t count, void *dst, const void *src, Opmode mode);

  void read_bstore(uint64_t offset, size_t count, void *dst);

  int fd;
  std::unique_ptr<L2Map_mmap> l1;
  std::unique_ptr<Layercache<L2Map_mmap>> layer_cache;
  size_t table_size_in_bytes;
  size_t cluster_mask;
  size_t cluster_size;
  size_t cluster_bits;
  size_t table_bits;
  size_t table_mask;
  uint64_t file_size;
  uint64_t logical_image_size;
  const bool readonly;
  std::unique_ptr<QEDImage> bstore;
  std::string myname;
};
