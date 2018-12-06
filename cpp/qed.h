#include <memory>

template <class T> class Layercache;

class L2Map_mmap;

class QEDImage {
public:
  QEDImage(const char *filename, uint64_t size, const char *backing_store = NULL);
  QEDImage(const char *filename, bool readonly = false);
  ~QEDImage();

  void print() const;
  void read(uint64_t offset, size_t count, void *dst) { io(offset, count, (uint8_t *)dst, NULL, Read); }
  void write(uint64_t offset, size_t count, const void *src) { io(offset, count, NULL, (const uint8_t *)src, Write); }
  void write_zeroes(uint64_t offset, size_t count) { io(offset, count, NULL, NULL, WriteZeroes); }

  void alloc_maps(uint64_t offset, size_t count) { io(offset, count, NULL, NULL, AllocMaps); }
  void alloc_data(uint64_t offset, size_t count) { io(offset, count, NULL, NULL, AllocData); }
  uint64_t get_logical_image_size() const { return logical_image_size; }
  void fdatasync() const;

private:
  void initialize(const char *filename);
  void create(const char *filename, uint64_t size, const char *backing_store);
  enum Opmode { Read, Write, AllocMaps, AllocData, WriteZeroes };
  uint64_t allocate(uint64_t count);
  void io(uint64_t logical_offset, size_t count, uint8_t *dst, const uint8_t *src, Opmode mode);
  void read_bstore(uint64_t offset, size_t count, uint8_t *dst);
  int fd;
  std::unique_ptr<L2Map_mmap> l1;
  std::unique_ptr<Layercache<L2Map_mmap>> layer_cache;
  size_t table_size_in_bytes;
  uint64_t cluster_mask;
  uint64_t cluster_size;
  unsigned int cluster_bits;
  unsigned int table_bits;
  uint64_t table_mask;
  uint64_t file_size;
  uint64_t logical_image_size;
  const bool readonly;
  std::unique_ptr<QEDImage> bstore;
};
