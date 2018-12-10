
// TODO: validate indexes (upper value) as table_size_in_bytes / sizeof(uint64_t)
// TODO: index -> size_t ?
class L2Map_fio {
public:
    L2Map_fio(int fd_, uint64_t offset_, uint64_t cluster_mask_, uint64_t table_size_in_bytes, bool readonly_)
            : fd(fd_), l2_table_file_offset(offset_), cluster_mask(~cluster_mask_), readonly(readonly_) {

        (void) table_size_in_bytes;
    }

    uint64_t get_offset(uint64_t l2_index) const {

        uint64_t abs_offset;
        if ((size_t)
                    pread(fd, &abs_offset, sizeof(abs_offset), l2_table_file_offset + l2_index * sizeof(uint64_t)) !=
            sizeof(abs_offset))
            throw "Fail to read L2 entry";

        return le64_to_cpu(abs_offset) & cluster_mask;
    }

    void set_offset(uint64_t l2_index, uint64_t xxx) {
        if (readonly)
            throw "This map is readonly";
        xxx = cpu_to_le64(xxx & cluster_mask);
        if ((size_t) pwrite(fd, &xxx, sizeof(xxx), l2_table_file_offset + l2_index * sizeof(uint64_t)) != sizeof(xxx))
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

        if ((size_t) pread(fd, cache.get(), table_size_in_bytes, l2_table_file_offset) != table_size_in_bytes)
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
        if ((size_t) pwrite(fd, &xxx, sizeof(xxx), l2_table_file_offset + l2_index * sizeof(uint64_t)) != sizeof(xxx))
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
