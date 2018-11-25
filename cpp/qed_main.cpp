#include <iostream>
#include <random>

#include "qed.h"
#include <cstring> // memset

using namespace ::std;

// qemu-img create  -f qed -b test.img -F qcow2 qwe.qed 2G
// Formatting 'qwe.qed', fmt=qed size=2147483648 cluster_size=65536
int main() {
  try {
    QEDImage qwe("qwe.qed", 2llu * 1024 * 1024 * 1024);

    random_device r;
    default_random_engine e1(r());

    char buf[1024 * 1024];

    uniform_int_distribution<uint64_t> uniform_dist(0, qwe.get_logical_image_size() - sizeof(buf));
    memset(buf, '*', sizeof(buf));
    uniform_int_distribution<uint64_t> uniform_dist2(1, sizeof(buf));
    for (int i = 0; i < 20000; i++) {
      qwe.write(uniform_dist(e1), uniform_dist2(e1), buf);
    }
    /*
        cerr<<"---- alloc maps"<<endl;
        qwe.alloc_maps(0, 1);
        qwe.alloc_maps(65537, 5);

        cerr<<"---- alloc data"<<endl;
        qwe.alloc_data(0, 1);
        qwe.alloc_data(65537, 5);

        cerr<<"---- write"<<endl;
        qwe.write(0, 1, "q");
        qwe.write(65537, 5, "dddff");

        qwe.write(2ull*1024*1024*1024-1,1,"q");
        //    std::cout << "Read: " << sss << endl;
    */
  } catch (const char *msg) {
    cerr << "Unhandled exception:" << endl << msg << endl;
    return 1;
  }
  return 0;
}
