#include <condition_variable>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>

// https://stackoverflow.com/questions/4362831/scoped-using-directive-within-a-struct-class-declaration
namespace XXX {
using namespace std;
template <class T> class LimitedQ {

public:
  LimitedQ(size_t max_items_) : terminated(false), max_items(max_items_){};
  typedef unique_ptr<T> cell;
  void push_front(const T &item) { push_front(new T(item)); }
  void push_front(T *item) { push_front(cell(item)); }

  void terminate() {
    lock_guard<mutex> lg(m);
    if (terminated)
      return;
    terminated = true;
    cv_pop.notify_all();
    cv_push.notify_all();
  }

  void push_front(LimitedQ::cell &&item) {
    if (!item)
      throw "Passsing empty item is not allowed";

    unique_lock<mutex> lk(m);

    cv_push.wait(lk, [this]() -> bool { return terminated || items.size() < max_items; });

    if (terminated)
      throw "Adding elements in the terminated state is not allowed";

    // emplace_front ?
    items.emplace_front(move(item));
    // items.push_front(item);

    lk.unlock();
    cv_pop.notify_one();
  }

  cell pop_back() {
    unique_lock<mutex> lk(m);

    cv_pop.wait(lk, [this]() -> bool { return terminated || items.size(); });

    if (terminated && !items.size())
      return cell();

    cell p(move(items.back()));
    items.pop_back();

    lk.unlock();
    cv_push.notify_one();
    return p;
  }

  class Terminator {
  public:
    Terminator(LimitedQ<T> &q_) : q(q_) {}
    ~Terminator() { q.terminate(); }

  private:
    LimitedQ<T> &q;
  };

private:
  // TODO: https://en.cppreference.com/w/cpp/container/deque
  list<cell> items;
  mutex m;
  condition_variable cv_push;
  condition_variable cv_pop;
  bool terminated;
  const size_t max_items;
};

} // namespace XXX

using namespace XXX;