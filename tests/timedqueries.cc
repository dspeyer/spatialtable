#include <cstdlib>
#include <iostream>
#include "../common/client/libclient.h"

long long get_usec() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (long long)tv.tv_sec*1000000+(long long)tv.tv_usec;
}

int main(){
  rpcz::application::options opts;
  opts.connection_manager_threads = 64;
  rpcz::application application(opts);
  TableStub stub("starbucks", &application);
  for (int i=0; i<1000; i++) {
    double xs=rand()%360-180;
    double ys=rand()%180-90;
    double xw=rand()/(10.0*RAND_MAX);
    double yw=rand()/(10.0*RAND_MAX);
    double xe=xs+xw;
    double ye=ys+yw;
    long long start = get_usec();
    Box q;
    q.add_start(xs);
    q.add_start(ys);
    q.add_end(xw);
    q.add_end(yw);
    auto res = stub.Query(q, true);
    long long end = get_usec();
    int resc=-1;
    if (res.status().status() == Status::NoSuchRow) {
      resc=0;
    } else if (res.status().status() == Status::Success) {
      resc=res.results_size();
    } else {
      std::cerr << "Error: " << Status::StatusValues_Name(res.status().status());
    }
    std::cout << resc << ", " << (end-start) << std::endl;
  }
  return 0;
}
