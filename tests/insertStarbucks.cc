#include <iostream>
#include <string>
#include "../common/gen/tabletserver.pb.h"
#include "../common/client/libclient.h"

TableStub* ts;

void addStarbucks(const std::string& name, double lat, double longi) {
  Box b;
  b.add_start(lat);
  b.add_start(longi);
  b.add_end(lat+0.000001);
  b.add_end(longi+0.000001);
  auto s = ts->Insert(b, name, 2);
  static int count=0;
  if (s==Status::Success) {
    std::cout << "Success #" << (++count) << " " << name << std::endl;
  } else {
    std::cout << "Error: " << Status::StatusValues_Name(s) << std::endl;
    exit(1);
  }
}

int main() {
  rpcz::application::options opts;
  opts.connection_manager_threads = 64;
  rpcz::application application(opts);
  ts = new TableStub("starbucks", &application);
#include "insertCommands"
  return 0;
}
