#include <string>
#include "../common/gen/tabletserver.pb.h"
#include "../common/client/libclient.h"

TableStub* ts;

void addStarbucks(const std::string& name, double lat, double long) {
  Box b;
  b.add_start(lat);
  b.add_start(long);
  b.add_end(lat+0.000001);
  b.add_end(long+0.000001);
  ts->Insert(b, name, 2);
}

main() {
  ts = new TableStub("world");
#include "insertCommands"
}
