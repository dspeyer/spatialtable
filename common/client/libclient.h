#ifndef _LIBCLIENT_H_
#define _LIBCLIENT_H_

#include <rpcz/rpcz.hpp>
#include "../gen/tabletserver.pb.h"
#include "../gen/tabletserver.rpcz.h"
#include <string>
#include <vector>

// Make this a class so that we can easily add caching later if we want to
class TableStub {
 public:
  TableStub(const std::string& _table, rpcz::application* _application);
  Status::StatusValues Insert(const Box& b, const std::string& value, int layer);
  Status::StatusValues Remove(const Box& b, int layer);
  QueryResponse Query(const Box& b, bool is_within);
  ~TableStub();
 private:
  std::vector<TabletInfo> findTabletWithBox(const Box& b, int layer, TabletInfo in, bool justone);
  std::vector<TabletInfo> findTabletWithBox(const Box& b, int layer, bool justone);
  TabletServerService_Stub* getStub(const std::string& server);
  std::string table;
  std::string lastKnownMd0Server;
};

#endif //_LIBCLIENT_H_
