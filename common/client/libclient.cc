#include "libclient.h"
#include <string>
#include <rpcz/rpcz.hpp>
#include "../gen/tabletserver.pb.h"
#include "../gen/tabletserver.rpcz.h"
#include "../utils.h"
#include <iostream>
#include "hdfs.h"

static rpcz::application* application;
static std::map<std::string, TabletServerService_Stub*> cache;

TableStub::TableStub(const std::string& _table, rpcz::application* _application) : table(_table) {
  if (!application) {
    application=_application;
  }
}

TabletServerService_Stub* TableStub::getStub(const std::string& server) {
  if (!cache[server]) {
    cache[server] = new TabletServerService_Stub(application->create_rpc_channel("tcp://"+server), true);
  }
  return cache[server];
}

TableStub::~TableStub() {
  /*  for (auto& i : cache) {
    delete i.second;
    }*/
}

Status::StatusValues TableStub::Insert(const Box& b, const std::string& value, int layer) {
  std::vector<TabletInfo> ti = findTabletWithBox(b, layer, true);
  if (ti[0].status().status()!=Status::Success) {
    return ti[0].status().status();
  }
  TabletServerService_Stub* stub = getStub(ti[0].server());
  Status response;
  InsertRequest request;
  request.set_tablet(ti[0].name());
  request.mutable_data()->mutable_box()->CopyFrom(b);
  request.mutable_data()->set_value(value);
  try {
    stub->Insert(request, &response, 1000);
    return response.status();
  } catch (rpcz::rpc_error &e) {
    return Status::ServerDown;
  }
}

Status::StatusValues TableStub::Remove(const Box& b, int layer) {
  std::vector<TabletInfo> ti = findTabletWithBox(b, layer, true);
  if (ti[0].status().status()!=Status::Success) {
    return ti[0].status().status();
  }
  TabletServerService_Stub* stub=getStub(ti[0].server());
  Status response;
  RemoveRequest request;
  request.set_tablet(ti[0].name());
  request.mutable_key()->CopyFrom(b);
  try {
    stub->Remove(request, &response, 1000);
    return response.status();
  } catch (rpcz::rpc_error &e) {
    return Status::ServerDown;
  }
}

QueryResponse TableStub::Query(const Box& b, bool is_within) {
  QueryResponse out;
  std::vector<TabletInfo> tis = findTabletWithBox(b, 2, false);
  if (tis[0].status().status()!=Status::Success) {
    out.mutable_status()->set_status(tis[0].status().status());
    return out;
  }
  for (TabletInfo ti : tis) {
    TabletServerService_Stub* stub = getStub(ti.server());
    QueryResponse response;
    QueryRequest request;
    request.set_tablet(ti.name());
    request.set_is_within(is_within);
    request.mutable_query()->CopyFrom(b);
    try {
      stub->Query(request, &response, 1000);
      if (response.status().status()==Status::Success) {
	out.mutable_results()->MergeFrom(response.results());
      }
    } catch (rpcz::rpc_error &e) {
      out.mutable_status()->set_status(Status::ServerDown);
    }
  }
  if (out.results_size()) {
    out.mutable_status()->set_status(Status::Success);
  } else {
    out.mutable_status()->set_status(Status::NoSuchRow);
  }
  return out;
}


std::vector<TabletInfo> tabletInfoFromStatus(Status::StatusValues status) {
  TabletInfo ti;
  ti.mutable_status()->set_status(status);
  std::vector<TabletInfo> out = {ti};
  return out;
}

std::vector<TabletInfo> TableStub::findTabletWithBox(const Box& b, int layer, bool justone) {
  static hdfsFS fs=hdfsConnect("default",0);
  TabletInfo md0;
  md0.set_name("md0;;"+table);
  std::string buffer;
  buffer=lastKnownMd0Server;
  std::vector<int> retry_delays = {0, 1, 5, 1000, 5000, -1};
  for (int retry_delay : retry_delays) {
    if (buffer.size()) {
      md0.set_server(buffer);
      std::vector<TabletInfo> ret = findTabletWithBox(b, layer, md0, justone);
      if (!ret.size() || ret[0].status().status() != Status::NoSuchTablet) {
	return ret;
      }
    }
    if (retry_delay==-1) {
      return tabletInfoFromStatus(Status::NoSuchTablet);
    }
    std::cout << "No such tablet: sleeping " << retry_delay << "ms and retrying\n";
    usleep(retry_delay*1000);
    /*    hdfsFile readFile = hdfsOpenFile(fs, ("/md0/"+table).c_str(), O_RDONLY, 0, 0, 0);
    int bytesToRead = hdfsAvailable(fs, readFile);
    buffer.resize(bytesToRead);
    tSize num_read_bytes = hdfsRead(fs, readFile, (void*)buffer.c_str(), bytesToRead);
    hdfsCloseFile(fs, readFile);*/
    buffer = "localhost:5555";
    lastKnownMd0Server = buffer;
  }
}

std::vector<TabletInfo> TableStub::findTabletWithBox(const Box& b, int layer, TabletInfo in, bool justone) {
  //  std::cerr << "looking for " << stringFromBox(b) << "  in " << in.name() << std::endl;
  if (layer>0) {
    TabletServerService_Stub* stub=getStub(in.server());
    QueryRequest request;
    QueryResponse response;
    request.set_tablet(in.name());
    request.set_is_within(false);
    request.mutable_query()->CopyFrom(b);
    try {
      stub->Query(request, &response, 1000);
    } catch (rpcz::rpc_error &e) {
      return tabletInfoFromStatus(Status::ServerDown);
    }
    if (response.status().status() != Status::Success) {
      return tabletInfoFromStatus(response.status().status());
    }
    bool found=false;
    std::vector<TabletInfo> out;
    for (int i=0; i<response.results_size(); i++) {
      Row *r = response.mutable_results(i);
      TabletInfo ti;
      std::vector<TabletInfo> res;
      ti.ParseFromString(r->value());
      if (!justone || is_within(r->box(), b)){
	res=findTabletWithBox(b, layer-1, ti, justone);
	for (TabletInfo& r : res) {
	  if (r.status().status()==Status::Success) {
	    out.push_back(r);
	    if (justone) return out;
	  }
	}
      }
    }
    if (out.size()) {
      return out;
    } else {
      return tabletInfoFromStatus(Status::CouldNotFindTablet);
    }
  } else {
    bool failsCrossing=false;
    //    std::cerr << "contemplating " << in.name() << " as a place to find " << stringFromBox(b) << std::endl;
    if (justone) {
      for (int i=0; i<in.must_cross_dims_size(); i++) {
        int dim = in.must_cross_dims(i);
        double val = in.must_cross_vals(i);
        if (b.end(dim)<=val || b.start(dim)>=val) {
	  //	std::cerr << "...fails to cross " << dim << "=" << val << std::endl;
	  failsCrossing=true;
	  break;
        }
      }
    }
    if (!failsCrossing) {
      //      std::cerr << "...crosses\n";
      in.mutable_status()->set_status(Status::Success);
      std::vector<TabletInfo> out = {in};
      return out;
    } else {
      return tabletInfoFromStatus(Status::CouldNotFindTablet);
    }
  }
}
