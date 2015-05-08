#include "libclient.h"
#include <string>
#include <rpcz/rpcz.hpp>
#include "../gen/tabletserver.pb.h"
#include "../gen/tabletserver.rpcz.h"
#include "../utils.h"
#include <iostream>
#include "../wraphdfs.h"

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
  //  std::cerr << "inserting into " << ti[0].name() << " on " << ti[0].server() << std::endl;
  TabletServerService_Stub* stub = getStub(ti[0].server());
  Status response;
  InsertRequest request;
  request.set_tablet(ti[0].name());
  request.mutable_data()->mutable_box()->CopyFrom(b);
  request.mutable_data()->set_value(value);
  try {
    stub->Insert(request, &response, 1000);
    if (response.status()!=Status::Success) {
      std::cerr << "error " << Status::StatusValues_Name(response.status()) << " while inserting into " << ti[0].name() << " on " << ti[0].server() << std::endl;
    }
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
  std::vector<rpcz::rpc> rpcs(tis.size());
  std::vector<QueryResponse> responses(tis.size());
  std::vector<QueryRequest> requests(tis.size());
  for (unsigned int i=0; i<tis.size(); i++) {
    TabletServerService_Stub* stub = getStub(tis[i].server());
    requests[i].set_tablet(tis[i].name());
    requests[i].set_is_within(is_within);
    requests[i].mutable_query()->CopyFrom(b);
    rpcs[i].set_deadline_ms(1000);
    stub->Query(requests[i], &responses[i], &rpcs[i], NULL);
  }
  for (unsigned int i=0; i<tis.size(); i++) {
    rpcs[i].wait();    
  }
  for (unsigned int i=0; i<tis.size(); i++) {
    if (rpcs[i].ok()) {
      out.mutable_results()->MergeFrom(responses[i].results());
    } else {
      out.mutable_status()->set_status(Status::ServerDown);
      return out;
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
  TabletInfo md0;
  md0.set_name("md0;;"+table);
  std::vector<int> retry_delays = {0, 1, 5, 1000, 5000, -1};
  for (int retry_delay : retry_delays) {
    if (lastKnownMd0Server.size()) {
      md0.set_server(lastKnownMd0Server);
      std::vector<TabletInfo> ret = findTabletWithBox(b, layer, md0, justone);
      if (!ret.size() || ret[0].status().status() != Status::NoSuchTablet) {
	return ret;
      }
      std::cout << "No such tablet: sleeping " << retry_delay << "ms and retrying\n";
    }
    if (retry_delay==-1) {
      return tabletInfoFromStatus(Status::NoSuchTablet);
    }
    usleep(retry_delay*1000);
    HdfsFile readFile("/md0/"+table, HdfsFile::READ);
    lastKnownMd0Server = readFile.read();
    std::cout << "Now looking for md0 on " << lastKnownMd0Server << std::endl;
  }
  return tabletInfoFromStatus(Status::NoSuchTablet);
}

std::vector<TabletInfo> TableStub::findTabletWithBox(const Box& b, int layer, TabletInfo in, bool justone) {
  //  std::cerr << "looking for " << stringFromBox(b) << "  in " << in.name() << std::endl;
  if (layer>0) {
    //    std::cerr << "getting stub for '" << in.server() << "'" << std::endl << std::flush;
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
      std::cerr << "got Status " << Status::StatusValues_Name(response.status().status()) << " from " << in.server() << " for " << in.name() << std::endl;
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
      //      std::cerr << "contemplating " << in.must_cross_dims_size() << " crosses " << std::endl;
      for (int i=0; i<in.must_cross_dims_size(); i++) {
        int dim = in.must_cross_dims(i);
        double val = in.must_cross_vals(i);
        if (b.end(dim)<=val || b.start(dim)>=val) {
	  failsCrossing=true;
	  break;
        }
      }
    }
    if (!failsCrossing) {
      in.mutable_status()->set_status(Status::Success);
      std::vector<TabletInfo> out = {in};
      //      std::cerr << "return out\n";
      return out;
    } else {
      //      std::cerr << "return CFNT\n";
      return tabletInfoFromStatus(Status::CouldNotFindTablet);
    }
  }
}
