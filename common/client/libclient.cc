#include "libclient.h"
#include <string>
#include <rpcz/rpcz.hpp>
#include "../gen/tabletserver.pb.h"
#include "../gen/tabletserver.rpcz.h"
#include "../utils.h"
#include <iostream>

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

Status::StatusValues TableStub::Insert(const Box& b, const std::string& value, int layer) {
  TabletInfo ti = findTabletWithBox(b, layer);
  if (ti.status().status()!=Status::Success) {
    return ti.status().status();
  }
  TabletServerService_Stub* stub = getStub(ti.server());
  Status response;
  InsertRequest request;
  request.set_tablet(ti.name());
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
  TabletInfo ti = findTabletWithBox(b, layer);
  if (ti.status().status()!=Status::Success) {
    return ti.status().status();
  }
  TabletServerService_Stub* stub=getStub(ti.server());
  Status response;
  RemoveRequest request;
  request.set_tablet(ti.name());
  request.mutable_key()->CopyFrom(b);
  try {
    stub->Remove(request, &response, 1000);
    return response.status();
  } catch (rpcz::rpc_error &e) {
    return Status::ServerDown;
  }
}

TabletInfo tabletInfoFromStatus(Status::StatusValues status) {
  TabletInfo out;
  out.mutable_status()->set_status(status);  
  return out;
}

TabletInfo TableStub::findTabletWithBox(const Box& b, int layer) {
  TabletInfo md0;
  md0.set_server("localhost:5555"); // FIXME
  md0.set_name("md0;;"+table);
  return findTabletWithBox(b, layer, md0);
}

TabletInfo TableStub::findTabletWithBox(const Box& b, int layer, TabletInfo in) {
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
    for (int i=0; i<response.results_size(); i++) {
      Row *r = response.mutable_results(i);
      TabletInfo ti,res;
      ti.ParseFromString(r->value());
      if (is_within(r->box(), b)){
	res=findTabletWithBox(b, layer-1, ti);
	if (res.status().status()==Status::Success) {
	  return res;
	}
      }
    }
    return tabletInfoFromStatus(Status::CouldNotFindTablet);
  } else {
    bool failsCrossing=false;
    //    std::cerr << "contemplating " << in.name() << " as a place to find " << stringFromBox(b) << std::endl;
    for (int i=0; i<in.must_cross_dims_size(); i++) {
      int dim = in.must_cross_dims(i);
      double val = in.must_cross_vals(i);
      if (b.end(dim)<=val || b.start(dim)>=val) {
	//	std::cerr << "...fails to cross " << dim << "=" << val << std::endl;
	failsCrossing=true;
	break;
      }
    }
    if (!failsCrossing) {
      //      std::cerr << "...crosses\n";
      in.mutable_status()->set_status(Status::Success);
      return in;
    } else {
      return tabletInfoFromStatus(Status::CouldNotFindTablet);
    }
  }
}
