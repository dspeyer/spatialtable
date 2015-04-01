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


TabletInfo TableStub::findTabletWithBox(const Box& b, int layer) {
  TabletInfo out;
  out.set_name("md0::"+table);
  out.set_server("localhost:5555"); // TODO: fixme  
  for (int curlayer=0; curlayer<layer; curlayer++) {
    TabletServerService_Stub* stub=getStub(out.server());
    QueryRequest request;
    QueryResponse response;
    request.set_tablet(out.name());
    request.set_is_within(false);
    request.mutable_query()->CopyFrom(b);
    try {
      stub->Query(request, &response, 1000);
    } catch (rpcz::rpc_error &e) {
      out.mutable_status()->set_status(Status::ServerDown);
      return out;
    }
    if (response.status().status() != Status::Success) {
      out.mutable_status()->set_status(response.status().status());
      return out;
    }
    bool found=false;
    for (int i=0; i<response.results_size(); i++) {
      Row *r = response.mutable_results(i);
      out.ParseFromString(r->value());
      if (!is_within(r->box(), b)){
	continue;
      }
      bool failsCrossing=false;
      for (int i=0; i<out.must_cross_dims_size(); i++) {
	int dim = out.must_cross_dims(i);
	double val = out.must_cross_vals(i);
	if (b.end(dim)<val || b.start(dim)>val) {
	  failsCrossing=true;
	  break;
	}
      }
      if (!failsCrossing) {
	found=true;
	break;
      }
    }
    if (!found) {
      out.mutable_status()->set_status(Status::CouldNotFindTablet);
      return out;
    }
  }
  out.mutable_status()->set_status(Status::Success);
  return out;
}
