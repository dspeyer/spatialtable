#include <iostream>
#include <map>
#include <vector>
#include <rpcz/rpcz.hpp>
#include "../common/gen/tabletserver.pb.h"
#include "../common/gen/tabletserver.rpcz.h"
#include "../common/utils.h"
#include "tablet.h"

using namespace std;

std::map<string,tablet*> tablets;

class TabletServerServiceImpl : public TabletServerService {
  virtual void CreateTable(const Table& request, rpcz::reply<Status> reply) {
    Status response;
    tablet* t = tablet::New(request.name(),request.dim());
    if (!t) {
      response.set_status(Status::WrongDimension);
      reply.send(response);
      return;
    }
    tablets[t->get_name()]=t;
    response.set_status(Status::Success);
    reply.send(response);
  }

  virtual void Insert(const InsertRequest& request, rpcz::reply<Status> reply) {
    Status response;
    auto it = tablets.find(request.tablet());
    if (it==tablets.end()) {
      response.set_status(Status::NoSuchTablet);
      reply.send(response);
      return;
    }
    int dim = it->second->get_dim();
    if (request.data().box().start_size()!=dim || request.data().box().end_size()!=dim) {
      response.set_status(Status::WrongDimension);
      reply.send(response);
      return;
    }
    it->second->insert(request.data().box(), request.data().value());
    response.set_status(Status::Success);
    reply.send(response);
  }

  virtual void Remove(const RemoveRequest& request, rpcz::reply<Status> reply) {
    Status response;
    auto it = tablets.find(request.tablet());
    if (it==tablets.end()) {
      response.set_status(Status::NoSuchTablet);
      reply.send(response);
      return;
    }
    int dim = it->second->get_dim();
    if (request.key().start_size()!=dim || request.key().end_size()!=dim) {
      response.set_status(Status::WrongDimension);
      reply.send(response);
      return;
    }
    bool result = it->second->remove(request.key());
    
    if (result) {
      response.set_status(Status::Success);
    } else {
      response.set_status(Status::NoSuchRow);
    }
    reply.send(response);
  }

  virtual void Query(const QueryRequest& request, rpcz::reply<QueryResponse> reply) {
    QueryResponse response;
    auto it = tablets.find(request.tablet());
    if (it==tablets.end()) {
      response.mutable_status()->set_status(Status::NoSuchTablet);
      reply.send(response);
      return;
    }
    int dim = it->second->get_dim();
    if (request.query().start_size()!=dim || request.query().end_size()!=dim) {
      response.mutable_status()->set_status(Status::WrongDimension);
      reply.send(response);
      return;
    }
    it->second->query(request.query(), request.is_within(), response);
    response.mutable_status()->set_status(Status::Success);    
    reply.send(response);
  }

  virtual void ListTablets(const ListRequest& request, rpcz::reply<ListResponse> reply) {
    ListResponse response;
    for (auto i : tablets) {
      TabletDescription* t = response.add_results();
      t->set_name(i.second->get_name());
      t->set_dim(i.second->get_dim());      
    }
    reply.send(response);
  }

};

int main(int argc, char **argv) {
  string port;
  if (argc==2) {
    port = argv[1];
  } else {
    port = "5555";
  }
  rpcz::application application;
  rpcz::server server(application);
  TabletServerServiceImpl tabletserver_service;
  server.register_service(&tabletserver_service);
  cout << "Serving requests on port " << port << endl;
  server.bind("tcp://*:"+port);
  application.run();
}
