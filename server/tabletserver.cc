#include <iostream>
#include <map>
#include <vector>
#include <mutex>
#include <rpcz/rpcz.hpp>
#include <boost/archive/archive_exception.hpp>
#include "../common/gen/tabletserver.pb.h"
#include "../common/gen/tabletserver.rpcz.h"
#include "../common/utils.h"
#include "../common/client/libclient.h"
#include "tablet.h"
#include "hdfs.h"  // for hdfs access

using namespace std;

rpcz::application *application;

std::map<string,tablet*> tablets;
std::map<string,std::mutex> tabletlocks;
std::mutex namelistlock;

std::string my_hostport;

std::string tablet_description(tablet* t) {
  TabletInfo ci;
  t->mostly_fill_tabletinfo(&ci);
  ci.set_server(my_hostport);
  std::string out;
  int size=ci.ByteSize();
  out.resize(size);
  ci.SerializeToArray(const_cast<char*>(out.c_str()), size);
  return out;
}

class TabletServerServiceImpl : public TabletServerService {
  virtual void CreateTable(const Table& request, rpcz::reply<Status> reply) {
    Status response;
    tablet* content = tablet::New(request.name(),request.dim(), 2);
    if (!content) {
      response.set_status(Status::WrongDimension);
      reply.send(response);
      return;
    }
    tablets[content->get_name()]=content;
    tablet* meta = tablet::New(request.name(), request.dim(), 1);
    tablets[meta->get_name()]=meta;
    tablet* md0 = tablet::New(request.name(), request.dim(), 0);
    tablets[md0->get_name()]=md0;
    meta->insert(content->get_borders(), tablet_description(content));
    md0->insert(meta->get_borders(), tablet_description(meta));
    hdfsFS fs = hdfsConnect("default", 0);
    hdfsFile writeFile = hdfsOpenFile(fs, ("/md0/"+content->get_table()).c_str(), O_WRONLY|O_CREAT, 0, 0, 0);
    hdfsWrite(fs, writeFile, (void*)my_hostport.c_str(), my_hostport.length());
    hdfsFlush(fs, writeFile);
    hdfsCloseFile(fs, writeFile);
    response.set_status(Status::Success);
    reply.send(response);
  }

  virtual void Insert(const InsertRequest& request, rpcz::reply<Status> reply) {
    std::lock_guard<std::mutex> guard(tabletlocks[request.tablet()]);
    Status response;
    auto it = tablets.find(request.tablet());
    if (it==tablets.end()) {
      response.set_status(Status::NoSuchTablet);
      reply.send(response);
      return;
    }
    tablet *t = it->second;
    int dim = t->get_dim();
    if (request.data().box().start_size()!=dim || request.data().box().end_size()!=dim) {
      response.set_status(Status::WrongDimension);
      reply.send(response);
      return;
    }
    t->insert(request.data().box(), request.data().value());
    if (t->get_size()>200 && t->get_layer()>0) {
      std::vector<tablet*> newt = t->split();
      if (newt.size()>0) {
	TableStub stub(t->get_table(),application);
	auto status = stub.Remove(t->get_borders(), t->get_layer()-1);
	if (status!=Status::Success) {
	  std::cerr << "Failed to remove old " << t->get_name() << " code " << Status::StatusValues_Name(status) << std::endl;
	}
	namelistlock.lock();
	tablets.erase(it);
	namelistlock.unlock();
	for (unsigned int i=0; i<newt.size(); i++) {
	  namelistlock.lock();
	  tablets[newt[i]->get_name()] = newt[i];
	  namelistlock.unlock();
	  status = stub.Insert(newt[i]->get_borders(), tablet_description(newt[i]), newt[i]->get_layer()-1);
	  if (status!=Status::Success) {
	    std::cerr << "Failed to insert " << newt[i]->get_name() << " code " << Status::StatusValues_Name(status) << std::endl;
	  }
	}
	delete t;
      }
    }
    response.set_status(Status::Success);
    reply.send(response);
  }
  
  virtual void Remove(const RemoveRequest& request, rpcz::reply<Status> reply) {
    std::lock_guard<std::mutex> guard(tabletlocks[request.tablet()]);
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
    std::lock_guard<std::mutex> guard(tabletlocks[request.tablet()]);
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
    std::lock_guard<std::mutex> listguard(namelistlock);
    ListResponse response;
    for (auto i : tablets) {
      TabletDescription* t = response.add_results();
      t->set_name(i.second->get_name());
      t->set_dim(i.second->get_dim());      
    }
    reply.send(response);
  }

  virtual void UnLoadTablet(const UnLoadRequest& request, rpcz::reply<Status> reply) {
    std::lock_guard<std::mutex> guard(tabletlocks[request.tablet()]);
    std::lock_guard<std::mutex> listguard(namelistlock);
    Status response;
    auto it = tablets.find(request.tablet());
    if (it==tablets.end()) {
      response.set_status(Status::NoSuchTablet);
      reply.send(response);
      return;
    }
    tablet *t = it->second;
    t->save();
    TableStub stub(t->get_table(), application);
    auto status = stub.Remove(t->get_borders(), t->get_layer()-1);
    if (status!=Status::Success) {
      std::cerr << "Failed to remove old " << t->get_name() << " code " << Status::StatusValues_Name(status) << std::endl;
    }
    tablets.erase(it);
    delete t;
    response.set_status(Status::Success);
    reply.send(response);
  }

  virtual void LoadTablet(const LoadRequest& request, rpcz::reply<Status> reply) {
    std::lock_guard<std::mutex> guard(tabletlocks[request.tablet()]);
    std::lock_guard<std::mutex> listguard(namelistlock);
    Status response;
    tablet * t = tablet::New("",request.dim(),2);
    if (t==NULL) {
      response.set_status(Status::WrongDimension);
      reply.send(response);
      return;
    }
    try {
      auto status = t->load(request.tablet());
      if (status!=Status::Success) {
	response.set_status(status);
	reply.send(response);
	return;
      }
    } catch (typename boost::archive::archive_exception &e) {
      std::cerr << e.what() << std::endl;
      response.set_status(Status::CorruptFile);
      reply.send(response);
      return;
    }
    std::cout << "done with tablet stuff\n";
    tablets[t->get_name()]=t;
    std::cout << "inserting self\n";
    if (t->get_layer() > 0) {
      TableStub stub(t->get_table(), application);
      std::cout << "made stub\n";
      auto status = stub.Insert(t->get_borders(), tablet_description(t), t->get_layer()-1);
      std::cout << "inserted into metadata\n";
      if (status!=Status::Success) {
	std::cerr << "Failed to insert " << t->get_name() << " code " << Status::StatusValues_Name(status) << std::endl;
      }
    } else {
      hdfsFS fs = hdfsConnect("default", 0);
      hdfsFile writeFile = hdfsOpenFile(fs, ("/md0/"+t->get_table()).c_str(), O_WRONLY|O_CREAT, 0, 0, 0);
      hdfsWrite(fs, writeFile, (void*)my_hostport.c_str(), my_hostport.length());
      hdfsFlush(fs, writeFile);
      hdfsCloseFile(fs, writeFile);
    }
    std::cout << "done loading rpc\n";
    response.set_status(Status::Success);
    reply.send(response);
  }
};

int main(int argc, char **argv) {
  rpcz::application::options opts;
  opts.connection_manager_threads = 64;
  application = new rpcz::application(opts);
  string port;
  if (argc==2) {
    port = argv[1];
  } else {
    port = "5555";
  }
  char hn[1024];
  gethostname(hn,sizeof(hn));
  my_hostport = std::string(hn)+":"+port;
  rpcz::server server(*application);
  TabletServerServiceImpl tabletserver_service;
  server.register_service(&tabletserver_service);
  cout << "Serving requests on port " << port << endl;
  server.bind("tcp://*:"+port);
  application->run();
}
