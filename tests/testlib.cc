#include "../common/client/libclient.h"
#include "../common/gen/tabletserver.rpcz.h"
#include <rpcz/rpcz.hpp>
#include <iostream>
#include "testlib.h"

class stdoutStub : public dbStub {
  virtual void insert(const std::vector<double>& v) {
    std::cout << "inserting ";
    for (double d : v) {
      std::cout << d << ", ";
    }
    std::cout << std::endl;
  }
  virtual int query(const std::vector<double>& start, const std::vector<double>& end) {
    std::cout << "querying ";
    for (unsigned int i=0; i<start.size(); i++) {
      std::cout << start[i] << ".." << end[i] << ", ";
    }
    std::cout << std::endl;
    return 0;
  } 
};

class spatialStub : public dbStub {
public:
  spatialStub() : stub("randtest", &app) {}
    
  virtual void insert(const std::vector<double>& v) {
    Box b;
    for (double val : v) {
      b.add_start(val);
      b.add_end(val+.00001);
    }
    stub.Insert(b, "x", 2);
  }
  virtual int query(const std::vector<double>& start, const std::vector<double>& end) {
    Box b;
    for (double val : start) b.add_start(val);
    for (double val : end) b.add_end(val);
    QueryResponse qr = stub.Query(b, true);
    if (qr.status().status() == Status::Success) {
      return qr.results_size();
    } else {
      return -1;
    }
  }
  rpcz::application app;
  TableStub stub;
};

/*static*/  dbStub* dbStub::New(const std::string& type) {
  if (type=="stdout") {
    return new stdoutStub();
  } else if (type=="spatialtable") {
    return  new spatialStub();
  } else {
    std::cout << "Unrecognized database: " << type << std::endl;
    exit(1);
  }
}
