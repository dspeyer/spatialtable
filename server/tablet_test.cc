#include "tablet.h"
#include "../common/utils.h"
#include "../common/gen/tabletserver.pb.h"
#include <iostream>

int main(){
  tablet* t = tablet::New("space",2);
  Box a,b,q;
  a.add_start(1);
  a.add_start(1);
  a.add_end(2);
  a.add_end(2);
  t->insert(a,"hello");
  b.add_start(3);
  b.add_start(3);
  b.add_end(4);
  b.add_end(4);
  t->insert(b,"world");
  q.add_start(0);
  q.add_start(0);
  q.add_end(5);
  q.add_end(5);
  QueryResponse resp;
  t->query(q, true, resp);
  std::cerr << "returned\n";
  for (int i=0; i<resp.results_size(); i++) {
    Row* r = resp.mutable_results(i);
    std::cout << r->box().start(0) << ".." << r->box().end(0) << " x " << r->box().start(1) << ".." << r->box().end(1) << " = " << r->value() << std::endl;
  }
 t->save();
}
