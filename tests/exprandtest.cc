#include <random>
#include <vector>
#include <iostream>
#include <cstring>
#include <math.h>
#include <sys/time.h>
#include "testlib.h"

std::default_random_engine gen;

long long get_usec() {
  timeval t;
  gettimeofday(&t,NULL);
  return (long long)t.tv_sec*1000000+t.tv_usec;
}

class Randomizer {
public:
  std::uniform_real_distribution<double> norm;
  std::uniform_int_distribution<int> sign;
  int dim;
  Randomizer(int _dim) : norm(-3,3), sign(0,1), dim(_dim) { }
  std::vector<double> get() {
    std::vector<double> v(dim);
    for (int d=0; d<dim; d++) {
      v[d]=(pow(1000,norm(gen))-1e-9)*(sign(gen)*2-1);
    }
    return v;
  }
};

int main(int argc, char** argv) {
  // Process args
  if (argc!=5) {
    std::cout << "Usage: ./exprandtest dimensions npoints nqueries database\n";
    return 1;
  }
  int dim=atoi(argv[1]);
  int n=atoi(argv[2]);
  int q=atoi(argv[3]);    
  Randomizer r(dim);

  dbStub *db = dbStub::New(argv[4]);

  // Insertions
  
  for (int i=0; i<n; i++) {
    std::vector<double> v=r.get();
    db->insert(v);
    if ((i+1)%1000 == 0) {
      std::cout << "inserted " << (i+1) << " of " << n << std::endl;
    }
  }

  // Queries

  std::uniform_real_distribution<double> qd(500,2000);
  for (int i=0; i<q; i++) {
    std::vector<double> pt = r.get();
    std::vector<double> pt2(pt.size());
    double volume = 1;
    for (int d=0; d<dim; d++) {
      pt2[d] = pt[d] * pow(1000, 1+10*qd(gen)*7/n);
      if (pt[d]<0) std::swap(pt[d], pt2[d]);
      volume *= pt2[d]-pt[d];
    }
    long long start = get_usec();
    int found = db->query(pt,pt2);
    long long end = get_usec();
    std::cout << fabs(volume) << ", " << found << ", " << (end-start)/1e3 << std::endl;
  }
}
