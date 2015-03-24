#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <vector>
#include <map>
#include <string>
#include "../common/utils.h"
#include "tablet.h"

template<int DIM>
class tabletImpl : public tablet{
 public:
  typedef typename geom<DIM>::point point;
  typedef typename geom<DIM>::box box;
  typedef typename geom<DIM>::value value;

  const int dim=DIM;

  virtual void query(const /*protobuf*/ Box& q, bool is_within, QueryResponse& out) {
    assert(q.start().size()==DIM && q.end().size()==DIM);
    box qb = boxFromProtoBox<DIM>(q);
    std::vector<value> res;
    if (is_within) {
      rtree.query(boost::geometry::index::covered_by(qb), std::back_inserter(res));
    } else {
      rtree.query(boost::geometry::index::intersects(qb), std::back_inserter(res));
    }
    out.mutable_results()->Reserve(res.size());
    for (auto& i : res) {
      Row *v=out.add_results();
      protoBoxFromBox<DIM>(v->mutable_box(), i.first);
      v->set_value(i.second);
    }
  }

  virtual void insert(const /*protobuf*/ Box& k, const std::string& v) {
    assert(k.start().size()==DIM && k.end().size()==DIM);
    rtree.insert(make_pair(boxFromProtoBox<DIM>(k),v));
  }

  tabletImpl(const std::string& _table ) : table(_table) {
    
  }
  
  virtual int get_dim() {
    return DIM;
  }

  virtual std::string get_name() {
    return table +"/all";
  }

 private:
  box borders; // TODO: do something with this
  std::string table;
  boost::geometry::index::rtree< value, boost::geometry::index::quadratic<16> > rtree;
};

typedef tablet* (*TabletFactory)(const std::string&);

static const int nFactories = 8;
static TabletFactory tabletFactories[nFactories];

template<int N>
struct tabletFactoryInitializer {
  static tablet* New(const std::string& table) {
    return new tabletImpl<N>(table);
  }
  static bool initialize() {
    tabletFactories[N-1]=(New);
    tabletFactoryInitializer<N-1>::initialize();
  }
};
template<>
struct tabletFactoryInitializer<0> {
  static void initialize() {
  }
};

static bool initializeTabletFactories = tabletFactoryInitializer<nFactories>::initialize();

/*static*/ tablet* tablet::New(const std::string& table, int dim) {
  if (dim<=0 || dim>nFactories) {
    return NULL;
  } else {
    return tabletFactories[dim-1](table);
  }
}
