#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <vector>
#include <map>
#include <fstream>
#include <string>
#include "../common/utils.h"
#include "tablet.h"
#include "../common/gen/tabletserver.pb.h"
#include <boost/serialization/vector.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/type_traits.hpp>

struct alwaysTrue{
  template<typename T>
  bool operator()(T t) const{
    return true;
  }
};


namespace boost {
namespace serialization {
template<typename ARCHIVE, typename POINT>
void serialize(ARCHIVE& ar, boost::geometry::model::box<POINT>& b, unsigned int version) {
  const POINT &mi=b.min_corner();
  const POINT &ma=b.max_corner();
  int size = sizeof(POINT)/sizeof(double);
  for (int i=0; i<size; i++) {
    ar << getFromPoint(mi,i);
  }
  for (int i=0; i<size; i++) {
    ar << getFromPoint(ma,i);
  }
}
}
}

template<int DIM>
class tabletImpl : public tablet{
 friend class boost::serialization::access;
 template<class Archive>
 void serialize(Archive &ar, const unsigned int version)
{
  ar << table;
  ar << rtree;
  ar << borders;
}

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

  virtual bool remove(const /*protobuf*/ Box& q) {
    assert(q.start().size()==DIM && q.end().size()==DIM);
    box qb = boxFromProtoBox<DIM>(q);
    std::vector<value> res;
    rtree.query(boost::geometry::index::covered_by(qb) &&
		boost::geometry::index::covers(qb),
		std::back_inserter(res));
    for (value& i : res) {
      if (qb==i.first) {
	rtree.remove(i);
	return true;
      }
    }
    return false;
  }

  tabletImpl(const std::string& _table ) : table(_table) {
    for (int i=0; i<DIM; i++) {
      borders.add_start(-Inf);
      borders.add_end(Inf);
    }
  }
  
  virtual int get_dim() {
    return DIM;
  }

  virtual std::string get_name() {
    return table + "::" + stringFromBox(borders);
  }

 virtual void save(){
  std::ofstream ofs("testFile");
  boost::archive::text_oarchive oa(ofs);
  std::vector<value> v;
  rtree.query(boost::geometry::index::satisfies(alwaysTrue()), std::back_inserter(v));
  oa << v;
 
 }

 private:
  /*protobuf*/ Box borders;
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
