#ifndef _TABLET_H_
#define _TABLET_H_

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <vector>
#include <map>
#include <string>
#include <limits>
#include "../common/utils.h"

template<int DIM>
class tabletImpl;

class tablet {
 public:
  template<int DIM>
  static tablet* New(const std::string& _table) {
    return new tabletImpl<DIM>(_table);
  }
  virtual std::vector<std::pair<vecBox,std::string>> query(const vecBox& q, bool is_within) = 0;
  virtual void query(const /*protobuf*/ Box& q, bool is_within, QueryResponse& out) = 0;
  virtual void insert(const vecBox& k, const std::string& v) = 0;
  virtual void insert(const /*protobuf*/ Box& k, const std::string& v) = 0;
  virtual int get_dim() = 0;
  virtual std::string get_name() = 0;
};


template<int DIM>
class tabletImpl : public tablet{
 public:
  typedef typename geom<DIM>::point point;
  typedef typename geom<DIM>::box box;
  typedef typename geom<DIM>::value value;

  const int dim=DIM;

  virtual std::vector<std::pair<vecBox,std::string>> query(const vecBox& q, bool is_within) {
    assert(q.start.size()==DIM && q.end.size()==DIM);
    box qb = boxFromVecBox<DIM>(q);
    std::vector<value> res;
    if (is_within) {
      rtree.query(boost::geometry::index::covered_by(qb), std::back_inserter(res));
    } else {
      rtree.query(boost::geometry::index::intersects(qb), std::back_inserter(res));
    }
    std::vector<std::pair<vecBox,std::string>> out;
    out.reserve(res.size());
    for (auto& i : res) {
      auto vb = vecBoxFromBox<DIM>(i.first);
      std::cerr << "returning " << vb.start[0] << "," << vb.end[0] << "\n";
      out.push_back(make_pair(vb, i.second));
    }
    return out;
  }

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


  virtual void insert(const vecBox& k, const std::string& v) {
    assert(k.start.size()==DIM && k.end.size()==DIM);
    rtree.insert(make_pair(boxFromVecBox<DIM>(k),v));
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

const double Inf = std::numeric_limits<double>::infinity();

#endif //_TABLET_H_
