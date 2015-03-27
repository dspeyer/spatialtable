#ifndef _UTILS_H_
#define _UTILS_H_

#include "../common/gen/tabletserver.pb.h"
#include "../common/gen/tabletserver.rpcz.h"
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <vector>
#include <limits>
#include <cstdlib>

using google::protobuf::RepeatedField;

template<int DIM>
struct geom {
  typedef boost::geometry::cs::cartesian cartesian;
  typedef boost::geometry::model::point<double, DIM, cartesian> point;
  typedef boost::geometry::model::box<point> box;
  typedef std::pair<box, std::string> value;
};

// Copy the index specified and all lower indecies from a vector to a point
// This exists solely to service pointFromVector
template<int N>
struct setRecursive {
  template<typename POINT>
  static void pfrf(POINT* pt, const RepeatedField<double>& v) {
    pt->template set<N>(v.Get(N));
    setRecursive<N-1>::pfrf(pt, v);
  }
  template<typename POINT>
  static void rffp(const POINT* pt, RepeatedField<double>* rf) {
    setRecursive<N-1>::rffp(pt, rf);
    rf->Add(pt->template get<N>());
  }
};
template<>
struct setRecursive<-1> {
  template<typename POINT>
  static void pfrf(POINT* pt, const RepeatedField<double>& v) {
  }
  template<typename POINT>
  static void rffp(const POINT* pt, RepeatedField<double>* rf) {
  }
};

template<int N>
typename geom<N>::point pointFromProto(const RepeatedField<double>& v) {
  assert(v.size()==N);
  typename geom<N>::point out;
  setRecursive<N-1>::pfrf(&out, v);
  return out;
}

template<int N>
void ProtoFromPoint(RepeatedField<double>* out, const typename geom<N>::point& pt) {
  out->Reserve(N);
  setRecursive<N-1>::rffp(&pt, out);
}

template<int N>
void protoBoxFromBox(Box* out, const typename geom<N>::box& b) {
  ProtoFromPoint<N>(out->mutable_start(), b.min_corner());
  ProtoFromPoint<N>(out->mutable_end(), b.max_corner());
}

template<int N>
typename geom<N>::box boxFromProtoBox(const Box& b) {
  assert(b.start().size()==N);
  assert(b.end().size()==N);
  typename geom<N>::point s=pointFromProto<N>(b.start());
  typename geom<N>::point e=pointFromProto<N>(b.end());
  return typename geom<N>::box(s,e);
}

template<typename POINT>
double& getFromPoint(POINT& p, int idx) {
  double* ptr = const_cast<double*>(&(p.template get<0>()));
  return ptr[idx];
}

// It would be better to make this a static_assert, but I can't see a way to do it.
// This way, at least, if our assumptions about how points are implemented aren't valid, we crash and burn immediately.
static bool sanityCheckPointHack(){
  geom<8>::point p;
  const double* ptr0 = &p.get<0>();
  const double* ptr1 = &p.get<1>();
  const double* ptr7 = &p.get<7>();
  assert(ptr1 == ptr0+1);
  assert(ptr7 == ptr0+7);
  return true;
}
static bool sanityChecked=sanityCheckPointHack();

const double Inf = std::numeric_limits<double>::infinity();

static std::string stringFromDouble(double d) {
  if (d==Inf) {
    return "Inf";
  } else if (d==-Inf) {
    return "-Inf";
  } else {
    char buf[32];
    snprintf(buf,32,"%f",d);
    return buf;
  }
}

static std::string stringFromBox(const /*protobuf*/ Box& box) {
  std::string out = "";
  for (int d=0; d<box.start_size(); d++) {
    if (d!=0) out+="_x_";
    out += stringFromDouble(box.start(d));
    out += "..";
    out += stringFromDouble(box.end(d));
  }
  return out;
}

#endif // _UTILS_H_
