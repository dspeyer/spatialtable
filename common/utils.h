#ifndef _UTILS_H_
#define _UTILS_H_

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <vector>

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
  template<typename POINT, typename ARRAY>
  static void pfa(POINT* pt, const ARRAY& v) {
    pt->template set<N>(v[N]);
    setRecursive<N-1>::pfa(pt, v);
  }
  template<typename POINT, typename ARRAY>
  static void afp(const POINT* pt, ARRAY& v) {
    v[N]=pt->template get<N>();
    setRecursive<N-1>::afp(pt, v);
  }
};
template<>
struct setRecursive<-1> {
  template<typename POINT,typename ARRAY>
  static void pfa(POINT* pt, const ARRAY& v) {
  }
  template<typename POINT,typename ARRAY>
  static void afp(const POINT* pt, ARRAY& v) {
  }
};


template<int N>
typename geom<N>::point pointFromVector(const std::vector<double>& v) {
  assert(v.size()==N);
  typename geom<N>::point out;
  setRecursive<N-1>::pfa(&out, v);
  return out;
}

template<int N>
std::vector<double> VectorFromPoint(const typename geom<N>::point& pt) {
  std::vector<double> out(N);
  setRecursive<N-1>::afp(&pt, out);
  return out;
}

struct vecBox {
  std::vector<double> start;
  std::vector<double> end;
  vecBox(const std::vector<double>& _s, const std::vector<double>& _e) : start(_s), end(_e) {}
  vecBox(std::vector<double>&& _s, std::vector<double>&& _e) : start(_s), end(_e) {}
  vecBox() : start(0), end(0) {}
};

template<int N>
vecBox vecBoxFromBox(const typename geom<N>::box& b) {
  vecBox out;
  out.start=VectorFromPoint<N>(b.min_corner());
  out.end=VectorFromPoint<N>(b.max_corner());
  std::cerr << "Converting " << b.min_corner().template get<0>() << "," << b.max_corner().template get<0>() << " to " << out.start[0] << "," << out.end[0] << "\n";
  return out;
}

template<int N>
typename geom<N>::box boxFromVecBox(const vecBox& b) {
  assert(b.start.size()==N);
  assert(b.end.size()==N);
  typename geom<N>::point s=pointFromVector<N>(b.start);
  typename geom<N>::point e=pointFromVector<N>(b.end);
  return typename geom<N>::box(s,e);
}


#endif // _UTILS_H_
