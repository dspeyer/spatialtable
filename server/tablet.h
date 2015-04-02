#ifndef _TABLET_H_
#define _TABLET_H_

#include<string>
#include "../common/gen/tabletserver.pb.h"

class tablet {
 public:
  static tablet* New(const std::string& _table, int dim);
  virtual void query(const /*protobuf*/ Box& q, bool is_within, QueryResponse& out) = 0;
  virtual void insert(const /*protobuf*/ Box& k, const std::string& v) = 0;
  virtual void save() = 0;
  virtual void load(const std::string& fileName) = 0;
  virtual int get_dim() = 0;
  virtual std::string get_name() = 0;
};

#endif //_TABLET_H_
