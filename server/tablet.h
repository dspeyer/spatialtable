#ifndef _TABLET_H_
#define _TABLET_H_

#include<string>
#include "../common/gen/tabletserver.pb.h"

class tablet {
 public:
  static tablet* New(const std::string& _table, int dim, int layer);
  virtual void query(const /*protobuf*/ Box& q, bool is_within, QueryResponse& out) = 0;
  virtual void insert(const /*protobuf*/ Box& k, const std::string& v) = 0;
  virtual bool remove(const /*protobuf*/ Box&) = 0;
  virtual void save() = 0;
  virtual int get_size() = 0;
  virtual int get_dim() = 0;
  virtual std::vector<tablet*> split() = 0;
  virtual std::string get_name() = 0;
  virtual void mostly_fill_tabletinfo(TabletInfo *ti) = 0;
  virtual const /*protobuf*/ Box& get_borders() = 0;
  virtual std::string get_table() = 0;
  virtual int get_layer() = 0;
  virtual ~tablet() {}
};

#endif //_TABLET_H_
