#ifndef _TESTLIB_H_
#define _TESTLIB_H_

#include <vector>
#include <string>

class dbStub {
public:
  virtual void insert(const std::vector<double>& v) = 0;
  virtual int query(const std::vector<double>& start, const std::vector<double>& end) = 0;
  static dbStub* New(const std::string& type);
};

#endif // _TESTLIB_H_
