#ifndef _WRAP_HDFS_
#define _WRAP_HDFS_

#include <string>

struct hdfs_internal;
typedef struct hdfs_internal* hdfsFS;
    
struct hdfsFile_internal;
typedef struct hdfsFile_internal* hdfsFile;

class HdfsFile {
 public:
  HdfsFile(std::string name, int mode);
  std::string read();
  void write(const std::string& data);
  static bool exists(const std::string& fn);
  ~HdfsFile();
  static int WRITE;
  static int READ;
 private:
  static hdfsFS fs;
  hdfsFile file;
};

#endif //_WRAP_HDFS_
