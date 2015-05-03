#include "wraphdfs.h"
#include "hdfs.h"

HdfsFile::HdfsFile(std::string name, int mode) {
  if (!fs) {
    fs=hdfsConnect("default",0);
  }
  file = hdfsOpenFile(fs, name.c_str(), mode, 0, 0, 0);
}

std::string HdfsFile::read() {
  if (file) {
    std::string buffer;
    int bytesToRead = hdfsAvailable(fs, file);
    buffer.resize(bytesToRead);
    tSize num_read_bytes = hdfsRead(fs, file, (void*)buffer.c_str(), bytesToRead);
    return buffer;
  } else {
    return "";
  }
}

void HdfsFile::write(const std::string& data) {
  hdfsWrite(fs, file, (void*)data.c_str(), data.length());
  hdfsFlush(fs, file);
}

/*static*/ bool HdfsFile::exists(const std::string& fn) {
  return hdfsExists(fs, fn.c_str());
}

/*static*/ void HdfsFile::init() {
  if (!fs) {
    fs=hdfsConnect("default",0);
  }
}

HdfsFile::~HdfsFile() {
  hdfsCloseFile(fs, file);
}

/*static*/ int HdfsFile::WRITE = O_WRONLY|O_CREAT;
/*static*/ int HdfsFile::READ = O_RDONLY;
/*static*/ hdfsFS HdfsFile::fs = NULL;
