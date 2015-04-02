#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/archive/text_oarchive.hpp> // for serialization
#include <boost/archive/text_iarchive.hpp> // for serialization
#include <cstdlib>
#include <vector>
#include <utility>
#include <map>
#include <sstream>
#include <fstream>
#include <string>
#include <sstream>
#include "../common/utils.h"
#include "tablet.h"
#include "../common/gen/tabletserver.pb.h"
#include <boost/serialization/vector.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/type_traits.hpp>
#include "hdfs.h"  // for hdfs access
#include <cmath>

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
    ar & getFromPoint(mi,i);
  }
  for (int i=0; i<size; i++) {
    ar & getFromPoint(ma,i);
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

  tabletImpl(const std::string& _table, int _layer) : table(_table), layer(_layer) {
    for (int i=0; i<DIM; i++) {
      borders.add_start(-Inf);
      borders.add_end(Inf);
    }
  }
  
  tabletImpl(const tabletImpl<DIM>* old) : table(old->table), layer(old->layer) {
    borders.CopyFrom(old->borders);
    must_cross = old->must_cross;
  }

  virtual int get_dim() {
    return DIM;
  }

  virtual int get_size() {
    return rtree.size();
  }

  virtual std::vector<tablet*> split() {
    box bound = rtree.bounds();
    int bestdim=-1;
    double cut;
    double widest=-1;
    for (int i=0; i<DIM; i++) {
      bool already_crossing=false;
      for (auto &line : must_cross) {
	if (line.first == i) {
	  already_crossing = true;
	  break;
	}
      }
      if (already_crossing) {
	continue;
      }
      double width = getFromPoint(bound.max_corner(),i)-getFromPoint(bound.min_corner(),i);
      if (width>widest) {
	bestdim=i;
	widest=width;
	cut=(getFromPoint(bound.max_corner(),i)+getFromPoint(bound.min_corner(),i))/2;
      }
    }
    if (bestdim==-1) return {};
    std::vector<value> v;
    rtree.query(boost::geometry::index::satisfies(alwaysTrue()), std::back_inserter(v));
    if (!std::isfinite(cut)) {
      std::vector<double> vals;
      vals.reserve(v.size()*2);
      for (auto& i : v) {
	double l = getFromPoint(i.first.min_corner(),bestdim);
	if (std::isfinite(l)) vals.push_back(l);
	l = getFromPoint(i.first.max_corner(),bestdim);
	if (std::isfinite(l)) vals.push_back(l);
      }
      if (vals.size()==0) {
	cut = 0;
      } else {
	std::sort(vals.begin(), vals.end());
	if (vals[0]!=vals[vals.size()-1]) {
	  int firstnb, lastnb;
	  for (firstnb=0; vals[firstnb]==vals[0]; firstnb++);
	  for (lastnb=vals.size()-1; vals[lastnb]==vals[vals.size()-1]; lastnb--);
	  if (vals[lastnb]==vals[0]) { // only two values
	    cut = (vals[firstnb]+vals[lastnb])/2;
	  } else {
	    cut=vals[(firstnb+lastnb)/2];
	  }
	} else {
	  cut=vals[0];
	}
      }
    }
    //    std::cerr << "splitting " << get_name() << " at " << bestdim << "=" << cut << std::endl;
    tabletImpl<DIM> *less, *cross, *more;
    less = new tabletImpl<DIM>(this);
    less->borders.set_end(bestdim,cut);
    cross = new tabletImpl<DIM>(this);
    cross->must_cross.push_back(std::make_pair(bestdim,cut));
    more = new tabletImpl<DIM>(this);
    more->borders.set_start(bestdim,cut);
    for (auto& i : v) {
      if (getFromPoint(i.first.max_corner(),bestdim)<=cut) {
	less->rtree.insert(i);
      } else if (getFromPoint(i.first.min_corner(),bestdim)>=cut) {
	more->rtree.insert(i);
      } else {
	cross->rtree.insert(i);
      }
    }
    std::vector<tablet*> out = {less, cross, more};
    return out;
  }

  virtual std::string get_name() {
    std::stringstream out;
    if (layer==0) {
      out << "md0;;";
    } else if (layer==1) {
      out << "metadata;;";
    }
    out << table;
    if (layer==0) {
      return out.str();
    }
    out << ";;"  << stringFromBox(borders);
    for (int i=0; i<must_cross.size(); i++) {
      out << ((i==0) ? ";;" : ",") << must_cross[i].first;
    }
    return out.str();
  }

 virtual void save(){
  // get tablet_name
  std::string tn = this->get_name();
  tn = "/tablets/" + tn;
  const char *tName = tn.c_str();
  // connect to hdfs
  hdfsFS fs = hdfsConnect("default", 0); //default hdfs instance
  //char writePath[100] = "/tablets/test1";
  //const char *tabletDefPath = "/tablets/";
  //strcpy(writePath, tabletDefPath);
  //strcpy(writePath, tName);
  printf("writePath = %s\n",tName);
  hdfsFile writeFile = hdfsOpenFile(fs, tName, O_WRONLY|O_CREAT, 0, 0, 0);
  
  if(!writeFile){
    fprintf(stderr, "Failed to open %s for writing\n", tName);
    exit(-1);
  }
  // create the archive file
  //std::ofstream ofs("test3"); //name of tablet
  std::ostringstream sfs;
  boost::archive::text_oarchive oa(sfs);
  std::vector<value> v;
  rtree.query(boost::geometry::index::satisfies(alwaysTrue()), std::back_inserter(v));
  oa << v;
  //ofs.close(); 
  /*const char* archiveFile = "test3";
   FILE *fp;
   fp = fopen(archiveFile, "r"); 
   if(fp == NULL){
      printf("Failed to open archive file\n");
   }
   fseek(fp, 0, SEEK_END);
   long fsize = ftell(fp);
   printf("file size = %d",(int)fsize);
   fseek(fp, 0, SEEK_SET);
   char *buffer = (char*)malloc(fsize+1);
   fread(buffer, fsize, 1, fp);
   fclose(fp);
  buffer[fsize] = 0;
  printf("about to write = %s to HDFS\n",buffer);*/

  //sfs.str().c_str(); 
  tSize num_bytes = hdfsWrite(fs, writeFile, (void*)sfs.str().c_str(), sfs.str().length());
  if(hdfsFlush(fs, writeFile)){
       fprintf(stderr, "Failed to 'flush' %s\n", tName);
        exit(-1);
  } 
  
  printf("num of bytes written to hdfs = %d\n", num_bytes); 
  hdfsCloseFile(fs, writeFile);
 
 }

virtual void load(const std::string& file){
  //TODO:  load tablet name file from args
  hdfsFS fs=hdfsConnect("default",0);
  std::string rp = "/tablets/" +file;
  const char *readPath = rp.c_str();      
  //char readPath[100] = "/tablets/test1";
  int exists = hdfsExists(fs, readPath);
  if (exists){
    fprintf(stderr, "Failed to validate existance of %s\n", readPath);
   }
  
  hdfsFile readFile = hdfsOpenFile(fs, readPath, O_RDONLY, 0, 0, 0);
 
  if(!readFile){
    fprintf(stderr, "Failed to open %s for reading\n", readPath);
   exit(-1);
   }
  
  int bytesToRead = hdfsAvailable(fs, readFile);
  char *buffer = (char *)malloc(bytesToRead);
  tSize num_read_bytes = hdfsRead(fs, readFile, (void*)buffer, bytesToRead);
  
  std::istringstream ifs(buffer);
  boost::archive::text_iarchive ia(ifs);
  std::vector<value> v;
  ia >> v;	
  for(int i = 0; i < v.size(); i++){
	rtree.insert(v[i]);
   }
 }

  virtual void mostly_fill_tabletinfo(TabletInfo* ti) {
    ti->set_name(get_name());
    for (auto& cross : must_cross) {
      ti->add_must_cross_dims(cross.first);
      ti->add_must_cross_vals(cross.second);
    }
  }

  virtual const /*protobuf*/ Box& get_borders() {
    return borders;
  }

  virtual std::string get_table() {
    return table;
  }

  virtual int get_layer() {
    return layer;
  }


 private:
  /*protobuf*/ Box borders;
  std::string table;
  int layer;
  std::vector<std::pair<int,double>> must_cross;
  boost::geometry::index::rtree< value, boost::geometry::index::quadratic<16> > rtree;
};

typedef tablet* (*TabletFactory)(const std::string&, int layer);

static const int nFactories = 8;
static TabletFactory tabletFactories[nFactories];

template<int N>
struct tabletFactoryInitializer {
  static tablet* New(const std::string& table, int layer) {
    return new tabletImpl<N>(table, layer);
  }
  static bool initialize() {
    tabletFactories[N-1]=(New);
    tabletFactoryInitializer<N-1>::initialize();
    return true;
  }
};
template<>
struct tabletFactoryInitializer<0> {
  static void initialize() {
  }
};

static bool initializeTabletFactories = tabletFactoryInitializer<nFactories>::initialize();


/*static*/ tablet* tablet::New(const std::string& table, int dim, int layer) {
  if (dim<=0 || dim>nFactories || layer<0 || layer>2) {
    return NULL;
  } else {
    return tabletFactories[dim-1](table,layer);
  }
}
