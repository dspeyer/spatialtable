#include <random>
#include <vector>
#include <iostream>
#include <cstring>
#include <Magick++.h>
#include <math.h>
#include <sys/time.h>
#include "testlib.h"

using namespace Magick;

std::default_random_engine gen;

long long get_usec() {
  timeval t;
  gettimeofday(&t,NULL);
  return (long long)t.tv_sec*1000000+t.tv_usec;
}

class HeatMap {
public:
  static const int imgSize=768;
  int cnt[imgSize][imgSize];
  HeatMap() {
    std::memset(cnt, 0, sizeof(cnt));
  }
  void count(const std::vector<double>& v) {
    int x = imgSize*(v[0]+.5)/2;
    int y = imgSize*(v[1]+.5)/2;
    if (x>=0 && x<imgSize && y>=0 && y<imgSize) {
      cnt[x][y]++;
    }
  }
  void write() {
    int maxcnt=0;
    for (int x=0; x<imgSize; x++) {
      for (int y=0; y<imgSize; y++) {
	if (cnt[x][y]>maxcnt) {
	  maxcnt=cnt[x][y];
	}
      }
    }
    auto image = new char[imgSize][imgSize][3];
    for (int x=0; x<imgSize; x++) {
      for (int y=0; y<imgSize; y++) {
	double v = ((double)cnt[x][y])/maxcnt;
	v = sqrt(v);
	//std::cout << v << std::endl;
	if (v<.25) {
	  image[x][y][0] = 0;
	  image[x][y][1] = 0;
	  image[x][y][2] = sqrt(v*4)*255;
	} else if (v<.5) {
	  image[x][y][0] = 0;
	  image[x][y][1] = sqrt((v-.25)*4)*255;
	  image[x][y][2] = sqrt((.5-v)*4)*255;
	} else if (v<.75) {
	  image[x][y][0] = sqrt((v-.5)*4)*255;
	  image[x][y][1] = sqrt((.75-v)*4)*255;
	  image[x][y][2] = 0;
	} else {
	  image[x][y][0] = 255;
	  image[x][y][1] = sqrt((v-.75)*4)*255;
	  image[x][y][2] = sqrt((v-.75)*4)*255;
	}
      }
    }

    Blob blob(image, imgSize*imgSize*3);
    Image img(imgSize, imgSize, "RGB", CharPixel, image);
    img.write("map.png");
    delete image;
  }
};

double normpdf(double x, double stdev) {
  return exp(-x*x/(2*stdev*stdev))/(sqrt(2*M_PI)*stdev);
}

// Would be a constant, but g++ has issues about float constants
#define STDEV 0.1

class Randomizer {
public:
  std::uniform_real_distribution<double> distribution;
  std::normal_distribution<double> norm;
  std::uniform_int_distribution<int> modepicker;
  std::vector<std::vector<double>> modes;
  int dim;
  int nmodes;
  Randomizer(int _dim, int _nmodes) : distribution(0,1), norm(0,STDEV), modepicker(0, _nmodes-1), modes(_nmodes), dim(_dim), nmodes(_nmodes) {
    for (int i=0; i<nmodes; i++) {
      modes[i].resize(dim);
      for (int j=0; j<dim; j++) {
	modes[i][j] = distribution(gen);
      }
    }
  }
  std::vector<double> get() {
    std::vector<double> v(dim);
    int which = modepicker(gen);
    for (int d=0; d<dim; d++) {
      v[d]=norm(gen)+modes[which][d];
    }
    return v;
  }
  float p(const std::vector<double>& pt) {
    double out=0;
    for (int i=0; i<nmodes; i++) {
      double p=1;
      for (int d=0; d<dim; d++) {
	double z=fabs(pt[d]-modes[i][d]);
	p*=normpdf(z, STDEV);
      }
      out += p/nmodes;
    }
    return out;
  }
};

int main(int argc, char** argv) {
  // Process args
  InitializeMagick(*argv);
  if (argc!=7) {
    std::cout << "Usage: ./rand dimensions nmodes npoints nqueries database redrawheatmap\n";
    return 1;
  }
  int dim=atoi(argv[1]);
  int nmodes=atoi(argv[2]);
  int n=atoi(argv[3]);
  int q=atoi(argv[4]);    
  Randomizer r(dim, nmodes);
  HeatMap *hm = NULL;
    
  if (dim==2 && !std::strcmp(argv[6],"yes")) {
    hm = new HeatMap();
    hm->write();
  }

  dbStub *db = dbStub::New(argv[5]);

  if (hm) {
    hm->write();
  }
  // Insertions
  
  for (int i=0; i<n; i++) {
    std::vector<double> v=r.get();
    db->insert(v);
    if (hm) {
      hm->count(v);
    }
    if ((i+1)%1000 == 0) {
      std::cout << "inserted " << (i+1) << " of " << n << std::endl;
    }
  }

  // Heatmap

  if (hm) {
    hm->write();
    delete hm;
  }

  // Queries

  std::uniform_real_distribution<double> qd(500,2000);
  for (int i=0; i<q; i++) {
    std::vector<double> pt = r.get();
    double p = r.p(pt);
    double volume = (qd(gen)/n) / p;
    double side = pow(volume, 1.0/dim);
    if (side>0.2) {
      side=0.2;
      volume=pow(0.2,dim);
    }
    std::vector<double> pt2=pt;
    for (int d=0; d<dim; d++) {
      pt[d] -= side/2;
      pt2[d] += side/2;
    }
    long long start = get_usec();
    int found = db->query(pt,pt2);
    long long end = get_usec();
    std::cout << volume << ", " << found << ", " << (end-start)/1e3 << std::endl;
  }
}
