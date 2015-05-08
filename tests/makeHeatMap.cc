#include <vector>
#include <iostream>
#include <Magick++.h>
#include <sstream>
#include <fstream>
#include <string.h>


using namespace Magick;

class HeatMap {
public:
  static const int imgSize = 768;
  double dataWidth, dataHeight, dataMinX, dataMinY;
  int cnt[imgSize][imgSize];
  HeatMap(double _dataWidth, double _dataHeight, double _dataMinX, double _dataMinY) :
    dataWidth(_dataWidth),
    dataHeight(_dataHeight),
    dataMinX(_dataMinX),
    dataMinY(_dataMinY) 
  {
    memset(cnt, 0, sizeof(cnt));
  }

  void count(const std::vector<double>& v) {
    int x = imgSize*(v[0]-dataMinX)/dataWidth;
    int y = imgSize*(v[1]-dataMinY)/dataHeight;
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
	v = pow(v,.25);
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

std::vector<std::string> split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

int main(int argc, char ** argv) {
  std::ifstream ifs(argv[1]);
  HeatMap hm(atof(argv[2]), atof(argv[3]), atof(argv[4]), atof(argv[5]));
  while(!ifs.eof()) {
    char buf[1024];
    ifs.getline(buf, sizeof(buf));
    auto words = split(buf, ',');
    if (words.size()!=3) continue;
    std::vector<double> coords(2);
    coords[0]=atof(words[1].c_str());
    coords[1]=atof(words[2].c_str());
    hm.count(coords);
  }
  hm.write();
}
