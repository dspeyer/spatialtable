#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <rpcz/rpcz.hpp>
#include "../common/gen/tabletserver.pb.h"
#include "../common/gen/tabletserver.rpcz.h"
#include "../common/utils.h"
#include "../common/client/libclient.h"

using namespace std;
rpcz::application application;


int main(int argc, char ** argv) {

vector<string> server = {"128.59.146.138:5555", "128.59.150.196:5555","128.59.46.146:5555","128.59.149.122:5555"};
vector<int> serverTotalLoad = {0, 0, 0, 0};

vector<ListResponse> tablets(4);


while(1){
for (int i = 0; i < 4; i++) {
 TabletServerService_Stub stub(application.create_rpc_channel("tcp://"+server[i]));
 ListRequest request;
  try{
        stub.ListTablets(request, &tablets[i], 1000);
        for(int j = 0; j < tablets[i].results_size(); j++){
	  serverTotalLoad[i] += tablets[i].results(j).size();
        }
   } catch (rpcz::rpc_error &e) {
      cout << "Error: " << e.what() << endl;;     
  }
   cout << "server: "<< server[i] << " load: " << serverTotalLoad[i] << endl;
}

int min = 0;
int max = 0;
int maxval = -1;
int minval = 1<<30;
for (int i=0; i < 4; i++){
  if (serverTotalLoad[i] < minval) {
        min = i;
	minval = serverTotalLoad[i];
  }
  if (serverTotalLoad[i] > maxval) {
        max = i;
	maxval = serverTotalLoad[i];
  }
}

cout << "Max load server: " << server[max] << " load: " << serverTotalLoad[max] << ":" << maxval << endl;
cout << "Min load server: " << server[min] << " load: " << serverTotalLoad[min] << ":" << minval << endl;
int numRowsToMove; 
if (serverTotalLoad[max] == 0) {
     numRowsToMove = 0;
} else {
   numRowsToMove = (serverTotalLoad[max] - serverTotalLoad[min])/2;
   cout << "set numRowsToMove to " << numRowsToMove << endl;
}
int diff = 0;
int minDiff = 0;
int tabletToMove = -1;
cout << "tabletToMove is " << tabletToMove <<" and numRowsToMove is " << numRowsToMove << endl;

if (numRowsToMove != 0) {
        for(int i=0; i< tablets[max].results_size(); i++){
                if( (numRowsToMove > tablets[max].results(i).size() )  ){
                        if( minDiff < tablets[max].results(i).size()) {
                                minDiff = tablets[max].results(i).size();
                                tabletToMove = i;
                        }
                } else {
                continue;
        }       
        }
        
        if(tabletToMove > -1){ 
        cout << "tabletToMove is " << tablets[max].results(tabletToMove).name() << " (" << tabletToMove <<") from " << server[max] << " to " << server[min] << "and mindiff is " << minDiff << endl; 

        TabletServerService_Stub stub(application.create_rpc_channel("tcp://"+server[max]));
        UnLoadRequest request;
        Status response;
        request.set_tablet(tablets[max].results(tabletToMove).name());
        try{
                stub.UnLoadTablet(request, &response, 5000);
                cout << "Unloaded... " << Status::StatusValues_Name(response.status()) << endl;
        } catch (rpcz::rpc_error &e) {
                cout << "Error: " << e.what() << endl;;
		exit(1);
        }

        TabletServerService_Stub stub2(application.create_rpc_channel("tcp://"+server[min]));
        LoadRequest request2;
        Status response2;
        request2.set_tablet(tablets[max].results(tabletToMove).name());
        request2.set_dim(tablets[max].results(tabletToMove).dim());
        try {
                stub2.LoadTablet(request2, &response2, 5000);
                cout << "Loaded... " << Status::StatusValues_Name(response2.status()) << endl;
        } catch (rpcz::rpc_error &e) {
                cout << "Error: " << e.what() << endl;
		exit(1);
        }
        } else { 
                sleep(5);
        }
        // clear totals
        serverTotalLoad = {0, 0, 0, 0};
        numRowsToMove = 0;
        cout << "******** Iteration done ********" << endl;
        
} else{
cout << "******** Nothing to do ***********" << endl;
serverTotalLoad = {0, 0, 0, 0};
numRowsToMove = 0;
sleep(5);
      }
} 
}

