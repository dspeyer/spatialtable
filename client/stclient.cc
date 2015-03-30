#include <string>
#include <iostream>
#include <map>
#include <rpcz/rpcz.hpp>
#include "../common/gen/tabletserver.pb.h"
#include "../common/gen/tabletserver.rpcz.h"
#include "../common/utils.h"

using namespace std;

void list_tablets(TabletServerService_Stub* stub, int argc, char** argv) {
  ListRequest request;
  ListResponse response;
  try {
    stub->ListTablets(request, &response, 1000);
    cout << "Tablets loaded are:\n";
    for (int i=0; i<response.results_size(); i++) {
      cout << "  '" << response.results(i).name() << "' (" << response.results(i).dim() << " dimensional)\n";
    }
  } catch (rpcz::rpc_error &e) {
    cout << "Error: " << e.what() << endl;;
  }
}

void insert(TabletServerService_Stub* stub, int argc, char** argv) {
  InsertRequest request;
  Status response;
  if (argc<4 || argc%2) {
    cout << "Usage: insert tablet value start_0 end_0 start_1 end_1...\n";
    return;
  }
  request.set_tablet(argv[0]);
  request.mutable_data()->set_value(argv[1]);
  for (int i=2; i<argc-1; i+=2) {
    request.mutable_data()->mutable_box()->add_start(atof(argv[i]));
    request.mutable_data()->mutable_box()->add_end(atof(argv[i+1]));
  }
  try {
    stub->Insert(request, &response, 1000);
    if (response.status() == Status::Success) {
      cout << "...Success!\n";
    } else {
      cout << "...error: " << Status::StatusValues_Name(response.status()) << endl;
    }
  } catch (rpcz::rpc_error &e) {
    cout << "Error: " << e.what() << endl;;
  }
}

void remove(TabletServerService_Stub* stub, int argc, char** argv) {
  RemoveRequest request;
  Status response;
  if (argc<3 || argc%2==0) {
    cout << "Usage: insert tablet start_0 end_0 start_1 end_1...\n";
    return;
  }
  request.set_tablet(argv[0]);
  for (int i=1; i<argc-1; i+=2) {
    request.mutable_key()->add_start(atof(argv[i]));
    request.mutable_key()->add_end(atof(argv[i+1]));
  }
  try {
    stub->Remove(request, &response, 1000);
    if (response.status() == Status::Success) {
      cout << "...Success!\n";
    } else {
      cout << "...error: " << Status::StatusValues_Name(response.status()) << endl;
    }
  } catch (rpcz::rpc_error &e) {
    cout << "Error: " << e.what() << endl;;
  }
}

void query(TabletServerService_Stub* stub, int argc, char** argv) {
  if (argc<4 || argc%2) {
    cout << "Usage: query tablet [within|intersect] start_0 end_0 start_1 end_1...\n";
    return;
  }
  QueryRequest request;
  QueryResponse response;
  request.set_tablet(argv[0]);
  string type=argv[1];
  if (type!="within" && type!="intersect") {
    cout << "Unknown query type: '" << type << "' (use 'within' or 'intersect')\n";
    return;
  }
  request.set_is_within(type=="within");
  for (int i=2; i<argc-1; i+=2) {
    request.mutable_query()->add_start(atof(argv[i]));
    request.mutable_query()->add_end(atof(argv[i+1]));
  }
  try {
    stub->Query(request, &response, 1000);
    if (response.status().status() == Status::Success) {
      cout << "Matching rows are:\n";
      for (int i=0; i<response.results_size(); i++) {
        cout << "  " << stringFromBox(response.results(i).box()) << "=> '" << response.results(i).value() << "'\n";
      }
    } else {
      cout << "Error: " << Status::StatusValues_Name(response.status().status()) << endl;
    }

  } catch (rpcz::rpc_error &e) {
    cout << "Error: " << e.what() << endl;;
  }
}

void create(TabletServerService_Stub* stub, int argc, char** argv) {
  if (argc!=2) {
    cout << "Usage: create tablename dimension\n";
    return;
  }
  Table request;
  Status response;
  request.set_name(argv[0]);
  request.set_dim(atoi(argv[1]));
  cout << "Creating table named '" << request.name() << "' with " << request.dim() << " dimensions...\n";
  try {
    stub->CreateTable(request, &response, 1000);
    if (response.status() == Status::Success) {
      cout << "...Success!\n";
    } else {
      cout << "...error: " << Status::StatusValues_Name(response.status()) << endl;
    }
  } catch (rpcz::rpc_error &e) {
    cout << "Error: " << e.what() << endl;;
  }
}

typedef void (*callback) (TabletServerService_Stub*, int, char**);

map<string, callback> ops = {
  {"list", list_tablets},
  {"insert", insert},
  {"remove", remove},
  {"query", query},
  {"create", create}
};


int main(int argc, char ** argv) {
  if (argc<3) {
    cout << "Usage: \n  ./client serverhost:serverport command <args>\n  commands are:\n";
    for (auto i : ops) {
      cout << "    " << i.first << "\n";
    }
    return 1;
  }
  string server = argv[1];
  rpcz::application application;
  TabletServerService_Stub stub(application.create_rpc_channel("tcp://"+server), true);

  callback cb = ops[argv[2]];
  if (cb) {
    cb(&stub, argc-3, argv+3);
  } else {
    cout << "Unknown command: " << argv[2] << "\n";
  }

  /*  SearchRequest request;
  SearchResponse response;
  request.set_query("gold");

  cout << "Sending request." << endl;
  try {
    search_stub.Search(request, &response, 1000);
    cout << response.DebugString() << endl;
  } catch (rpcz::rpc_error &e) {
    cout << "Error: " << e.what() << endl;;
    }*/
}
