all: tabletserver stclient

# Common

common/gen/tabletserver.pb.cc common/gen/tabletserver.pb.h: common/tabletserver.proto
	cd common && protoc tabletserver.proto --cpp_out=gen

common/gen/tabletserver.rpcz.cc common/gen/tabletserver.rpcz.h: common/tabletserver.proto
	cd common && protoc tabletserver.proto --cpp_rpcz_out=gen

bin/tabletserver.pb.o: common/gen/tabletserver.pb.cc common/gen/tabletserver.pb.h
	g++ -c -o bin/tabletserver.pb.o common/gen/tabletserver.pb.cc

bin/tabletserver.rpcz.o: common/gen/tabletserver.rpcz.cc common/gen/tabletserver.rpcz.h common/gen/tabletserver.pb.h
	g++ -c -o bin/tabletserver.rpcz.o common/gen/tabletserver.rpcz.cc

# Client

bin/stclient.o: client/stclient.cc common/gen/tabletserver.pb.h common/gen/tabletserver.rpcz.h
	g++ -c -o bin/stclient.o client/stclient.cc --std=c++11

stclient: bin/stclient.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o 
	g++ -o stclient bin/stclient.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o -lrpcz -lprotobuf

# Server

bin/tabletserver.o: server/tabletserver.cc common/gen/tabletserver.pb.h common/gen/tabletserver.rpcz.h common/utils.h server/tablet.h
	g++ -g -c -o bin/tabletserver.o server/tabletserver.cc -I/usr/local/include/boost_1_57_0 --std=c++11

bin/tablet_test.o: server/tablet_test.cc common/gen/tabletserver.pb.h common/utils.h server/tablet.h
	g++ -c -o bin/tablet_test.o server/tablet_test.cc -I/usr/local/include/boost_1_57_0 --std=c++11


tabletserver: bin/tabletserver.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o 
	g++ -g -o tabletserver bin/tabletserver.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o -lrpcz -lprotobuf -lboost_system

tablet_test: bin/tablet_test.o bin/tabletserver.pb.o
	g++ -o tablet_test bin/tablet_test.o bin/tabletserver.pb.o -lrpcz -lprotobuf -lboost_system


# Misc

clean:
	rm bin/*.o
	rm common/gen/*.h
	rm common/gen/*.cc
	rm tabletserver tablet_test stclient