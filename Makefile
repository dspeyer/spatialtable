all: tabletserver

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

#bin/echo.o: client/echo.cc common/gen/echo.pb.h common/gen/echo.rpcz.h
#	g++ -c -o bin/echo.o client/echo.cc

#echo: bin/echo.o bin/echo.pb.o bin/echo.rpcz.o 
#	g++ -o echo bin/echo.o bin/echo.pb.o bin/echo.rpcz.o -lrpcz -lprotobuf

# Server

bin/tabletserver.o: server/tabletserver.cc common/gen/tabletserver.pb.h common/gen/tabletserver.rpcz.h common/utils.h server/tablet.h
	g++ -c -o bin/tabletserver.o server/tabletserver.cc -I/usr/local/include/boost_1_57_0 --std=c++11

tabletserver: bin/tabletserver.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o 
	g++ -o tabletserver bin/tabletserver.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o -lrpcz -lprotobuf -lboost_system


#tablet_test: server/tablet_test.cc server/tablet.h common/utils.h
#	g++ -g -o tablet_test server/tablet_test.cc --std=c++11 -I/usr/local/include/boost_1_57_0  -lprotobuf -lboost_system -lrpcz


# Misc

clean:
	rm bin/*.o
	rm common/gen/*.h
	rm common/gen/*.cc
	rm tabletserver 