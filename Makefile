COMPILE = g++ -c -g --std=c++11  -I/usr/local/include/boost_1_57_0
LIBS = -lrpcz -lprotobuf -lboost_system -lboost_serialization

all: tabletserver stclient

# Common

common/gen/tabletserver.pb.cc common/gen/tabletserver.pb.h: common/tabletserver.proto
	cd common && protoc tabletserver.proto --cpp_out=gen

common/gen/tabletserver.rpcz.cc common/gen/tabletserver.rpcz.h: common/tabletserver.proto
	cd common && protoc tabletserver.proto --cpp_rpcz_out=gen

bin/tabletserver.pb.o: common/gen/tabletserver.pb.cc common/gen/tabletserver.pb.h
	${COMPILE} -o bin/tabletserver.pb.o common/gen/tabletserver.pb.cc

bin/tabletserver.rpcz.o: common/gen/tabletserver.rpcz.cc common/gen/tabletserver.rpcz.h common/gen/tabletserver.pb.h
	${COMPILE} -o bin/tabletserver.rpcz.o common/gen/tabletserver.rpcz.cc

# Client

bin/stclient.o: client/stclient.cc common/gen/tabletserver.pb.h common/gen/tabletserver.rpcz.h
	${COMPILE} -o bin/stclient.o client/stclient.cc

stclient: bin/stclient.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o 
	g++ -g -o stclient bin/stclient.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o ${LIBS}

# Server

bin/tablet.o: server/tablet.cc common/gen/tabletserver.pb.h common/utils.h server/tablet.h
	${COMPILE} -o bin/tablet.o server/tablet.cc

bin/tabletserver.o: server/tabletserver.cc common/gen/tabletserver.pb.h common/gen/tabletserver.rpcz.h common/utils.h server/tablet.h
	${COMPILE} -o bin/tabletserver.o server/tabletserver.cc

bin/tablet_test.o: server/tablet_test.cc common/gen/tabletserver.pb.h common/utils.h server/tablet.h
	${COMPILE} -o bin/tablet_test.o server/tablet_test.cc


tabletserver: bin/tabletserver.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o bin/tablet.o
	g++ -g -o tabletserver bin/tabletserver.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o bin/tablet.o ${LIBS}

tablet_test: bin/tablet_test.o bin/tabletserver.pb.o bin/tablet.o
	g++ -g -o tablet_test bin/tablet_test.o bin/tabletserver.pb.o bin/tablet.o ${LIBS}


# Misc

clean:
	rm bin/*.o
	rm common/gen/*.h
	rm common/gen/*.cc
	rm tabletserver tablet_test stclient
