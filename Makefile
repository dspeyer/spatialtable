HADOOP_HOME = /usr/local/hadoop
COMPILE = g++ -c -g --std=c++11  -I/usr/local/include/boost_1_57_0 -DBOOST_GEOMETRY_INDEX_DETAIL_EXPERIMENTAL -Wall -Wno-unused-variable -Wno-unused-function -I$(HADOOP_HOME)/include
LIBS = -lrpcz -lprotobuf -lboost_system -lboost_serialization -lhdfs -L$(HADOOP_HOME)/lib/native -L-L/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server

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

bin/libclient.o:  common/gen/tabletserver.rpcz.h common/gen/tabletserver.pb.h common/client/libclient.h common/client/libclient.cc common/utils.h
	${COMPILE} -o bin/libclient.o common/client/libclient.cc

# Client

bin/stclient.o: client/stclient.cc common/gen/tabletserver.pb.h common/gen/tabletserver.rpcz.h common/client/libclient.h
	${COMPILE} -o bin/stclient.o client/stclient.cc

stclient: bin/stclient.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o bin/libclient.o
	g++ -g -o stclient bin/stclient.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o bin/libclient.o ${LIBS}

# Server

bin/tablet.o: server/tablet.cc common/gen/tabletserver.pb.h common/utils.h server/tablet.h
	${COMPILE} -o bin/tablet.o server/tablet.cc

bin/tabletserver.o: server/tabletserver.cc common/gen/tabletserver.pb.h common/gen/tabletserver.rpcz.h common/utils.h server/tablet.h
	${COMPILE} -o bin/tabletserver.o server/tabletserver.cc

bin/tablet_test.o: server/tablet_test.cc common/gen/tabletserver.pb.h common/utils.h server/tablet.h
	${COMPILE} -o bin/tablet_test.o server/tablet_test.cc


tabletserver: bin/tabletserver.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o bin/tablet.o bin/libclient.o
	g++ -g -o tabletserver bin/tabletserver.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o bin/tablet.o bin/libclient.o ${LIBS}

tablet_test: bin/tablet_test.o bin/tabletserver.pb.o bin/tablet.o
	g++ -g -o tablet_test bin/tablet_test.o bin/tabletserver.pb.o bin/tablet.o ${LIBS}


# Test

tests/insertCommands: tests/starbucks.csv
	cat tests/starbucks.csv  | sed 's/,/",/' | sed 's/^/  addStarbucks("/' | sed 's/$$/);/'   > tests/insertCommands

bin/insertStarbucks.o: tests/insertStarbucks.cc tests/insertCommands common/client/libclient.h common/gen/tabletserver.pb.h
	${COMPILE}  -o bin/insertStarbucks.o tests/insertStarbucks.cc

insertStarbucks: bin/insertStarbucks.o bin/libclient.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o
	g++ -g -o insertStarbucks bin/insertStarbucks.o bin/libclient.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o  ${LIBS}

bin/timedqueries.o: tests/timedqueries.cc tests/insertCommands common/client/libclient.h common/gen/tabletserver.pb.h
	${COMPILE}  -o bin/timedqueries.o tests/timedqueries.cc

timedqueries: bin/timedqueries.o bin/libclient.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o
	g++ -g -o timedqueries bin/timedqueries.o bin/libclient.o bin/tabletserver.pb.o bin/tabletserver.rpcz.o  ${LIBS}



# Misc

midterm.pdf: midterm.tex querytimes.png
	pdflatex midterm.tex

clean:
	rm bin/*.o
	rm common/gen/*.h
	rm common/gen/*.cc
	rm tabletserver tablet_test stclient
