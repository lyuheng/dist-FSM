CC=mpic++
LIBS=-lhwloc
CFLAGS=-std=c++14 -O3 -march=native -fopenmp -lpthread $(LIBS)
OBJDIR=objs/
OBJS=$(OBJDIR)intersection.o 

HEADERS=canonical.h comper.h decompose.h global.h gmatch.h grami.h \
graph.h leapfrogjoin.h setting.h taskprogmap.h types.h worker.h \
concurrent/conmap_zero.h concurrent/conmap.h concurrent/conque.h concurrent/constack.h \
mpi/serialization.h mpi/mpi_global.h mpi/communication.h mpi/timer.h \
core/cache_table.h core/cache_gc.h core/req_queue.h core/req_server.h core/resp_queue.h core/resp_server.h \
utils/bind.h utils/pretty_print.h utils/rwlock.h utils/systemI.h

INCLUDE=-Iconcurrent -Impi -Icore -Iutils -I.

all: run

run: $(OBJDIR)run.o $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(INCLUDE)

$(OBJDIR)intersection.o: intersection/computesetintersection.cpp intersection/computesetintersection.h 
	$(MKDIR_P) $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@ $(INCLUDE)

$(OBJDIR)run.o: run.cpp $(HEADERS)
	$(MKDIR_P) $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@ $(INCLUDE)

clean:
	rm -rf run $(OBJDIR)


MKDIR_P = mkdir -p
