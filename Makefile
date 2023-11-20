CC=mpic++
CFLAGS=-std=c++14 -O3 -march=native -fopenmp -lpthread
OBJDIR=objs/
OBJS=$(OBJDIR)intersection.o 
HEADERS=canonical.h comper.h concurrent/conmap.h concurrent/conque.h concurrent/constack.h decompose.h global.h gmatch.h grami.h \
graph.h leapfrogjoin.h pretty_print.h rwlock.h setting.h systemI.h task.h taskprogmap.h types.h worker.h \
mpi/serialization.h mpi/mpi_global.h mpi/communication.h
INCLUDE=-Iconcurrent -Impi
	
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
