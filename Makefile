OBJS = DataQueryStoring.o PredicatesExecution.o QueryManagement.o RadixHashJoin.o JobScheduler.o cardinality.o QueryOptimization.o
HEADER_1 = RadixHashJoin.h
HEADER_2 = Intermediate.h
OUT = QueryManagement
CC = gcc
FLAGS = -c

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -o $@ -lpthread -lm

DataQueryStoring.o: DataQueryStoring.c
	$(CC) $(FLAGS) DataQueryStoring.c

PredicatesExecution.o: PredicatesExecution.c
	$(CC) $(FLAGS) PredicatesExecution.c

QueryManagement.o: QueryManagement.c
	$(CC) $(FLAGS) QueryManagement.c

RadixHashJoin.o: RadixHashJoin.c
	$(CC) $(FLAGS) RadixHashJoin.c -lpthread

JobScheduler.o: JobScheduler.c
	$(CC) $(FLAGS) JobScheduler.c

cardinality.o: cardinality.c
	$(CC) $(FLAGS) cardinality.c -lm

QueryOptimization.o: QueryOptimization.c
	$(CC) $(FLAGS) QueryOptimization.c

clean:
	rm -f $(OBJS) $(OUT)
