CC=gcc
CFLAGS=-Wall -Wextra -pthread

all: master worker

master: master.c common.h
	$(CC) $(CFLAGS) master.c -o master

worker: worker.c common.h
	$(CC) $(CFLAGS) worker.c -o worker

clean:
	rm -f master worker