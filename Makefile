CC=gcc
CFLAGS=-O3 -g -pedantic -std=gnu17 -Wall -Werror -Wextra
LDFLAGS=-pthread

.PHONY: all
all: nyuenc

nyuenc: nyuenc.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

.PHONY: clean
clean:
	rm -f *.o nyuenc
