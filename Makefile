CC := gcc
OS := $(shell uname)
CFLAGS := -Werror -g
OBJDIR := obj
OBJ := roxanne_db.o tuple_bits.o hash_32.o
DEPS := roxanne_db.h
ifeq (${OS},Linux)
	LIBS := -lrt -lm
endif

default: dbr

dbr: $(OBJ) $(DEPS)
	gcc -o dbr $(OBJ) $(CFLAGS) $(LIBS)
	chmod 755 dbr

.PHONY: clean
clean:
	rm -rf dbr.dSYM dbr *.o 

install:
	install dbr /usr/local/bin
	install dbr_ctl /usr/local/bin
