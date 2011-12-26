
default: dbr

dbr: roxanne_db.c
	gcc -Werror -g hash_32.c roxanne_db.c -o dbr
	chmod 755 dbr

.PHONY: clean
clean:
	rm -rf dbr.dSYM dbr

install:
	install dbr /usr/local/bin
	install dbr_ctl /usr/local/bin
