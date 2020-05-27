PREFIX=/usr/local
BINDIR=${PREFIX}/bin
DESTDIR=
BLDDIR = build
BLDFLAGS=
EXT=
ifeq (${GOOS},windows)
    EXT=.exe
endif

CMDS = server client
all: $(CMDS)

$(BLDDIR)/server:     $(wildcard cmd/server/*.go  internal/*.go)
$(BLDDIR)/client:     $(wildcard cmd/client/*.go  internal/*.go)

$(BLDDIR)/%:
	@mkdir -p $(dir $@)
	go build ${BLDFLAGS} -o $@ ./cmd/$*

$(APPS): %: $(BLDDIR)/%

clean:
	rm -fr $(BLDDIR)

.PHONY: install clean all
.PHONY: $(CMDS)

install: $(CMDS)
	install -m 755 -d ${DESTDIR}${BINDIR}
	for CMD in $^ ; do install -m 755 ${BLDDIR}/$$CMD ${DESTDIR}${BINDIR}/$$CMD${EXT} ; done
