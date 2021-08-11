# run this via `FILTER_PROFILES=/path/to/here make`
# from the main git directory. That way we inherit useful variables.

all:

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

ifneq ($(findstring s,$(MAKEFLAGS)),s)
ifndef V
	QUIET_CXX      = @echo '   ' CXX $@;
	QUIET_LINK     = @echo '   ' LINK $@;
endif
endif

CXX = c++
CPPFLAGS += --std=c++17
CPPFLAGS += -I$(PROFILE_DIR)
CPPFLAGS += -O3 -fdiagnostics-color=always
CPPFLAGS += -Wmissing-prototypes -Wstrict-prototypes

ifeq ($(uname_S),Darwin)
LDFLAGS += -bundle -bundle_loader '$(PROFILE_DIR)/git'
else
LDFLAGS += -shared --just-symbols='$(PROFILE_DIR)/git'
endif

S2_PATH := $(realpath ../s2geometry/src)

CPPFLAGS += -I/usr/local/opt/sqlite3/include
CPPFLAGS += -I$(S2_PATH)/src -DABSL_MIN_LOG_LEVEL=1

LDFLAGS += -L$(S2_PATH)/build -ls2 -rpath $(S2_PATH)/build
LDFLAGS += -L/usr/local/opt/sqlite3/lib -lsqlite3

FILTER_LIB = spatial-filter.so

all: $(FILTER_LIB)
ifeq ($(MAKELEVEL),0)
	$(error "Run via parent git make")
endif
	@:

$(FILTER_LIB): spatial-filter.o
	$(QUIET_LINK)$(CXX) $^ $(ALL_LDFLAGS) $(LDFLAGS) -o $@

spatial-filter.o: spatial-filter.cpp
	$(QUIET_CXX)$(CXX) -c $(ALL_CFLAGS) $(CPPFLAGS) $<

clean:
	$(RM) $(FILTER_LIB) spatial-filter.o

install: all
	$(INSTALL) -m 644 $(FILTER_LIB) '$(DESTDIR_SQ)$(gitexec_instdir_SQ)'

.PHONY: all clean install
