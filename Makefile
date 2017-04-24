SHELL = /bin/sh

libdir = lib
CXXFLAGS = -std=c++14 -Wall -O2 -pipe -ggdb -fPIC -DPIC

include cxx/roofit/roofit.mk

.PHONY: all install clean
all: roofit-all
install: roofit-install
clean: roofit-clean

$(libdir):
	mkdir -p $@
