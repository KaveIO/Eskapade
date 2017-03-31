rflib = esroofit
rfprefix = cxx/roofit

rfincdir = $(rfprefix)/include
rfsrcdir = $(rfprefix)/src
rfdictdir = $(rfprefix)/dict
rfbuilddir = $(rfprefix)/.build
rfsources = $(wildcard $(rfsrcdir)/*.cxx)
rfincludes = $(wildcard $(rfincdir)/*.h)
rfobjects = $(rfsources:$(rfsrcdir)/%.cxx=$(rfbuilddir)/%.o) $(rfbuilddir)/$(rflib).o

rfcxxflags = $(shell root-config --cflags) -I$(rfincdir)
rfldflags = $(shell root-config --libs) -lRooFit -lRooFitCore

vpath %.cxx $(rfsrcdir):$(rfbuilddir)
vpath %.h $(rfincdir)

.PHONY: roofit-all roofit-install roofit-clean

roofit-all: $(rfbuilddir)/lib$(rflib).so $(rfbuilddir)/$(rflib)_rdict.pcm

roofit-install: roofit-all $(libdir)
	cp $(rfbuilddir)/lib$(rflib).so $(rfbuilddir)/$(rflib)_rdict.pcm $(libdir)/

roofit-clean:
	rm -f $(rfbuilddir)/lib$(rflib).so $(rfbuilddir)/$(rflib)_rdict.pcm $(rfobjects) $(rfbuilddir)/$(rflib).cxx

$(rfbuilddir)/lib$(rflib).so: $(rfobjects)
	$(CXX) -shared $(LDFLAGS) $(rfldflags) -o $@ $^

$(rfbuilddir)/$(rflib)_rdict.pcm: $(rfbuilddir)/$(rflib).cxx

$(rfbuilddir)/%.o: %.cxx %.h | $(rfbuilddir)
	$(CXX) -c $(CXXFLAGS) $(rfcxxflags) -o $@ $<

$(rfbuilddir)/$(rflib).o: $(rfbuilddir)/$(rflib).cxx
	$(CXX) -c $(CXXFLAGS) $(rfcxxflags) -o $@ $<

$(rfbuilddir)/$(rflib).cxx: $(rfdictdir)/LinkDef.h $(rfincludes) | $(rfbuilddir)
	rootcling -f $@ -I$(abspath $(rfincdir)) $(notdir $(rfincludes)) $<

$(rfbuilddir):
	mkdir -p $@
