# Uncomment the following line to compile without PCM support
#INTEL_PCM_HOME=/dfs/scratch0/caberger/IntelPerformanceCounterMonitorV2.8

#
#CXX = /dfs/scratch0/noetzli/downloads/tmp/cilkplus-install/bin/g++ -Wcast-align
#override CXX = g++
 
override CXXFLAGS += -mavx -std=c++0x -pedantic -O3 -Wall -Wno-unused-function -Wextra -march=native -mtune=native

INCLUDE_DIRS=-Isrc
OBJDIR=build
EXEDIR=bin
HEADERS=$(wildcard src/*hpp)
SOURCES=$(wildcard src/*cpp)
OBJECTS=$(SOURCES:src/%.cpp=$(OBJDIR)/%.o)

ifdef TRAVIS
	CXX = g++-4.9
else 
	CXX = clang++
endif
ifdef INTEL_PCM_HOME
	INCLUDE_DIRS+=-I$(INTEL_PCM_HOME)
	EXT_OBJECTS=$(INTEL_PCM_HOME)/cpucounters.o $(INTEL_PCM_HOME)/msr.o $(INTEL_PCM_HOME)/pci.o $(INTEL_PCM_HOME)/client_bw.o
endif

APPS_SOURCES=$(shell ls apps)
TOOLS_SOURCES=$(shell ls tools)
TESTS_SOURCES=$(shell find tests -name '*.cpp' -exec basename {} \;)
SERVER_SOURCES=server.cpp
GENERATED_SOURCES=$(shell ls generated)
BENCHMARKS_SOURCES=$(shell ls benchmarks)

APPS=$(APPS_SOURCES:.cpp=)
TOOLS=$(TOOLS_SOURCES:.cpp=)
TESTS=$(TESTS_SOURCES:.cpp=)
BENCHMARKS=$(BENCHMARKS_SOURCES:.cpp=)


APPS_EXES=$(APPS:%=$(EXEDIR)/%)
TOOLS_EXES=$(TOOLS:%=$(EXEDIR)/%)
TESTS_EXES=$(TESTS:%=$(EXEDIR)/%)
BENCHMARKS_EXES=$(BENCHMARKS:%=$(EXEDIR)/%)

LIBS=-ltbb -lpthread

all: $(APPS_EXES) $(TOOLS_EXES) $(SERVER_EXES)

$(OBJDIR):
	mkdir -p $(OBJDIR)

$(EXEDIR):
	mkdir -p $(EXEDIR)

$(APPS_EXES): $(OBJECTS) $(APP_SOURCES) $(EXEDIR)
	$(CXX) $(CXXFLAGS) $(@:bin%=apps%.cpp) $(OBJECTS) $(EXT_OBJECTS) $(LIBS) -o $@ $(INCLUDE_DIRS)

$(TOOLS_EXES): $(OBJECTS) $(EXEDIR)
	$(CXX) $(CXXFLAGS) $(@:bin%=tools%.cpp) $(OBJECTS) $(EXT_OBJECTS) $(LIBS) -o $@ $(INCLUDE_DIRS)

$(BENCHMARKS_EXES): $(OBJECTS) $(EXEDIR)
	$(CXX) $(CXXFLAGS) $(@:bin%=benchmarks%.cpp) $(OBJECTS) $(EXT_OBJECTS) $(LIBS) -o $@ $(INCLUDE_DIRS)

$(GENERATED_EXES): $(OBJECTS) $(EXEDIR)

$(TESTS_EXES): $(OBJECTS) $(EXEDIR)
	$(CXX) $(CXXFLAGS) $(@:bin%=tests%.cpp) $(OBJECTS) $(EXT_OBJECTS) -lgtest $(LIBS) -o $@ $(INCLUDE_DIRS) -DGOOGLE_TEST -I./apps/

$(OBJECTS): $(SOURCES) $(HEADERS) $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(LIB_INCS) -o $@ -c $(@:build%.o=src%.cpp) $(INCLUDE_DIRS)

generated: $(OBJECTS) $(EXEDIR)
	GENERATED=${GENERATED_SOURCES:.cpp=}
	GENERATED_EXES=$(GENERATED:%=$(EXEDIR)/%)
	$(CXX) $(CXXFLAGS) $(@:bin%=generated%.cpp) $(OBJECTS) $(EXT_OBJECTS) $(LIBS) -lzmq -ldl -o $bin/@ $(INCLUDE_DIRS)

server: $(OBJECTS) $(EXEDIR)
	SERVER=$(SERVER_SOURCES:.cpp=)
	SERVER_EXES=$(SERVER:%=$(EXEDIR)/%)
	$(CXX) $(CXXFLAGS) $(@:bin%=server%.cpp) $(OBJECTS) $(EXT_OBJECTS) $(LIBS) -lzmq -ldl -o $bin/@ $(INCLUDE_DIRS)

tests: $(TESTS_EXES)
	@$(foreach bin, $(TESTS_EXES), $(bin);)

clean:
	rm -rf $(OBJDIR) $(EXEDIR)

.PHONY: all libs tests clean
