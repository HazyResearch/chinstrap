# Uncomment the following line to compile without PCM support
#INTEL_PCM_HOME=/afs/cs.stanford.edu/u/noetzli/IntelPerformanceCounterMonitorV2.7

CXX ?= g++-4.9
#CXX = /dfs/scratch0/noetzli/downloads/tmp/cilkplus-install/bin/g++ -Wcast-align
override CXX = g++
#override CXX = clang++
override CXXFLAGS += -ltbb -lpthread -mavx -std=c++0x -pedantic -O3 -Wall -Wno-unused-function -Wextra -march=native -mtune=native

INCLUDE_DIRS=-Isrc
OBJDIR=build
EXEDIR=bin
HEADERS=$(wildcard src/*hpp)
SOURCES=$(wildcard src/*cpp)
OBJECTS=$(SOURCES:src/%.cpp=$(OBJDIR)/%.o)

ifdef INTEL_PCM_HOME
	INCLUDE_DIRS+=-I$(INTEL_PCM_HOME)
	EXT_OBJECTS=$(INTEL_PCM_HOME)/cpucounters.o $(INTEL_PCM_HOME)/msr.o $(INTEL_PCM_HOME)/pci.o $(INTEL_PCM_HOME)/client_bw.o
endif

APPS_SOURCES=$(shell ls apps)
TOOLS_SOURCES=$(shell ls tools)
TESTS_SOURCES=$(shell find tests -name '*.cpp' -exec basename {} \;)
SERVER_SOURCES=server.cpp
GENERATED_SOURCES=$(shell ls generated)

APPS=$(APPS_SOURCES:.cpp=)
TOOLS=$(TOOLS_SOURCES:.cpp=)
TESTS=$(TESTS_SOURCES:.cpp=)

APPS_EXES=$(APPS:%=$(EXEDIR)/%)
TOOLS_EXES=$(TOOLS:%=$(EXEDIR)/%)
TESTS_EXES=$(TESTS:%=$(EXEDIR)/%)

all: $(APPS_EXES) $(TOOLS_EXES) $(SERVER_EXES)

$(OBJDIR):
	mkdir -p $(OBJDIR)

$(EXEDIR):
	mkdir -p $(EXEDIR)

$(APPS_EXES): $(OBJECTS) $(APP_SOURCES) $(EXEDIR)
	$(CXX) $(CXXFLAGS) $(@:bin%=apps%.cpp) $(OBJECTS) $(EXT_OBJECTS) $(LIBS) -o $@ $(INCLUDE_DIRS)

$(TOOLS_EXES): $(OBJECTS) $(EXEDIR)
	$(CXX) $(CXXFLAGS) $(@:bin%=tools%.cpp) $(OBJECTS) $(EXT_OBJECTS) $(LIBS) -o $@ $(INCLUDE_DIRS)

$(GENERATED_EXES): $(OBJECTS) $(EXEDIR)

$(TESTS_EXES): $(OBJECTS) $(EXEDIR)
	$(CXX) $(CXXFLAGS) $(@:bin%=tests%.cpp) $(OBJECTS) ./lib/gtest-1.7.0/src/gtest_main.o ./lib/gtest-1.7.0/src/gtest-all.o $(EXT_OBJECTS) $(LIBS) -o $@ $(INCLUDE_DIRS) -DGOOGLE_TEST -I./lib/gtest-1.7.0/include/ -I./lib/gtest-1.7.0/ -I./apps/

$(OBJECTS): $(SOURCES) $(HEADERS) $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(LIB_INCS) -o $@ -c $(@:build%.o=src%.cpp) $(INCLUDE_DIRS)

generated: $(OBJECTS) $(EXEDIR)
	GENERATED=${GENERATED_SOURCES:.cpp=}
	GENERATED_EXES=$(GENERATED:%=$(EXEDIR)/%)
	$(CXX) $(CXXFLAGS) $(@:bin%=generated%.cpp) $(OBJECTS) $(EXT_OBJECTS) $(LIBS) -lzmq -ldl -o $@ $(INCLUDE_DIRS)

server: $(OBJECTS) $(EXEDIR)
	SERVER=$(SERVER_SOURCES:.cpp=)
	SERVER_EXES=$(SERVER:%=$(EXEDIR)/%)
	$(CXX) $(CXXFLAGS) $(@:bin%=server%.cpp) $(OBJECTS) $(EXT_OBJECTS) $(LIBS) -lzmq -ldl -o $@ $(INCLUDE_DIRS)

gtest:
	cd ./lib/ && unzip gtest-1.7.0.zip 
	cd ./lib/gtest-1.7.0/ && ./configure
	make -C ./lib/gtest-1.7.0

tests: $(TESTS_EXES)
	@$(foreach bin, $(TESTS_EXES), $(bin);)

clean:
	rm -rf $(OBJDIR) $(EXEDIR)

