#include <cstdlib>
#include <dlfcn.h>
#include <iostream>
#include <unistd.h>
#include <unordered_map>
#include <string>

#include "zmq.hpp"

int main () {
  //  Prepare our context and socket
  zmq::context_t context (1);
  zmq::socket_t socket (context, ZMQ_REP);
  socket.bind ("tcp://*:5555");

  std::unordered_map<std::string, void*> relations;

  assert(std::system(NULL));

  std::system("clang++ -dynamiclib run.cpp -o run.o");

  void* handle = dlopen("/Users/adamperelman/Code/chinstrap/emptyheaded/run.o", RTLD_NOW);
  if (!handle) {
    std::cerr << dlerror() << std::endl;
    return 1;
  }



  typedef void (*run_t)(std::unordered_map<std::string, void*>& relations);

  run_t run = (run_t)dlsym(handle, "run");

  char* error = dlerror();
  if (error)  {
    std::cerr << error << std::endl;
    return 1;
  }

  run(relations);

  dlclose(handle);

  while (true) {
    zmq::message_t request;

    //  Wait for next request from client
    socket.recv (&request);
    char* msg = (char*)request.data();

    std::cout << "Received " << msg << std::endl;

    //  Do some 'work'
	 sleep(1);

    //  Send reply back to client
    zmq::message_t reply (5);
    memcpy ((void *) reply.data (), "World", 5);
    socket.send (reply);
  }
  return 0;
}
