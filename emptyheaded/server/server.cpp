#include <cstdlib>
#include <dlfcn.h>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <unordered_map>
#include <string>

#include "zmq.hpp"

const size_t PATH_BUFFER_SIZE = 512;
const char* CPP_FILE_NAME = "runnable.cpp";
const char* OBJ_FILE_NAME = "runnable.o";
const std::string COMPILE_COMMAND = (std::string("clang++ -O3 -dynamiclib -std=c++11 -march=native -mtune=native -Isrc") + CPP_FILE_NAME + " -o " + OBJ_FILE_NAME);

typedef void (*run_t)(std::unordered_map<std::string, void*>& relations);

int main () {
  //  Prepare our context and socket
  zmq::context_t context (1);
  zmq::socket_t socket (context, ZMQ_REP);
  socket.bind ("tcp://*:5555");

  char dir_buffer[PATH_BUFFER_SIZE];
  getcwd(dir_buffer, PATH_BUFFER_SIZE);
  std::string dir(dir_buffer);

  std::unordered_map<std::string, void*> relations;

  assert(std::system(NULL));

  while (true) {
    zmq::message_t request;

    //  Wait for next request from client.
    socket.recv (&request);
    char* msg = (char*)request.data();
    std::cout << "received a message" << std::endl;

    // Write the message to a file.
    std::ofstream outfile(CPP_FILE_NAME);
    outfile << msg;
    outfile.close();

    // Compile the file.
    std::system(COMPILE_COMMAND.c_str());

    // Open and run the file.
    void* handle = dlopen((dir + "/" + OBJ_FILE_NAME).c_str(), RTLD_NOW);
    if (!handle) {
      std::cerr << dlerror() << std::endl;
      return 1;
    }

    run_t run = (run_t)dlsym(handle, "run");

    char* error = dlerror();
    if (error)  {
      std::cerr << error << std::endl;
      dlclose(handle);
      return 1;
    }

    run(relations);

    dlclose(handle);

    //  Send reply back to client
    const char* reply_message = "executed file successfully";
    zmq::message_t reply (strlen(reply_message) + 1);
    memcpy ((void*)reply.data(), reply_message, strlen(reply_message) + 1);
    socket.send(reply);
  }
  return 0;
}
