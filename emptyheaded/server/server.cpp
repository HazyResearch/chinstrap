#include <cstdlib>
#include <dlfcn.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include <unordered_map>
#include <string>

#include "zmq.hpp"

const size_t PATH_BUFFER_SIZE = 512;
const char* CPP_FILE_NAME = "runnable.cpp";
const char* OBJ_FILE_NAME = "runnable.o";
const std::string COMPILE_COMMAND = (std::string("clang++ -O3 -dynamiclib -std=c++11 -march=native -mtune=native -Isrc ") + CPP_FILE_NAME + " -o " + OBJ_FILE_NAME);

typedef void (*run_t)(std::unordered_map<std::string, void*>& relations);

void replyToClient(const char* reply_message, zmq::socket_t& socket) {
  zmq::message_t reply (strlen(reply_message) + 1);
  memcpy ((void*)reply.data(), reply_message, strlen(reply_message) + 1);
  socket.send(reply);
}

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
    std::cout << COMPILE_COMMAND << std::endl;
    int status = std::system(COMPILE_COMMAND.c_str());
    if (status != 0) {
      replyToClient("FAILURE: compilation errors", socket);
      continue;
    }

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

    // Redirect cout while running the file
    std::streambuf* oldCoutStreamBuf = std::cout.rdbuf();
    std::ostringstream strCout;
    std::cout.rdbuf( strCout.rdbuf() );

    run(relations);

    // Restore old cout
    std::cout.rdbuf( oldCoutStreamBuf );

    dlclose(handle);
    replyToClient(("SUCCESS: executed file\n" + strCout.str()).c_str(), socket);
  }
  return 0;
}
