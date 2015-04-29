#include "emptyheaded.hpp"
#include <getopt.h>

template<class T> application<T>* init_app();

typedef hybrid mylayout;

inline void printUsage() {
  std::cout << "USAGE: ./application <OPTIONS>" << std::endl;
  std::cout << "OPTIONS: " << std::endl;
  std::cout <<"\tREQUIRED: --graph=<path to graph>  --t=<NUM THREADS>" << std::endl;
  std::cout << "\t\t--layout=<uint,bitsets,hybrid>" << std::endl;
  exit (0);
}

std::pair<std::string,std::string> parse(int argc, char* argv[]) {
  //return path for file
  //get the number of threads
  char *path = NULL;
  int local_threads = -1;
  char *layout = NULL;
  bool help = false;

  if(argc < 3)
    printUsage();

  int c;
  while (1){
    static struct option long_options[] = {
        /* These options don’t set a flag.
           We distinguish them by their indices. */
        {"help",required_argument,0,'h'},
        {"t",required_argument,0,'t'},
        {"graph",required_argument,0,'g'},
        {"layout",required_argument,0,'l'},
        {0, 0, 0, 0}
      };
    /* getopt_long stores the option index here. */
    int option_index = 0;

    c = getopt_long (argc, argv, "t:g:h:l",long_options, &option_index);

    /* Detect the end of the options. */
    if (c == -1)
      break;

    switch (c) {
      case 0:
        /* If this option set a flag, do nothing else now. */
        if (long_options[option_index].flag != 0)
          break;
        printf ("option %s", long_options[option_index].name);
        if (optarg)
          printf (" with arg %s", optarg);
        printf ("\n");
        break;
      case 'h':
        help = true;
        break;
      case 't':
        local_threads = atoi(optarg);
        break;
      case 'g':
        path = optarg;
        break;
      case 'l':
        layout = optarg;
        break;
      case '?':
        /* getopt_long already printed an error message. */
        break;

      default:
        printUsage();
      }
  }

  if(local_threads != -1){
    NUM_THREADS = local_threads;
  }

  std::string l(layout);
  if(path == NULL || help){
    printUsage();
  }
  std::string p(path);

  return std::make_pair(l,p);
}

#ifndef TEST
int main (int argc, char* argv[]) {
  std::tuple<std::string,std::string> tup_in = parse(argc,argv);
  std::string l = std::get<0>(tup_in);
  std::string p = std::get<1>(tup_in);

  if(l == "uinteger"){
    std::cout << "LAYOUT: uinteger, # THREADS: " << NUM_THREADS << ", PATH: " << p << std::endl;
    application<uinteger>* myapp = init_app<uinteger>();
    myapp->run(p);
  } else if(l == "bitset"){
    std::cout << "LAYOUT: bitset, # THREADS: " << NUM_THREADS << ", PATH: " << p << std::endl;
    application<bitset>* myapp = init_app<bitset>();
    myapp->run(p);
  } else if(l == "hybrid"){
    std::cout << "LAYOUT: hybrid, # THREADS: " << NUM_THREADS << ", PATH: " << p  << std::endl;
    application<hybrid>* myapp = init_app<hybrid>();
    myapp->run(p);
  } else{
    abort ();
  }
  return 0;
}
#endif