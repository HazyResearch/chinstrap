#include "Attribute.hpp"

class Relation{
private: 
  //Relation(){} // private default constructor
 std::vector<void*> *attributes;
 std::vector<type::primitives> *attribute_types;
 size_t column_size;
public:
  Relation(std::vector<void*> *r_in, std::vector<type::primitives> *a_in,size_t c_in){
    attributes = r_in;
    attribute_types = a_in;
    column_size = c_in;
  }

  //for debug purposes
  void print();

  //read a relation from a file
  static Relation from_file(const std::string path, //path to file 
    const type::file ft, //file type (csv,tsv,binary)
    std::vector<type::primitives> * attribute_types); //list of attribute types
};

//initialize attributes(column wise) of the Relation based on the type
inline void init_attributes(std::vector<void*> *attributes, 
 std::vector<type::primitives> *attribute_types,
 const size_t lSize){

  for(size_t i = 0; i < attribute_types->size(); i++){
    switch(attribute_types->at(i)){
      case type::uint64_t:
        attributes->push_back((void*)(new Attribute<uint64_t>(lSize/attribute_types->size())));
        break; 
      case type::uint32_t:
        attributes->push_back((void*)(new Attribute<uint32_t>(lSize/attribute_types->size())));
        break;
      case type::string:
        attributes->push_back((void*)(new Attribute<std::string>(lSize/attribute_types->size())));
        break;
      default:
        fputs ("ERROR: PRIMITIVE TYPE NOT COVERED",stderr); 
        exit (1);
    }
  }
}

//Read in a relation from a tsv file.
Relation from_tsv(const std::string path, std::vector<type::primitives> *attribute_types){
 const size_t num_attributes = attribute_types->size();
 std::vector<void*> *attributes_in = new std::vector<void*>();
 attributes_in->reserve(num_attributes);

  //open file for reading
  FILE *pFile = fopen(path.c_str(),"r");
  if (pFile==NULL) {fputs ("File error",stderr); exit (1);}
  
  // obtain file size:
  fseek(pFile,0,SEEK_END);
  size_t lSize = ftell(pFile);
  rewind(pFile);

  // allocate memory to contain the whole file:
  char *buffer = (char*) malloc (sizeof(char)*lSize + 1);
  if (buffer == NULL) {fputs ("Memory error",stderr); exit (2);}

  // copy the file into the buffer:
  size_t result = fread (buffer,1,lSize,pFile);
  if (result != lSize) {fputs ("Reading error",stderr); exit (3);}
  buffer[result] = '\0';

  init_attributes(attributes_in,attribute_types,lSize);

  //actually read the file, force a tsv for now
  char *test = strtok(buffer," |\t\nA");
  size_t column_size = 0;
  while(test != NULL){
    column_size++;
    for(size_t i = 0; i < num_attributes; i++){
      switch(attribute_types->at(i)){
        case type::uint64_t:
          ((Attribute<uint64_t>*)attributes_in->at(i))->add(test);
          break; 
        case type::uint32_t:
          ((Attribute<uint32_t>*)attributes_in->at(i))->add(test);
          break;
        case type::string:
          ((Attribute<std::string>*)attributes_in->at(i))->add(test);
          break;
        default:
          fputs ("ERROR: PRIMITIVE TYPE NOT COVERED",stderr); 
          exit (1);
      }
      test = strtok(NULL," |\t\nA");
    }
  }
  return Relation(attributes_in,attribute_types,column_size);
}

//Reads a relation from a file.
Relation Relation::from_file(const std::string path, 
  const type::file ft, 
  std::vector<type::primitives> *attribute_types){
  
  std::cout << "Reading file" << std::endl;
  (void) ft; //add a switch when other methods are added in for reading files.
  return from_tsv(path,attribute_types);
}

void Relation::print(){
  assert(attributes->size() == attribute_types->size());
  for(size_t r = 0; r < column_size; r++){
    for(size_t a = 0; a < attributes->size(); a++){
      switch(attribute_types->at(a)){
        case type::uint64_t:
          std::cout << ((Attribute<uint64_t>*)attributes->at(a))->column->at(r);
          break; 
        case type::uint32_t:
          std::cout << ((Attribute<uint32_t>*)attributes->at(a))->column->at(r);
          break;
        case type::string:
          std::cout << ((Attribute<std::string>*)attributes->at(a))->column->at(r);
          break;
        default:
          fputs ("ERROR: PRIMITIVE TYPE NOT COVERED",stderr); 
          exit (1);
      }
      std::cout << "\t";
    }
    std::cout << "\n";
  }
}
