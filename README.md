# Dependencies

+Clang 3.6 or GCC 4.9.2

+Java 1.8

+SBT 0.13.8

+Intel TBB

It may work with different versions of these but this is what the system is currently tested on.

Need GCC?

sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test  
sudo apt-get update
sudo apt-get install g++-4.9

Need Java 1.8?

https://www.digitalocean.com/community/tutorials/how-to-install-java-on-ubuntu-with-apt-get

Need SBT?

http://www.scala-sbt.org/download.html

Need TBB?

sudo apt-get install libtbb-dev

For more information....

https://www.threadingbuildingblocks.org/

# DunceCap

### Repl

Start repl: `sbt run` (note that to quit, you need to :quit out of the repl and then ctrl-c out of sbt) from the duncecap directory.

The generated file will appear in emptyheaded/generated.

Run tests: `sbt test`

You can also use :load as in the standard scala repl to load in multiple lines to be interpreted. There are a few sample scripts under duncecap/scripts/.   

### Compile to a single file

`sbt "run datalog_filepath runnable.cpp"`

### Server

Compile using makefile in emptyheaded. Run `./bin/server` to start the server.

