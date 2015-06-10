# EmptyHeaded

Some information about emptyheaded here.

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

