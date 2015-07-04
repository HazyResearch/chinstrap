#There is probably a better way to do this but this is how we go for now
set -e
cd duncecap
#sbt update
sbt start-script
target/start DunceCap.QueryCompiler $1
cd ..
