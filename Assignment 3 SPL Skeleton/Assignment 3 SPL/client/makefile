CFLAGS:=-c -Wall -Weffc++ -g -std=c++11 -Iinclude
LDFLAGS:=-lboost_system -lpthread

# All targets to build
all: bin/StompWCIClient

# The final executable depends on all object files
bin/StompWCIClient: bin/ConnectionHandler.o bin/StompClient.o bin/StompProtocol.o bin/event.o
	g++ -o bin/StompWCIClient bin/ConnectionHandler.o bin/StompClient.o bin/StompProtocol.o bin/event.o $(LDFLAGS)

# Rule for ConnectionHandler
bin/ConnectionHandler.o: src/ConnectionHandler.cpp
	g++ $(CFLAGS) -o bin/ConnectionHandler.o src/ConnectionHandler.cpp

# NEW RULE: Rule for StompProtocol
bin/StompProtocol.o: src/StompProtocol.cpp include/StompProtocol.h
	g++ $(CFLAGS) -o bin/StompProtocol.o src/StompProtocol.cpp

# Rule for StompClient
bin/StompClient.o: src/StompClient.cpp
	g++ $(CFLAGS) -o bin/StompClient.o src/StompClient.cpp

# Rule for event
bin/event.o: src/event.cpp
	g++ $(CFLAGS) -o bin/event.o src/event.cpp

# Clean the bin directory
clean:
	rm -f bin/*