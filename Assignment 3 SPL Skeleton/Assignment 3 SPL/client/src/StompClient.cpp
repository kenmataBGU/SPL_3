#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <thread>
#include <atomic>
#include <cstdlib>
#include "../include/ConnectionHandler.h"
#include "../include/StompProtocol.h"

int main(int argc, char *argv[]) {
    ConnectionHandler* handler = nullptr;
    std::atomic<bool> loggedIn(false);
    
    while (true) {
        StompProtocol protocol(loggedIn); 
        // Initial Login Phase
        while (!loggedIn) {
            std::string line;
            if (!std::getline(std::cin, line)) return 0;
            std::stringstream ss(line);
            std::string command;
            ss >> command;
            
            if (command == "login") {
                std::string hostPort, username, password;
                ss >> hostPort >> username >> password;
                
                size_t colonPos = hostPort.find(':');
                std::string host = hostPort.substr(0, colonPos);
                short port = std::stoi(hostPort.substr(colonPos + 1));
                
                handler = new ConnectionHandler(host, port);
                if (!handler->connect()) {
                    std::cout << "Could not connect to server" << std::endl;
                    delete handler;
                    handler = nullptr;
                    continue;
                }

                std::string connectFrame = "CONNECT\naccept-version:1.2\nhost:stomp.cs.bgu.ac.il\nlogin:" + username + "\npasscode:" + password + "\n\n";
                handler->sendFrameAscii(connectFrame, '\0');
                
                std::string response;
                handler->getFrameAscii(response, '\0');
                protocol.processServerFrame(response);
                
                if (response.find("CONNECTED") != std::string::npos) {
                    loggedIn = true;
                    protocol.setUserName(username);
                    // Seed protocol with login data
                    protocol.processInput(line);
                } else {
                    delete handler;
                    handler = nullptr;
                }
            }
        }

        // Active Connection Phase
        std::thread socketThread([&handler, &protocol, &loggedIn]() {
            while (loggedIn) {
                std::string frame;
                if (!handler->getFrameAscii(frame, '\0')) {
                    loggedIn = false;
                    break;
                }
                protocol.processServerFrame(frame); 
            }
            
            // If the loop ends, it means we are logged out or disconnected.
            // Enabling re-login when pressing enter
            std::cout << "Exiting..." << std::endl;
        });

        while (loggedIn) {
            std::string line;
            if (!std::getline(std::cin, line)) break;

            std::stringstream ss(line);
            std::string cmd;
            ss >> cmd;

            // FIX: Requirement for duplicate login check
            if (cmd == "login") {
                std::cout << "The client is already logged in, log out before trying again" << std::endl;
                continue;
            }

            std::vector<std::string> framesToSend = protocol.processInput(line);
            for (const std::string& frame : framesToSend) {
                if (!handler->sendFrameAscii(frame, '\0')) {
                    loggedIn = false;
                    break;
                }
            }
        }

        if (socketThread.joinable()) socketThread.join();
        if (handler) {
            delete handler;
            handler = nullptr;
        }
    }
    return 0;
}