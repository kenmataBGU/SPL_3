#pragma once
#include <string>
#include <map>
#include <vector>
#include <mutex> 
#include <atomic>
#include "../include/ConnectionHandler.h"
#include "event.h"

class StompProtocol
{
    private:
        std::map<std::string, std::map<std::string, std::vector<Event>>> gameReports; 
        std::string userName;
        std::atomic<bool>& shouldContinue;
        int subscriptionCounter;
        int receiptCounter;

        std::map<std::string, int> channelToSubId;
        std::map<int, std::string> receiptToCommand;
        
        // Added for thread safety between the Socket Thread and Input Thread
        std::mutex reportsMutex; 

    public:
        StompProtocol(std::atomic<bool>& loggedIn);
        std::vector<std::string> processInput(std::string line);
        void processServerFrame(std::string frame);
};