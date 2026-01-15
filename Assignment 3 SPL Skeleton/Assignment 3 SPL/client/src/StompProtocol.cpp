#include "../include/StompProtocol.h"
#include <sstream>
#include <iostream>
#include "../include/event.h"
#include <fstream>
#include <algorithm>

StompProtocol::StompProtocol(std::atomic<bool>& loggedIn) : 
    gameReports(), userName(""), shouldContinue(loggedIn), 
    subscriptionCounter(0), receiptCounter(0), channelToSubId(), receiptToCommand() {}

std::vector<std::string> StompProtocol::processInput(std::string line) {
    std::stringstream ss(line);
    std::string command;
    ss >> command;
    std::vector<std::string> frames;

    if (command == "login") {
        std::string hostPort, password;
        ss >> hostPort >> userName >> password;
        // Connection logic is handled in StompClient.cpp
        return frames;
    }

    if (command == "join") {
        std::string gameName;
        ss >> gameName;
        int subId = subscriptionCounter++;
        int recId = receiptCounter++;
        std::lock_guard<std::mutex> lock(reportsMutex);
        channelToSubId[gameName] = subId;
        receiptToCommand[recId] = "Joined channel " + gameName;

        frames.push_back("SUBSCRIBE\ndestination:/" + gameName + "\nid:" + std::to_string(subId) + "\nreceipt:" + std::to_string(recId) + "\n\n\0");
        return frames;
    }

    if (command == "exit") {
        std::string gameName;
        ss >> gameName;
        if (channelToSubId.find(gameName) != channelToSubId.end()) {
            int subId = channelToSubId[gameName];
            int recId = receiptCounter++;
            {
                std::lock_guard<std::mutex> lock(reportsMutex);
                receiptToCommand[recId] = "Exited channel " + gameName;
            }
            frames.push_back("UNSUBSCRIBE\nid:" + std::to_string(subId) + "\nreceipt:" + std::to_string(recId) + "\n\n\0");
            channelToSubId.erase(gameName);
        }
        return frames;
    }

    if (command == "logout") {
        int recId = receiptCounter++;
        {
            std::lock_guard<std::mutex> lock(reportsMutex);
            receiptToCommand[recId] = "DISCONNECT";
        }
        frames.push_back("DISCONNECT\nreceipt:" + std::to_string(recId) + "\n\n\0");
        return frames;
    }

    if (command == "report") {
        std::string filePath;
        ss >> filePath;
        names_and_events n_e = parseEventsFile(filePath);
        std::string gameName = n_e.team_a_name + "_" + n_e.team_b_name;
        
        for (const Event& event : n_e.events) {
            {
                std::lock_guard<std::mutex> lock(reportsMutex);
                gameReports[gameName][userName].push_back(event);

            }
            std::string frame = "SEND\n";
            frame += "destination:/" + n_e.team_a_name + "_" + n_e.team_b_name + "\n\n";  
            frame += "user: " + userName + "\n";  
            frame += "team a: " + event.get_team_a_name() + "\n";
            frame += "team b: " + event.get_team_b_name() + "\n";
            frame += "event name: " + event.get_name() + "\n";  
            frame += "time: " + std::to_string(event.get_time()) + "\n";  
            frame += "general game updates:\n";
            for (auto const& [key, val] : event.get_game_updates()) {
                frame += "\t" + key + ": " + val + "\n";  
            }
            frame += "team a updates:\n";
            for (auto const& [key, val] : event.get_team_a_updates()) {
                frame += "\t" + key + ": " + val + "\n";  
            }
            frame += "team b updates:\n";
            for (auto const& [key, val] : event.get_team_b_updates()) {
                frame += "\t" + key + ": " + val + "\n";  
            }
            frame += "description:\n" + event.get_discription() + "\n";
            
            frames.push_back(frame);
        }
        return frames;
    }

    if (command == "summary") {
        std::string gameName, user, file;
        ss >> gameName >> user >> file;
        
        std::lock_guard<std::mutex> lock(reportsMutex);
        if (gameReports.find(gameName) != gameReports.end() && 
            gameReports[gameName].find(user) != gameReports[gameName].end()) {
            
            std::vector<Event> events = gameReports[gameName][user];
            // Sort events by time
            std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
                return a.get_time() < b.get_time();
            });

            // Write to file as required by assignment
            std::ofstream outFile(file);
            
            // Header
            if (!events.empty()) {
                outFile << events[0].get_team_a_name() << " vs " << events[0].get_team_b_name() << "\n";
            }
            outFile << "Game stats:\n";

            // 1. Aggregate Statistics
            std::map<std::string, std::string> general_stats;
            std::map<std::string, std::string> team_a_stats;
            std::map<std::string, std::string> team_b_stats;

            for (const auto& event : events) {
                for (auto const& [key, val] : event.get_game_updates()) {
                    general_stats[key] = val;
                }
                for (auto const& [key, val] : event.get_team_a_updates()) {
                    team_a_stats[key] = val;
                }
                for (auto const& [key, val] : event.get_team_b_updates()) {
                    team_b_stats[key] = val;
                }
            }

            // 2. Print General Stats
            outFile << "General stats:\n";
            for (auto const& [key, val] : general_stats) {
                outFile << key << ": " << val << "\n";
            }

            // 3. Print Team A Stats (SAFE VERSION)
            if (!events.empty()) {
                outFile << events[0].get_team_a_name() << " stats:\n";
            } else {
                outFile << "Team A stats:\n"; // Fallback if no events
            }
            for (auto const& [key, val] : team_a_stats) {
                outFile << key << ": " << val << "\n";
            }

            // 4. Print Team B Stats (SAFE VERSION)
            if (!events.empty()) {
                outFile << events[0].get_team_b_name() << " stats:\n";
            } else {
                 outFile << "Team B stats:\n"; // Fallback if no events
            }
            for (auto const& [key, val] : team_b_stats) {
                outFile << key << ": " << val << "\n";
            }
            
            outFile << "Game event reports:\n";
            for (const auto& event : events) {
                 outFile << event.get_time() << " - " << event.get_name() << ":\n\n";
                 outFile << event.get_discription() << "\n\n";
            }
            outFile.close();
        }
        return frames;
    }

    return frames;
}

void StompProtocol::processServerFrame(std::string frame) {
    std::stringstream ss(frame);
    std::string command;
    std::string line;

    // 1. Get the Command (First Line)
    // std::getline consumes the \n, so 'command' is clean (e.g., "MESSAGE")
    if (!std::getline(ss, command)) {
        return; 
    }

    // 2. Parse Headers into a Map
    std::map<std::string, std::string> headers;
    // Loop terminates at the empty line between headers and body
    while (std::getline(ss, line) && !line.empty() && line != "\r") {
        size_t colonPos = line.find(':');
        if (colonPos != std::string::npos) {
            std::string key = line.substr(0, colonPos);
            std::string value = line.substr(colonPos + 1);
            headers[key] = value;
        }
    }

    // 3. Process Logic based on Command
    if (command == "CONNECTED") {
        std::cout << "Login successful" << std::endl;
    } 
    else if (command == "ERROR") {
        std::cout << "Error received: " << headers["message"] << std::endl;
        std::string body((std::istreambuf_iterator<char>(ss)), std::istreambuf_iterator<char>());
        std::cout << body << std::endl;
        shouldContinue = false; 
    } 
    else if (command == "RECEIPT") {
        std::string receiptId = headers["receipt-id"];
        std::lock_guard<std::mutex> lock(reportsMutex);
        if (receiptToCommand.count(std::stoi(receiptId))) {
            std::string originalCmd = receiptToCommand[std::stoi(receiptId)];
            if (originalCmd.find("DISCONNECT") != std::string::npos) {
                shouldContinue = false; 
            } else {
                std::cout << originalCmd << std::endl; 
            }
        }
    } 
    else if (command == "MESSAGE") {
        std::string gameName = headers["destination"];
        // Logic to strip "/topic/" prefix if it exists
        if (gameName.find("/") != std::string::npos) {
            gameName = gameName.substr(gameName.rfind("/") + 1);
        }

        std::string reportingUser = headers["user"];

        // We now correctly check the 'user' HEADER against our local userName
        if (reportingUser == userName) {
            return; 
        }

        // Parse Body (The Event JSON)
        std::string content((std::istreambuf_iterator<char>(ss)), std::istreambuf_iterator<char>());

        try {
            Event event(content);
            std::lock_guard<std::mutex> lock(reportsMutex);
            gameReports[gameName][reportingUser].push_back(event);
        } catch (...) {
            // Ignore malformed events
        }
    }
}