package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {

    private static final AtomicInteger globalMessageId = new AtomicInteger(1);
    
    // Global Subscriptions: Topic Name -> Map<ConnectionId, SubscriptionId>
    // We need the SubId to include it in the MESSAGE frame "subscription" header.
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> topicSubscribers = new ConcurrentHashMap<>();

    private boolean shouldTerminate = false;
    private int connectionId;
    private Connections<String> connections;
    private boolean isConnected = false;
    private String currentUsername = null;
    
    // Per-Client Subscriptions: SubscriptionId -> Topic Name
    // Used to quickly find the topic when a client sends UNSUBSCRIBE id:X
    private Map<String, String> mySubscriptions = new HashMap<>();

    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
        this.shouldTerminate = false;
        this.isConnected = false;
        this.currentUsername = null;
        this.mySubscriptions.clear();
    }

    @Override
    public void process(String message) {
        Frame frame;
        try {
            frame = Frame.parse(message);
        } catch (Exception e) {
            shouldTerminate = true;
            return;
        }

        switch (frame.command) {
            case "CONNECT":
            case "STOMP":
                handleConnect(frame);
                break;
            case "SUBSCRIBE":
                if (validateSession()) handleSubscribe(frame);
                break;
            case "UNSUBSCRIBE":
                if (validateSession()) handleUnsubscribe(frame);
                break;
            case "SEND":
                if (validateSession()) handleSend(frame);
                break;
            case "DISCONNECT":
                if (validateSession()) handleDisconnect(frame);
                break;
            default:
                sendError("malformed frame", "Unknown command: " + frame.command);
        }
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    // --- Command Handlers ---

    private void handleConnect(Frame frame) {
        if (isConnected) {
            sendError("concurrency error", "Client is already logged in");
            return;
        }

        String login = frame.headers.get("login");
        String passcode = frame.headers.get("passcode");
        String acceptVersion = frame.headers.get("accept-version");
        String host = frame.headers.get("host");

        if (login == null || passcode == null || acceptVersion == null || host == null) {
            sendError("malformed frame", "Missing mandatory headers in CONNECT");
            return;
        }

        boolean success = DatabaseService.validateUser(login, passcode);
        if (!success) {
            sendError("login failed", "Wrong password or database error");
            return;
        }

        this.isConnected = true;
        this.currentUsername = login;

        Frame connectedFrame = new Frame("CONNECTED");
        connectedFrame.headers.put("version", "1.2");
        connections.send(connectionId, connectedFrame.toString());
    }

    private void handleSubscribe(Frame frame) {
        String dest = frame.headers.get("destination");
        String id = frame.headers.get("id");

        if (dest == null || id == null) {
            sendError("malformed frame", "Missing destination or id header");
            return;
        }

        // 1. Add to global topic map
        topicSubscribers.computeIfAbsent(dest, k -> new ConcurrentHashMap<>())
                        .put(connectionId, id);
        
        // 2. Add to local map (for Unsubscribe)
        mySubscriptions.put(id, dest);

        String receiptId = frame.headers.get("receipt");
        if (receiptId != null) {
            Frame receipt = new Frame("RECEIPT");
            receipt.headers.put("receipt-id", receiptId);
            connections.send(connectionId, receipt.toString());
        }
    }

    private void handleUnsubscribe(Frame frame) {
        String id = frame.headers.get("id");
        if (id == null) {
            sendError("malformed frame", "Missing id header");
            return;
        }

        // Find which topic this subscription ID belongs to
        String topic = mySubscriptions.remove(id);
        if (topic != null) {
            // Remove from global map
            if (topicSubscribers.containsKey(topic)) {
                topicSubscribers.get(topic).remove(connectionId);
            }
        }

        String receiptId = frame.headers.get("receipt");
        if (receiptId != null) {
            Frame receipt = new Frame("RECEIPT");
            receipt.headers.put("receipt-id", receiptId);
            connections.send(connectionId, receipt.toString());
        }
    }

    private void handleSend(Frame frame) {
        String dest = frame.headers.get("destination");
        if (dest == null) {
            sendError("malformed frame", "Missing destination");
            return;
        }

        // FEATURE: Report Generation
        if (frame.body != null && frame.body.trim().equals("report")) {
            String report = DatabaseService.generateReport();
            
            Frame reportMsg = new Frame("MESSAGE");
            reportMsg.headers.put("subscription", "0"); // Direct message
            reportMsg.headers.put("destination", dest);
            reportMsg.headers.put("message-id", String.valueOf(globalMessageId.getAndIncrement()));
            reportMsg.body = report;
            
            connections.send(connectionId, reportMsg.toString());
            return;
        }

        // FEATURE: File Upload Tracking
        String filename = frame.headers.get("file-name");
        if (filename != null) {
            DatabaseService.addFile(currentUsername, filename);
        }

        // BROADCAST LOGIC (FIXED)
        ConcurrentHashMap<Integer, String> subscribers = topicSubscribers.get(dest);
        if (subscribers != null) {
            for (Map.Entry<Integer, String> entry : subscribers.entrySet()) {
                Integer targetConnId = entry.getKey();
                String subId = entry.getValue();

                Frame msgFrame = new Frame("MESSAGE");
                msgFrame.headers.put("subscription", subId); // Must match recipient's sub ID
                msgFrame.headers.put("message-id", String.valueOf(globalMessageId.getAndIncrement()));
                msgFrame.headers.put("destination", dest);
                msgFrame.body = frame.body;

                connections.send(targetConnId, msgFrame.toString());
            }
        }
    }

    private void handleDisconnect(Frame frame) {
        if (currentUsername != null) {
            DatabaseService.logoutUser(currentUsername);
        }

        // Remove all subscriptions for this user
        for (String topic : mySubscriptions.values()) {
            if (topicSubscribers.containsKey(topic)) {
                topicSubscribers.get(topic).remove(connectionId);
            }
        }
        mySubscriptions.clear();

        String receiptId = frame.headers.get("receipt");
        if (receiptId != null) {
            Frame receipt = new Frame("RECEIPT");
            receipt.headers.put("receipt-id", receiptId);
            connections.send(connectionId, receipt.toString());
        }

        this.isConnected = false;
        this.shouldTerminate = true;
        connections.disconnect(connectionId);
    }

    // --- Helpers ---

    private boolean validateSession() {
        if (!isConnected) {
            sendError("access denied", "Client is not logged in");
            return false;
        }
        return true;
    }

    private void sendError(String message, String description) {
        Frame error = new Frame("ERROR");
        error.headers.put("message", message);
        error.body = "The message:\n-----\n" + description + "\n-----";
        connections.send(connectionId, error.toString());
        
        if (currentUsername != null) {
            DatabaseService.logoutUser(currentUsername);
        }
        shouldTerminate = true;
        connections.disconnect(connectionId);
    }

    // Lightweight Frame Parser
    private static class Frame {
        String command;
        Map<String, String> headers = new HashMap<>();
        String body;

        Frame(String command) { this.command = command; }

        static Frame parse(String msg) {
            int nullIdx = msg.indexOf('\0');
            String effectiveMsg = (nullIdx >= 0) ? msg.substring(0, nullIdx) : msg;
            
            String[] parts = effectiveMsg.split("\n\n", 2);
            String headerPart = parts[0];
            String bodyPart = (parts.length > 1) ? parts[1] : "";

            String[] lines = headerPart.split("\n");
            if (lines.length == 0) throw new IllegalArgumentException("Empty frame");

            Frame f = new Frame(lines[0].trim());
            for (int i = 1; i < lines.length; i++) {
                String line = lines[i];
                int colon = line.indexOf(':');
                if (colon > 0) {
                    f.headers.put(line.substring(0, colon).trim(), line.substring(colon + 1).trim());
                }
            }
            f.body = bodyPart;
            return f;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(command).append('\n');
            for (Map.Entry<String, String> h : headers.entrySet()) {
                sb.append(h.getKey()).append(':').append(h.getValue()).append('\n');
            }
            sb.append('\n');
            if (body != null) sb.append(body);
            sb.append('\0'); // Important: The protocol adds the terminator
            return sb.toString();
        }
    }
}