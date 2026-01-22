package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionsImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {

    private static final AtomicInteger globalMessageId = new AtomicInteger(1);

    private boolean shouldTerminate = false;
    private int connectionId;
    private Connections<String> connections;
    private boolean isConnected = false;
    private String currentUsername = null;

    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
        this.shouldTerminate = false;
        this.isConnected = false;
        this.currentUsername = null;
    }

    @Override
    public void process(String message) {
        Frame frame;
        try {
            frame = Frame.parse(message);
        } catch (Exception e) {
            // If parsing fails, we cannot safely send an error frame as we don't know the state
            shouldTerminate = true;
            return;
        }

        // Route commands
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

        // Basic validation
        if (login == null || passcode == null || acceptVersion == null || host == null) {
            sendError("malformed frame", "Missing mandatory headers in CONNECT");
            return;
        }

        // DATABASE: Verify User
        boolean success = DatabaseService.validateUser(login, passcode);
        if (!success) {
            sendError("login failed", "Wrong password or database error");
            return;
        }

        this.isConnected = true;
        this.currentUsername = login;

        // Send CONNECTED frame
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

        // INTEGRATION: We must cast to ConnectionsImpl to access 'subscribe'
        if (connections instanceof ConnectionsImpl) {
            ((ConnectionsImpl<String>) connections).subscribe(connectionId, dest, id);
        } else {
            // Fallback if the server structure changes (should not happen with provided files)
            System.err.println("Error: Connections object does not support subscriptions.");
        }

        // Send RECEIPT if requested
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

        if (connections instanceof ConnectionsImpl) {
            ((ConnectionsImpl<String>) connections).unsubscribe(connectionId, id);
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

        // FEATURE 1: Report Generation
        // Triggered if the body is exactly "report" (or via a specific header if you prefer)
        if (frame.body != null && frame.body.trim().equals("report")) {
            String report = DatabaseService.generateReport();
            
            Frame reportMsg = new Frame("MESSAGE");
            reportMsg.headers.put("subscription", "0"); // Dummy ID for direct message
            reportMsg.headers.put("destination", dest);
            reportMsg.headers.put("message-id", String.valueOf(globalMessageId.getAndIncrement()));
            reportMsg.body = report;
            
            connections.send(connectionId, reportMsg.toString());
            return;
        }

        // FEATURE 2: File Upload Tracking
        // The assignment requires logging filenames. The client must send a "file-name" header.
        String filename = frame.headers.get("file-name");
        if (filename != null) {
            DatabaseService.addFile(currentUsername, filename);
        }

        // FEATURE 3: Broadcast
        Frame msgFrame = new Frame("MESSAGE");
        msgFrame.headers.put("subscription", "0"); // In full impl, map to subscriber's actual ID
        msgFrame.headers.put("message-id", String.valueOf(globalMessageId.getAndIncrement()));
        msgFrame.headers.put("destination", dest);
        msgFrame.body = frame.body;

        connections.send(dest, msgFrame.toString());
    }

    private void handleDisconnect(Frame frame) {
        // DATABASE: Update logout time
        if (currentUsername != null) {
            DatabaseService.logoutUser(currentUsername);
        }

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
        
        // Ensure DB logout even on error
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
            // STOMP frames end with \0. Remove it for parsing.
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
            sb.append('\0'); // Important: Re-add null terminator
            return sb.toString();
        }
    }
}