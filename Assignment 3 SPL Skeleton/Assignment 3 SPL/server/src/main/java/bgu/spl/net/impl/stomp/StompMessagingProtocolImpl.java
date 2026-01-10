package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {

    // ===== Fields =====
    private boolean shouldTerminate;
    private int connectionId;
    private Connections<String> connections;
    private boolean isConnected;

    // destination -> (connectionId -> subscriptionId)
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> destConSub =
            new ConcurrentHashMap<>();
    // connectionId -> (subscriptionId -> destination)
    private static final ConcurrentHashMap<Integer, ConcurrentHashMap<String, String>> conSubDest =
            new ConcurrentHashMap<>();
    // generating unique message-ids
    private static final AtomicInteger messageIdCounter = new AtomicInteger(0);


    // ===== Constructor =====
    public StompMessagingProtocolImpl() {
        this.shouldTerminate = false;
        this.connectionId = -1;
        this.connections = null;
        this.isConnected = false;
    }

    // ===== Methods =====
    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
    }

    @Override
    public void process(String frame) {
        if (frame == null) {
            sendErrorAndClose(null, "", "Empty frame received.");
            return;
        }

        // extract command and headers
        String command = extractCommand(frame);
        Map<String, String> headers = parseHeaders(frame);
        String receipt = headers.get("receipt"); 

        // must be connected for all commands except CONNECT
        if (!"CONNECT".equals(command) && !isConnected) {
            sendErrorAndClose(receipt, frame, "Client is not connected (CONNECT required).");
            return;
        }

        switch (command) {

            case "CONNECT": {
                if (isConnected) {
                    sendErrorAndClose(receipt, frame, "A second CONNECT is not allowed.");
                    return;
                }
                
                isConnected = true;
                connections.send(connectionId, "CONNECTED\nversion:1.2\n\n\0");
                sendReceipt(receipt);
                return;
            }

            case "DISCONNECT": {               
                sendReceipt(receipt);
                shouldTerminate = true;
                cleanupSubscriptions();
                connections.disconnect(connectionId);
                return;
            }

            case "SUBSCRIBE": {
                // extract and validate headers
                String destination = headers.get("destination");
                String id = headers.get("id");
                
                if (isNullOrEmpty(destination) || isNullOrEmpty(id)) {
                    sendErrorAndClose(receipt, frame, "SUBSCRIBE must contain destination and id headers.");
                    return;
                }

                // Register to conSubDest if not already present and extract subscriptions
                conSubDest.putIfAbsent(connectionId, new ConcurrentHashMap<>());
                ConcurrentHashMap<String, String> subDest = conSubDest.get(connectionId);

                // Check if subscription id is already used by this client
                String prevDest = subDest.putIfAbsent(id, destination);
                if (prevDest != null) {
                    sendErrorAndClose(receipt, frame, "Subscription id already in use for this client.");
                    return;
                }

                // Register to destConSub if not already present and add the subscription
                destConSub.putIfAbsent(destination, new ConcurrentHashMap<>());
                destConSub.get(destination).put(connectionId, id);

                sendReceipt(receipt);
                return;
            }

            case "UNSUBSCRIBE": {
                // extract and validate id header
                String id = headers.get("id");
                
                if (isNullOrEmpty(id)) {
                    sendErrorAndClose(receipt, frame, "UNSUBSCRIBE must contain an id header.");
                    return;
                }

                // extract subscriptions for this connection
                ConcurrentHashMap<String, String> subDest = conSubDest.get(connectionId);
                if (subDest == null) {
                    sendErrorAndClose(receipt, frame, "No subscriptions exist for this client.");
                    return;
                }

                // remove the subscription and validate
                String destination = subDest.remove(id);
                if (destination == null) {
                    sendErrorAndClose(receipt, frame, "No such subscription id for this client.");
                    return;
                }

                // remove from destConSub
                ConcurrentHashMap<Integer, String> destMap = destConSub.get(destination);
                if (destMap != null) {
                    destMap.remove(connectionId);
                    if (destMap.isEmpty()) 
                        destConSub.remove(destination, destMap);
                }

                // clean up conSubDest if empty
                if (subDest.isEmpty()) {
                    conSubDest.remove(connectionId, subDest);
                }

                sendReceipt(receipt);
                return;
            }

            case "SEND": {
                // extract and validate destination header
                String destination = headers.get("destination");
                if (isNullOrEmpty(destination)) {
                    sendErrorAndClose(receipt, frame,
                            "SEND must contain a destination header.");
                    return;
                }

                String body = extractBody(frame);
                int msgId = messageIdCounter.incrementAndGet();

                // send MESSAGE frames to all subscribers
                ConcurrentHashMap<Integer, String> destMap = destConSub.get(destination);
                if (destMap != null) {
                    for (Map.Entry<Integer, String> conSub : destMap.entrySet()) {
                        int con = conSub.getKey();
                        String sub = conSub.getValue();

                        String msgFrame = buildMessageFrame(destination, sub, msgId, body);
                        connections.send(con, msgFrame);
                    }
                }

                sendReceipt(receipt);
                return;
            }

            default:
                sendErrorAndClose(receipt, frame, "Unknown command.");
        }
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    // ===== Helpers =====

    /**
     * Builds an ERROR frame.
     */
    private String buildError(String messageHeader, String receiptId, String originalMessage, String reason) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("ERROR\n");

        if (receiptId != null && !receiptId.isEmpty()) {
            sb.append("receipt-id: ").append(receiptId).append("\n");
        }

        sb.append("message: ").append(messageHeader).append("\n").append("\n");

        sb.append("The message:\n").append("----\n");

        sb.append(originalMessage == null ? "" : originalMessage).append("\n").append("----\n");
        
        sb.append(reason == null ? "" : reason).append("\n").append("\0");
        
        return sb.toString();
    }
    
    /**
     * Sends an ERROR frame to the client and closes the connection.
     */
    private void sendErrorAndClose(String receipt, String originalFrame, String reason) {
        connections.send(connectionId, buildError("malformed frame received", receipt, originalFrame, reason));
        shouldTerminate = true;
        cleanupSubscriptions();
        connections.disconnect(connectionId);
    }

    /**
     * Cleans up all subscriptions for this connection.
     */
    private void cleanupSubscriptions() {
        ConcurrentHashMap<String, String> subDest = conSubDest.remove(connectionId);

        if (subDest == null) 
            return;

        for (String dest : subDest.values()) {
            ConcurrentHashMap<Integer, String> destMap = destConSub.get(dest);
            if (destMap != null) {
                destMap.remove(connectionId);
                if (destMap.isEmpty()) 
                    destConSub.remove(dest, destMap);
            }
        }
    }

    /**
     * Fixes a string by removing trailing carriage return if present.
     */
    private String fixString(String s) {
        if (s == null) 
            return null;
        
        if (!s.isEmpty() && s.charAt(s.length() - 1) == '\r') 
            return s.substring(0, s.length() - 1);
        
        return s;
    }

    /**
     * Extracts the command from a STOMP frame.
     */
    private String extractCommand(String frame) {
        int nl = frame.indexOf('\n');
        
        String command = (nl == -1) 
            ? frame 
            : frame.substring(0, nl);

        return fixString(command).trim();
    }

    /**
     * Parses headers from a STOMP frame into a map.
     */
    private Map<String, String> parseHeaders(String frame) {
        Map<String, String> headers = new HashMap<>();
        String[] lines = frame.split("\n");

        for (int i = 1; i < lines.length; i++) { // skip command
            String line = lines[i];

            if (line.isEmpty() || line.equals("\r")) break;
            line = fixString(line);

            int colon = line.indexOf(':');
            if (colon <= 0) continue; // skip malformed

            String key = line.substring(0, colon).trim();
            String value = line.substring(colon + 1).trim();
            headers.put(key, value);
        }
        return headers;
    }

    /**
     * Extracts the body from a STOMP frame.
     */
    private String extractBody(String frame) {
        int i = frame.indexOf("\n\n");
        
        if (i == -1) 
            return "";

        String body = frame.substring(i + 2);
        int zero = body.indexOf('\0');
        
        if (zero != -1) 
            body = body.substring(0, zero);

        if (!body.isEmpty() && body.charAt(0) == '\r')
            body = body.substring(1);
        
        return body;
    }

    /**
     * Builds a RECEIPT frame.
     */
    private String buildReceipt(String receiptId) {
        return "RECEIPT\nreceipt-id: " + receiptId + "\n\n\0";
    }

    /**
     * Sends a RECEIPT frame if receipt is requested.
     */
    private void sendReceipt(String receipt) {
        if (receipt != null && !receipt.isEmpty()) {
            connections.send(connectionId, buildReceipt(receipt));
        }
    }

    /**
     * Builds a MESSAGE frame.
     */
    private String buildMessageFrame(String destination, String subscriptionId, int messageId, String body) {
        return "MESSAGE\n" +
                "destination:" + destination + "\n" +
                "subscription:" + subscriptionId + "\n" +
                "message-id:" + messageId + "\n" +
                "\n" +
                (body == null ? "" : body) +
                "\0";
    }

    /**
     * Checks if a string is null or empty.
     */
    private boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    
}