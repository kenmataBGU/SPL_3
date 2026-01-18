package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionsImpl;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {

    private static final AtomicInteger globalMessageId = new AtomicInteger(1);

    private boolean shouldTerminate = false;
    private int connectionId = -1;
    private Connections<String> connections;
    private boolean isConnected = false;

    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
        this.shouldTerminate = false;
        this.isConnected = false;
    }

    @Override
    public void process(String message) {
        Frame frame;
        try {
            frame = Frame.parse(message);
        } catch (Exception e) {
            sendErrorAndDisconnect("malformed frame", "Could not parse frame");
            return;
        }

        switch (frame.command) {
            case "CONNECT":
                handleConnect(frame);
                break;
            case "STOMP": 
                handleConnect(frame);
                break;
            case "SUBSCRIBE":
                checkIfConnected();
                if (!shouldTerminate) 
                    handleSubscribe(frame);
                break;
            case "UNSUBSCRIBE":
                checkIfConnected();
                if (!shouldTerminate) 
                    handleUnsubscribe(frame);
                break;
            case "SEND":
                checkIfConnected();
                if (!shouldTerminate) 
                    handleSend(frame);
                break;
            case "DISCONNECT":
                checkIfConnected();
                if (!shouldTerminate) 
                    handleDisconnect(frame);
                break;
            default:
                sendErrorAndDisconnect("unknown command", "Unsupported command: " + frame.command);
        }
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    // ===== helpers =====

    private void handleConnect(Frame frame) {
        if (isConnected) {
            sendErrorAndDisconnect("already connected", "Duplicate CONNECT");
            return;
        }

        String acceptVersion = frame.headers.get("accept-version");
        if (acceptVersion == null) {
            sendErrorAndDisconnect("missing header", "CONNECT must include accept-version");
            return;
        }

        Frame connected = new Frame("CONNECTED");
        connected.headers.put("version", "1.2");

        maybeSendReceipt(frame);
        isConnected = true;
        connections.send(connectionId, connected.build());
    }

    private void handleSubscribe(Frame frame) {
        String destination = frame.headers.get("destination");
        String subId = frame.headers.get("id");

        if (destination == null || subId == null) {
            sendErrorAndDisconnect("missing header", "SUBSCRIBE requires destination and id");
            return;
        }

        ConnectionsImpl<String> c = getConnections();
        boolean ok = c.subscribe(connectionId, destination, subId);
        if (!ok) {
            sendErrorAndDisconnect("subscription error", "Duplicate subscription id: " + subId);
            return;
        }

        maybeSendReceipt(frame);
    }

    private void handleUnsubscribe(Frame frame) {
        String subId = frame.headers.get("id");
        if (subId == null) {
            sendErrorAndDisconnect("missing header", "UNSUBSCRIBE requires id");
            return;
        }

        ConnectionsImpl<String> c = getConnections();
        String dest = c.unsubscribe(connectionId, subId);
        if (dest == null) {
            sendErrorAndDisconnect("unsubscribe error", "No such subscription id: " + subId);
            return;
        }

        maybeSendReceipt(frame);
    }

    private void handleSend(Frame frame) {
        String destination = frame.headers.get("destination");
        if (destination == null) {
            sendErrorAndDisconnect("missing header", "SEND requires destination");
            return;
        }

        ConnectionsImpl<String> c = getConnections();
        int msgId = globalMessageId.getAndIncrement();
       
        for (Integer connId : cSubscribersSnapshot(c, destination)) {
            String subscriptionId = c.getSubscriptionId(connId, destination);
            if (subscriptionId == null) 
                continue;

            Frame msg = new Frame("MESSAGE");
            msg.headers.put("subscription", subscriptionId);
            msg.headers.put("message-id", String.valueOf(msgId));
            msg.headers.put("destination", destination);
            msg.body = (frame.body == null) 
                ? "" 
                : frame.body;
            connections.send(connId, msg.build());
        }

        maybeSendReceipt(frame);
    }

    private void handleDisconnect(Frame frame) {
        String receipt = frame.headers.get("receipt");
        if (receipt != null) {
            Frame r = new Frame("RECEIPT");
            r.headers.put("receipt-id", receipt);
            connections.send(connectionId, r.build());
        }

        connections.disconnect(connectionId);
        isConnected = false;
        shouldTerminate = true;
    }

    private void checkIfConnected() {
        if (!isConnected) {
            sendErrorAndDisconnect("not connected", "You must CONNECT first");
        }
    }

    private void maybeSendReceipt(Frame frame) {
        String receipt = frame.headers.get("receipt");
        if (receipt == null) 
            return;

        Frame r = new Frame("RECEIPT");
        r.headers.put("receipt-id", receipt);
        connections.send(connectionId, r.build());
    }

    private void sendErrorAndDisconnect(String msgHeader, String body) {
        Frame err = new Frame("ERROR");
        err.headers.put("message", msgHeader);
        err.body = (body == null) 
            ? "" 
            : body;
        connections.send(connectionId, err.build());

        connections.disconnect(connectionId);
        isConnected = false;
        shouldTerminate = true;
    }

    private ConnectionsImpl<String> getConnections() {
        if (!(connections instanceof ConnectionsImpl)) 
            throw new IllegalStateException("Connections is not ConnectionsImpl");
        
        ConnectionsImpl<String> c = (ConnectionsImpl<String>) connections;
        return c;
    }

    private List<Integer> cSubscribersSnapshot(ConnectionsImpl<String> c, String destination) {
        try {
            return c.subscribersSnapshot(destination);
        } catch (NoSuchMethodError e) {
            return Collections.emptyList();
        }
    }

    // ===== Frame helper class =====

    static class Frame {
        final String command;
        final Map<String, String> headers = new LinkedHashMap<>();
        String body = "";

        Frame(String command) {
            this.command = command;
        }

        static Frame parse(String raw) {
            if (raw == null) 
                throw new IllegalArgumentException("null frame");
            
            int zero = raw.indexOf('\0');
            String s = (zero >= 0) 
                ? raw.substring(0, zero) 
                : raw;

            String[] parts = s.split("\n\n", 2);
            String head = parts[0];
            String body = (parts.length == 2) 
                ? parts[1] 
                : "";

            String[] lines = head.split("\n");
            if (lines.length == 0) 
                throw new IllegalArgumentException("empty frame");

            Frame f = new Frame(lines[0].trim());
            for (int i = 1; i < lines.length; i++) {
                String line = lines[i];
                if (line.isEmpty()) 
                    continue;
                
                int idx = line.indexOf(':');
                if (idx <= 0) 
                    continue;
                
                String k = line.substring(0, idx).trim();
                String v = line.substring(idx + 1).trim();
                f.headers.put(k, v);
            }
            f.body = body;
            return f;
        }

        String build() {
            StringBuilder sb = new StringBuilder();
            sb.append(command).append('\n');
            
            for (Map.Entry<String, String> e : headers.entrySet()) 
                sb.append(e.getKey()).append(':').append(e.getValue()).append('\n');

            sb.append('\n');
            if (body != null) 
                sb.append(body);

            sb.append('\0');
            return sb.toString();
        }
    }
}