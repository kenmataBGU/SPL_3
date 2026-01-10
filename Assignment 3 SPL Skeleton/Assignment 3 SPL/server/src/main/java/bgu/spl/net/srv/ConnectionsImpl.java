package bgu.spl.net.srv;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionsImpl<T> implements Connections<T> {

    // ===== Fields =====

    private final ConcurrentHashMap<Integer, ConnectionHandler<T>> connectionHandlers; // connection IDs -> ConnectionHandlers
    private final ConcurrentHashMap<String, Set<Integer>> channelSubscriptions; // channel names -> set of subscribed connection IDs
    

    // ===== Constructor =====

    public ConnectionsImpl() {
        this.connectionHandlers = new ConcurrentHashMap<>();
        this.channelSubscriptions = new ConcurrentHashMap<>();
    }


    // ===== Methods =====

    /**
     * Registers a new connection handler with the given connection ID.
     */
    public void register(int connectionId, ConnectionHandler<T> handler) {
        if (handler == null)
            throw new IllegalArgumentException("ConnectionHandler cannot be null");

        ConnectionHandler<T> existing = connectionHandlers.putIfAbsent(connectionId, handler);

        if (existing != null)
            throw new IllegalArgumentException("Connection ID already registered");
    }
    
    /**
     * Sends a message to the client with the given connection ID.
     */
    @Override
    public boolean send(int connectionId, T msg) {
        ConnectionHandler<T> handler = connectionHandlers.get(connectionId);

        // Clean up if the handler is missing
        if (handler == null) {
            disconnect(connectionId); 
            return false;
        }

        try {
            handler.send(msg);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Sends a message to all clients subscribed to the given channel.
     */
    @Override
    public void send(String channel, T msg) {
        Set<Integer> subscribers = channelSubscriptions.get(channel);
        if (subscribers != null && !subscribers.isEmpty()) {
            for (int connectionId : subscribers) {
                send(connectionId, msg);
            }
        }
    }

    /**
     * Disconnects the client with the given connection ID.
     */
    @Override
    public void disconnect(int connectionId) {
        // Remove the connection handler
        connectionHandlers.remove(connectionId);

        // Remove the connection ID from all channel subscriptions
        for (Map.Entry<String, Set<Integer>> entry : channelSubscriptions.entrySet()) {
            Set<Integer> subscribers = entry.getValue();
            subscribers.remove(connectionId);
            // Remove the channel if no subscribers left
            if (subscribers.isEmpty()) {
                channelSubscriptions.remove(entry.getKey(), subscribers);
            }
        }
    }

}
