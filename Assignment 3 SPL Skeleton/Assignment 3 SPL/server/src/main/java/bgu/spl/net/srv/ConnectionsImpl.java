package bgu.spl.net.srv;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionsImpl<T> implements Connections<T> {

    // ===== fields =====

    // connectionId -> handler
    private final ConcurrentHashMap<Integer, ConnectionHandler<T>> active = new ConcurrentHashMap<>();
    // destination -> (connectionId -> subscriptionId)
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> destToConnSubId = new ConcurrentHashMap<>();
    // connectionId -> (subscriptionId -> destination)
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, String>> connToSubIdDest = new ConcurrentHashMap<>();


    // ===== methods =====
    
    public void connect(int connectionId, ConnectionHandler<T> handler) {
        if (handler == null) 
            throw new IllegalArgumentException("handler is null");
        
        active.put(connectionId, handler);
        connToSubIdDest.putIfAbsent(connectionId, new ConcurrentHashMap<>());
    }

    @Override
    public boolean send(int connectionId, T msg) {
        ConnectionHandler<T> handler = active.get(connectionId);
        if (handler == null) 
            return false;
        
        handler.send(msg);
        return true;
    }

    @Override
    public void send(String destination, T msg) {
        if (destination == null) 
            return;
        
        ConcurrentHashMap<Integer, String> subscribers = destToConnSubId.get(destination);
        if (subscribers == null) 
            return;

        for (Integer connId : subscribers.keySet()) 
            send(connId, msg);
    }

    @Override
    public void disconnect(int connectionId) {
        // remove handler
        active.remove(connectionId);

        // remove all subscriptions of this connection
        ConcurrentHashMap<String, String> subIdToDest = connToSubIdDest.remove(connectionId);
        if (subIdToDest == null) 
            return;

        for (Map.Entry<String, String> e : subIdToDest.entrySet()) {
            String destination = e.getValue();
            ConcurrentHashMap<Integer, String> map = destToConnSubId.get(destination);
            if (map != null) {
                map.remove(connectionId);
                if (map.isEmpty()) 
                    destToConnSubId.remove(destination, map);
            }
        }
    }

    public boolean subscribe(int connectionId, String destination, String subscriptionId) {
        if (destination == null || subscriptionId == null) 
            return false;

        connToSubIdDest.putIfAbsent(connectionId, new ConcurrentHashMap<>());
        ConcurrentHashMap<String, String> subIdToDest = connToSubIdDest.get(connectionId);

        // check if subscription-id unique per connection
        String prev = subIdToDest.putIfAbsent(subscriptionId, destination);
        if (prev != null) 
            return false;

        destToConnSubId.putIfAbsent(destination, new ConcurrentHashMap<>());
        destToConnSubId.get(destination).put(connectionId, subscriptionId);
        return true;
    }

    public String unsubscribe(int connectionId, String subscriptionId) {
        if (subscriptionId == null) 
            return null;

        ConcurrentHashMap<String, String> subIdToDest = connToSubIdDest.get(connectionId);
        if (subIdToDest == null) 
            return null;

        String destination = subIdToDest.remove(subscriptionId);
        if (destination == null) 
            return null;

        ConcurrentHashMap<Integer, String> map = destToConnSubId.get(destination);
        if (map != null) {
            map.remove(connectionId);
            if (map.isEmpty()) 
                destToConnSubId.remove(destination, map);
        }
        return destination;
    }

    public String getSubscriptionId(int connectionId, String destination) {
        ConcurrentHashMap<Integer, String> map = destToConnSubId.get(destination);
        if (map == null) 
            return null;
        return map.get(connectionId);
    }

    public boolean isConnected(int connectionId) {
        return active.containsKey(connectionId);
    }

    public java.util.List<Integer> subscribersSnapshot(String destination) {
        ConcurrentHashMap<Integer, String> map = destToConnSubId.get(destination);
        if (map == null) 
            return java.util.Collections.emptyList();
        
        return new java.util.ArrayList<>(map.keySet());
    }
}