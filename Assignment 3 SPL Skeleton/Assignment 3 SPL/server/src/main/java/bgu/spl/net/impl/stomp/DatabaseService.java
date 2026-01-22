package bgu.spl.net.impl.stomp;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class DatabaseService {

    private static final String PYTHON_HOST = "127.0.0.1";
    private static final int PYTHON_PORT = 7778;

    /**
     * Handles the raw TCP connection to the Python server.
     * Uses a short-lived socket per request to match the Python server's threaded architecture.
     */
    private static String sendToPython(String message) {
        try (Socket socket = new Socket(PYTHON_HOST, PYTHON_PORT);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

            // 1. Send the message with the required null terminator
            out.write(message);
            out.write('\0');
            out.flush();

            // 2. Read the response until the null terminator
            StringBuilder response = new StringBuilder();
            int c;
            while ((c = in.read()) != -1) {
                if (c == '\0') {
                    break;
                }
                response.append((char) c);
            }
            return response.toString();

        } catch (IOException e) {
            // In a production server, you might log this to a file.
            System.err.println("Database Service Error: " + e.getMessage());
            return "ERROR";
        }
    }

    // --- API Methods ---

    public static boolean validateUser(String username, String password) {
        // Protocol: LOGIN <user> <pass>
        String response = sendToPython("LOGIN " + username + " " + password);
        return "login success".equals(response);
    }

    public static void logoutUser(String username) {
        // Protocol: LOGOUT <user>
        // We read the response to ensure the Python server processes the command before we close the socket
        sendToPython("LOGOUT " + username);
    }

    public static void addFile(String username, String filename) {
        if (filename == null || filename.isEmpty()) return;
        // Protocol: add_file <user> <filename>
        sendToPython("add_file " + username + " " + filename);
    }

    public static String generateReport() {
        // 1. Fetch data sequentially
        // The Python server distinguishes queries by the "SELECT" keyword
        String usersResp = sendToPython("SELECT username FROM users");
        String loginsResp = sendToPython("SELECT username, login_time, logout_time FROM logins");
        String filesResp = sendToPython("SELECT username, filename, upload_time FROM files");

        // 2. Format the data into the final string
        StringBuilder sb = new StringBuilder();
        sb.append("Server Stats:\n");
        
        // Users Section
        sb.append("Users:\n");
        if (!usersResp.startsWith("ERROR") && !usersResp.isEmpty()) {
            String[] lines = usersResp.split("\n");
            for (String line : lines) {
                if (!line.trim().isEmpty()) {
                    sb.append(" ").append(line.trim()).append("\n");
                }
            }
        }

        // Stats Section (Logins)
        sb.append("Stats:\n");
        if (!loginsResp.startsWith("ERROR") && !loginsResp.isEmpty()) {
            String[] lines = loginsResp.split("\n");
            for (String line : lines) {
                if (line.trim().isEmpty()) continue;
                String[] parts = line.split("\\|"); // Python returns pipe-delimited data
                if (parts.length >= 2) {
                    String user = parts[0];
                    String login = parts[1];
                    // Handle cases where logout is None or missing
                    String logout = (parts.length > 2 && !parts[2].equals("None")) ? parts[2] : "null";
                    sb.append(" ").append(user).append(": ").append(login).append(" - ").append(logout).append("\n");
                }
            }
        }
        
        // Files Section
        sb.append("Files:\n");
        if (!filesResp.startsWith("ERROR") && !filesResp.isEmpty()) {
            String[] lines = filesResp.split("\n");
            for (String line : lines) {
                if (line.trim().isEmpty()) continue;
                String[] parts = line.split("\\|");
                if (parts.length >= 2) {
                    sb.append(" ").append(parts[0]).append(" uploaded ").append(parts[1]).append("\n");
                }
            }
        }
        
        return sb.toString();
    }
}