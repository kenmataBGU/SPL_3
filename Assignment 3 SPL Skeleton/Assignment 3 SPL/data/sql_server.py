#!/usr/bin/env python3
"""
Basic Python Server for STOMP Assignment – Stage 3.3

IMPORTANT:
DO NOT CHANGE the server name or the basic protocol.
Students should EXTEND this server by implementing
the methods below.
"""

import socket
import sys
import threading
import sqlite3
import os

SERVER_NAME = "STOMP_PYTHON_SQL_SERVER"
DB_FILE = "stomp_server.db"

def recv_null_terminated(sock):
    """
    Receives data from the socket until a null character '\0' is found.
    
    CORRECTION:
    This function now uses a persistent buffer attached to the socket object
    (sock._buffer) to handle TCP packet coalescing. This prevents data loss 
    when multiple commands arrive in a single recv() call.
    """
    # Initialize the buffer on the socket object if it doesn't exist
    if not hasattr(sock, '_buffer'):
        sock._buffer = b""

    while True:
        # Check if we already have a complete message in the buffer
        if b"\0" in sock._buffer:
            # Split exactly at the first null terminator
            msg_bytes, remainder = sock._buffer.split(b"\0", 1)
            # Update the buffer with the remaining data for the next call
            sock._buffer = remainder
            # Return the decoded message
            return msg_bytes.decode("utf-8", errors="replace")
        
        # If no null terminator, read more data from the network
        try:
            chunk = sock.recv(1024)
            if not chunk:
                return ""  # Connection closed by client
            sock._buffer += chunk
        except OSError:
            return "" # Socket error implies disconnection

def init_database():
    """
    Initialize the database with the schema required by Assignment Section 3.3.
    """
    try:
        # Use a context manager to ensure the connection closes
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            # 1. Users: Tracks registration
            # Requirement: "insert a record when a new username is created"
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password TEXT NOT NULL
                )
            """)

            # 2. Logins: Tracks login/logout history
            # Requirement: "insert a record for each successful login... update... upon disconnect"
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS logins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    login_time DATETIME NOT NULL,
                    logout_time DATETIME,
                    FOREIGN KEY(username) REFERENCES users(username)
                )
            """)

            # 3. Files: Tracks uploaded files
            # Requirement: "log every filename uploaded via the report command"
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    upload_time DATETIME,
                    FOREIGN KEY(username) REFERENCES users(username)
                )
            """)
            
            conn.commit()
            print(f"[{SERVER_NAME}] Database initialized successfully at {DB_FILE}.")
    except Exception as e:
        print(f"[{SERVER_NAME}] DB Init Error: {e}")

def execute_sql_command(sql_command):
    """
    Executes INSERT, UPDATE, DELETE commands.
    Returns 'done' on success.
    """
    try:
        # Open a new connection per request for thread safety
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_command)
            conn.commit()
            return "done"
    except sqlite3.Error as e:
        return f"ERROR: {e}"
    except Exception as e:
        return f"ERROR: {e}"

def execute_sql_query(sql_query):
    """
    Executes SELECT queries.
    Returns a string representing the rows, formatted for easy parsing in Java.
    Format: val1|val2|val3 (lines separated by \n)
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            rows = cursor.fetchall()

            if not rows:
                return ""

            # Format the output using a pipe '|' delimiter.
            # Java Parser Tip: String[] cols = line.split("\\|");
            result = []
            for row in rows:
                # Convert all items to string and join with pipe
                row_str = "|".join([str(item) for item in row])
                result.append(row_str)
            
            return "\n".join(result)

    except sqlite3.Error as e:
        return f"ERROR: {e}"
    except Exception as e:
        return f"ERROR: {e}"

def handle_client(client_socket, addr):
    """
    Main loop to handle a connection with the Java server.
    """
    print(f"[{SERVER_NAME}] Client connected from {addr}")

    try:
        while True:
            # 1. Receive Request (Blocking call using the buffered reader)
            message = recv_null_terminated(client_socket)
            
            # Empty message indicates disconnection
            if message == "":
                break 

            print(f"[{SERVER_NAME}] Received: {message}")

            # 2. Process Request
            message = message.strip()
            if not message:
                continue # Ignore empty lines/keep-alives

            command = message.upper()
            response = ""

            if command.startswith("SELECT"):
                response = execute_sql_query(message)
            else:
                response = execute_sql_command(message)

            # 3. Send Response
            # Crucial: Append \0 to match the STOMP-like protocol
            try:
                client_socket.sendall((response + "\0").encode("utf-8"))
            except OSError:
                break # Failed to send, client likely gone

    except Exception as e:
        print(f"[{SERVER_NAME}] Error handling client {addr}: {e}")
    finally:
        try:
            client_socket.close()
        except Exception:
            pass
        print(f"[{SERVER_NAME}] Client {addr} disconnected")

def start_server(host="127.0.0.1", port=7778):
    init_database()
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"[{SERVER_NAME}] Server started on {host}:{port}")
        
        while True:
            client_socket, addr = server_socket.accept()
            # Spawn a new thread for each client to allow concurrent connections
            t = threading.Thread(
                target=handle_client, 
                args=(client_socket, addr), 
                daemon=True
            )
            t.start()

    except KeyboardInterrupt:
        print(f"\n[{SERVER_NAME}] Shutting down server...")
    except Exception as e:
        print(f"[{SERVER_NAME}] Server Error: {e}")
    finally:
        try:
            server_socket.close()
        except Exception:
            pass

if __name__ == "__main__":
    port = 7778
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass
    start_server(port=port)