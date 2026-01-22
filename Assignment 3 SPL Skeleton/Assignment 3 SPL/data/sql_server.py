#!/usr/bin/env python3
"""
Basic Python Server for STOMP Assignment – Stage 3.3
FIXED VERSION: Implements LOGIN, LOGOUT, and ADD_FILE logic
"""

import socket
import sys
import threading
import sqlite3
import datetime

SERVER_NAME = "STOMP_PYTHON_SQL_SERVER"
DB_FILE = "stomp_server.db"

# Global buffer dictionary
buffers = {}

def recv_null_terminated(sock):
    if sock not in buffers:
        buffers[sock] = b""

    while True:
        if b"\0" in buffers[sock]:
            msg_bytes, remainder = buffers[sock].split(b"\0", 1)
            buffers[sock] = remainder
            return msg_bytes.decode("utf-8", errors="replace")
        
        try:
            chunk = sock.recv(1024)
            if not chunk:
                return ""
            buffers[sock] += chunk
        except OSError:
            return ""

def init_database():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password TEXT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS logins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    login_time DATETIME NOT NULL,
                    logout_time DATETIME,
                    FOREIGN KEY(username) REFERENCES users(username)
                )
            """)
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

def execute_sql_query(sql_query):
    """ Executes SELECT queries. """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            if not rows:
                return ""
            result = []
            for row in rows:
                row_str = "|".join([str(item) for item in row])
                result.append(row_str)
            return "\n".join(result)
    except Exception as e:
        return f"ERROR: {e}"

# --- NEW BUSINESS LOGIC HANDLERS ---

def handle_login(username, password):
    """
    Checks user/pass. Registers if new. Logs the login event.
    Returns: "login success" or "login failed"
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            # 1. Check if user exists
            cursor.execute("SELECT password FROM users WHERE username=?", (username,))
            row = cursor.fetchone()
            
            if row:
                # User exists, check password
                stored_pass = row[0]
                if stored_pass != password:
                    return "login failed" # Wrong password
            else:
                # User does not exist, register them
                cursor.execute("INSERT INTO users (username, password) VALUES (?, ?)", (username, password))
            
            # 2. Record the login in 'logins' table
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("INSERT INTO logins (username, login_time) VALUES (?, ?)", (username, now))
            conn.commit()
            
            return "login success"
    except Exception as e:
        return f"ERROR: {e}"

def handle_logout(username):
    """ Updates the logout_time for the most recent active login. """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Update the latest entry where logout_time is NULL
            cursor.execute("""
                UPDATE logins 
                SET logout_time=? 
                WHERE id = (
                    SELECT id FROM logins 
                    WHERE username=? AND logout_time IS NULL 
                    ORDER BY id DESC LIMIT 1
                )
            """, (now, username))
            conn.commit()
            return "done"
    except Exception as e:
        return f"ERROR: {e}"

def handle_add_file(username, filename):
    """ Logs a file upload. """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("INSERT INTO files (username, filename, upload_time) VALUES (?, ?, ?)", (username, filename, now))
            conn.commit()
            return "done"
    except Exception as e:
        return f"ERROR: {e}"

def handle_client(client_socket, addr):
    print(f"[{SERVER_NAME}] Client connected from {addr}")
    try:
        while True:
            message = recv_null_terminated(client_socket)
            if message == "":
                break 

            print(f"[{SERVER_NAME}] Received: {message}")
            message = message.strip()
            if not message:
                continue 

            # Parse Command
            parts = message.split()
            cmd_upper = parts[0].upper()
            response = ""

            # --- ROUTING LOGIC ---
            if cmd_upper == "LOGIN":
                # Expect: LOGIN <user> <pass>
                if len(parts) >= 3:
                    response = handle_login(parts[1], parts[2])
                else:
                    response = "ERROR: Invalid LOGIN format"
            
            elif cmd_upper == "LOGOUT":
                # Expect: LOGOUT <user>
                if len(parts) >= 2:
                    response = handle_logout(parts[1])
                else:
                    response = "ERROR: Invalid LOGOUT format"
            
            elif cmd_upper == "ADD_FILE":
                # Expect: add_file <user> <filename>
                if len(parts) >= 3:
                    response = handle_add_file(parts[1], parts[2])
                else:
                    response = "ERROR: Invalid ADD_FILE format"

            elif cmd_upper == "SELECT":
                # Raw SQL for reports
                response = execute_sql_query(message)
                
            else:
                response = "ERROR: Unknown Command"

            # Send Response
            try:
                client_socket.sendall((response + "\0").encode("utf-8"))
            except OSError:
                break 

    except Exception as e:
        print(f"[{SERVER_NAME}] Error handling client {addr}: {e}")
    finally:
        if client_socket in buffers:
            del buffers[client_socket]
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
            t = threading.Thread(target=handle_client, args=(client_socket, addr), daemon=True)
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