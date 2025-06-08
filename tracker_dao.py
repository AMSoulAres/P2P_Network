import sqlite3
from datetime import datetime, timedelta

DB_NAME = "p2p_tracker.db"

class TrackerDao:
    def __init__(self):
        self.conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        self.create_tables()

    def create_tables(self):
        cursor = self.conn.cursor()
        # Tabela de usuários
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                active_peer INTEGER DEFAULT 0,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # TODO: Implementar tabela de peers ativos (realmente necessário?)
        
        # Tabela de arquivos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                file_hash TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                size INTEGER NOT NULL
                chunk_hashes TEXT -- JSON com hashes de chunks
            )
        ''')
        
        # Tabela de associação peer-arquivo
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS peer_files (
                username TEXT,
                file_hash TEXT,
                PRIMARY KEY (username, file_hash),
                FOREIGN KEY (username) REFERENCES users(username),
                FOREIGN KEY (file_hash) REFERENCES files(file_hash)
            )
        ''')

        self.conn.commit()

    def register_user(self, username, password_hash, ip, port):
        try:
            self.conn.execute(
                "INSERT INTO users (username, password, ip, port) VALUES (?, ?, ?, ?)",
                (username, password_hash, ip, port)
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def verify_user(self, username, password_hash):
        cursor = self.conn.execute(
            "SELECT password FROM users WHERE username = ?",
            (username,)
        )
        result = cursor.fetchone()
        return result and result[0] == password_hash

    def verify_active_peer(self, username) -> tuple[bool, str]:
        cursor = self.conn.execute(
            "SELECT active_peer, DATETIME(last_seen, '-03:00') FROM users WHERE username = ?",
            (username,)
        )

        return cursor.fetchone()

    def add_active_peer(self, username, ip, port):
        # Timestamp atual do banco de dados (atualizar quando implementar heartbeat)
        # Atualiza o timestamp do último login
        # Atualiza o IP e a porta do peer ativo
        self.conn.execute(
            "UPDATE users SET ip = ?, port = ?, active_peer = 1, last_seen = CURRENT_TIMESTAMP WHERE username = ?",
            (ip, port, username,)
        )
        self.conn.commit()

    def remove_active_peer(self, username):
        try:
            self.conn.execute(
                "UPDATE users SET active_peer = 0 WHERE username = ?",
                (username,)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Database error: {e}")

    def register_file(self, username, name, size, file_hash, chunk_hashes):
        try:
            # Registrar arquivo se não existir
            self.conn.execute(
                "INSERT OR IGNORE INTO files VALUES (?, ?, ?, ?)",
                (file_hash, name, size, chunk_hashes)
            )
            # Associar peer ao arquivo
            self.conn.execute(
                "INSERT INTO peer_files VALUES (?, ?)",
                (username, file_hash)
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            print(f"Arquivo {name} já registrado")
            return True
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return False

    def remove_peer_files(self, username):
        try:
            # Remover associações do peer
            self.conn.execute(
                "DELETE FROM peer_files WHERE username = ?",
                (username,)
            )
            # Remover arquivos sem donos
            self.conn.execute(
                "DELETE FROM files WHERE file_hash NOT IN "
                "(SELECT file_hash FROM peer_files)"
            )
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return False
        
    def get_active_peers_with_file(self, file_hash):
        cursor = self.db.conn.execute(
                "SELECT ip, port FROM peer_info WHERE username IN "
                "(SELECT username FROM peer_files WHERE file_hash = ?)",
                (file_hash,)
            )
        return cursor.fetchall()
    
    def get_file_metadata(self, file_hash):
        # Obter metadados do arquivo
        cursor = self.db.conn.execute(
            "SELECT name, size, chunk_hashes FROM files WHERE file_hash = ?",
            (file_hash,)
        )
        return cursor.fetchone()
        
