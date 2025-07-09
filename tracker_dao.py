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
                chat_port INTEGER DEFAULT 0,
                active_peer INTEGER DEFAULT 0,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS peer_scores (
                username TEXT PRIMARY KEY,
                total_time_seconds REAL DEFAULT 0,
                total_chunks_served INTEGER DEFAULT 0,
                FOREIGN KEY(username) REFERENCES users(username)
            )
        ''')
        # TODO: Implementar tabela de peers ativos (realmente necessário?)
        
        # Tabela de arquivos
        # chunk_hashes possui um JSON com hashes de chunks
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                file_hash TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                size INTEGER NOT NULL,
                chunk_hashes TEXT NOT NULL
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

         # Tabela de salas de chat
         #history_max_size é o tamanho máximo do histórico de mensagens definido pelo número de mensagens ou tempo
         #history_type pode ser 'count' (número de mensagens) ou 'time'
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_rooms (
                room_name TEXT PRIMARY KEY,
                moderator_username TEXT NOT NULL,
                creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                history_max_size INTEGER DEFAULT 100,
                history_type TEXT DEFAULT 'count',
                FOREIGN KEY(moderator_username) REFERENCES users(username) ON DELETE CASCADE
            )
        ''')

        # Tabela de membros da sala de chat
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS room_members (
                room_name TEXT,
                username TEXT,
                PRIMARY KEY (room_name, username),
                FOREIGN KEY(room_name) REFERENCES chat_rooms(room_name) ON DELETE CASCADE,
                FOREIGN KEY(username) REFERENCES users(username) ON DELETE CASCADE
            )
        ''')

        self.conn.commit()

    def register_user(self, username, password_hash, ip, port, chat_port):
        try:
            self.conn.execute(
                "INSERT INTO users (username, password, ip, port, chat_port) VALUES (?, ?, ?, ?, ?)",
                (username, password_hash, ip, port, chat_port)
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

    def add_active_peer(self, username, ip, port, chat_port):
        # Timestamp atual do banco de dados (atualizar quando implementar heartbeat)
        # Atualiza o timestamp do último login
        # Atualiza o IP e a porta do peer ativo
        self.conn.execute(
            "UPDATE users SET ip = ?, port = ?, chat_port = ?, active_peer = 1, last_seen = CURRENT_TIMESTAMP WHERE username = ?",
            (ip, port, chat_port, username,)
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
        
    def register_partial_file(self, username, file_hash):
        try:
            # Apenas associa o peer ao arquivo (sem criar arquivo novo)
            self.conn.execute(
                "INSERT OR IGNORE INTO peer_files (username, file_hash) VALUES (?, ?)",
                (username, file_hash)
            )
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Erro no registro parcial: {e}")
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
        cursor = self.conn.execute('''
            SELECT u.ip, u.port, u.username, ps.total_time_seconds, ps.total_chunks_served
            FROM users u
            LEFT JOIN peer_scores ps ON u.username = ps.username
            WHERE u.active_peer = 1 AND u.username IN (SELECT username FROM peer_files WHERE file_hash = ?)
        ''', (file_hash,))
        return cursor.fetchall()
    
    def get_file_metadata(self, file_hash):
        # Obter metadados do arquivo
        cursor = self.conn.execute(
            "SELECT name, size, chunk_hashes FROM files WHERE file_hash = ?",
            (file_hash,)
        )
        return cursor.fetchone()
    
    def refresh_peer_files(self, username, current_hashes):
        cursor = self.conn.cursor()
        # garantir peer ativo
        cursor.execute("UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE username = ?", (username,))
        # obter hashes antigos
        cursor.execute("SELECT file_hash FROM peer_files WHERE username = ?", (username,))
        old = {row[0] for row in cursor.fetchall()}
        new = set(current_hashes)
        # inserir novos
        for files in new - old:
            cursor.execute("INSERT OR IGNORE INTO peer_files(username, file_hash) VALUES (?, ?)", (username, files))
        # remover ausentes
        for files in old - new:
            cursor.execute("DELETE FROM peer_files WHERE username = ? AND file_hash = ?", (username, files))
        # remover arquivos órfãos
        cursor.execute(
            "DELETE FROM files WHERE file_hash NOT IN (SELECT file_hash FROM peer_files)"
        )
        self.conn.commit()

    def get_online_users(self):
        cursor = self.conn.execute(
            "SELECT username FROM users WHERE active_peer = 1"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_peer_address(self, username):
        cursor = self.conn.execute(
            "SELECT ip, port FROM users WHERE username = ? AND active_peer = 1",
            (username,)
        )
        return cursor.fetchone()
    
    def get_peer_chat_address(self, username):
        cursor = self.conn.execute(
            "SELECT ip, chat_port FROM users WHERE username = ? AND active_peer = 1",
            (username,)
        )
        return cursor.fetchone()
    
    def update_peer_score(self, username, time_online, chunks_served):
        # Atualizar totais
        self.conn.execute('''
            INSERT OR REPLACE INTO peer_scores (username, total_time_seconds, total_chunks_served)
            VALUES (?, COALESCE((SELECT total_time_seconds FROM peer_scores WHERE username = ?), 0) + ?,
                    COALESCE((SELECT total_chunks_served FROM peer_scores WHERE username = ?), 0) + ?)
        ''', (username, username, time_online, username, chunks_served))

        self.conn.commit()

    def get_peer_score(self, username):
        cursor = self.conn.execute(
            "SELECT total_time_seconds, total_chunks_served FROM peer_scores WHERE username = ?",
            (username,)
        )
        return cursor.fetchone() or (0, 0)
    
    def list_files(self):
        cursor = self.conn.execute(
            "SELECT file_hash, name, size, chunk_hashes FROM files"
        )
        return cursor.fetchall()
    
    def create_room(self, room_name, moderator):
        try:
            self.conn.execute(
                "INSERT INTO chat_rooms (room_name, moderator_username) VALUES (?, ?)",
                (room_name, moderator)
            )
            # O moderador é automaticamente um membro
            self.add_room_member(room_name, moderator)
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Sala com este nome já existe
            return False

    def delete_room(self, room_name):
        try:
            self.conn.execute("DELETE FROM chat_rooms WHERE room_name = ?", (room_name,))
            self.conn.commit()
            return True
        except sqlite3.Error:
            return False

    def add_room_member(self, room_name, username):
        try:
            # Verifica se o usuário e a sala existem antes de adicionar
            user_exists = self.conn.execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone()
            room_exists = self.conn.execute("SELECT 1 FROM chat_rooms WHERE room_name=?", (room_name,)).fetchone()
            if not user_exists or not room_exists:
                return False
                
            self.conn.execute(
                "INSERT OR IGNORE INTO room_members (room_name, username) VALUES (?, ?)",
                (room_name, username)
            )
            self.conn.commit()
            return True
        except sqlite3.Error:
            return False

    def remove_room_member(self, room_name, username):
        try:
            self.conn.execute(
                "DELETE FROM room_members WHERE room_name = ? AND username = ?",
                (room_name, username)
            )
            self.conn.commit()
            return True
        except sqlite3.Error:
            return False

    def get_room_info(self, room_name):
        cursor = self.conn.execute(
            "SELECT moderator_username, creation_timestamp FROM chat_rooms WHERE room_name = ?",
            (room_name,)
        )
        return cursor.fetchone()

    def get_room_members(self, room_name):
        cursor = self.conn.execute(
            "SELECT username FROM room_members WHERE room_name = ?",
            (room_name,)
        )
        return [row[0] for row in cursor.fetchall()]

    def list_all_rooms(self):
        cursor = self.conn.execute(
            "SELECT room_name, moderator_username FROM chat_rooms"
        )
        return cursor.fetchall()

    def is_moderator(self, username, room_name):
        cursor = self.conn.execute(
            "SELECT 1 FROM chat_rooms WHERE room_name = ? AND moderator_username = ?",
            (room_name, username)
        )
        return cursor.fetchone() is not None

    def is_user_in_room(self, username, room_name):
        cursor = self.conn.execute(
            "SELECT 1 FROM room_members WHERE room_name = ? AND username = ?",
            (room_name, username)
        )
        return cursor.fetchone() is not None