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

        # Tabelas para sistema de salas de chat
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_rooms (
                room_id TEXT PRIMARY KEY,
                moderator TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                max_history INTEGER DEFAULT 100,
                FOREIGN KEY (moderator) REFERENCES users(username)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS room_members (
                room_id TEXT,
                username TEXT,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (room_id, username),
                FOREIGN KEY (room_id) REFERENCES chat_rooms(room_id),
                FOREIGN KEY (username) REFERENCES users(username)
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

    # Métodos para gerenciamento de salas de chat
    
    def create_chat_room(self, room_id, moderator, max_history=100):
        """Cria uma nova sala de chat"""
        try:
            self.conn.execute(
                "INSERT INTO chat_rooms (room_id, moderator, max_history) VALUES (?, ?, ?)",
                (room_id, moderator, max_history)
            )
            # Adiciona o moderador como membro da sala
            self.conn.execute(
                "INSERT INTO room_members (room_id, username) VALUES (?, ?)",
                (room_id, moderator)
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def delete_chat_room(self, room_id, requester):
        """Remove uma sala de chat (apenas moderador)"""
        try:
            # Verificar se o usuário é o moderador
            cursor = self.conn.execute(
                "SELECT moderator FROM chat_rooms WHERE room_id = ?",
                (room_id,)
            )
            result = cursor.fetchone()
            if not result or result[0] != requester:
                return False
            
            # Remover membros e sala
            self.conn.execute("DELETE FROM room_members WHERE room_id = ?", (room_id,))
            self.conn.execute("DELETE FROM chat_rooms WHERE room_id = ?", (room_id,))
            self.conn.commit()
            return True
        except sqlite3.Error:
            return False

    def add_member_to_room(self, room_id, username, requester):
        """Adiciona um membro à sala (apenas moderador)"""
        try:
            # Verificar se o usuário é o moderador
            cursor = self.conn.execute(
                "SELECT moderator FROM chat_rooms WHERE room_id = ?",
                (room_id,)
            )
            result = cursor.fetchone()
            if not result or result[0] != requester:
                return False
            
            self.conn.execute(
                "INSERT OR IGNORE INTO room_members (room_id, username) VALUES (?, ?)",
                (room_id, username)
            )
            self.conn.commit()
            return True
        except sqlite3.Error:
            return False

    def remove_member_from_room(self, room_id, username, requester):
        """Remove um membro da sala (moderador ou o próprio usuário)"""
        try:
            # Verificar se o usuário é o moderador ou está removendo a si mesmo
            cursor = self.conn.execute(
                "SELECT moderator FROM chat_rooms WHERE room_id = ?",
                (room_id,)
            )
            result = cursor.fetchone()
            if not result:
                return False
            
            is_moderator = result[0] == requester
            is_self_removal = username == requester
            
            if not (is_moderator or is_self_removal):
                return False
            
            self.conn.execute(
                "DELETE FROM room_members WHERE room_id = ? AND username = ?",
                (room_id, username)
            )
            self.conn.commit()
            return True
        except sqlite3.Error:
            return False

    def get_room_members(self, room_id, requester):
        """Lista membros da sala (apenas para membros da sala)"""
        # Verificar se o usuário é membro da sala
        cursor = self.conn.execute(
            "SELECT username FROM room_members WHERE room_id = ? AND username = ?",
            (room_id, requester)
        )
        if not cursor.fetchone():
            return None
        
        # Retornar lista de membros
        cursor = self.conn.execute(
            "SELECT username, joined_at FROM room_members WHERE room_id = ?",
            (room_id,)
        )
        return cursor.fetchall()

    def list_chat_rooms(self, username):
        """Lista salas disponíveis para o usuário"""
        cursor = self.conn.execute('''
            SELECT cr.room_id, cr.moderator, cr.created_at,
                   CASE WHEN rm.username IS NOT NULL THEN 1 ELSE 0 END as is_member
            FROM chat_rooms cr
            LEFT JOIN room_members rm ON cr.room_id = rm.room_id AND rm.username = ?
        ''', (username,))
        return cursor.fetchall()

    def is_room_member(self, room_id, username):
        """Verifica se o usuário é membro da sala"""
        cursor = self.conn.execute(
            "SELECT 1 FROM room_members WHERE room_id = ? AND username = ?",
            (room_id, username)
        )
        return cursor.fetchone() is not None

    def get_room_info(self, room_id, username):
        """Obtém informações da sala (apenas para membros)"""
        if not self.is_room_member(room_id, username):
            return None
        
        cursor = self.conn.execute(
            "SELECT room_id, moderator, created_at, max_history FROM chat_rooms WHERE room_id = ?",
            (room_id,)
        )
        return cursor.fetchone()