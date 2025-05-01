import cmd
import os
import socket
import json
import hashlib

class Peer(cmd.Cmd):
    prompt = 'peer> '
    def __init__(self, tracker_host, tracker_port):
        super().__init__()
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.sock = None
        self.logged_in = False
        self.username = None
        self.connect_to_tracker()

    def connect_to_tracker(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.tracker_host, self.tracker_port))
        except Exception as e:
            print(f"Falha ao conectar ao tracker: {e}")
            self.sock = None

    def send_request(self, request):
        """Envia uma requisição para o tracker e retorna a resposta"""
        if not self.sock:
            print("Não conectado ao tracker")
            return None
        try:
            message = json.dumps(request) + '\n'
            self.sock.sendall(message.encode())
            response = ''
            while True:
                data = self.sock.recv(1024 * 1024).decode()
                if not data:
                    break
                response += data
                if '\n' in response:
                    response = response.split('\n', 1)
                    break
            return json.loads(response)
        except Exception as e:
            print(f"Erro na comunicação com o tracker: {e}")
            return None

    # O prefixo 'do_' é necessário para que o cmd reconheça os métodos como comandos
    def do_register(self, arg):
        """Registra um novo usuário no tracker"""
        if self.logged_in:
            print("Usuário já autenticado")
            return

        if not arg.strip():
            username = input("Username: ")
            password = input("Password: ")
            if not username or not password:
                print("Usuário ou senha não podem ser vazios")
                return
        else:
            args = arg.split()
            if len(args) != 2:
                print("Uso: 'register <usuário> <senha>' ou apenas 'register'")
                return
            username, password = args

        request = {
            'method': 'register',
            'username': username,
            'password': password
        }

        response = self.send_request(request)
        if response:
            if response.get('status') == 'success':
                print("Usuário registrado com sucesso")
            else:
                print(response.get('message'))

    def do_login(self, arg):
        """Faz login no tracker"""
        if self.logged_in:
            print("Usuário já autenticado")
            return

        if not arg.strip():
            username = input("Username: ")
            password = input("Password: ")
            if not username or not password:
                print("Usuário ou senha não podem ser vazios")
                return
        else:
            args = arg.split()
            if len(args) != 2:
                print("Uso: login <usuário> <senha> ou apenas 'login'")
                return
            username, password = args

        request = {
            'method': 'login',
            'username': username,
            'password': password
        }

        response = self.send_request(request)
        if response:
            if response.get('status') == 'success':
                self.logged_in = True
                self.username = username
                print("Login bem-sucedido. Salve, " + username)
            else:
                print(response.get('message'))

    def do_announce(self, arg):
        """Anuncia um arquivo para o tracker"""

        # Verificações iniciais (se o usuário está logado e se o caminho do arquivo é válido)
        if not self.logged_in:
            print("Autentique-se primeiro")
            return

        if not arg.strip():
            path = input("Caminho para o arquivo (path): ")
            if not path:
                print("Caminho do arquivo não pode ser vazio")
                return
        else:
            args = arg.split()
            if len(args) != 1:
                print("Uso: announce <caminho do arquivo>")
                return
            path = args[0]

        # Verifica se o arquivo existe e lê os dados
        # Se o arquivo não existir, retorna uma mensagem de erro
        # Se o arquivo existir, lê os dados e calcula o hash
        try:
            if not os.path.isfile(path):
                print("Arquivo não encontrado")
                return
            name = os.path.basename(path)
            size = os.path.getsize(path)
            file_hash = self.compute_file_checksum(path)

            request = {
                'method': 'announce',
                'name': name,
                'size': size,
                'hash': file_hash
            }
            response = self.send_request(request)
            if response:
                print(response.get('message', 'Erro desconhecido'))
        except PermissionError:
            print(f"Permissão negada para ler o arquivo: {path}")
        except Exception as e:
            print(f"Erro ao processar arquivo: {str(e)}")

    def do_exit(self, arg):
        print("Saindo...")
        if self.sock:
            self.sock.close()
        return True
    
    # Utils

    def compute_file_checksum(self, file_name):
        with open(file_name, "rb") as f:
            data = f.read()
        return self.calculate_checksum(data)
    
        # Alternativa para calcular o hash do arquivo para arquivos grandes. Implementar depois (talvez seja necessário enviar chunk a chunk e não salvar o arquivo inteiro na memória)
        # sha256 = hashlib.sha256()
        # with open(file_path, 'rb') as f:
        #     while chunk := f.read(CHUNK_SIZE):
        #         sha256.update(chunk)
        # file_hash = sha256.hexdigest()

if __name__ == '__main__':
    cli = Peer('localhost', 5000)
    cli.cmdloop()