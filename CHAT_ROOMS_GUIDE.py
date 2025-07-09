#!/usr/bin/env python3
"""
Exemplo de uso do sistema de salas de chat P2P

Este script demonstra como usar o sistema de salas de chat privadas
implementado no sistema P2P.
"""

print("""
=== SISTEMA DE SALAS DE CHAT P2P ===

Este sistema implementa salas privadas com moderação e histórico persistente.

FUNCIONALIDADES IMPLEMENTADAS:

1. GERENCIAMENTO DE SALAS:
   - create_room <id> <nome> [max_historico] : Cria nova sala (moderador)
   - delete_room <id>                       : Remove sala (apenas moderador)
   - list_rooms                             : Lista todas as salas disponíveis
   - join_room <id>                         : Ingressa numa sala
   - leave_room <id>                        : Sai de uma sala

2. GERENCIAMENTO DE MEMBROS:
   - add_to_room <id> <usuario>      : Adiciona membro (apenas moderador)
   - remove_from_room <id> <usuario> : Remove membro (moderador ou próprio)
   - room_members <id>               : Lista membros da sala

3. COMUNICAÇÃO EM SALA (BROADCAST):
   - enter_room <id>    : Entra no modo chat da sala
   - <mensagem>         : Envia mensagem para todos os membros
   - /exit              : Sai do modo chat (continua como membro)

4. HISTÓRICO PERSISTENTE:
   - room_history <id> [linhas] : Mostra histórico de mensagens
   - Mensagens são salvas automaticamente em arquivos de log
   - Hash de integridade para cada mensagem
   - Sincronização automática entre peers

5. SEGURANÇA:
   - Controle de acesso: apenas membros autorizados podem participar
   - Moderação: apenas o criador da sala pode adicionar/remover membros
   - Integridade: cada mensagem tem hash para verificação
   - Timestamps para ordenação cronológica

EXEMPLO DE USO:

# Terminal 1 (Moderador)
python3 client.py
> register alice senha123
> login alice senha123
> create_room sala01 "Discussão Geral" 50
> enter_room sala01
sala(Discussão Geral)> Olá pessoal!
sala(Discussão Geral)> /exit
> add_to_room sala01 bob

# Terminal 2 (Membro)
python3 client.py
> register bob senha456
> login bob senha456
> list_rooms
> join_room sala01
> enter_room sala01
sala(Discussão Geral)> Oi Alice!
sala(Discussão Geral)> /exit
> room_history sala01 10

ARQUITETURA:

O sistema é híbrido:
- Tracker centralizado gerencia metadados das salas
- Comunicação P2P direta entre membros para mensagens em tempo real
- Histórico replicado localmente em cada peer
- Sincronização periódica para garantir consistência

ARQUIVOS DE LOG:

As mensagens são salvas em:
chat_logs/room_<id>.log

Formato:
[timestamp] usuario: mensagem (hash: hash_integridade)

REQUISITOS ATENDIDOS:

✓ Criação/exclusão de salas por moderador
✓ Inclusão/exclusão de peers na sala  
✓ Listagem de salas e membros ativos
✓ Controle de acesso (apenas membros autorizados)
✓ Broadcast de mensagens para todos os membros
✓ Mensagens com remetente, timestamp e nome da sala
✓ Histórico persistente em logs locais
✓ Acesso a mensagens anteriores ao ingressar
✓ Recepção de novas mensagens em tempo real
✓ Tamanho máximo do histórico configurável
✓ Integridade com hashes e timestamps
✓ Prevenção de duplicação de mensagens

Para testar o sistema, inicie o tracker e múltiplos clientes:

Terminal 1: python3 tracker.py
Terminal 2: python3 client.py
Terminal 3: python3 client.py
Terminal 4: python3 client.py

""")
