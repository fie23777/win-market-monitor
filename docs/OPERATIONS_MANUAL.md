# Manual Operacional: win-market-monitor

Guia de execucao, parada segura e retomada do coletor de mercado de alta frequencia para o mini-indice B3 (WINZ26).

---

## 1. Pre-Requisitos Operacionais
1. Terminal MetaTrader 5 (XP): Terminal aberto e autenticado (Build 6230 ou superior).
2. Simbolo Ativo: WINZ26 visivel e habilitado no Market Watch.
3. PostgreSQL 17 no Docker: Subir o banco via docker-compose na porta 5434:
   docker compose -f docker/docker-compose.yml up -d

---

## 2. Comandos Operacionais

### A. Inicializacao do Coletor
cd c:\Users\fie23\Projetos\win-market-monitor
python -m src.main

*Comportamento de Inicializacao:*
- O coletor NUNCA apaga o buffer data/buffer/spool.db.
- Verifica registros pendentes do encerramento anterior e os drena imediatamente para o PostgreSQL.
- Consulta o checkpoint last_captured_msc e executa recuperacao historica de lacunas com a corretora se necessario.

### B. Parada Segura (Graceful Shutdown)
- Pressione Ctrl + C no terminal ou envie sinal SIGINT/SIGTERM.
- O coletor interrompe o loop, conclui a gravacao do lote em andamento, executa o dreno final das pendencias para o PostgreSQL e fecha o SQLite WAL com integridade fisica.

### C. Retomada apos Encerramento Forcado ou Falha de Energia
- Reexecute: python -m src.main
- O buffer local SQLite configurado com PRAGMA synchronous = FULL recupera o estado transacional a partir do arquivo WAL.
- Itens nao replicados mantem status PENDING e sao sincronizados antes de aceitar novos dados ao vivo.
- O checkpoint last_replicated_msc so e avancado apos confirmacao do PostgreSQL, garantindo idempotencia total.
