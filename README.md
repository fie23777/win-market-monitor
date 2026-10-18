# win-market-monitor

Plataforma autônoma e resiliente para coleta de dados de mercado de alta frequência do mini-índice B3 (WINZ26), com persistência em buffer local durável, PostgreSQL 17 e arquivo histórico Parquet ZSTD.

## Arquitetura de Dois Estágios
1. **Captura Local Durável (Estágio 1):** Ingestão com SQLite WAL (PRAGMA synchronous = FULL), gravação atômica em lote e avanço de last_captured_msc.
2. **Replicação Remota (Estágio 2):** Ingestão no PostgreSQL 17 e avanço de last_replicated_msc somente após confirmação do banco.

## Princípios de Projeto
- **Modo Somente Leitura:** Zero implementação ou importação de funções de ordens ou trading (order_send, etc.).
- **Deduplicação Determinística:** Reconciliação por Assinatura de Evento e Multiplicidade Temporal, preservando múltiplos trades reais no mesmo milissegundo.
- **Safe Drop Policy:** A exclusão de partições no PostgreSQL (30 pregões) exige validação prévia de Parquet ZSTD, manifest com SHA-256 e réplica em backup independente.
- **Isolamento Total:** Repositório e diretório 100% independentes do experimento científico D015.
