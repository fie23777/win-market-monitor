# H002 — Análise de Causa-Raiz (Root Cause Analysis)
## Intervenção de Engenharia: Hardening de Captura, Reconciliação e Integridade

**Data de Emissão:** 22/10/2026 11:46 BRT (America/Sao_Paulo)  
**Branch:** `develop`  
**Commit-Base:** `8431469393d0182eb26d203793bc5ef0b28c6e47`  
**Repositório:** `win-market-monitor`  
**Símbolo Canônico:** `WINZ26` (Dezembro/2026)  
**Status da Fase:** Fase A (Diagnóstico e Reprodução por Testes Concluída)

---

## 1. Inventário de Arquivos Auditados e Afetados

| Componente | Arquivo | Responsabilidade Atual | Defeito Estrutural Identificado |
| :--- | :--- | :--- | :--- |
| **Leitor MT5** | `src/connection/mt5_reader.py` | Consulta C-API do MT5 (`copy_ticks_*`) | Truncamento para segundos inteiros em `_resolve_query_datetime`; retorno de `[]` indistinguível para `None` (erro MT5). |
| **Orquestrador** | `src/engine.py` | Loop de streaming e ciclo de vida | `capture_step` com lote rígido de 1.000 sem paginação intra-intervalo; avanço de checkpoint sem garantia de percurso completo; `sync_m2_candles` rotula `is_closed=True` baseado unicamente no relógio. |
| **Configuração** | `src/config.py` | Parâmetros de coleta e buffer | Ausência de limites de capacidade paginada, thresholds de estagnação de cursor e time-budgets. |
| **Reconciliador Periódico** | `src/reconciliation/periodic_reconciler.py` | Auditoria histórica retrospectiva | `audit_ceiling_msc` acoplado ao `last_captured_msc`; bloqueio total da auditoria quando a captura estagna (`audit_ceiling_msc <= from_msc`). |
| **Buffer Local (SQLite)** | `src/buffer/durable_spool.py` | Ingestão em buffer WAL local | `append_ticks_batch` retorna `len(records)` (tentados) em vez de inseridos reais via `INSERT OR IGNORE`; avança `last_captured_msc` baseado no `max_msc` de lote repetido. |
| **Persistência PostgreSQL** | `src/storage/postgres_writer.py` | Replicação no PG 17 e Safe Drop | `write_ticks_batch` retorna `len(records)` com `ON CONFLICT DO NOTHING`; `evaluate_safe_drop_partition` retorna `DROPPED` mesmo após `Exception` no `DROP`; `ensure_daily_partition` não é fail-closed. |
| **Reconciliador de Ticks** | `src/reconciliation/tick_reconciler.py` | Filtragem de book e deduplicação | Mascaramento incompleto de flags (`flags & ~256`), gerando divergência de assinatura com flags de transporte (ex: bit 1024) entre fluxo ao vivo e histórico. |
| **Modelos de Domínio** | `src/domain/models.py` | Entidades `TradeTick`, `Checkpoints`, etc. | Cálculo padrão de `event_signature` no `__post_init__` não aplica mascaramento canônico de flags. |

---

## 2. Diagnóstico Detalhado por Item

### H002-P0-01: Paginação e Progresso da Captura MT5
- **Arquivo:** `src/connection/mt5_reader.py:L71` e `src/engine.py:L327-L331`
- **Mecanismo da Falha:**
  1. `_resolve_query_datetime` executa `sec = int(dt_or_msc // 1000) if dt_or_msc > 1e11 else int(dt_or_msc)` e retorna `datetime.fromtimestamp(sec, tz=timezone.utc)`.
  2. Qualquer fração de milissegundo (ex: `15.789s`) é truncada para `15.000s`.
  3. No `engine.py`, `from_msc = max(session_start, ckp.last_captured_msc - lookback_ms)` e chama `fetch_ticks_from(symbol, from_msc, count=1000)`.
  4. Durante pregões de alto volume (abertura às 09:00 ou divulgação de payroll/taxa de juros), um único segundo ou intervalo de lookback de 2 segundos pode conter **mais de 1.000 ticks**.
  5. Como o `fetch_ticks_from` busca a partir do segundo truncado com limite fixo de 1.000 ticks, ele recebe sempre os **primeiros 1.000 ticks daquele mesmo segundo**.
  6. Os ticks excedentes (> 1.000) nunca são alcançados porque o cursor não avança além do segundo truncado enquanto houver sobreposição, gerando loop infinito de repetição de lotes ou estagnação com perda de trades legítimos.
- **Evidência de Teste:** `tests/test_h002_reproduction.py::test_reproduce_p0_01_mt5_reader_truncates_subsecond_precision` (**PASSED**).

---

### H002-P0-02: Acoplamento da Reconciliação Histórica à Captura
- **Arquivo:** `src/reconciliation/periodic_reconciler.py:L62-L88`
- **Mecanismo da Falha:**
  1. O reconciliador periódico calcula o teto de auditoria como:
     ```python
     audit_ceiling_msc = now_server_msc - self.safety_lag_ms
     if ckp.last_captured_msc > 0:
         audit_ceiling_msc = min(audit_ceiling_msc, ckp.last_captured_msc)
     ```
  2. Se a captura travar ou desacelerar devido ao P0-01, `ckp.last_captured_msc` estaciona em um ponto do passado (ex: 10:00:00).
  3. Quando `watermark_complete_msc` atinge `last_captured_msc`, a condição de guarda `if audit_ceiling_msc <= from_msc: return UP_TO_DATE` é avaliada como verdadeira.
  4. O reconciliador retorna prematuramente `UP_TO_DATE`, sem sequer consultar o histórico do MT5 (`fetch_ticks_range`), bloqueando a detecção e recuperação retrospectiva de ticks tardios.
- **Evidência de Teste:** `tests/test_h002_reproduction.py::test_reproduce_p0_02_reconciler_blocked_when_capture_is_stagnant` (**PASSED**).

---

### H002-P0-03: Indistinção entre Resposta Vazia e Falha MT5
- **Arquivo:** `src/connection/mt5_reader.py:L101-L102` e `L135-L136`
- **Mecanismo da Falha:**
  1. No leitor MT5:
     ```python
     ticks_arr = mt5.copy_ticks_range(symbol, dt_from, dt_to, mt5.COPY_TICKS_ALL)
     if ticks_arr is None or len(ticks_arr) == 0:
         return []
     ```
  2. Quando a C-extension do MT5 falha (queda de conexão transitória, erro interno `-10004`, timeout IPC), ela retorna `None`.
  3. O leitor converte `None` para `[]`, idêntico ao caso em que o mercado estava legítimamente sem negócios.
  4. O reconciliador ou coletor recebe `[]`, não detecta erro e pode assumir erroneamente que a janela foi verificada sem negócios, avançando `W_complete` sobre uma lacuna não inspecionada.
- **Evidência de Teste:** `tests/test_h002_reproduction.py::test_reproduce_p0_03_mt5_reader_returns_empty_list_on_none_failure` (**PASSED**).

---

### H002-P1-04: Contabilização Incorreta de Inserções no SQLite
- **Arquivo:** `src/buffer/durable_spool.py:L104-L121`
- **Mecanismo da Falha:**
  1. `append_ticks_batch` executa `cursor.executemany("INSERT OR IGNORE INTO spool_ticks ...", records)`.
  2. Ao final da transação, executa `return len(records)`.
  3. Se um lote contendo 1.000 ticks já capturados (duplicatas puras por lookback) for submetido, zero registros novos são inseridos no SQLite.
  4. No entanto, o método retorna `1000`, falsificando a telemetria e indicando falso progresso de captura.
  5. Além disso, o checkpoint `last_captured_msc` é atualizado para `max(time_msc)` do lote mesmo que nada tenha sido inserido.
- **Evidência de Teste:** `tests/test_h002_reproduction.py::test_reproduce_p1_04_sqlite_spool_accounting_returns_attempted_instead_of_inserted` (**PASSED**).

---

### H002-P1-05: Contabilização Incorreta de Persistência no PostgreSQL
- **Arquivo:** `src/storage/postgres_writer.py:L109-L130`
- **Mecanismo da Falha:**
  1. `write_ticks_batch` executa `INSERT INTO market_ticks_raw ... ON CONFLICT (...) DO NOTHING`.
  2. O método retorna `len(records)` sem verificar quantos registros foram de fato inseridos vs ignorados por duplicidade idempotente.
  3. Isso impede que o coletor saiba a taxa real de inserções novas no PostgreSQL e mascara tentativas repetidas de replicação.
- **Evidência de Teste:** `tests/test_h002_reproduction.py::test_reproduce_p1_05_postgres_writer_accounting_returns_len_records` (**PASSED**).

---

### H002-P1-06: Qualidade e Finalização Prematura de Candles M2
- **Arquivo:** `src/engine.py:L241-L260`
- **Mecanismo da Falha:**
  1. `sync_m2_candles` considera um candle fechado estritamente com base no tempo:
     `if candle_sec + 120 <= now_server_sec:`
  2. Em seguida, instancia `M2CandleClosed(..., is_closed=True)` e grava no banco.
  3. Não há verificação se os ticks correspondentes àquele período de 2 minutos foram capturados, reconciliados ou persistidos.
  4. Um candle pode ser marcado e consumido como "fechado e final" mesmo que a janela de ticks correspondente tenha sofrido perda de dados ou ainda esteja com status `PENDING`.
- **Evidência de Teste:** `tests/test_h002_reproduction.py::test_reproduce_p1_06_candle_is_closed_based_solely_on_clock` (**PASSED**).

---

### H002-P1-07: Comportamento Não Fail-Closed no Safe Drop e Partições
- **Arquivo:** `src/storage/postgres_writer.py:L90-L97` e `L208-L226`
- **Mecanismo da Falha:**
  1. Em `evaluate_safe_drop_partition`, o bloco `try/except` ao redor de `cur.execute(drop_sql)` apenas registra um log com `logger.info` e retorna:
     ```python
     return {
         "action": "DROPPED",
         "partition": part_name,
         ...
     }
     ```
  2. Se ocorrer erro de permissão, timeout ou queda de conexão no momento do `DROP`, o método declara falsamente que a partição foi `DROPPED`.
  3. Em `ensure_daily_partition`, falha na criação da partição não aborta a operação (apenas emite warning), permitindo que inserts subsequentes falhem downstream.
- **Evidência de Teste:** `tests/test_h002_reproduction.py::test_reproduce_p1_07_safe_drop_returns_dropped_on_sql_exception` (**PASSED**).

---

### H002-P1-08: Mutações de Flags MT5 e Divergência de Assinatura Canônica
- **Arquivo:** `src/reconciliation/tick_reconciler.py:L77` e `src/domain/models.py:L41-L50`
- **Mecanismo da Falha:**
  1. No streaming em tempo real, o MT5 entrega flags limpas como `40` (bits 32 + 8: BUY + LAST).
  2. Na consulta histórica retrospectiva (`copy_ticks_range`), o MT5 injeta bits de transporte/metadados adicionais, como `1024` ou `16` (ex: `1064`).
  3. `tick_reconciler.py` mascara unicamente o bit 256 (`flags & ~256`).
  4. O bit 1024 permanece na assinatura de evento `sig = compute_tick_event_signature(...)`.
  5. Como a assinatura diverge (`sig_rt != sig_hist`), o reconciliador calcula um `tick_fingerprint` diferente para o **mesmo negócio econômico**, gerando duplicação artificial na base de dados histórica.
- **Evidência de Teste:** `tests/test_h002_reproduction.py::test_reproduce_p1_08_flags_mutations_and_signature_divergence` (**PASSED**).

---

## 3. Matriz de Resultados dos Testes de Reprodução (Fase A)

| Teste Automatizado | Anomalia Reproduzida | Resultado |
| :--- | :--- | :---: |
| `test_reproduce_p0_01_mt5_reader_truncates_subsecond_precision` | Truncamento de milissegundos no cursor do leitor MT5 | **PASSED** (defeito confirmado) |
| `test_reproduce_p0_02_reconciler_blocked_when_capture_is_stagnant` | Bloqueio do reconciliador periódico por acoplamento a `last_captured_msc` | **PASSED** (defeito confirmado) |
| `test_reproduce_p0_03_mt5_reader_returns_empty_list_on_none_failure` | Falhas da API MT5 mascaradas como listas vazias legítimas | **PASSED** (defeito confirmado) |
| `test_reproduce_p1_04_sqlite_spool_accounting_returns_attempted_instead_of_inserted` | Falsa contagem de inserção em lotes repetidos no SQLite WAL | **PASSED** (defeito confirmado) |
| `test_reproduce_p1_05_postgres_writer_accounting_returns_len_records` | Falsa contagem de inserções com `ON CONFLICT DO NOTHING` no PostgreSQL | **PASSED** (defeito confirmado) |
| `test_reproduce_p1_06_candle_is_closed_based_solely_on_clock` | Marcação de candle fechado sem comprovação de cobertura de ticks | **PASSED** (defeito confirmado) |
| `test_reproduce_p1_07_safe_drop_returns_dropped_on_sql_exception` | Safe Drop reportando `DROPPED` após falha de execução SQL | **PASSED** (defeito confirmado) |
| `test_reproduce_p1_08_flags_mutations_and_signature_divergence` | Duplicações artificiais decorrentes de mutações de bits de transporte (1024) | **PASSED** (defeito confirmado) |

---

## 4. Próximos Passos (Transição para a Fase B)

Com a conclusão da Fase A e a reprodução determinística de 100% dos defeitos, o plano para a Fase B (Implementação) seguirá a ordem rigorosa de prioridade:
1. **P0-01:** Implementação do leitor com resolução sub-segundo, detecção de lotes cheios e estratégia de janelas temporais verificáveis sem salto indiscriminado de milissegundo.
2. **P0-02:** Desacoplamento do teto de auditoria do reconciliador periódico com cursor autônomo, respeitando `W_complete <= W_durable <= W_captured`.
3. **P0-03:** Contrato explícito de retorno no leitor (`MT5QueryResult`) diferenciando dados, vazio verificado, erro e desconexão.
4. **P1-04 & P1-05:** Métricas granulares de inserção (`attempted`, `inserted`, `duplicates`, `failed`) no SQLite e PostgreSQL.
5. **P1-06:** Separação entre fechamento temporal e candle auditado canônico.
6. **P1-07:** Política estrita fail-closed no Safe Drop e particionamento.
7. **P1-08:** Identidade canônica isolando bits econômicos (`flags & 104`) mantendo `raw_flags` inalterado.
