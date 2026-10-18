from src.reconciliation.tick_reconciler import TickReconciler

def test_multiple_identical_trades_in_same_millisecond_are_preserved(sample_raw_ticks_same_ms):
    """
    Comprova que 3 negócios reais no mesmo milissegundo com preço e volume idênticos
    são reconhecidos como 3 ocorrências distintas e recebem ordinais 1, 2 e 3.
    """
    reconciler = TickReconciler()
    reconciled = reconciler.reconcile_raw_ticks(
        raw_ticks=sample_raw_ticks_same_ms,
        symbol="WINZ26"
    )

    assert len(reconciled) == 3
    # Mesma assinatura de evento
    assert reconciled[0].event_signature == reconciled[1].event_signature == reconciled[2].event_signature
    # Ordinais estritamente crescentes
    assert [t.occurrence_ordinal for t in reconciled] == [1, 2, 3]
    # Fingerprints únicos
    assert len({t.tick_fingerprint for t in reconciled}) == 3

def test_overlapping_queries_deduplicate_without_event_loss(sample_raw_ticks_same_ms):
    """
    Comprova o comportamento de consultas com sobreposição de intervalo:
    - Consulta 1 traz 3 negócios de 10:00:00.150.
    - Consulta 2 sobreposta traz novamente 2 desses 3 negócios, mais 1 novo de 10:00:00.150 e 1 de 10:00:00.200.
    Resultado esperado: A Consulta 2 emite APENAS o 4º negócio de 10:00:00.150 e o de 10:00:00.200 (zero duplicatas).
    """
    reconciler = TickReconciler()
    
    # 1. Primeira consulta retorna 3 negócios no ms 150
    batch1 = sample_raw_ticks_same_ms
    res1 = reconciler.reconcile_raw_ticks(batch1, symbol="WINZ26")
    assert len(res1) == 3

    # 2. Segunda consulta traz 4 negócios no ms 150 (3 anteriores + 1 novo) e 1 negócio no ms 200
    batch2 = sample_raw_ticks_same_ms + [
        {"time": 1791881700, "time_msc": 1791881700150, "last": 125000.0, "volume_real": 5.0, "flags": 32},
        {"time": 1791881700, "time_msc": 1791881700200, "last": 125005.0, "volume_real": 1.0, "flags": 64},
    ]
    res2 = reconciler.reconcile_raw_ticks(batch2, symbol="WINZ26")

    # Devem ser emitidos apenas 2 novos ticks: o 4º do ms 150 e o do ms 200
    assert len(res2) == 2
    assert res2[0].time_msc == 1791881700150
    assert res2[0].occurrence_ordinal == 4
    assert res2[1].time_msc == 1791881700200
    assert res2[1].occurrence_ordinal == 1

def test_identical_subsequent_batch_produces_zero_duplicates(sample_raw_ticks_same_ms):
    """Uma requisição idêntica subsequente deve retornar lista vazia de novos ticks."""
    reconciler = TickReconciler()
    res1 = reconciler.reconcile_raw_ticks(sample_raw_ticks_same_ms, symbol="WINZ26")
    assert len(res1) == 3

    # Re-executar mesmo lote
    res2 = reconciler.reconcile_raw_ticks(sample_raw_ticks_same_ms, symbol="WINZ26")
    assert len(res2) == 0
