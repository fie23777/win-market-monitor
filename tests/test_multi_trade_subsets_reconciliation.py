from src.reconciliation.tick_reconciler import TickReconciler

def test_multi_trade_subsets_and_partial_responses_reconciliation():
    """
    Valida a prova matemática de multiplicidade de ocorrências:
    Mesmo que consultas sobrepostas tragam subconjuntos parciais (2 -> 3 -> 1 -> 4 ocorrências),
    o reconciliador emite rigorosamente cada uma das 4 ocorrências uma única vez.
    """
    reconciler = TickReconciler()
    t_msc = 1791882400150
    base_trade = {"time": 1791882400, "time_msc": t_msc, "last": 125000.0, "volume_real": 5.0, "flags": 32}

    # 1. Consulta 1 traz 2 ocorrências
    c1 = [base_trade, base_trade]
    res1 = reconciler.reconcile_raw_ticks(c1, symbol="WINZ26")
    assert len(res1) == 2
    assert [t.occurrence_ordinal for t in res1] == [1, 2]

    # 2. Consulta 2 sobreposta traz 3 ocorrências (as 2 já conhecidas + 1 nova)
    c2 = [base_trade, base_trade, base_trade]
    res2 = reconciler.reconcile_raw_ticks(c2, symbol="WINZ26")
    assert len(res2) == 1
    assert res2[0].occurrence_ordinal == 3

    # 3. Consulta 3 traz apenas 1 ocorrência (resposta parcial atrasada/truncada)
    c3 = [base_trade]
    res3 = reconciler.reconcile_raw_ticks(c3, symbol="WINZ26")
    assert len(res3) == 0  # 1 <= 3 (já conhecido), zero duplicação!

    # 4. Consulta 4 traz o conjunto completo de 4 ocorrências
    c4 = [base_trade, base_trade, base_trade, base_trade]
    res4 = reconciler.reconcile_raw_ticks(c4, symbol="WINZ26")
    assert len(res4) == 1
    assert res4[0].occurrence_ordinal == 4

    # Total de ocorrências emitidas no ciclo completo: exatamente 4
    total_emitted = len(res1) + len(res2) + len(res3) + len(res4)
    assert total_emitted == 4
