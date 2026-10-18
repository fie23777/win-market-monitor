from datetime import datetime, timezone, timedelta
from src.domain.models import TradeTick

def test_two_stage_checkpoint_isolation_and_idempotency(durable_spool):
    """
    Comprova a arquitetura de dois estágios:
    1. Captura local avança last_captured_msc mantendo last_replicated_msc em 0.
    2. Replicação remota só avança last_replicated_msc após a confirmação.
    """
    t_utc = datetime(2026, 10, 19, 13, 0, 0, tzinfo=timezone.utc)
    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1791882000000 + i * 50,
            time_utc=t_utc + timedelta(milliseconds=i * 50),
            price=125020.0,
            volume_real=1.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint=f"fp_stage_{i}"
        )
        for i in range(5)
    ]

    # Estágio 1: Captura durável
    durable_spool.append_ticks_batch(ticks, stream_key="WINZ26")
    ckp1 = durable_spool.get_checkpoints("WINZ26")
    assert ckp1.last_captured_msc == 1791882000200
    assert ckp1.last_replicated_msc == 0

    # Simular falha temporária de rede no Postgres: nada replicado ainda
    pending1 = durable_spool.get_pending_ticks()
    assert len(pending1) == 5

    # Estágio 2: Confirmação de persistência remota
    fps_to_replicate = [t.tick_fingerprint for t in pending1[:3]]
    max_replicated_msc = 1791882000100
    durable_spool.mark_ticks_replicated(fps_to_replicate, max_replicated_msc, stream_key="WINZ26")

    ckp2 = durable_spool.get_checkpoints("WINZ26")
    assert ckp2.last_captured_msc == 1791882000200
    assert ckp2.last_replicated_msc == 1791882000100

    # Apenas os 2 itens restantes continuam como PENDING
    pending2 = durable_spool.get_pending_ticks()
    assert len(pending2) == 2
    assert [t.tick_fingerprint for t in pending2] == ["fp_stage_3", "fp_stage_4"]
