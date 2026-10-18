from datetime import datetime, timezone
from collections import defaultdict
from typing import List, Dict, Tuple, Any
from src.domain.models import TradeTick, compute_tick_event_signature, compute_tick_fingerprint

class TickReconciler:
    """
    Reconciliador determinístico de ticks baseado em Assinatura de Evento e Multiplicidade Temporal.
    Garante que múltiplos negócios genuínos com campos idênticos no mesmo milissegundo sejam preservados,
    enquanto requisições sobrepostas não gerem duplicações.
    """

    def __init__(self, memory_retention_ms: int = 120_000):
        self.memory_retention_ms = memory_retention_ms
        # Mapeia (time_msc, event_signature) -> quantidade de ocorrências já emitidas
        self._emitted_multiplicity: Dict[Tuple[int, str], int] = defaultdict(int)
        self._last_seen_msc: int = 0

    def reconcile_raw_ticks(
        self,
        raw_ticks: List[Dict[str, Any]],
        symbol: str,
        provenance: str = "REALTIME_STREAM"
    ) -> List[TradeTick]:
        """
        Processa um lote de ticks brutos do MT5 e emite exclusivamente
        novos TradeTicks dedupicados por multiplicidade.
        """
        if not raw_ticks:
            return []

        # 1. Agrupar ticks do lote por (time_msc, event_signature) preservando ordem
        batch_groups: Dict[Tuple[int, str], List[Dict[str, Any]]] = defaultdict(list)
        
        for rt in raw_ticks:
            time_msc = int(rt["time_msc"])
            price = float(rt.get("last", rt.get("price", 0.0)))
            volume_real = float(rt.get("volume_real", rt.get("volume", 0.0)))
            flags = int(rt.get("flags", 0))
            trade_side = self._determine_trade_side(flags)
            
            sig = compute_tick_event_signature(time_msc, price, volume_real, flags, trade_side)
            batch_groups[(time_msc, sig)].append({
                "time_msc": time_msc,
                "price": price,
                "volume_real": volume_real,
                "flags": flags,
                "trade_side": trade_side,
                "sig": sig,
            })
            if time_msc > self._last_seen_msc:
                self._last_seen_msc = time_msc

        # 2. Avaliar multiplicidade contra o estado emitido
        reconciled_ticks: List[TradeTick] = []

        for (time_msc, sig), instances in batch_groups.items():
            k_batch = len(instances)
            m_emitted = self._emitted_multiplicity[(time_msc, sig)]

            if k_batch > m_emitted:
                # Existem novas ocorrências genuínas além das já emitidas
                for idx in range(m_emitted, k_batch):
                    inst = instances[idx]
                    ordinal = idx + 1
                    t_utc = datetime.fromtimestamp(time_msc / 1000.0, tz=timezone.utc)
                    fp = compute_tick_fingerprint(time_msc, sig, ordinal)

                    tt = TradeTick(
                        symbol=symbol,
                        time_msc=time_msc,
                        time_utc=t_utc,
                        price=inst["price"],
                        volume_real=inst["volume_real"],
                        flags=inst["flags"],
                        trade_side=inst["trade_side"],
                        occurrence_ordinal=ordinal,
                        event_signature=sig,
                        tick_fingerprint=fp,
                        provenance=provenance,
                    )
                    reconciled_ticks.append(tt)

                self._emitted_multiplicity[(time_msc, sig)] = k_batch

        # 3. Purga periódica de memória para milissegundos além da janela de sobreposição
        self._purge_old_memory()

        # Retornar ordenado temporalmente
        reconciled_ticks.sort(key=lambda t: (t.time_msc, t.occurrence_ordinal))
        return reconciled_ticks

    def _determine_trade_side(self, flags: int) -> str:
        """Determina o lado agressor com base nas flags MT5."""
        # MT5 TICK_FLAG_BUY = 32, TICK_FLAG_SELL = 64
        if flags & 32:
            return "BUY"
        elif flags & 64:
            return "SELL"
        return "CROSS"

    def _purge_old_memory(self):
        """Descarta chaves mais antigas que a janela de retenção de sobreposição."""
        if self._last_seen_msc <= 0:
            return
        cutoff = self._last_seen_msc - self.memory_retention_ms
        keys_to_remove = [k for k in self._emitted_multiplicity if k[0] < cutoff]
        for k in keys_to_remove:
            del self._emitted_multiplicity[k]

    def reset(self):
        """Limpa o estado interno."""
        self._emitted_multiplicity.clear()
        self._last_seen_msc = 0
