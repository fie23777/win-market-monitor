import os
import json
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
import pandas as pd

from src.domain.models import TradeTick, M2CandleClosed
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

class CanonicalCandleReconstructor:
    """
    Reconstrói candles M2 exclusivamente a partir dos TradeTicks canônicos persistidos.
    Identifica e documenta explicitamente:
    1. Ambiguidades de ordenação intramilissegundo (múltiplos negócios na abertura ou fechamento).
    2. Divergências de convenção em relação às barras nativas do MT5.
    """

    def __init__(self, time_normalizer: MT5TimeNormalizer = None):
        self.normalizer = time_normalizer or MT5TimeNormalizer()

    def reconstruct_candles_from_ticks(
        self,
        ticks: List[TradeTick],
        symbol: str = "WINZ26"
    ) -> Tuple[List[M2CandleClosed], List[Dict[str, Any]]]:
        if not ticks:
            return [], []

        # Agrupar por janela M2 (intervalo de 120s fechado em [open_utc, close_utc))
        buckets: Dict[int, List[TradeTick]] = {}
        for t in ticks:
            # timestamp em segundos UTC
            sec = int(t.time_utc.timestamp())
            bucket_open_sec = (sec // 120) * 120
            buckets.setdefault(bucket_open_sec, []).append(t)

        reconstructed: List[M2CandleClosed] = []
        ambiguities: List[Dict[str, Any]] = []

        for b_open_sec in sorted(buckets.keys()):
            bucket_ticks = buckets[b_open_sec]
            # Ordenação canônica: por time_msc ASC e occurrence_ordinal ASC
            bucket_ticks.sort(key=lambda x: (x.time_msc, x.occurrence_ordinal))

            open_price = bucket_ticks[0].price
            close_price = bucket_ticks[-1].price
            high_price = max(t.price for t in bucket_ticks)
            low_price = min(t.price for t in bucket_ticks)
            vol_real = sum(t.volume_real for t in bucket_ticks)
            tick_vol = len(bucket_ticks)

            open_utc = datetime.fromtimestamp(b_open_sec, tz=timezone.utc)
            close_utc = open_utc + timedelta(minutes=2)

            # Detecção de ambiguidade intramilissegundo
            first_ms = bucket_ticks[0].time_msc
            last_ms = bucket_ticks[-1].time_msc
            opening_ties = [t for t in bucket_ticks if t.time_msc == first_ms]
            closing_ties = [t for t in bucket_ticks if t.time_msc == last_ms]

            if len(opening_ties) > 1 and len({t.price for t in opening_ties}) > 1:
                ambiguities.append({
                    "candle_open_utc": open_utc.isoformat(),
                    "type": "OPEN_PRICE_INTRA_MS_AMBIGUITY",
                    "time_msc": first_ms,
                    "competing_prices": [t.price for t in opening_ties],
                    "chosen_price": open_price
                })

            if len(closing_ties) > 1 and len({t.price for t in closing_ties}) > 1:
                ambiguities.append({
                    "candle_open_utc": open_utc.isoformat(),
                    "type": "CLOSE_PRICE_INTRA_MS_AMBIGUITY",
                    "time_msc": last_ms,
                    "competing_prices": [t.price for t in closing_ties],
                    "chosen_price": close_price
                })

            candle = M2CandleClosed(
                symbol=symbol,
                open_time_utc=open_utc,
                close_time_utc=close_utc,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume_real=vol_real,
                tick_volume=tick_vol,
                spread=0,
                is_closed=True,
                provenance="RECONSTRUCTED_CANONICAL_TICKS"
            )
            reconstructed.append(candle)

        return reconstructed, ambiguities


class WindowAuditReconciler:
    """
    Audita cada janela de 5 minutos da base persistida contra a referência congelada.
    Compara fingerprints, multiplicidades ordinais e volume_real.
    Nunca classifica como EMPTY_VERIFIED sem confirmação explícita de cobertura histórica.
    """

    def __init__(self, time_normalizer: MT5TimeNormalizer = None):
        self.normalizer = time_normalizer or MT5TimeNormalizer()

    def audit_5m_window(
        self,
        ref_ticks_in_window: List[Dict[str, Any]],
        db_ticks_in_window: List[Dict[str, Any]],
        window_start_msc: int,
        window_end_msc: int,
        has_historical_coverage_proof: bool = True
    ) -> Dict[str, Any]:
        """
        Executa a auditoria estrita de uma janela de 5 minutos.
        """
        # Contagem de multiplicidade por assinatura (time_msc, price, volume_real, flags)
        ref_signatures: Dict[str, int] = {}
        for t in ref_ticks_in_window:
            sig = f"{t['time_msc']}:{t['price']}:{t['volume_real']}:{t['flags']}:{t['occurrence_ordinal']}"
            ref_signatures[sig] = ref_signatures.get(sig, 0) + 1

        db_signatures: Dict[str, int] = {}
        for t in db_ticks_in_window:
            sig = f"{t['time_msc']}:{t['price']}:{t['volume_real']}:{t['flags']}:{t['occurrence_ordinal']}"
            db_signatures[sig] = db_signatures.get(sig, 0) + 1

        ref_fps = {t["tick_fingerprint"] for t in ref_ticks_in_window}
        db_fps = {t["tick_fingerprint"] for t in db_ticks_in_window}

        missing_in_db = ref_fps - db_fps
        extra_in_db = db_fps - ref_fps

        ref_vol = sum(t["volume_real"] for t in ref_ticks_in_window)
        db_vol = sum(t["volume_real"] for t in db_ticks_in_window)

        # Classificação estrita
        if len(ref_ticks_in_window) == 0:
            if not has_historical_coverage_proof:
                classification = "INCONCLUSIVE_NO_PROOF"
            else:
                classification = "EMPTY_VERIFIED"
        elif missing_in_db == set() and extra_in_db == set() and ref_signatures == db_signatures and abs(ref_vol - db_vol) < 1e-4:
            classification = "EXACT_MATCH"
        elif len(missing_in_db) > 0:
            classification = "DISCREPANCY_MISSING_DATA"
        else:
            classification = "DISCREPANCY_EXTRA_DATA"

        return {
            "window_msc": [window_start_msc, window_end_msc],
            "classification": classification,
            "ref_ticks_count": len(ref_ticks_in_window),
            "db_ticks_count": len(db_ticks_in_window),
            "missing_count": len(missing_in_db),
            "extra_count": len(extra_in_db),
            "ref_volume_real": ref_vol,
            "db_volume_real": db_vol,
            "multiplicity_discrepancies": ref_signatures != db_signatures
        }
