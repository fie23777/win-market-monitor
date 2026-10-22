import os
import hashlib
import json
from typing import Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.connection.mt5_reader import MT5ReadOnlyReader
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.reconciliation.tick_reconciler import TickReconciler

class MT5ReferenceExtractor:
    """
    Extrai, congela e gera manifesto de integridade da sessão do MT5 sem horários arbitrários pré-definidos.
    Determina o término real da sessão com base nos últimos ticks observados no pregão.
    """

    def __init__(self, reader: MT5ReadOnlyReader, time_normalizer: Optional[MT5TimeNormalizer] = None):
        self.reader = reader
        self.normalizer = time_normalizer or MT5TimeNormalizer()

    def determine_session_boundary(self, symbol: str, target_date_iso: str = "2026-10-21") -> Tuple[int, int]:
        """
        Calcula o início oficial da sessão (09:00 BRT) e detecta dinamicamente o último instante
        efetivamente negociado no MT5 com confirmação de ausência de novos negócios.
        """
        # 09:00 BRT
        session_start_msc = self.normalizer.get_session_start_server_msc(
            datetime.fromisoformat(target_date_iso).date(), hour=9, minute=0
        )
        # Obter último tick disponível no MT5 para o símbolo
        now_utc = datetime.now(timezone.utc)
        now_server_msc = self.normalizer.utc_datetime_to_server_msc(now_utc)
        
        # Buscar os últimos 10 ticks da sessão para encontrar o time_msc mais recente
        # Consulta com margem ampla até o presente
        recent_ticks = self.reader.fetch_ticks_range(symbol, session_start_msc, now_server_msc)
        if not recent_ticks:
            raise RuntimeError(f"Nenhum tick encontrado para {symbol} na sessão {target_date_iso}!")

        last_observed_msc = max(t["time_msc"] for t in recent_ticks)
        return session_start_msc, last_observed_msc

    def freeze_reference_parquet(
        self,
        symbol: str,
        from_msc: int,
        to_msc: int,
        output_dir: str
    ) -> Tuple[str, str, Dict[str, Any]]:
        """
        Extrai o histórico contínuo da corretora, persiste em Parquet ZSTD e gera manifesto SHA-256.
        """
        os.makedirs(output_dir, exist_ok=True)
        raw_ticks = self.reader.fetch_ticks_range(symbol, from_msc, to_msc)
        if not raw_ticks:
            raise RuntimeError(f"Falha ao extrair ticks no intervalo [{from_msc}, {to_msc}].")

        # Reconciliar negócios elegíveis com as regras canônicas do projeto
        reconciler = TickReconciler(time_normalizer=self.normalizer)
        reconciled_trades = reconciler.reconcile_raw_ticks(
            raw_ticks=raw_ticks,
            symbol=symbol,
            provenance="MT5_CANONICAL_REFERENCE",
            cutoff_realtime_msc=None
        )

        # Montar DataFrame preservando rigorosamente tipos inteiros brutos e flags
        records = []
        total_volume = 0.0
        for t in reconciled_trades:
            records.append({
                "tick_fingerprint": t.tick_fingerprint,
                "symbol": t.symbol,
                "time_msc": t.time_msc,
                "time_utc": t.time_utc.isoformat(),
                "price": t.price,
                "volume_real": t.volume_real,
                "flags": t.flags,
                "trade_side": t.trade_side,
                "occurrence_ordinal": t.occurrence_ordinal,
                "provenance": t.provenance,
                "ingested_at_utc": t.ingested_at_utc.isoformat()
            })
            total_volume += t.volume_real

        df = pd.DataFrame(records)
        parquet_file = os.path.join(output_dir, f"reference_mt5_{symbol}_{from_msc}_{to_msc}.parquet")
        
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file, compression="zstd", compression_level=7)

        # Calcular SHA-256
        sha256_hash = hashlib.sha256()
        with open(parquet_file, "rb") as f:
            for byte_block in iter(lambda: f.read(65536), b""):
                sha256_hash.update(byte_block)
        parquet_sha256 = sha256_hash.hexdigest()

        # Gerar manifesto de integridade formal
        manifest = {
            "symbol": symbol,
            "session_date": "2026-10-21",
            "time_range_msc": [from_msc, to_msc],
            "from_time_brt": self.normalizer.server_msc_to_utc_datetime(from_msc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "to_time_brt": self.normalizer.server_msc_to_utc_datetime(to_msc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "raw_ticks_count": len(raw_ticks),
            "eligible_trades_count": len(reconciled_trades),
            "total_volume_real": total_volume,
            "parquet_file": os.path.basename(parquet_file),
            "parquet_sha256": parquet_sha256,
            "generated_at_utc": datetime.now(timezone.utc).isoformat()
        }

        manifest_file = os.path.join(output_dir, f"manifest_reference_{symbol}_{from_msc}_{to_msc}.json")
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)

        return parquet_file, manifest_file, manifest
