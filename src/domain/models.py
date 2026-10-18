from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
from typing import Optional

def compute_tick_event_signature(
    time_msc: int,
    price: float,
    volume_real: float,
    flags: int,
    trade_side: str
) -> str:
    """Calcula a assinatura de evento independente do lote ou ordem de chegada."""
    payload = f"{time_msc}_{price:.2f}_{volume_real:.4f}_{flags}_{trade_side}"
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]

def compute_tick_fingerprint(
    time_msc: int,
    event_signature: str,
    occurrence_ordinal: int
) -> str:
    """Gera um identificador determinístico para a ocorrência ordinal da assinatura."""
    return f"{time_msc}_{event_signature}_{occurrence_ordinal}"

@dataclass(frozen=True)
class TradeTick:
    symbol: str
    time_msc: int
    time_utc: datetime
    price: float
    volume_real: float
    flags: int
    trade_side: str
    occurrence_ordinal: int = 1
    event_signature: str = ''
    tick_fingerprint: str = ''
    provenance: str = 'REALTIME_STREAM'
    ingested_at_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        if not self.event_signature:
            sig = compute_tick_event_signature(
                self.time_msc, self.price, self.volume_real, self.flags, self.trade_side
            )
            object.__setattr__(self, 'event_signature', sig)
        if not self.tick_fingerprint:
            fp = compute_tick_fingerprint(
                self.time_msc, self.event_signature, self.occurrence_ordinal
            )
            object.__setattr__(self, 'tick_fingerprint', fp)

@dataclass(frozen=True)
class M2CandleClosed:
    symbol: str
    open_time_utc: datetime
    close_time_utc: datetime
    open: float
    high: float
    low: float
    close: float
    volume_real: float
    tick_volume: int
    spread: int
    is_closed: bool = True
    provenance: str = 'REALTIME_STREAM'
    received_at_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ingested_at_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def validate(self):
        if self.open <= 0 or self.high <= 0 or self.low <= 0 or self.close <= 0:
            raise ValueError(f"Preços devem ser estritamente positivos: {self}")
        if self.low > self.high:
            raise ValueError(f"Low ({self.low}) maior que High ({self.high}): {self}")
        if self.open < self.low or self.open > self.high:
            raise ValueError(f"Open ({self.open}) fora do intervalo [{self.low}, {self.high}]: {self}")
        if self.close < self.low or self.close > self.high:
            raise ValueError(f"Close ({self.close}) fora do intervalo [{self.low}, {self.high}]: {self}")

@dataclass
class QualityEvent:
    event_time_utc: datetime
    event_type: str
    symbol: str
    gap_start_utc: Optional[datetime] = None
    gap_end_utc: Optional[datetime] = None
    ticks_recovered: int = 0
    candles_recovered: int = 0
    broker_status: str = 'CONNECTED'
    provenance_uncertainty: bool = False
    description: str = ''

@dataclass
class Checkpoints:
    stream_key: str
    last_captured_msc: int
    last_replicated_msc: int
    last_candle_close_utc: Optional[datetime] = None
    updated_at_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
