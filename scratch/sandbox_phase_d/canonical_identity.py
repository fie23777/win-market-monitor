import hashlib
from typing import Dict, Any, List, Tuple
from collections import Counter
from datetime import datetime, timezone

class CanonicalEconomicIdentity:
    """
    Gerencia a identidade econômica canônica de negócios e a atribuição determinística
    de ordinais de multiplicidade temporal.
    
    Regra Canônica H001:
    - raw_flags é preservado inalterado no armazenamento.
    - identity_flags = raw_flags & (32 | 64 | 8) isola os bits econômicos reais
      (32 = DEAL_BUY, 64 = DEAL_SELL, 8 = DEAL execution), descartando mutações de transporte (bits 256, 1024).
    """

    @staticmethod
    def get_identity_flags(raw_flags: int) -> int:
        return raw_flags & (32 | 64 | 8)

    @staticmethod
    def make_economic_key(symbol: str, time_msc: int, price: float, volume_real: float, trade_side: str, raw_flags: int) -> Tuple:
        id_flags = CanonicalEconomicIdentity.get_identity_flags(raw_flags)
        return (symbol, time_msc, round(price, 4), round(volume_real, 4), trade_side.upper(), id_flags)

    @staticmethod
    def compute_canonical_fingerprint(
        symbol: str,
        time_msc: int,
        price: float,
        volume_real: float,
        trade_side: str,
        raw_flags: int,
        occurrence_ordinal: int
    ) -> str:
        """
        Calcula o fingerprint canônico incorporando identity_flags e o occurrence_ordinal estável.
        """
        id_flags = CanonicalEconomicIdentity.get_identity_flags(raw_flags)
        raw_sig = f"{symbol}:{time_msc}:{price:.4f}:{volume_real:.4f}:{trade_side}:{id_flags}:{occurrence_ordinal}"
        return hashlib.sha256(raw_sig.encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def reconcile_multisets_and_assign_ordinals(
        reference_ticks: List[Dict[str, Any]],
        existing_ticks: List[Dict[str, Any]],
        symbol: str = "WINZ26"
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Compara multiconjuntos de identidades econômicas antes de atribuir ordinais.
        Garante que negócios pré-existentes mantenham ordinais estáveis (1 .. N)
        e novos negócios recebam ordinais subsequentes (N+1 .. M).
        
        NÃO assume conhecimento da ordem física real do matching engine da B3 intramilissegundo,
        mas garante preservação exata da cardinalidade e volume econômico.
        """
        # 1. Mapear contagens e fingerprints dos ticks pré-existentes
        existing_multiset: Counter = Counter()
        existing_by_key: Dict[Tuple, List[Dict[str, Any]]] = {}
        for t in existing_ticks:
            key = CanonicalEconomicIdentity.make_economic_key(
                symbol, t["time_msc"], t["price"], t["volume_real"], t["trade_side"], t["flags"]
            )
            existing_multiset[key] += 1
            existing_by_key.setdefault(key, []).append(t)

        # 2. Processar a referência MT5 atribuindo ordinais estáveis
        ref_counter: Counter = Counter()
        reconciled_backfill_candidates: List[Dict[str, Any]] = []
        already_present_matches = 0
        new_insertions_identified = 0

        for r in reference_ticks:
            key = CanonicalEconomicIdentity.make_economic_key(
                symbol, r["time_msc"], r["price"], r["volume_real"], r["trade_side"], r["flags"]
            )
            ref_counter[key] += 1
            ordinal = ref_counter[key]

            fp = CanonicalEconomicIdentity.compute_canonical_fingerprint(
                symbol, r["time_msc"], r["price"], r["volume_real"], r["trade_side"], r["flags"], ordinal
            )

            # Verificar se este ordinal da identidade já estava presente na base existente
            is_already_present = (ordinal <= existing_multiset[key])

            rec_item = dict(r)
            rec_item["occurrence_ordinal"] = ordinal
            rec_item["tick_fingerprint"] = fp
            rec_item["is_already_present"] = is_already_present
            rec_item["identity_flags"] = CanonicalEconomicIdentity.get_identity_flags(r["flags"])

            if is_already_present:
                already_present_matches += 1
            else:
                new_insertions_identified += 1
                reconciled_backfill_candidates.append(rec_item)

        stats = {
            "total_reference_trades": len(reference_ticks),
            "total_existing_trades": len(existing_ticks),
            "already_present_matched": already_present_matches,
            "new_insertions_identified": new_insertions_identified,
            "distinct_economic_keys": len(ref_counter)
        }

        return reconciled_backfill_candidates, stats
