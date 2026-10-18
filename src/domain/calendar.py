from datetime import date, timedelta
from typing import Set

# Feriados Fixos Nacionais e B3 (Mês, Dia)
FIXED_HOLIDAYS = {
    (1, 1),    # Confraternização Universal
    (4, 21),   # Tiradentes
    (5, 1),    # Dia do Trabalho
    (9, 7),    # Independência do Brasil
    (10, 12),  # Nossa Senhora Aparecida
    (11, 2),   # Finados
    (11, 15),  # Proclamação da República
    (11, 20),  # Dia da Consciência Negra
    (12, 24),  # Véspera de Natal (Sem negociação B3)
    (12, 25),  # Natal
    (12, 31),  # Último dia útil do ano (Sem negociação B3)
}

def _calculate_easter(year: int) -> date:
    """Calcula a data do Domingo de Páscoa pelo algoritmo de Butcher/Meeus."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)

def get_b3_holidays(year: int) -> Set[date]:
    """Retorna o conjunto de feriados sem pregão na B3 para um determinado ano."""
    holidays = {date(year, m, d) for (m, d) in FIXED_HOLIDAYS}
    
    # Feriados Móveis baseados na Páscoa
    easter = _calculate_easter(year)
    carnival_monday = easter - timedelta(days=48)
    carnival_tuesday = easter - timedelta(days=47)
    good_friday = easter - timedelta(days=2)
    corpus_christi = easter + timedelta(days=60)
    
    holidays.add(carnival_monday)
    holidays.add(carnival_tuesday)
    holidays.add(good_friday)
    holidays.add(corpus_christi)
    
    return holidays

def is_b3_trading_day(d: date) -> bool:
    """Verifica se uma data é dia útil de negociação regular na B3."""
    # Finais de semana: 5 = Sábado, 6 = Domingo
    if d.weekday() in (5, 6):
        return False
    holidays = get_b3_holidays(d.year)
    return d not in holidays

def get_last_n_b3_trading_days(n: int, ref_date: date) -> list[date]:
    """Retorna os últimos N pregões válidos da B3 encerrando em ref_date."""
    trading_days = []
    curr = ref_date
    while len(trading_days) < n:
        if is_b3_trading_day(curr):
            trading_days.append(curr)
        curr -= timedelta(days=1)
    trading_days.reverse()
    return trading_days

def get_cutoff_trading_date_for_retention(retention_trading_days: int = 30, ref_date: date = None) -> date:
    """
    Retorna a data limite mais antiga que deve ser mantida para uma política de N pregões.
    Partições anteriores a esta data são elegíveis para Safe Drop.
    """
    if ref_date is None:
        ref_date = date.today()
    days = get_last_n_b3_trading_days(retention_trading_days, ref_date)
    return days[0] if days else ref_date
