import os
import re
from pathlib import Path

FORBIDDEN_PATTERNS = [
    r'order_send',
    r'OrderSend',
    r'order_check',
    r'OrderCheck',
    r'order_calc_margin',
    r'TRADE_ACTION_DEAL',
    r'TRADE_ACTION_PENDING',
    r'TRADE_ACTION_SLTP',
    r'TRADE_ACTION_MODIFY',
    r'TRADE_ACTION_CLOSE_BY'
]

def test_source_code_contains_zero_trading_order_functions():
    """
    Auditoria estática rigorosa do código-fonte:
    Comprova que nenhuma função ou constante de envio ou modificação de ordens
    existe nos módulos de conexão, storage, buffer ou engine do win-market-monitor.
    """
    src_dir = Path(__file__).resolve().parent.parent / "src"
    assert src_dir.exists()

    violations = []

    for root, _, files in os.walk(src_dir):
        for f in files:
            if f.endswith(".py"):
                file_path = Path(root) / f
                with open(file_path, "r", encoding="utf-8") as py_file:
                    content = py_file.read()
                    for pattern in FORBIDDEN_PATTERNS:
                        if re.search(pattern, content, re.IGNORECASE):
                            violations.append(f"{file_path.name}: encontrado padrão proibido '{pattern}'")

    assert len(violations) == 0, f"Violações de modo somente leitura detectadas: {violations}"
