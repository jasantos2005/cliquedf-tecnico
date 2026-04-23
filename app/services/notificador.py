"""
Serviço de notificações Telegram para HubTecnico.
Usado por: relatório diário, alertas de parada suspeita.
"""
import os, requests, logging
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
from datetime import datetime

logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_GRUPO = os.getenv("TELEGRAM_GRUPO")

def enviar_telegram(msg: str, chat_id: str = None, parse_mode: str = "HTML") -> bool:
    cid = chat_id or TELEGRAM_GRUPO
    if not TELEGRAM_TOKEN or not cid:
        logger.warning("Telegram não configurado")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": cid, "text": msg, "parse_mode": parse_mode, "disable_web_page_preview": True},
            timeout=10
        )
        return r.status_code == 200
    except Exception as e:
        logger.error(f"Telegram erro: {e}")
        return False
