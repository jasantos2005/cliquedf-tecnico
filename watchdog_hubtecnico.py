#!/usr/bin/env python3
"""
watchdog_hubtecnico.py — Monitor + Autocorreção do Hub Tecnico Cliquedf
Executa a cada 5 minutos via crontab.
"""

import os, sys, sqlite3, subprocess, urllib.request, urllib.parse, json, time
from datetime import datetime, timezone, timedelta

BOT_TOKEN  = "8027006096:AAHiJEdtFyPresI81tWgs-Je2PKdaYAyWtY"
CHAT_ID    = "2135602169"
DB_PATH    = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"
SERVICE    = "hubtecnico_cliquedf"
APP_DIR    = "/opt/automacoes/cliquedf/tecnico"
HEALTH_URL = "http://127.0.0.1:8008/health"
LOG_FILE   = "/var/log/watchdog_hubtecnico.log"
LOCK_FILE  = "/tmp/watchdog_hubtecnico.lock"

def brt():
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    linha = f"[{brt()}] {msg}"
    print(linha)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(linha + "\n")
    except: pass

def telegram(msg):
    try:
        url  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data, method="POST"), timeout=10)
    except Exception as e:
        log(f"Telegram erro: {e}")

def notificar_e_corrigir(titulo, descricao, acao_fn):
    log(f"AVISO → {titulo}: {descricao}")
    telegram(f"⚠️ <b>HubTecnico — {titulo}</b>\n\n{descricao}\n\n⏳ Corrigindo agora...\n🕐 {brt()}")
    time.sleep(1)
    try:
        resultado = acao_fn()
        log(f"OK → {titulo}: {resultado}")
        telegram(f"✅ <b>HubTecnico — Corrigido</b>\n\n<b>{titulo}</b>\n{resultado}\n🕐 {brt()}")
    except Exception as e:
        log(f"FALHA → {titulo}: {e}")
        telegram(f"❌ <b>HubTecnico — Falha na Correção</b>\n\n<b>{titulo}</b>\nErro: {e}\n🕐 {brt()}\n⚠️ Verifique manualmente!")

def check_servico():
    r = subprocess.run(["systemctl", "is-active", SERVICE], capture_output=True, text=True)
    status = r.stdout.strip()
    if status != "active":
        def corrigir():
            subprocess.run(["systemctl", "restart", SERVICE], check=True)
            time.sleep(4)
            req = urllib.request.urlopen(HEALTH_URL, timeout=5)
            resp = json.loads(req.read())
            return f"Serviço reiniciado. Health: {resp.get('status','?')}"
        notificar_e_corrigir("Serviço Fora do Ar", f"Serviço <code>{SERVICE}</code> estava <b>{status}</b>.", corrigir)
        return False
    try:
        req = urllib.request.urlopen(HEALTH_URL, timeout=5)
        resp = json.loads(req.read())
        if resp.get("status") != "ok":
            raise Exception(f"health retornou: {resp}")
    except Exception as e:
        def corrigir_health():
            subprocess.run(["systemctl", "restart", SERVICE], check=True)
            time.sleep(4)
            return f"Serviço reiniciado após health falhar: {e}"
        notificar_e_corrigir("Health Check Falhou", f"Endpoint /health não respondeu: <code>{e}</code>", corrigir_health)
        return False
    return True

def check_database_locked():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        conn.execute("PRAGMA journal_mode")
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        conn.close()
        return True
    except sqlite3.OperationalError as e:
        if "locked" in str(e).lower():
            def corrigir():
                conn2 = sqlite3.connect(DB_PATH, timeout=30)
                conn2.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                conn2.close()
                subprocess.run(["systemctl", "restart", SERVICE], check=True)
                time.sleep(3)
                return "WAL checkpoint forçado + serviço reiniciado."
            notificar_e_corrigir("Banco de Dados Travado", "SQLite com <code>database is locked</code>. Requests falhando com erro 500.", corrigir)
            return False
        raise

def check_wal_mode():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        row = conn.execute("PRAGMA journal_mode").fetchone()
        modo = row[0] if row else "unknown"
        conn.close()
        if modo != "wal":
            def corrigir():
                conn2 = sqlite3.connect(DB_PATH, timeout=30)
                conn2.execute("PRAGMA journal_mode=WAL")
                conn2.execute("PRAGMA synchronous=NORMAL")
                conn2.execute("PRAGMA busy_timeout=10000")
                conn2.close()
                return f"WAL mode ativado (estava: {modo})."
            notificar_e_corrigir("SQLite sem WAL Mode", f"Modo atual: <code>{modo}</code>. WAL evita database locked.", corrigir)
            return False
    except Exception as e:
        log(f"check_wal_mode erro: {e}")
    return True

def check_tabelas_criticas():
    tabelas = ["ht_os","ht_usuarios","ht_veiculos","ht_veiculo_posse","ht_gps_track","ht_tecnico_veiculo","ht_despesas","ht_revisoes","ht_checklists"]
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        existentes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        conn.close()
        faltando = [t for t in tabelas if t not in existentes]
        if faltando:
            def corrigir():
                result = subprocess.run(
                    [f"{APP_DIR}/venv/bin/python", "-c", "from app.bootstrap.create_tables import init; init()"],
                    capture_output=True, text=True, cwd=APP_DIR)
                return f"init_tables executado. Faltavam: {', '.join(faltando)}"
            notificar_e_corrigir("Tabelas Críticas Ausentes", f"Tabelas não encontradas: <code>{', '.join(faltando)}</code>", corrigir)
            return False
    except Exception as e:
        log(f"check_tabelas erro: {e}")
    return True

def check_erros_recentes_log():
    log_path = f"/var/log/{SERVICE}_err.log"
    if not os.path.exists(log_path): return True
    try:
        result = subprocess.run(["tail", "-200", log_path], capture_output=True, text=True)
        erros = [l for l in result.stdout.splitlines() if "OperationalError" in l or "Exception in ASGI" in l]
        if len(erros) >= 10:
            def corrigir():
                subprocess.run(["systemctl", "restart", SERVICE], check=True)
                time.sleep(3)
                with open(log_path, "w") as f:
                    f.write(f"[{brt()}] Log limpo pelo watchdog após {len(erros)} erros\n")
                return f"{len(erros)} erros críticos. Serviço reiniciado e log limpo."
            notificar_e_corrigir("Muitos Erros no Log", f"<b>{len(erros)} erros críticos</b> detectados.\nÚltimo: <code>{erros[-1][:100]}</code>", corrigir)
            return False
    except Exception as e:
        log(f"check_log erro: {e}")
    return True

def check_cron_sync_os():
    heartbeat = "/tmp/hubtecnico_cron_sync_ok"
    if os.path.exists(heartbeat):
        minutos = (time.time() - os.path.getmtime(heartbeat)) / 60
        if minutos > 15:
            def corrigir():
                result = subprocess.run(
                    f"cd {APP_DIR} && venv/bin/python -m app.bootstrap.cron_sync_os",
                    shell=True, capture_output=True, text=True, timeout=60)
                with open(heartbeat, "w") as f: f.write(brt())
                return f"Sync OS executado manualmente. {result.stdout[-150:] or result.stderr[-150:]}"
            notificar_e_corrigir("Cron Sync OS Atrasado", f"Último sync há <b>{minutos:.0f} minutos</b> (esperado a cada 5 min).", corrigir)
            return False
    return True


def check_tecnicos_sem_gps():
    """Avisa quando tecnico ativo fica mais de 2h sem enviar GPS em dia util."""
    from datetime import datetime, timezone, timedelta
    agora_utc = datetime.now(timezone.utc)
    hora_brt = (agora_utc - timedelta(hours=3)).hour
    dia_semana = (agora_utc - timedelta(hours=3)).weekday()  # 0=seg, 6=dom

    # Só verifica em horario comercial (7h-19h) dias uteis (seg-sab)
    if not (7 <= hora_brt <= 19 and dia_semana <= 5):
        return True

    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT u.nome, g.lat, g.lon, g.velocidade, g.registrado_em
            FROM ht_gps_track g
            JOIN ht_usuarios u ON u.id = g.id_tecnico
            WHERE g.id IN (
                SELECT MAX(id) FROM ht_gps_track GROUP BY id_tecnico
            )
            AND u.nivel = 10
            AND u.ativo = 1
        """).fetchall()
        conn.close()

        sem_gps = []
        agora_brt = agora_utc - timedelta(hours=3)

        for r in rows:
            if not r["registrado_em"]:
                sem_gps.append(f"• {r['nome']} — sem registro algum")
                continue
            try:
                reg = datetime.strptime(r["registrado_em"], "%Y-%m-%d %H:%M:%S")
                minutos = int((agora_brt - reg).total_seconds() / 60)
                if minutos > 120:
                    if minutos >= 1440:
                        tempo = f"{minutos//1440}d {(minutos%1440)//60}h"
                    elif minutos >= 60:
                        tempo = f"{minutos//60}h {minutos%60}min"
                    else:
                        tempo = f"{minutos}min"
                    sem_gps.append(f"• {r['nome']} — parado há {tempo} (último: {r['registrado_em'][11:16]})")
            except Exception as e:
                log(f"check_gps parse erro {r['nome']}: {e}")

        if sem_gps:
            lista = "\n".join(sem_gps)
            telegram(
                f"📡 <b>HubTecnico — Técnicos sem GPS</b>\n\n"
                f"Os seguintes técnicos estão sem atualizar localização há mais de 2h:\n\n"
                f"{lista}\n\n"
                f"🕐 {brt()}"
            )
            log(f"GPS ausente: {len(sem_gps)} tecnicos")

    except Exception as e:
        log(f"check_tecnicos_sem_gps erro: {e}")
    return True

def main():
    if os.path.exists(LOCK_FILE):
        if time.time() - os.path.getmtime(LOCK_FILE) < 240:
            log("Watchdog já rodando (lock ativo). Saindo.")
            return
    with open(LOCK_FILE, "w") as f: f.write(str(os.getpid()))
    try:
        log("=== Watchdog iniciado ===")
        check_servico()
        check_database_locked()
        check_wal_mode()
        check_tabelas_criticas()
        check_erros_recentes_log()
        check_cron_sync_os()
        log("=== Watchdog concluído ===")
    except Exception as e:
        log(f"ERRO INESPERADO: {e}")
        telegram(f"❌ <b>HubTecnico — Watchdog com Erro Inesperado</b>\n<code>{e}</code>\n🕐 {brt()}")
    finally:
        try: os.remove(LOCK_FILE)
        except: pass

if __name__ == "__main__":
    main()
