#!/usr/bin/env python3
"""
cron_sync_traccar.py — Sincroniza GPS do Traccar → ht_gps_track
Para o Traccar brevemente, lê o H2, reinicia o Traccar.
Roda a cada 2 minutos via crontab.
"""
import sqlite3, jaydebeapi, os, subprocess, time
from datetime import datetime, timedelta

HUB_DB     = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"
TRACCAR_DB = "/opt/traccar/data/database"
H2_JAR     = next((f"/opt/traccar/lib/{f}" for f in os.listdir("/opt/traccar/lib") if f.startswith("h2") and f.endswith(".jar")), None)
LOCK_FILE  = "/tmp/traccar_sync.lock"

# uniqueid Traccar → id_tecnico HubTecnico
MAPA = {
    "51150132":            3,   # DENISON
    "51839096":            6,   # LEANDRO
    "26939416":            7,   # RICARDO ILHA
    "70933442":            4,   # RODRIGO SANTOS
    "tecnico_5_cliquedf":  5,   # RODRIGO SANTOS 2
    "49985456":            8,   # ROGERIO
    "tecnico_9_cliquedf":  9,   # VICTOR FERREIRA
    "85214397":            10,  # WELINTON SANTOS
    "94729840":            11,  # WELLINGTON PIACABUCU
}

def brt():
    return (datetime.now() - timedelta(hours=0)).strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def sync():
    # Evitar execucoes simultaneas
    if os.path.exists(LOCK_FILE):
        if time.time() - os.path.getmtime(LOCK_FILE) < 100:
            log("Lock ativo — saindo")
            return
    with open(LOCK_FILE, "w") as f: f.write("1")

    log("=== Sync Traccar iniciado ===")
    total = 0

    try:
        # Parar Traccar para liberar o banco H2
        log("Parando Traccar...")
        subprocess.run(["systemctl", "stop", "traccar"], timeout=15)
        time.sleep(2)

        # Conectar ao H2
        conn = jaydebeapi.connect(
            "org.h2.Driver",
            f"jdbc:h2:{TRACCAR_DB}",
            ["sa", ""],
            H2_JAR
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT d.UNIQUEID, d.NAME, p.LATITUDE, p.LONGITUDE, p.SPEED, p.FIXTIME
            FROM TC_DEVICES d
            JOIN TC_POSITIONS p ON p.ID = d.POSITIONID
            WHERE d.DISABLED = FALSE
              AND p.LATITUDE IS NOT NULL
              AND p.LONGITUDE IS NOT NULL
        """)
        rows = cur.fetchall()
        conn.close()
        log(f"Devices com posicao: {len(rows)}")

        # Gravar no HubTecnico
        hub = sqlite3.connect(HUB_DB, timeout=15)
        hub.execute("PRAGMA journal_mode=WAL")
        hub.execute("PRAGMA busy_timeout=10000")

        agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for row in rows:
            uniqueid = str(row[0])
            nome     = str(row[1])
            lat      = float(row[2]) if row[2] else 0.0
            lon      = float(row[3]) if row[3] else 0.0
            speed    = float(row[4]) if row[4] else 0.0
            fixtime  = str(row[5])[:19] if row[5] else agora

            id_tecnico = MAPA.get(uniqueid)
            if not id_tecnico:
                log(f"  SKIP {nome} ({uniqueid})")
                continue

            hub.execute("""
                INSERT INTO ht_gps_track (id_tecnico, lat, lon, velocidade, registrado_em, status_tecnico)
                VALUES (?, ?, ?, ?, ?, 'online')
            """, (id_tecnico, lat, lon, speed, fixtime))
            total += 1
            log(f"  ✅ {nome} → {lat:.5f},{lon:.5f} {speed:.1f}km/h @ {fixtime}")

        hub.commit()
        hub.close()
        log(f"=== Sync concluido: {total} posicoes inseridas ===")

    except Exception as e:
        log(f"ERRO: {e}")
    finally:
        # Sempre reiniciar o Traccar
        log("Reiniciando Traccar...")
        subprocess.run(["systemctl", "start", "traccar"], timeout=15)
        time.sleep(2)
        try: os.remove(LOCK_FILE)
        except: pass

    with open("/tmp/hubtecnico_traccar_sync_ok", "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    sync()
