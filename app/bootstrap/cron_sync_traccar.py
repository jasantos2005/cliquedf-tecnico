#!/usr/bin/env python3
"""
cron_sync_traccar.py — Sincroniza HISTORICO COMPLETO do Traccar → ht_gps_track
Roda a cada 2 minutos. Na primeira vez importa tudo, depois só o novo.
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

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def sync():
    # Lock
    if os.path.exists(LOCK_FILE):
        if time.time() - os.path.getmtime(LOCK_FILE) < 100:
            log("Lock ativo — saindo")
            return
    with open(LOCK_FILE, "w") as f: f.write("1")

    log("=== Sync Traccar iniciado ===")
    total = 0

    try:
        # Parar Traccar para liberar H2
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

        # Abrir HubTecnico
        hub = sqlite3.connect(HUB_DB, timeout=15)
        hub.execute("PRAGMA journal_mode=WAL")
        hub.execute("PRAGMA busy_timeout=10000")

        for uniqueid, id_tecnico in MAPA.items():
            # Buscar ultimo registro ja importado para este tecnico
            row = hub.execute("""
                SELECT MAX(registrado_em) FROM ht_gps_track
                WHERE id_tecnico = ?
            """, (id_tecnico,)).fetchone()
            ultimo_importado = row[0] if row and row[0] else "2000-01-01 00:00:00"

            # Buscar posicoes novas do Traccar para este device
            cur.execute("""
                SELECT p.LATITUDE, p.LONGITUDE, p.SPEED, p.FIXTIME, p.COURSE,
                       p.ATTRIBUTES, d.NAME, p.ADDRESS
                FROM TC_POSITIONS p
                JOIN TC_DEVICES d ON d.ID = p.DEVICEID
                WHERE d.UNIQUEID = ?
                  AND p.FIXTIME > ?
                  AND p.VALID = TRUE
                  AND p.LATITUDE IS NOT NULL
                  AND p.LATITUDE != 0
                ORDER BY p.FIXTIME ASC
            """, (uniqueid, ultimo_importado))
            posicoes = cur.fetchall()

            if not posicoes:
                continue

            log(f"  {posicoes[0][6]}: {len(posicoes)} posicoes novas")

            for pos in posicoes:
                lat       = float(pos[0])
                lon       = float(pos[1])
                speed     = float(pos[2]) * 1.852 if pos[2] else 0.0  # knots → km/h
                fixtime   = str(pos[3])[:19].replace("T", " ")
                attrs_raw = pos[5]
                endereco  = pos[7]

                # Parse attributes JSON
                dist_m    = 0.0
                total_m   = 0.0
                motion    = 0
                bateria   = 0.0
                if attrs_raw:
                    try:
                        import json as _json
                        attrs = _json.loads(str(attrs_raw))
                        dist_m  = float(attrs.get('distance', 0) or 0)
                        total_m = float(attrs.get('totalDistance', 0) or 0)
                        motion  = 1 if attrs.get('motion', False) else 0
                        bateria = float(attrs.get('batteryLevel', 0) or 0)
                    except: pass

                hub.execute("""
                    INSERT INTO ht_gps_track
                        (id_tecnico, lat, lon, velocidade, registrado_em, status_tecnico,
                         distancia_m, total_distance_m, motion, endereco, bateria)
                    VALUES (?, ?, ?, ?, ?, 'traccar', ?, ?, ?, ?, ?)
                """, (id_tecnico, lat, lon, round(speed, 1), fixtime,
                      round(dist_m, 2), round(total_m, 2), motion, endereco, bateria))
                total += 1

            hub.commit()

        conn.close()
        hub.close()
        log(f"=== Sync concluido: {total} posicoes importadas ===")

    except Exception as e:
        log(f"ERRO: {e}")
        import traceback; traceback.print_exc()
    finally:
        log("Reiniciando Traccar...")
        subprocess.run(["systemctl", "start", "traccar"], timeout=15)
        time.sleep(2)
        try: os.remove(LOCK_FILE)
        except: pass

    with open("/tmp/hubtecnico_traccar_sync_ok", "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    sync()
