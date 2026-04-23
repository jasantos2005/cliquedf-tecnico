"""
Detector de desvio de rota — roda a cada 5min via cron.
Alerta quando técnico em deslocamento se afasta muito da rota esperada.
"""
import sqlite3, math, os, logging
from datetime import datetime, timedelta
from app.services.notificador import enviar_telegram
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

logger = logging.getLogger(__name__)
DB      = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"
DESVIO_MAX_KM  = 3.0   # km fora da rota para disparar alerta
GARAGEM_LAT    = -10.321962
GARAGEM_LON    = -36.579507

def get_db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

def _dist_km(la1, lo1, la2, lo2):
    R = 6371
    dlat = math.radians(la2-la1); dlon = math.radians(lo2-lo1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(la1))*math.cos(math.radians(la2))*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def _desvio_rota(ax, ay, bx, by, px, py):
    """Distância perpendicular do ponto P à linha AB (em graus, aprox km)."""
    # Vetor AB
    abx, aby = bx-ax, by-ay
    ab_len = math.sqrt(abx**2 + aby**2)
    if ab_len < 1e-9:
        return _dist_km(ax, ay, px, py)
    # Projeção de AP sobre AB
    apx, apy = px-ax, py-ay
    t = (apx*abx + apy*aby) / (ab_len**2)
    t = max(0, min(1, t))
    # Ponto mais próximo na linha
    cx = ax + t*abx
    cy = ay + t*aby
    return _dist_km(cx, cy, px, py)

def verificar_desvios():
    db  = get_db()
    cur = db.cursor()
    agora = datetime.utcnow() - timedelta(hours=3)

    if not (7 <= agora.hour < 19):
        db.close()
        return

    # Técnicos em deslocamento com OS destino
    cur.execute("""
        SELECT g.id_tecnico, g.lat, g.lon, g.registrado_em,
               u.nome as tecnico_nome, v.placa,
               o.ixc_os_id, o.lat as os_lat, o.lon as os_lon,
               o.cliente_nome, o.assunto_nome, o.endereco
        FROM ht_gps_track g
        JOIN ht_usuarios u ON u.id = g.id_tecnico
        LEFT JOIN ht_tecnico_veiculo tv ON tv.id_tecnico = g.id_tecnico
            AND tv.data = date('now','-3 hours')
        LEFT JOIN ht_veiculos v ON v.id = tv.id_veiculo
        JOIN ht_os o ON o.id_tecnico = g.id_tecnico
            AND o.status_hub = 'deslocamento'
            AND o.lat IS NOT NULL AND o.lat != 0
        WHERE g.id = (
            SELECT MAX(g2.id) FROM ht_gps_track g2 WHERE g2.id_tecnico = g.id_tecnico
        )
        AND u.ativo = 1
        AND tv.id_veiculo IS NOT NULL
    """)
    rows = cur.fetchall()

    # Alertas já enviados (evita spam — 1 por técnico por hora)
    cur.execute("""
        SELECT chave FROM ht_alertas_enviados
        WHERE date(enviado_em) = date('now','-3 hours')
    """)
    ja_alertados = {r["chave"] for r in cur.fetchall()}

    for row in rows:
        tid     = row["id_tecnico"]
        lat     = row["lat"]
        lon     = row["lon"]
        os_lat  = row["os_lat"]
        os_lon  = row["os_lon"]

        # Origem: última posição antes do deslocamento (usa garagem como fallback)
        cur.execute("""
            SELECT lat, lon FROM ht_gps_track
            WHERE id_tecnico = ? AND velocidade > 2
            ORDER BY id ASC LIMIT 1
        """, (tid,))
        orig = cur.fetchone()
        orig_lat = orig["lat"] if orig else GARAGEM_LAT
        orig_lon = orig["lon"] if orig else GARAGEM_LON

        # Calcular desvio perpendicular à rota origem→destino
        desvio_km = _desvio_rota(orig_lat, orig_lon, os_lat, os_lon, lat, lon)

        # Distância já percorrida e restante
        dist_percorrida = _dist_km(orig_lat, orig_lon, lat, lon)
        dist_restante   = _dist_km(lat, lon, os_lat, os_lon)
        dist_total      = _dist_km(orig_lat, orig_lon, os_lat, os_lon)

        # Ignora se já chegou perto (< 500m do destino)
        if dist_restante < 0.5:
            continue

        # Ignora desvios pequenos
        if desvio_km < DESVIO_MAX_KM:
            continue

        chave = f"desvio_{tid}_{agora.strftime('%Y%m%d%H')}"
        if chave in ja_alertados:
            continue

        maps_atual  = f"https://maps.google.com/?q={lat},{lon}"
        maps_destino = f"https://maps.google.com/?q={os_lat},{os_lon}"
        nome  = row["tecnico_nome"]
        placa = row["placa"] or "—"

        msg = (
            f"🔀 <b>Desvio de Rota Detectado</b>\n"
            f"{'─'*28}\n"
            f"👤 <b>{nome}</b> · {placa}\n"
            f"📋 OS #{row['ixc_os_id']} — {row['cliente_nome']}\n"
            f"🔧 {row['assunto_nome']}\n"
            f"{'─'*28}\n"
            f"📐 Desvio: <b>{desvio_km:.1f} km</b> da rota\n"
            f"📍 Destino: {row['endereco'][:50] if row['endereco'] else '—'}\n"
            f"🛣 Percorrido: {dist_percorrida:.1f} km · Restante: {dist_restante:.1f} km\n"
            f"{'─'*28}\n"
            f"📌 <a href='{maps_atual}'>Posição atual</a> · "
            f"<a href='{maps_destino}'>Destino</a>\n"
            f"🕐 {agora.strftime('%H:%M')}"
        )
        enviar_telegram(msg)

        cur.execute("""
            INSERT OR IGNORE INTO ht_alertas_enviados (chave, enviado_em)
            VALUES (?, datetime('now','-3 hours'))
        """, (chave,))
        db.commit()
        logger.info(f"Alerta desvio: {nome} — {desvio_km:.1f}km")

    db.close()

if __name__ == "__main__":
    verificar_desvios()
