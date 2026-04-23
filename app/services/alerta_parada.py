"""
Monitor de paradas suspeitas — roda a cada 5min via cron.
Alerta se técnico parar >20min em local sem OS vinculada.
"""
import sqlite3, math, os, logging, json
from datetime import datetime, timedelta
from app.services.notificador import enviar_telegram
from app.services.geocoder import geocoder as _geo

logger = logging.getLogger(__name__)
DB = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"
ALERTA_MIN = 20       # minutos parado para disparar alerta
GARAGEM_LAT = -10.321962
GARAGEM_LON = -36.579507
RAIO_GARAGEM = 500    # metros — parada na garagem não alerta

def get_db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

def _dist_m(la1, lo1, la2, lo2):
    R = 6371000
    dlat = math.radians(la2-la1); dlon = math.radians(lo2-lo1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(la1))*math.cos(math.radians(la2))*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def verificar_paradas():
    db  = get_db()
    cur = db.cursor()
    agora = datetime.utcnow() - timedelta(hours=3)

    # Só horário comercial
    if not (7 <= agora.hour < 19):
        db.close()
        return

    # Última posição de cada técnico ativo
    cur.execute("""
        SELECT g.id_tecnico, g.lat, g.lon, g.velocidade, g.registrado_em,
               u.nome as tecnico_nome, v.placa
        FROM ht_gps_track g
        JOIN ht_usuarios u ON u.id = g.id_tecnico
        LEFT JOIN ht_tecnico_veiculo tv ON tv.id_tecnico = g.id_tecnico
            AND tv.data = date('now','-3 hours')
        LEFT JOIN ht_veiculos v ON v.id = tv.id_veiculo
        WHERE g.id = (
            SELECT MAX(g2.id) FROM ht_gps_track g2 WHERE g2.id_tecnico = g.id_tecnico
        )
        AND u.ativo = 1
        AND tv.id_veiculo IS NOT NULL
    """)
    tecnicos = cur.fetchall()

    # Alertas já enviados hoje (evita spam)
    cur.execute("""
        SELECT chave FROM ht_alertas_enviados
        WHERE date(enviado_em) = date('now','-3 hours')
    """)
    ja_alertados = {r["chave"] for r in cur.fetchall()}

    for tec in tecnicos:
        tid  = tec["id_tecnico"]
        lat  = tec["lat"]
        lon  = tec["lon"]
        vel  = tec["velocidade"] or 0

        if vel > 2:
            continue  # em movimento, ignora

        # Quantos minutos parado nessa posição?
        cur.execute("""
            SELECT MIN(registrado_em) as desde
            FROM ht_gps_track
            WHERE id_tecnico = ?
            AND velocidade < 2
            AND registrado_em >= datetime('now','-3 hours','-2 hours')
            ORDER BY id ASC
        """, (tid,))
        row = cur.fetchone()
        if not row or not row["desde"]:
            continue

        desde = datetime.fromisoformat(row["desde"])
        min_parado = (agora - desde).total_seconds() / 60

        if min_parado < ALERTA_MIN:
            continue

        # Na garagem? Ignora
        if _dist_m(lat, lon, GARAGEM_LAT, GARAGEM_LON) < RAIO_GARAGEM:
            continue

        # Tem OS ativa (deslocamento ou execução)?
        cur.execute("""
            SELECT COUNT(*) as qtd FROM ht_os
            WHERE id_tecnico = ? AND status_hub IN ('deslocamento','execucao')
        """, (tid,))
        tem_os = cur.fetchone()["qtd"] > 0

        if tem_os:
            continue  # parado em execução é normal

        # Chave única para não repetir alerta
        chave = f"parada_{tid}_{agora.strftime('%Y%m%d%H')}"
        if chave in ja_alertados:
            continue

        # Geocoding do local
        endereco = _geo(round(lat, 4), round(lon, 4)) or f"{lat:.4f},{lon:.4f}"
        maps_url  = f"https://maps.google.com/?q={lat},{lon}"
        nome      = tec["tecnico_nome"]
        placa     = tec["placa"] or "—"

        msg = (
            f"⚠️ <b>Parada Suspeita Detectada</b>\n"
            f"{'─'*28}\n"
            f"👤 <b>{nome}</b> · {placa}\n"
            f"⏱ Parado há <b>{int(min_parado)} min</b> sem OS ativa\n"
            f"📍 {endereco}\n"
            f"🗺 <a href='{maps_url}'>Ver no Mapa</a>\n"
            f"🕐 {agora.strftime('%H:%M')}"
        )
        enviar_telegram(msg)

        # Registra alerta enviado
        cur.execute("""
            INSERT OR IGNORE INTO ht_alertas_enviados (chave, enviado_em)
            VALUES (?, datetime('now','-3 hours'))
        """, (chave,))
        db.commit()
        logger.info(f"Alerta parada suspeita: {nome} ({int(min_parado)}min)")

    db.close()

if __name__ == "__main__":
    verificar_paradas()
