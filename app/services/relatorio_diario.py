"""
Relatório diário de frota — enviado às 19h via cron.
Resumo por técnico: KM, OS, paradas longas, vel máxima.
"""
import sqlite3, math, os, logging
from datetime import datetime, timedelta
from app.services.notificador import enviar_telegram

logger = logging.getLogger(__name__)
DB = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"

def get_db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

def _dist(la1, lo1, la2, lo2):
    R = 6371
    dlat = math.radians(la2-la1); dlon = math.radians(lo2-lo1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(la1))*math.cos(math.radians(la2))*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def gerar_relatorio():
    db  = get_db()
    cur = db.cursor()
    hoje = (datetime.utcnow() - timedelta(hours=3)).strftime("%Y-%m-%d")

    # Técnicos ativos hoje (com GPS)
    cur.execute("""
        SELECT DISTINCT u.id, u.nome
        FROM ht_gps_track g
        JOIN ht_usuarios u ON u.id = g.id_tecnico
        WHERE date(g.registrado_em) = ?
        AND u.ativo = 1
        ORDER BY u.nome
    """, (hoje,))
    tecnicos = cur.fetchall()

    if not tecnicos:
        db.close()
        return

    linhas = [f"📊 <b>Relatório Diário de Frota</b>\n📅 {hoje}\n{'─'*28}"]

    for tec in tecnicos:
        tid  = tec["id"]
        nome = tec["nome"].split()[0]  # primeiro nome

        # Pontos GPS do dia
        cur.execute("""
            SELECT lat, lon, velocidade, registrado_em
            FROM ht_gps_track
            WHERE id_tecnico = ? AND date(registrado_em) = ?
            ORDER BY id ASC
        """, (tid, hoje))
        pontos = cur.fetchall()

        if not pontos:
            continue

        # KM total
        km = 0.0
        for i in range(1, len(pontos)):
            km += _dist(pontos[i-1]["lat"], pontos[i-1]["lon"], pontos[i]["lat"], pontos[i]["lon"])

        # Vel máxima
        vel_max = max((p["velocidade"] or 0) * 3.6 for p in pontos)

        # Paradas longas (>20min fora de OS)
        cur.execute("""
            SELECT COUNT(*) as qtd FROM ht_os
            WHERE id_tecnico = ? AND status_hub = 'finalizada'
            AND date(data_agenda) = ?
        """, (tid, hoje))
        os_fin = cur.fetchone()["qtd"]

        # Detectar paradas longas
        paradas_longas = 0
        em_parada = False
        ini_parada = None
        for p in pontos:
            if (p["velocidade"] or 0) < 2:
                if not em_parada:
                    em_parada = True
                    ini_parada = datetime.fromisoformat(p["registrado_em"])
            else:
                if em_parada:
                    dur = (datetime.fromisoformat(p["registrado_em"]) - ini_parada).total_seconds() / 60
                    if dur > 20:
                        paradas_longas += 1
                    em_parada = False

        # Tempo em campo
        t_ini = pontos[0]["registrado_em"][11:16]
        t_fim = pontos[-1]["registrado_em"][11:16]

        icone_vel = "🔴" if vel_max > 80 else "🟡" if vel_max > 60 else "🟢"
        vel_max_str = f"{vel_max:.0f} km/h"
        icone_par = "⚠️" if paradas_longas > 2 else "✅"

        linhas.append(
            f"\n👤 <b>{nome}</b>\n"
            f"  🕐 Campo: {t_ini}–{t_fim}\n"
            f"  🛣 KM: <b>{km:.1f} km</b>\n"
            f"  ✅ OS finalizadas: <b>{os_fin}</b>\n"
            f"  {icone_vel} Vel. máx: <b>{vel_max_str}</b>\n"
            f"  {icone_par} Paradas longas: <b>{paradas_longas}</b>"
        )

    linhas.append(f"\n{'─'*28}\n🤖 HubTecnico · {(datetime.utcnow() - timedelta(hours=3)).strftime('%H:%M')}")
    msg = "\n".join(linhas)
    enviar_telegram(msg)
    logger.info("Relatório diário enviado.")
    db.close()

if __name__ == "__main__":
    gerar_relatorio()
