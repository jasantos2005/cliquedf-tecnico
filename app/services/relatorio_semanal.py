"""
Relatório semanal de frota — enviado sexta-feira às 18h.
Comparativo por técnico: KM, OS, tempo em campo, paradas, vel, desvios.
"""
import sqlite3, math, os, logging
from datetime import datetime, timedelta
from app.services.notificador import enviar_telegram
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

logger = logging.getLogger(__name__)
DB = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"

def get_db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

def _dist_km(la1, lo1, la2, lo2):
    R = 6371
    dlat = math.radians(la2-la1); dlon = math.radians(lo2-lo1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(la1))*math.cos(math.radians(la2))*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def gerar_relatorio():
    db  = get_db()
    cur = db.cursor()
    agora = datetime.utcnow() - timedelta(hours=3)

    # Período: últimos 7 dias (seg a sex)
    data_fim   = agora.strftime("%Y-%m-%d")
    data_ini   = (agora - timedelta(days=6)).strftime("%Y-%m-%d")

    # Técnicos ativos na semana
    cur.execute("""
        SELECT DISTINCT u.id, u.nome
        FROM ht_gps_track g
        JOIN ht_usuarios u ON u.id = g.id_tecnico
        JOIN ht_tecnico_veiculo tv ON tv.id_tecnico = g.id_tecnico
            AND tv.data BETWEEN ? AND ?
        WHERE date(g.registrado_em) BETWEEN ? AND ?
        AND u.ativo = 1
        ORDER BY u.nome
    """, (data_ini, data_fim, data_ini, data_fim))
    tecnicos = cur.fetchall()

    if not tecnicos:
        db.close()
        return

    dados = []
    for tec in tecnicos:
        tid  = tec["id"]
        nome = tec["nome"]

        # GPS da semana
        cur.execute("""
            SELECT lat, lon, velocidade, registrado_em
            FROM ht_gps_track
            WHERE id_tecnico = ?
            AND date(registrado_em) BETWEEN ? AND ?
            ORDER BY id ASC
        """, (tid, data_ini, data_fim))
        pontos = cur.fetchall()

        if not pontos:
            continue

        # KM total — filtra por velocidade implícita (dist/tempo)
        # Se velocidade implícita > 150 km/h = salto GPS, ignora
        km = 0.0
        for i in range(1, len(pontos)):
            try:
                t0 = datetime.fromisoformat(pontos[i-1]["registrado_em"])
                t1 = datetime.fromisoformat(pontos[i]["registrado_em"])
                seg = (t1 - t0).total_seconds()
                if seg <= 0:
                    continue
                d = _dist_km(pontos[i-1]["lat"], pontos[i-1]["lon"], pontos[i]["lat"], pontos[i]["lon"])
                vel_impl = (d / seg) * 3600  # km/h implícita
                if vel_impl > 150:  # salto GPS impossível
                    continue
                km += d
            except:
                continue

        # Vel máxima (m/s → km/h)
        vel_max = max((p["velocidade"] or 0) * 3.6 for p in pontos)

        # OS finalizadas
        cur.execute("""
            SELECT COUNT(*) as qtd FROM ht_os
            WHERE id_tecnico = ? AND status_hub = 'finalizada'
            AND date(data_agenda) BETWEEN ? AND ?
        """, (tid, data_ini, data_fim))
        os_fin = cur.fetchone()["qtd"]

        # Tempo em campo por dia (média)
        dias_campo = {}
        for p in pontos:
            dia = p["registrado_em"][:10]
            if dia not in dias_campo:
                dias_campo[dia] = {"ini": p["registrado_em"], "fim": p["registrado_em"]}
            dias_campo[dia]["fim"] = p["registrado_em"]

        min_campo = 0
        for dia, t in dias_campo.items():
            try:
                t0 = datetime.fromisoformat(t["ini"])
                t1 = datetime.fromisoformat(t["fim"])
                min_campo += (t1 - t0).total_seconds() / 60
            except:
                pass
        dias_ativos = len(dias_campo)
        media_campo_h = (min_campo / 60 / dias_ativos) if dias_ativos else 0

        # Paradas longas (>20min)
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

        # Desvios de rota na semana
        cur.execute("""
            SELECT COUNT(*) as qtd FROM ht_alertas_enviados
            WHERE chave LIKE ? AND date(enviado_em) BETWEEN ? AND ?
        """, (f"desvio_{tid}_%", data_ini, data_fim))
        desvios = cur.fetchone()["qtd"]

        dados.append({
            "nome": nome.split()[0],
            "nome_completo": nome,
            "km": round(km, 1),
            "os_fin": os_fin,
            "vel_max": round(vel_max, 0),
            "media_campo_h": round(media_campo_h, 1),
            "paradas_longas": paradas_longas,
            "desvios": desvios,
            "dias_ativos": dias_ativos,
        })

    if not dados:
        db.close()
        return

    # Ordenar por OS finalizadas (ranking)
    dados.sort(key=lambda x: x["os_fin"], reverse=True)

    # Médias da equipe
    med_km  = sum(d["km"] for d in dados) / len(dados)
    med_os  = sum(d["os_fin"] for d in dados) / len(dados)
    med_vel = sum(d["vel_max"] for d in dados) / len(dados)

    periodo = f"{data_ini[8:]}/{data_ini[5:7]} a {data_fim[8:]}/{data_fim[5:7]}"
    linhas = [
        f"📊 <b>Relatório Semanal de Frota</b>\n"
        f"📅 {periodo}\n"
        f"{'─'*28}"
    ]

    medals = ["🥇", "🥈", "🥉"]
    for i, d in enumerate(dados):
        medal = medals[i] if i < 3 else f"{i+1}."
        icone_km  = "🔴" if d["km"] < med_km * 0.7 else "🟡" if d["km"] < med_km else "🟢"
        icone_os  = "🔴" if d["os_fin"] < med_os * 0.7 else "🟡" if d["os_fin"] < med_os else "🟢"
        icone_vel = "🔴" if d["vel_max"] > 80 else "🟡" if d["vel_max"] > 60 else "🟢"
        icone_par = "⚠️" if d["paradas_longas"] > 5 else "✅"
        icone_dev = "🚨" if d["desvios"] > 2 else "✅"

        linhas.append(
            f"\n{medal} <b>{d['nome']}</b> · {d['dias_ativos']} dias\n"
            f"  {icone_km} KM: <b>{d['km']} km</b>\n"
            f"  {icone_os} OS: <b>{d['os_fin']}</b>\n"
            f"  🕐 Média campo: <b>{d['media_campo_h']}h/dia</b>\n"
            f"  {icone_vel} Vel. máx: <b>{d['vel_max']:.0f} km/h</b>\n"
            f"  {icone_par} Paradas longas: <b>{d['paradas_longas']}</b>\n"
            f"  {icone_dev} Desvios de rota: <b>{d['desvios']}</b>"
        )

    # Destaques
    destaque_os  = max(dados, key=lambda x: x["os_fin"])
    destaque_km  = max(dados, key=lambda x: x["km"])
    destaque_vel = max(dados, key=lambda x: x["vel_max"])

    linhas.append(
        f"\n{'─'*28}\n"
        f"🏆 <b>Destaques da semana</b>\n"
        f"  📋 Mais OS: {destaque_os['nome']} ({destaque_os['os_fin']})\n"
        f"  🛣 Mais KM: {destaque_km['nome']} ({destaque_km['km']} km)\n"
        f"  ⚡ Vel. máx: {destaque_vel['nome']} ({destaque_vel['vel_max']:.0f} km/h)\n"
        f"{'─'*28}\n"
        f"📊 Média equipe: {med_km:.0f} km · {med_os:.0f} OS\n"
        f"🤖 HubTecnico · {agora.strftime('%d/%m %H:%M')}"
    )

    msg = "\n".join(linhas)
    enviar_telegram(msg)
    logger.info("Relatório semanal enviado.")
    db.close()

if __name__ == "__main__":
    gerar_relatorio()
