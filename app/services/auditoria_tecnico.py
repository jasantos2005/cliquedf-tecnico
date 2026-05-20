"""
auditoria_tecnico.py — Monitoramento completo por técnico
- OS: status Hub vs IXC
- Estoque: quantidades Hub vs IXC  
- Materiais: baixas nas OS
- GPS: velocidade máxima, paradas longas
Roda a cada hora via cron.
"""
import sqlite3, os
from datetime import datetime, timedelta
from app.services.ixc_db import ixc_select
from app.services.notificador import enviar_telegram

DB_PATH = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"
TELEGRAM_AILTON = os.getenv("TELEGRAM_AILTON", "2135602169")
VEL_MAX_ALERTA = 120  # km/h

def _db():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db

def auditar_os_status(id_tecnico, nome_tecnico, dias=3):
    """Verifica se status das OS no Hub bate com o IXC."""
    db = _db()
    os_hub = db.execute("""
        SELECT ixc_os_id, status_hub, cliente_nome
        FROM ht_os
        WHERE id_tecnico=? AND status_hub NOT IN ('finalizada','cancelada')
        AND DATE(data_agenda) >= DATE('now','-3 hours',? || ' days')
    """, (id_tecnico, f'-{dias}')).fetchall()
    db.close()

    if not os_hub:
        return []

    os_ids = [r['ixc_os_id'] for r in os_hub]
    ph = ','.join(['%s']*len(os_ids))
    os_ixc = ixc_select(f"SELECT id, status FROM ixcprovedor.su_oss_chamado WHERE id IN ({ph})", tuple(os_ids))
    ixc_map = {r['id']: r['status'] for r in os_ixc}

    STATUS_MAP = {'A':'pendente','AG':'agendada','AS':'deslocamento','E':'execucao','F':'finalizada','RAG':'reagendada','P':'pendente'}
    divergencias = []
    for r in os_hub:
        ixc_status_raw = ixc_map.get(r['ixc_os_id'], '')
        ixc_status = STATUS_MAP.get(ixc_status_raw, ixc_status_raw)
        hub_status = r['status_hub']
        if ixc_status and ixc_status != hub_status:
            divergencias.append({
                'os': r['ixc_os_id'],
                'cliente': r['cliente_nome'],
                'hub': hub_status,
                'ixc': ixc_status_raw
            })
    return divergencias

def auditar_estoque_tecnico(id_tecnico, nome_tecnico, ixc_almox_id):
    """Compara estoque Hub vs IXC para o técnico."""
    db = _db()
    hub_rows = db.execute("""
        SELECT p.ixc_produto_id, p.nome, e.quantidade
        FROM ht_estoque_tecnico e
        JOIN ht_produtos p ON p.id = e.id_produto
        WHERE e.id_tecnico=? AND e.quantidade > 0
    """, (id_tecnico,)).fetchall()
    db.close()

    if not hub_rows:
        return []

    ixc_rows = ixc_select("""
        SELECT mp.id_produto,
               COALESCE(SUM(CASE WHEN mp.tipo='E' THEN mp.quantidade ELSE -mp.quantidade END),0) as saldo
        FROM ixcprovedor.movimento_produtos mp
        WHERE mp.id_almox = %s
        GROUP BY mp.id_produto
        HAVING saldo > 0
    """, (ixc_almox_id,))
    ixc_map = {r['id_produto']: float(r['saldo']) for r in ixc_rows}

    divergencias = []
    for r in hub_rows:
        hub_qtd = float(r['quantidade'])
        ixc_qtd = ixc_map.get(r['ixc_produto_id'], 0)
        diff = abs(hub_qtd - ixc_qtd)
        tolerancia = max(5, hub_qtd * 0.10)  # 10% ou 5 unidades
        if diff > tolerancia:  # tolerância de 10%
            divergencias.append({
                'produto': r['nome'],
                'hub': hub_qtd,
                'ixc': ixc_qtd,
                'diff': diff
            })
    return divergencias[:10]  # limitar a 10 divergências

def auditar_velocidade(id_tecnico, nome_tecnico):
    """Verifica picos de velocidade no GPS."""
    db = _db()
    hoje = (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d')
    rows = db.execute("""
        SELECT MAX(velocidade) as vel_max, 
               COUNT(CASE WHEN velocidade > ? THEN 1 END) as picos
        FROM ht_gps_track
        WHERE id_tecnico=? AND DATE(registrado_em)=?
    """, (VEL_MAX_ALERTA, id_tecnico, hoje)).fetchone()
    db.close()

    if not rows or not rows['vel_max']:
        return None

    vel_max = float(rows['vel_max'])
    picos = rows['picos'] or 0
    if vel_max > VEL_MAX_ALERTA:
        return {'vel_max': vel_max, 'picos': picos}
    return None


def auditar_sincronismo_os(dias=2):
    """Verifica se OS do Hub batem com IXC em tecnico e status."""
    db = _db()
    # Buscar todos os técnicos do Hub
    tecnicos = db.execute(
        "SELECT id, nome, ixc_funcionario_id FROM ht_usuarios WHERE ixc_funcionario_id > 0 AND nivel=10"
    ).fetchall()
    tec_map = {t["ixc_funcionario_id"]: (t["id"], t["nome"]) for t in tecnicos}
    
    # OS ativas no Hub nos últimos dias
    os_hub = db.execute("""
        SELECT o.ixc_os_id, o.status_hub, o.id_tecnico, o.ixc_tecnico_id,
               o.cliente_nome, u.nome as tecnico_nome
        FROM ht_os o
        LEFT JOIN ht_usuarios u ON u.id = o.id_tecnico
        WHERE o.status_hub NOT IN ('finalizada','cancelada')
        AND DATE(o.data_agenda) >= DATE('now','-3 hours','-" + str(dias) + "' || ' days')
    """).fetchall()
    db.close()

    if not os_hub:
        return []

    os_ids = [r["ixc_os_id"] for r in os_hub]
    ph = ",".join(["%s"]*len(os_ids))
    os_ixc = ixc_select(
        f"SELECT id, status, id_tecnico FROM ixcprovedor.su_oss_chamado WHERE id IN ({ph})",
        tuple(os_ids)
    )
    ixc_map = {r["id"]: r for r in os_ixc}

    STATUS_MAP = {"A":"pendente","AG":"agendada","AS":"deslocamento",
                  "E":"execucao","F":"finalizada","RAG":"reagendada","P":"pendente"}

    divergencias = []
    for r in os_hub:
        ixc = ixc_map.get(r["ixc_os_id"])
        if not ixc:
            continue

        ixc_status = STATUS_MAP.get(ixc["status"], ixc["status"])
        ixc_tec_id = ixc["id_tecnico"]
        hub_tec_id = r["ixc_tecnico_id"]
        hub_status = r["status_hub"]

        problemas = []

        # Verificar técnico
        if ixc_tec_id != hub_tec_id:
            ixc_tec_nome = tec_map.get(ixc_tec_id, (None, f"ID:{ixc_tec_id}"))[1]
            hub_tec_nome = r["tecnico_nome"] or "Sem técnico"
            problemas.append(f"Técnico: Hub={hub_tec_nome} | IXC={ixc_tec_nome}")

        # Verificar status
        if ixc_status != hub_status:
            problemas.append(f"Status: Hub={hub_status} | IXC={ixc_status}")

        if problemas:
            divergencias.append({
                "os": r["ixc_os_id"],
                "cliente": r["cliente_nome"],
                "problemas": problemas
            })

    return divergencias

def rodar_auditoria_tecnico():
    from dotenv import load_dotenv
    load_dotenv('/opt/automacoes/cliquedf/tecnico/.env')

    db = _db()
    tecnicos = db.execute("""
        SELECT id, nome, ixc_almox_id 
        FROM ht_usuarios 
        WHERE ativo=1 AND ixc_almox_id > 0 AND nivel=10
    """).fetchall()
    db.close()

    now = (datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y %H:%M')
    alertas = []

    # Validar sincronismo OS Hub vs IXC (global, não por técnico)
    div_sync = auditar_sincronismo_os(dias=2)
    if div_sync:
        linhas_sync = ["OS DESSINCRONIZADAS: " + str(len(div_sync))]
        for d in div_sync[:10]:
            linhas_sync.append(f"  OS #{d['os']} — {d['cliente'][:30]}")
            for p in d['problemas']:
                linhas_sync.append(f"    ⚠️ {p}")
        alertas.append("".join(linhas_sync))

    for tec in tecnicos:
        linhas_tec = []

        # OS Status
        div_os = auditar_os_status(tec['id'], tec['nome'])
        if div_os:
            linhas_tec.append(f"\n📋 <b>OS com status divergente:</b>")
            for d in div_os[:5]:
                linhas_tec.append(f"  OS #{d['os']} — Hub:{d['hub']} | IXC:{d['ixc']}")

        # Estoque
        div_est = auditar_estoque_tecnico(tec['id'], tec['nome'], tec['ixc_almox_id'])
        if div_est:
            linhas_tec.append(f"\n📦 <b>Estoque divergente ({len(div_est)} itens):</b>")
            for d in div_est[:5]:
                linhas_tec.append(f"  {d['produto'][:35]}: Hub:{d['hub']:.0f} | IXC:{d['ixc']:.0f}")

        # Velocidade
        vel = auditar_velocidade(tec['id'], tec['nome'])
        if vel:
            linhas_tec.append(f"\n🚨 <b>Velocidade alta:</b> {vel['vel_max']:.0f}km/h ({vel['picos']} picos acima de {VEL_MAX_ALERTA}km/h)")

        if linhas_tec:
            alertas.append(f"\n👤 <b>{tec['nome']}</b>" + "".join(linhas_tec))

    if not alertas:
        print(f"[{now}] Auditoria técnicos OK — sem divergências")
        return

    msg = f"🔍 <b>AUDITORIA TÉCNICOS — {now}</b>\n" + "".join(alertas)
    enviar_telegram(msg, chat_id=TELEGRAM_AILTON)
    print(f"[{now}] Alerta enviado — {len(alertas)} técnico(s) com divergências")

if __name__ == "__main__":
    rodar_auditoria_tecnico()
