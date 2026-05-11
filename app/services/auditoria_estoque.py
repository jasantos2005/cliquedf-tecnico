"""
auditoria_estoque.py — Valida sincronismo Hub ↔ IXC para materiais e comodatos
Roda a cada hora via cron. Envia alerta no Telegram se encontrar divergência.
"""
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
from app.services.ixc_db import ixc_select, ixc_insert
from app.services.notificador import enviar_telegram

DB_PATH = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"

def _db():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db

def auditar_materiais(dias=7):
    """Compara materiais registrados no Hub com movimento_produtos do IXC."""
    db = _db()
    desde = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    os_mats = db.execute("""
        SELECT m.ixc_os_id, p.ixc_produto_id, p.nome as produto_nome,
               m.quantidade as hub_qtd, u.nome as tecnico,
               e.finalizada_em
        FROM ht_os_materiais m
        JOIN ht_produtos p ON p.id = m.id_produto
        JOIN ht_os o ON o.ixc_os_id = m.ixc_os_id
        JOIN ht_os_execucao e ON e.ixc_os_id = m.ixc_os_id
        JOIN ht_usuarios u ON u.id = m.id_tecnico
        WHERE DATE(e.finalizada_em) >= ?
        ORDER BY m.ixc_os_id
    """, (desde,)).fetchall()
    db.close()

    if not os_mats:
        return []

    os_ids = list(set(r['ixc_os_id'] for r in os_mats))
    placeholders = ','.join(['%s'] * len(os_ids))

    # Buscar movimentos detalhados para permitir auto-correção
    mov_ixc = ixc_select(f"""
        SELECT id, id_oss_chamado, id_produto, quantidade, qtde_saida
        FROM ixcprovedor.movimento_produtos
        WHERE id_oss_chamado IN ({placeholders})
        AND tipo = 'S'
        ORDER BY id_oss_chamado, id_produto
    """, tuple(os_ids))

    # Agrupar por OS+produto (soma quantidade real)
    from collections import defaultdict
    ixc_idx = defaultdict(float)
    ixc_corrigiveis = defaultdict(list)  # ids com quantidade=0 mas qtde_saida>0
    for r in mov_ixc:
        key = (r['id_oss_chamado'], r['id_produto'])
        ixc_idx[key] += float(r['quantidade'])
        if float(r['quantidade']) == 0 and float(r['qtde_saida']) > 0:
            ixc_corrigiveis[key].append(r['id'])

    # Auto-corrigir quantidade=0
    corrigidos = []
    for key, ids in ixc_corrigiveis.items():
        ph = ','.join(['%s'] * len(ids))
        ixc_insert(f"""
            UPDATE ixcprovedor.movimento_produtos
            SET quantidade = qtde_saida
            WHERE id IN ({ph}) AND quantidade = 0 AND qtde_saida > 0
        """, tuple(ids))
        # Recalcular após correção
        rows_fix = ixc_select(f"""
            SELECT SUM(quantidade) as qtd FROM ixcprovedor.movimento_produtos
            WHERE id IN ({ph})
        """, tuple(ids))
        nova_qtd = float(rows_fix[0]['qtd'] or 0)
        ixc_idx[key] += nova_qtd
        corrigidos.append({'os': key[0], 'id_produto': key[1], 'ids': ids})

    divergencias = []
    for r in os_mats:
        key = (r['ixc_os_id'], r['ixc_produto_id'])
        ixc_qtd = ixc_idx.get(key, None)
        hub_qtd = float(r['hub_qtd'])

        if key not in ixc_idx:
            divergencias.append({
                'tipo': 'MATERIAL_NAO_BAIXADO',
                'os': r['ixc_os_id'],
                'tecnico': r['tecnico'],
                'produto': r['produto_nome'],
                'hub_qtd': hub_qtd,
                'ixc_qtd': 0,
                'finalizada_em': r['finalizada_em'],
            })
        elif abs(ixc_qtd - hub_qtd) > 0.001:
            divergencias.append({
                'tipo': 'QUANTIDADE_DIVERGENTE',
                'os': r['ixc_os_id'],
                'tecnico': r['tecnico'],
                'produto': r['produto_nome'],
                'hub_qtd': hub_qtd,
                'ixc_qtd': ixc_qtd,
                'finalizada_em': r['finalizada_em'],
            })

    return divergencias, corrigidos

def auditar_comodatos(dias=7):
    """Verifica se comodatos registrados no Hub existem no IXC."""
    db = _db()
    desde = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    os_comodatos = db.execute("""
        SELECT m.ixc_os_id, p.ixc_produto_id, p.nome as produto_nome,
               u.nome as tecnico, e.finalizada_em
        FROM ht_os_materiais m
        JOIN ht_produtos p ON p.id = m.id_produto
        JOIN ht_os o ON o.ixc_os_id = m.ixc_os_id
        JOIN ht_os_execucao e ON e.ixc_os_id = m.ixc_os_id
        JOIN ht_usuarios u ON u.id = m.id_tecnico
        WHERE m.tipo_uso = 'comodato'
        AND DATE(e.finalizada_em) >= ?
    """, (desde,)).fetchall()
    db.close()

    if not os_comodatos:
        return []

    os_ids = list(set(r['ixc_os_id'] for r in os_comodatos))
    placeholders = ','.join(['%s'] * len(os_ids))

    comodatos_ixc = ixc_select(f"""
        SELECT mc.id_os, mc.id_produto
        FROM ixcprovedor.movimento_comodatos mc
        WHERE mc.id_os IN ({placeholders})
    """, tuple(os_ids))

    ixc_set = {(r['id_os'], r['id_produto']) for r in comodatos_ixc}

    divergencias = []
    for r in os_comodatos:
        key = (r['ixc_os_id'], r['ixc_produto_id'])
        if key not in ixc_set:
            divergencias.append({
                'tipo': 'COMODATO_NAO_REGISTRADO',
                'os': r['ixc_os_id'],
                'tecnico': r['tecnico'],
                'produto': r['produto_nome'],
                'finalizada_em': r['finalizada_em'],
            })

    return divergencias

def auditar_duplicatas(dias=7):
    """Detecta materiais ou comodatos duplicados no IXC para OS recentes."""
    db = _db()
    desde = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
    os_ids_row = db.execute("""
        SELECT DISTINCT o.ixc_os_id
        FROM ht_os o
        JOIN ht_os_execucao e ON e.ixc_os_id = o.ixc_os_id
        WHERE o.status_hub = 'finalizada'
        AND DATE(e.finalizada_em) >= ?
    """, (desde,)).fetchall()
    db.close()

    if not os_ids_row:
        return []

    os_ids = [r['ixc_os_id'] for r in os_ids_row]
    placeholders = ','.join(['%s'] * len(os_ids))

    rows = ixc_select(f"""
        SELECT id_oss_chamado, id_produto, COUNT(*) as cnt,
               SUM(quantidade) as qtd_total
        FROM ixcprovedor.movimento_produtos
        WHERE id_oss_chamado IN ({placeholders})
        AND tipo = 'S'
        GROUP BY id_oss_chamado, id_produto
        HAVING cnt > 1
    """, tuple(os_ids))

    divergencias = []
    for r in rows:
        divergencias.append({
            'tipo': 'MOVIMENTO_DUPLICADO',
            'os': r['id_oss_chamado'],
            'id_produto_ixc': r['id_produto'],
            'ocorrencias': r['cnt'],
            'qtd_total': float(r['qtd_total']),
        })

    return divergencias

def rodar_auditoria():
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    divs_mat, autocorrigidos = auditar_materiais(dias=7)
    divs_cod = auditar_comodatos(dias=7)
    divs_dup = auditar_duplicatas(dias=7)

    total = len(divs_mat) + len(divs_cod) + len(divs_dup)

    # Notificar auto-correções
    if autocorrigidos:
        linhas_fix = [f"🔧 <b>AUTO-CORREÇÃO — {now}</b>"]
        linhas_fix.append(f"✅ {len(autocorrigidos)} item(s) corrigidos automaticamente no IXC:")
        for ac in autocorrigidos:
            linhas_fix.append(f"  • OS #{ac['os']} prod_id={ac['id_produto']} ({len(ac['ids'])} registro(s))")
        import os
        chat_pessoal = os.getenv("TELEGRAM_AILTON")
        enviar_telegram("\n".join(linhas_fix), chat_id=chat_pessoal)
        print(f"[{now}] Auto-corrigidos: {len(autocorrigidos)} itens")

    if total == 0:
        print(f"[{now}] Auditoria OK — sem divergências")
        return

    linhas = [f"🔍 <b>AUDITORIA ESTOQUE — {now}</b>", f"⚠️ {total} divergência(s) encontrada(s)\n"]

    if divs_mat:
        linhas.append("📦 <b>MATERIAIS:</b>")
        for d in divs_mat:
            if d['tipo'] == 'MATERIAL_NAO_BAIXADO':
                linhas.append(f"  ❌ OS #{d['os']} [{d['tecnico']}]\n     {d['produto']}\n     Hub: {d['hub_qtd']} | IXC: NÃO BAIXADO")
            else:
                linhas.append(f"  ⚠️ OS #{d['os']} [{d['tecnico']}]\n     {d['produto']}\n     Hub: {d['hub_qtd']} | IXC: {d['ixc_qtd']}")

    if divs_cod:
        linhas.append("\n📱 <b>COMODATOS:</b>")
        for d in divs_cod:
            linhas.append(f"  ❌ OS #{d['os']} [{d['tecnico']}]\n     {d['produto']}\n     Comodato não registrado no IXC")

    if divs_dup:
        linhas.append("\n🔁 <b>DUPLICATAS IXC:</b>")
        for d in divs_dup:
            linhas.append(f"  ⚠️ OS #{d['os']} prod_id={d['id_produto_ixc']} — {d['ocorrencias']}x registrado")

    msg = "\n".join(linhas)
    import os
    chat_pessoal = os.getenv("TELEGRAM_AILTON")
    enviar_telegram(msg, chat_id=chat_pessoal)
    print(f"[{now}] Alerta enviado — {total} divergências")

if __name__ == "__main__":
    rodar_auditoria()
