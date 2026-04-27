"""
cron_sync_estoque.py — Sync estoque técnicos do IXC → SQLite
"""
import sys, sqlite3
sys.path.insert(0, "/opt/automacoes/cliquedf/tecnico")
from app.services.ixc_db import ixc_select
from datetime import datetime

DB = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"

def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    prod_map = {r["ixc_produto_id"]: r["id"] for r in conn.execute(
        "SELECT id, ixc_produto_id FROM ht_produtos WHERE ixc_produto_id > 0"
    ).fetchall()}

    tecnicos = conn.execute(
        "SELECT id, nome, ixc_almox_id FROM ht_usuarios WHERE ixc_almox_id > 0"
    ).fetchall()

    total = 0
    for tec in tecnicos:
        saldos = ixc_select(f"""
            SELECT id_produto, saldo, produto_descricao, produto_unidade, produto_tipo
            FROM estoque_produtos_almox_filial
            WHERE id_almox = {tec['ixc_almox_id']} AND produto_ativo = 'S'
        """)
        conn.execute("UPDATE ht_estoque_tecnico SET quantidade=0 WHERE id_tecnico=?", (tec['id'],))
        for s in saldos:
            local_id = prod_map.get(s["id_produto"])
            if not local_id:
                conn.execute(
                    "INSERT OR IGNORE INTO ht_produtos (nome, unidade, tipo, ativo, ixc_produto_id) VALUES (?,?,?,1,?)",
                    (s["produto_descricao"], s.get("produto_unidade","un"), s.get("produto_tipo","O"), s["id_produto"])
                )
                conn.commit()
                row = conn.execute("SELECT id FROM ht_produtos WHERE ixc_produto_id=?", (s["id_produto"],)).fetchone()
                if row:
                    local_id = row["id"]
                    prod_map[s["id_produto"]] = local_id
            if not local_id:
                continue
            conn.execute("""
                INSERT INTO ht_estoque_tecnico (id_tecnico, id_produto, quantidade, ixc_almox_id, ultima_atualizacao)
                VALUES (?, ?, ?, ?, datetime('now','-3 hours'))
                ON CONFLICT(id_tecnico, id_produto) DO UPDATE SET
                    quantidade=excluded.quantidade,
                    ultima_atualizacao=excluded.ultima_atualizacao
            """, (tec['id'], local_id, float(s["saldo"]), tec['ixc_almox_id']))
            total += 1
        conn.commit()

    # Sync principal (almox 1)
    saldos_p = ixc_select("SELECT id_produto, saldo FROM estoque_produtos_almox_filial WHERE id_almox=1 AND produto_ativo='S'")
    for s in saldos_p:
        local_id = prod_map.get(s["id_produto"])
        if not local_id:
            continue
        conn.execute("""
            INSERT INTO ht_estoque_principal (id_produto, quantidade, ixc_almox_id)
            VALUES (?, ?, 1)
            ON CONFLICT(id_produto) DO UPDATE SET quantidade=excluded.quantidade
        """, (local_id, float(s["saldo"])))
    conn.commit()
    conn.close()
    print(f"[{datetime.now().strftime('%d/%m/%Y %H:%M')}] sync_estoque: {total} itens | {len(tecnicos)} técnicos")

if __name__ == "__main__":
    run()
