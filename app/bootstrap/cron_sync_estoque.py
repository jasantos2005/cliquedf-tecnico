"""
cron_sync_estoque.py — Sync estoque técnicos do IXC → SQLite
FIX: SUM(saldo) + GROUP BY para consolidar múltiplas linhas do mesmo produto
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
        # FIX: SUM(saldo) + GROUP BY para consolidar múltiplas linhas do mesmo produto
        saldos = ixc_select(
            """SELECT id_produto, SUM(saldo) AS saldo,
                      MAX(produto_descricao) AS produto_descricao,
                      MAX(produto_unidade)   AS produto_unidade,
                      MAX(produto_tipo)      AS produto_tipo
               FROM estoque_produtos_almox_filial
               WHERE id_almox = %s AND produto_ativo = 'S'
               GROUP BY id_produto""",
            (tec['ixc_almox_id'],)
        )

        # Zera antes de reprocessar
        conn.execute("UPDATE ht_estoque_tecnico SET quantidade=0 WHERE id_tecnico=?", (tec['id'],))

        for s in saldos:
            local_id = prod_map.get(s["id_produto"])
            if not local_id:
                conn.execute(
                    "INSERT OR IGNORE INTO ht_produtos (nome, unidade, tipo, ativo, ixc_produto_id) VALUES (?,?,?,1,?)",
                    (s["produto_descricao"], s.get("produto_unidade", "un"), s.get("produto_tipo", "O"), s["id_produto"])
                )
                conn.commit()
                row = conn.execute("SELECT id FROM ht_produtos WHERE ixc_produto_id=?", (s["id_produto"],)).fetchone()
                if row:
                    local_id = row["id"]
                    prod_map[s["id_produto"]] = local_id

            if not local_id:
                continue

            conn.execute("""
                INSERT INTO ht_estoque_tecnico
                    (id_tecnico, id_produto, quantidade, ixc_almox_id, ultima_atualizacao)
                VALUES (?, ?, ?, ?, datetime('now','-3 hours'))
                ON CONFLICT(id_tecnico, id_produto) DO UPDATE SET
                    quantidade         = excluded.quantidade,
                    ultima_atualizacao = excluded.ultima_atualizacao
            """, (tec['id'], local_id, float(s["saldo"]), tec['ixc_almox_id']))
            total += 1

        conn.commit()

    # Sync estoque principal (almox 1) — também com SUM + GROUP BY
    saldos_p = ixc_select(
        """SELECT id_produto, SUM(saldo) AS saldo
           FROM estoque_produtos_almox_filial
           WHERE id_almox = 1 AND produto_ativo = 'S'
           GROUP BY id_produto"""
    )
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


def sync_status_requisicoes():
    """Sincroniza status das requisicoes pendentes com o IXC."""
    try:
        import sqlite3 as _sq
        conn = _sq.connect(DB)
        conn.row_factory = _sq.Row
        reqs = conn.execute(
            "SELECT id, ixc_requisicao_id FROM ht_requisicoes WHERE status = ? AND ixc_requisicao_id > 0",
            ("pendente",)
        ).fetchall()
        if not reqs:
            conn.close()
            return
        ids = ",".join(str(r["ixc_requisicao_id"]) for r in reqs)
        ixc_rows = ixc_select(f"SELECT id, status FROM requisicao_material WHERE id IN ({ids})")
        ixc_map = {r["id"]: r["status"] for r in ixc_rows}
        STATUS_MAP = {"C": "cancelada", "F": "aprovada", "A": "pendente", "P": "pendente"}
        updated = 0
        for r in reqs:
            ixc_status = ixc_map.get(r["ixc_requisicao_id"])
            if not ixc_status:
                continue
            local_status = STATUS_MAP.get(ixc_status, "pendente")
            if local_status != "pendente":
                conn.execute("UPDATE ht_requisicoes SET status=? WHERE id=?", (local_status, r["id"]))
                updated += 1
                print(f"  [REQ] #{r['id']} -> {local_status}")
        conn.commit()
        conn.close()
        if updated:
            print(f"  [REQ] {updated} requisicoes atualizadas")
    except Exception as e:
        print(f"  [REQ] ERRO: {e}")

sync_status_requisicoes()
