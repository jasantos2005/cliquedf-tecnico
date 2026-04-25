"""
estoque.py — Requisições, estoque técnico e sync IXC
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from app.services.auth import requer_tecnico, requer_supervisor, get_db
from app.services.ixc_db import ixc_select, ixc_insert

router = APIRouter(prefix="/api/estoque", tags=["estoque"])

IXC_ALMOX_PRINCIPAL = 1

def brt():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ── Modelos ───────────────────────────────────────────────────────────────────
class ItemRequisicao(BaseModel):
    id_produto: int
    qtd_solicitada: float
    obs: Optional[str] = ""

class CriarRequisicaoBody(BaseModel):
    itens: List[ItemRequisicao]
    obs: Optional[str] = ""

class AprovarItemBody(BaseModel):
    id_item: int
    qtd_aprovada: float

class AprovarRequisicaoBody(BaseModel):
    id_requisicao: int
    itens: List[AprovarItemBody]


# ── Estoque do técnico ────────────────────────────────────────────────────────
@router.get("/meu")
def meu_estoque(usuario=Depends(requer_tecnico)):
    db = get_db()
    rows = db.execute("""
        SELECT e.id, e.quantidade, e.ultima_atualizacao,
               p.nome AS produto_nome, p.unidade, p.tipo, p.ixc_produto_id
        FROM ht_estoque_tecnico e
        JOIN ht_produtos p ON p.id = e.id_produto
        WHERE e.id_tecnico = ? AND e.quantidade > 0
        ORDER BY p.nome
    """, (usuario["id"],)).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Estoque principal ─────────────────────────────────────────────────────────
@router.get("/principal")
def estoque_principal(usuario=Depends(requer_supervisor)):
    db = get_db()
    rows = db.execute("""
        SELECT e.quantidade, p.id, p.nome AS produto_nome, p.unidade,
               p.tipo, p.estoque_minimo, p.ixc_produto_id
        FROM ht_estoque_principal e
        JOIN ht_produtos p ON p.id = e.id_produto
        ORDER BY p.nome
    """).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Produtos disponíveis para requisição ─────────────────────────────────────
@router.get("/produtos")
def listar_produtos(usuario=Depends(requer_tecnico)):
    db = get_db()
    rows = db.execute("""
        SELECT p.id, p.nome, p.unidade, p.tipo, e.quantidade AS saldo_principal
        FROM ht_produtos p
        LEFT JOIN ht_estoque_principal e ON e.id_produto = p.id
        WHERE p.ativo = 1
        ORDER BY p.nome
    """).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Criar requisição (técnico) ────────────────────────────────────────────────
@router.post("/requisicao")
def criar_requisicao(body: CriarRequisicaoBody, usuario=Depends(requer_tecnico)):
    if not body.itens:
        raise HTTPException(400, "Nenhum item informado")

    db = get_db()
    try:
        # Busca almox do técnico
        tec = db.execute(
            "SELECT ixc_funcionario_id, ixc_almox_id FROM ht_usuarios WHERE id=?",
            (usuario["id"],)
        ).fetchone()
        if not tec or not tec["ixc_almox_id"]:
            raise HTTPException(400, "Técnico sem almoxarifado configurado")

        ixc_func_id  = tec["ixc_funcionario_id"]
        ixc_almox_id = tec["ixc_almox_id"]

        # Cria requisição local
        cur = db.execute("""
            INSERT INTO ht_requisicoes (id_tecnico, status, obs, id_almox_destino, criada_em)
            VALUES (?, 'pendente', ?, ?, datetime('now','-3 hours'))
        """, (usuario["id"], body.obs or "", ixc_almox_id))
        req_id = cur.lastrowid

        # Itens locais
        for item in body.itens:
            prod = db.execute(
                "SELECT ixc_produto_id FROM ht_produtos WHERE id=?", (item.id_produto,)
            ).fetchone()
            db.execute("""
                INSERT INTO ht_requisicao_itens (id_requisicao, id_produto, qtd_solicitada, obs)
                VALUES (?, ?, ?, ?)
            """, (req_id, item.id_produto, item.qtd_solicitada, item.obs or ""))

        db.commit()

        # Cria no IXC
        try:
            ixc_insert(
                """INSERT INTO ixcprovedor.requisicao_material
                   (id_tecnico, id_almox, `data`, status, id_filial, obs, pref_almox, tipo)
                   VALUES (%s, %s, %s, 'A', 1, %s, 1, 'M')""",
                (ixc_func_id, ixc_almox_id, brt(), body.obs or "")
            )
            # Busca id gerado no IXC
            ixc_req = ixc_select(
                "SELECT id FROM requisicao_material WHERE id_tecnico=%s ORDER BY id DESC LIMIT 1" % ixc_func_id
            )
            if ixc_req:
                ixc_req_id = ixc_req[0]["id"]
                db.execute(
                    "UPDATE ht_requisicoes SET ixc_requisicao_id=? WHERE id=?",
                    (ixc_req_id, req_id)
                )
                # Itens no IXC
                itens_local = db.execute(
                    "SELECT ri.*, p.ixc_produto_id FROM ht_requisicao_itens ri JOIN ht_produtos p ON p.id=ri.id_produto WHERE ri.id_requisicao=?",
                    (req_id,)
                ).fetchall()
                for it in itens_local:
                    if it["ixc_produto_id"]:
                        ixc_insert(
                            """INSERT INTO ixcprovedor.requisicao_material_item
                               (id_produto, qtde, qtde_saldo, status, id_requisicao, descricao)
                               VALUES (%s, %s, %s, 'A', %s, '')""",
                            (it["ixc_produto_id"], it["qtd_solicitada"], it["qtd_solicitada"], ixc_req_id)
                        )
                db.commit()
        except Exception as e:
            # IXC falhou mas requisição local foi salva
            db.execute(
                "UPDATE ht_requisicoes SET obs=? WHERE id=?",
                (f"[IXC erro: {e}] {body.obs or ''}", req_id)
            )
            db.commit()

        return {"ok": True, "id_requisicao": req_id, "msg": "Requisição criada com sucesso"}
    finally:
        db.close()


# ── Listar requisições pendentes (supervisor) ─────────────────────────────────
@router.get("/requisicoes")
def listar_requisicoes(usuario=Depends(requer_supervisor)):
    db = get_db()
    rows = db.execute("""
        SELECT r.*, u.nome AS tecnico_nome, u.ixc_almox_id
        FROM ht_requisicoes r
        JOIN ht_usuarios u ON u.id = r.id_tecnico
        WHERE r.status = 'pendente'
        ORDER BY r.criada_em DESC
    """).fetchall()
    result = []
    for r in rows:
        itens = db.execute("""
            SELECT ri.*, p.nome AS produto_nome, p.unidade, p.ixc_produto_id,
                   ep.quantidade AS saldo_principal
            FROM ht_requisicao_itens ri
            JOIN ht_produtos p ON p.id = ri.id_produto
            LEFT JOIN ht_estoque_principal ep ON ep.id_produto = ri.id_produto
            WHERE ri.id_requisicao = ?
        """, (r["id"],)).fetchall()
        result.append({**dict(r), "itens": [dict(i) for i in itens]})
    db.close()
    return result


# ── Minhas requisições (técnico) ──────────────────────────────────────────────
@router.get("/minhas-requisicoes")
def minhas_requisicoes(usuario=Depends(requer_tecnico)):
    db = get_db()
    rows = db.execute("""
        SELECT r.* FROM ht_requisicoes r
        WHERE r.id_tecnico = ?
        ORDER BY r.criada_em DESC LIMIT 20
    """, (usuario["id"],)).fetchall()
    result = []
    for r in rows:
        itens = db.execute("""
            SELECT ri.*, p.nome AS produto_nome, p.unidade
            FROM ht_requisicao_itens ri
            JOIN ht_produtos p ON p.id = ri.id_produto
            WHERE ri.id_requisicao = ?
        """, (r["id"],)).fetchall()
        result.append({**dict(r), "itens": [dict(i) for i in itens]})
    db.close()
    return result


# ── Aprovar requisição (supervisor) ──────────────────────────────────────────
@router.post("/aprovar")
def aprovar_requisicao(body: AprovarRequisicaoBody, usuario=Depends(requer_supervisor)):
    db = get_db()
    try:
        req = db.execute(
            "SELECT * FROM ht_requisicoes WHERE id=? AND status='pendente'",
            (body.id_requisicao,)
        ).fetchone()
        if not req:
            raise HTTPException(404, "Requisição não encontrada ou já processada")

        tec = db.execute(
            "SELECT ixc_funcionario_id, ixc_almox_id FROM ht_usuarios WHERE id=?",
            (req["id_tecnico"],)
        ).fetchone()
        ixc_almox_destino = tec["ixc_almox_id"] if tec else 0

        total_aprovado = 0
        for item_ap in body.itens:
            item = db.execute(
                "SELECT * FROM ht_requisicao_itens WHERE id=?", (item_ap.id_item,)
            ).fetchone()
            if not item:
                continue

            qtd = item_ap.qtd_aprovada
            if qtd <= 0:
                continue

            # Atualiza item local
            db.execute(
                "UPDATE ht_requisicao_itens SET qtd_aprovada=?, qtd_entregue=? WHERE id=?",
                (qtd, qtd, item_ap.id_item)
            )

            # Baixa estoque principal
            db.execute("""
                UPDATE ht_estoque_principal SET quantidade = quantidade - ?
                WHERE id_produto = ? AND quantidade >= ?
            """, (qtd, item["id_produto"], qtd))

            # Credita no técnico
            db.execute("""
                INSERT INTO ht_estoque_tecnico (id_tecnico, id_produto, quantidade, ixc_almox_id, ultima_atualizacao)
                VALUES (?, ?, ?, ?, datetime('now','-3 hours'))
                ON CONFLICT(id_tecnico, id_produto) DO UPDATE SET
                    quantidade = quantidade + ?,
                    ultima_atualizacao = datetime('now','-3 hours')
            """, (req["id_tecnico"], item["id_produto"], qtd, ixc_almox_destino, qtd))

            total_aprovado += 1

            # Transf no IXC
            if req["ixc_requisicao_id"] and tec:
                prod = db.execute(
                    "SELECT ixc_produto_id FROM ht_produtos WHERE id=?", (item["id_produto"],)
                ).fetchone()
                if prod and prod["ixc_produto_id"]:
                    try:
                        ixc_insert(
                            """INSERT INTO ixcprovedor.transf_almox
                               (`data`, obs, id_produto, qtde, id_filial, id_almox_saida,
                                id_almox_entrada, id_filial_entrada, id_requisicao_material, status, operador)
                               VALUES (%s, %s, %s, %s, 1, %s, %s, 1, %s, 'F', 0)""",
                            (brt(), f"Req #{req['ixc_requisicao_id']}",
                             prod["ixc_produto_id"], qtd,
                             IXC_ALMOX_PRINCIPAL, ixc_almox_destino,
                             req["ixc_requisicao_id"])
                        )
                    except Exception as e:
                        pass  # log apenas

        # Atualiza status da requisição
        db.execute("""
            UPDATE ht_requisicoes SET status='aprovada', aprovada_em=datetime('now','-3 hours'),
            aprovado_por=? WHERE id=?
        """, (usuario["id"], body.id_requisicao))

        db.commit()
        return {"ok": True, "msg": f"{total_aprovado} itens aprovados e transferidos"}
    finally:
        db.close()


# ── Sync estoque do IXC ───────────────────────────────────────────────────────
@router.post("/sync")
def sync_estoque(usuario=Depends(requer_supervisor)):
    db = get_db()
    try:
        tecnicos = db.execute(
            "SELECT id, ixc_almox_id FROM ht_usuarios WHERE ixc_almox_id > 0"
        ).fetchall()

        prod_map = {
            r["ixc_produto_id"]: r["id"]
            for r in db.execute("SELECT id, ixc_produto_id FROM ht_produtos WHERE ixc_produto_id > 0").fetchall()
        }

        updated = 0
        for tec in tecnicos:
            saldos = ixc_select(f"""
                SELECT id_produto, saldo FROM estoque_produtos_almox_filial
                WHERE id_almox = {tec['ixc_almox_id']} AND produto_ativo = 'S'
            """)
            for s in saldos:
                local_id = prod_map.get(s["id_produto"])
                if not local_id:
                    continue
                db.execute("""
                    INSERT INTO ht_estoque_tecnico (id_tecnico, id_produto, quantidade, ixc_almox_id, ultima_atualizacao)
                    VALUES (?, ?, ?, ?, datetime('now','-3 hours'))
                    ON CONFLICT(id_tecnico, id_produto) DO UPDATE SET
                        quantidade=excluded.quantidade,
                        ultima_atualizacao=excluded.ultima_atualizacao
                """, (tec["id"], local_id, float(s["saldo"]), tec["ixc_almox_id"]))
                updated += 1
            db.commit()

        # Sync principal
        saldos_p = ixc_select("""
            SELECT id_produto, saldo FROM estoque_produtos_almox_filial
            WHERE id_almox = 1 AND produto_ativo = 'S'
        """)
        for s in saldos_p:
            local_id = prod_map.get(s["id_produto"])
            if not local_id:
                continue
            db.execute("""
                INSERT INTO ht_estoque_principal (id_produto, quantidade, ixc_almox_id)
                VALUES (?, ?, 1)
                ON CONFLICT(id_produto) DO UPDATE SET quantidade=excluded.quantidade
            """, (local_id, float(s["saldo"])))
        db.commit()

        return {"ok": True, "msg": f"Sync concluído — {updated} itens atualizados"}
    finally:
        db.close()
