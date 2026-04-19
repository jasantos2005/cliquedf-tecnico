from fastapi import APIRouter, Depends
from app.services.auth import requer_tecnico, requer_supervisor, get_db
from datetime import datetime

router = APIRouter(prefix="/api/estoque", tags=["estoque"])

def brt(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@router.get("/meu")
def meu_estoque(usuario=Depends(requer_tecnico)):
    db = get_db()
    rows = db.execute("""
        SELECT e.*, p.nome AS produto_nome, p.unidade, p.tipo
        FROM ht_estoque_tecnico e
        JOIN ht_produtos p ON p.id = e.id_produto
        WHERE e.id_tecnico = ? AND e.quantidade > 0
        ORDER BY p.nome
    """, (usuario["id"],)).fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.get("/principal")
def estoque_principal(usuario=Depends(requer_supervisor)):
    db = get_db()
    rows = db.execute("""
        SELECT e.*, p.nome AS produto_nome, p.unidade, p.tipo, p.estoque_minimo
        FROM ht_estoque_principal e
        JOIN ht_produtos p ON p.id = e.id_produto
        ORDER BY p.nome
    """).fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.get("/requisicoes")
def listar_requisicoes(usuario=Depends(requer_supervisor)):
    db = get_db()
    rows = db.execute("""
        SELECT r.*, u.nome AS tecnico_nome
        FROM ht_requisicoes r
        JOIN ht_usuarios u ON u.id = r.id_tecnico
        WHERE r.status = 'pendente'
        ORDER BY r.criada_em DESC
    """).fetchall()
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
