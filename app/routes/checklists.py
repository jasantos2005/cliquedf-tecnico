from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timedelta, timezone
from app.services.auth import requer_supervisor, get_db

router = APIRouter(prefix="/api/checklists", tags=["checklists"])

ITENS_PADRAO = [
    "Nivel de oleo",
    "Nivel de agua/arrefecimento",
    "Pneus (calibragem e estado)",
    "Freios",
    "Luzes e sinalizacao",
    "Combustivel",
    "Documentos (CRLV, seguro)",
    "Equipamentos de seguranca",
    "Limpeza do veiculo",
]

def brt():
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

def brt_date():
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d")

@router.get("/itens-padrao")
def itens_padrao(usuario=Depends(requer_supervisor)):
    return ITENS_PADRAO

@router.get("")
def listar(
    id_veiculo: Optional[int] = None,
    data: Optional[str] = None,
    tipo: Optional[str] = None,
    usuario=Depends(requer_supervisor)
):
    db = get_db()
    sql = """
        SELECT c.*, v.placa, v.marca_modelo, u.nome as tecnico_nome
        FROM ht_checklists c
        JOIN ht_veiculos v ON v.id = c.id_veiculo
        LEFT JOIN ht_usuarios u ON u.id = c.id_tecnico
        WHERE 1=1
    """
    params = []
    if id_veiculo:
        sql += " AND c.id_veiculo = ?"
        params.append(id_veiculo)
    if data:
        sql += " AND c.data = ?"
        params.append(data)
    if tipo:
        sql += " AND c.tipo = ?"
        params.append(tipo)
    sql += " ORDER BY c.data DESC, c.criado_em DESC LIMIT 100"
    rows = db.execute(sql, params).fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.get("/hoje")
def checklists_hoje(usuario=Depends(requer_supervisor)):
    db = get_db()
    hoje = brt_date()
    rows = db.execute("""
        SELECT c.*, v.placa, v.marca_modelo, u.nome as tecnico_nome
        FROM ht_checklists c
        JOIN ht_veiculos v ON v.id = c.id_veiculo
        LEFT JOIN ht_usuarios u ON u.id = c.id_tecnico
        WHERE c.data = ?
        ORDER BY c.tipo, v.placa
    """, (hoje,)).fetchall()
    db.close()
    return [dict(r) for r in rows]

class ChecklistInput(BaseModel):
    id_veiculo: int
    tipo: str  # "saida" ou "retorno"
    km: Optional[int] = None
    itens: List[dict]  # [{"item": str, "ok": bool, "obs": str}]
    observacao: Optional[str] = None

@router.post("")
def criar(data: ChecklistInput, usuario=Depends(requer_supervisor)):
    db = get_db()
    agora = brt()
    hoje = agora[:10]
    import json
    nao_ok = sum(1 for i in data.itens if not i.get("ok", True))
    db.execute("""
        INSERT INTO ht_checklists
            (id_veiculo, id_tecnico, tipo, data, km, itens_json,
             observacao, itens_nao_ok, criado_em)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        data.id_veiculo, usuario["id"], data.tipo, hoje,
        data.km, json.dumps(data.itens, ensure_ascii=False),
        data.observacao or "", nao_ok, agora
    ))
    db.commit()
    local_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    db.close()
    return {"ok": True, "id": local_id}

@router.get("/{id}")
def buscar(id: int, usuario=Depends(requer_supervisor)):
    db = get_db()
    row = db.execute("""
        SELECT c.*, v.placa, v.marca_modelo, u.nome as tecnico_nome
        FROM ht_checklists c
        JOIN ht_veiculos v ON v.id = c.id_veiculo
        LEFT JOIN ht_usuarios u ON u.id = c.id_tecnico
        WHERE c.id = ?
    """, (id,)).fetchone()
    db.close()
    if not row:
        raise HTTPException(404, "Checklist nao encontrado")
    import json
    r = dict(row)
    r["itens"] = json.loads(r["itens_json"] or "[]")
    return r
