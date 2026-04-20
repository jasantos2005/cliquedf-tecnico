from fastapi import APIRouter, Depends, HTTPException
from app.services.auth import requer_admin, requer_supervisor
from app.services.auth import get_db
from ..services.ixc_db import ixc_conn
from datetime import datetime

router = APIRouter(prefix="/api/admin", tags=["admin"])

def brt(): 
    from datetime import timezone, timedelta
    return (datetime.now(timezone.utc)-timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

@router.get("/veiculos")
def listar_veiculos(usuario=Depends(requer_admin)):
    db = get_db()
    rows = db.execute("SELECT * FROM ht_veiculos ORDER BY tipo, marca_modelo").fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.post("/veiculos/sync")
def sync_veiculos(usuario=Depends(requer_admin)):
    db = get_db()
    def tipo(m):
        m = str(m).upper()
        if any(x in m for x in ['HONDA','NXR','CG ','POP 1','BROS']): return 'moto'
        if any(x in m for x in ['STRADA','TORO','FIORINO']): return 'pickup'
        if any(x in m for x in ['BYD','DOLPHIN']): return 'eletrico'
        return 'carro'
    with ixc_conn() as c:
        cur = c.cursor()
        cur.execute("SELECT id, placa, descricao, cor, ano_fabricacao, status FROM ixcprovedor.veiculos")
        veiculos = cur.fetchall()
    for v in veiculos:
        db.execute("""
            INSERT INTO ht_veiculos (ixc_veiculo_id, marca_modelo, placa, ano_fab, cor, tipo, ativo)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(ixc_veiculo_id) DO UPDATE SET
                marca_modelo=excluded.marca_modelo, placa=excluded.placa,
                cor=excluded.cor, ativo=excluded.ativo
        """, (v['id'], v['descricao'] or '', v['placa'] or '',
              v['ano_fabricacao'] or 0, v['cor'] or '',
              tipo(v['descricao'] or ''), 1 if v['status']=='A' else 0))
    db.commit()
    db.close()
    return {"ok": True, "total": len(veiculos)}

@router.get("/usuarios")
def listar_usuarios(usuario=Depends(requer_admin)):
    db = get_db()
    rows = db.execute("SELECT id, nome, login, nivel, ixc_funcionario_id, ativo FROM ht_usuarios ORDER BY nivel DESC, nome").fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.post("/usuarios/{id}/veiculo")
def vincular_veiculo(id: int, data: dict, usuario=Depends(requer_admin)):
    db = get_db()
    hoje = brt()[:10]
    db.execute("""
        INSERT INTO ht_tecnico_veiculo (id_tecnico, ixc_condutor_id, id_veiculo, data)
        VALUES (?,?,?,?)
        ON CONFLICT(id_tecnico, data) DO UPDATE SET
            ixc_condutor_id=excluded.ixc_condutor_id,
            id_veiculo=excluded.id_veiculo
    """, (id, data.get('ixc_condutor_id', 0), data.get('id_veiculo'), hoje))
    db.commit()
    db.close()
    return {"ok": True}

@router.get("/condutores-ixc")
def listar_condutores_ixc(usuario=Depends(requer_admin)):
    with ixc_conn() as c:
        cur = c.cursor()
        cur.execute("SELECT id, nome FROM ixcprovedor.veiculos_condutor ORDER BY nome")
        return [dict(r) for r in cur.fetchall()]

@router.get("/vinculos-hoje")
def vinculos_hoje(usuario=Depends(requer_admin)):
    db = get_db()
    hoje = brt()[:10]
    rows = db.execute("""
        SELECT tv.*, u.nome as tecnico_nome, v.marca_modelo, v.placa, v.tipo
        FROM ht_tecnico_veiculo tv
        JOIN ht_usuarios u ON u.id = tv.id_tecnico
        LEFT JOIN ht_veiculos v ON v.id = tv.id_veiculo
        WHERE tv.data = ?
    """, (hoje,)).fetchall()
    db.close()
    return [dict(r) for r in rows]

import hashlib
from pydantic import BaseModel
from typing import Optional

class UsuarioInput(BaseModel):
    nome: str
    login: str
    senha: Optional[str] = None
    nivel: int = 10
    ixc_funcionario_id: int = 0
    ativo: int = 1

@router.post("/usuarios")
def criar_usuario(data: UsuarioInput, usuario=Depends(requer_admin)):
    db = get_db()
    existe = db.execute("SELECT id FROM ht_usuarios WHERE login=?", (data.login,)).fetchone()
    if existe:
        db.close()
        raise HTTPException(400, "Login já existe")
    senha_hash = hashlib.sha256(data.senha.encode()).hexdigest() if data.senha else hashlib.sha256(b"tecnico123").hexdigest()
    db.execute("""
        INSERT INTO ht_usuarios (nome, login, senha_hash, nivel, ixc_funcionario_id, ativo)
        VALUES (?,?,?,?,?,?)
    """, (data.nome, data.login, senha_hash, data.nivel, data.ixc_funcionario_id, data.ativo))
    db.commit()
    db.close()
    return {"ok": True}

@router.put("/usuarios/{id}")
def editar_usuario(id: int, data: UsuarioInput, usuario=Depends(requer_admin)):
    db = get_db()
    if data.senha:
        senha_hash = hashlib.sha256(data.senha.encode()).hexdigest()
        db.execute("""
            UPDATE ht_usuarios SET nome=?, login=?, senha_hash=?, nivel=?,
            ixc_funcionario_id=?, ativo=? WHERE id=?
        """, (data.nome, data.login, senha_hash, data.nivel, data.ixc_funcionario_id, data.ativo, id))
    else:
        db.execute("""
            UPDATE ht_usuarios SET nome=?, login=?, nivel=?,
            ixc_funcionario_id=?, ativo=? WHERE id=?
        """, (data.nome, data.login, data.nivel, data.ixc_funcionario_id, data.ativo, id))
    db.commit()
    db.close()
    return {"ok": True}
