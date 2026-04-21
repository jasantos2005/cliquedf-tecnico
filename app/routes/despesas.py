from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from app.services.auth import requer_supervisor, get_db
from app.services.ixc_db import ixc_select, ixc_insert

router = APIRouter(prefix="/api/despesas", tags=["despesas"])

def brt():
    from datetime import timezone, timedelta
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

@router.get("/condutores")
def listar_condutores(usuario=Depends(requer_supervisor)):
    rows = ixc_select("SELECT id, nome FROM ixcprovedor.veiculos_condutor ORDER BY nome")
    return [dict(r) for r in rows]

@router.get("/veiculos")
def listar_veiculos(usuario=Depends(requer_supervisor)):
    db = get_db()
    rows = db.execute(
        "SELECT id, ixc_veiculo_id, marca_modelo, placa FROM ht_veiculos WHERE ativo=1 ORDER BY marca_modelo"
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.get("")
def listar_despesas(
    data_ini: Optional[str] = None,
    data_fim: Optional[str] = None,
    id_veiculo: Optional[int] = None,
    usuario=Depends(requer_supervisor)
):
    db = get_db()
    sql = """
        SELECT d.*, v.marca_modelo, v.placa
        FROM ht_despesas d
        LEFT JOIN ht_veiculos v ON v.ixc_veiculo_id = d.id_veiculo
        WHERE 1=1
    """
    params = []
    if data_ini:
        sql += " AND d.data >= ?"
        params.append(data_ini)
    if data_fim:
        sql += " AND d.data <= ?"
        params.append(data_fim)
    if id_veiculo:
        sql += " AND d.id_veiculo = ?"
        params.append(id_veiculo)
    sql += " ORDER BY d.data DESC, d.id DESC LIMIT 200"
    rows = db.execute(sql, params).fetchall()
    db.close()
    return [dict(r) for r in rows]

class DespesaInput(BaseModel):
    id_veiculo: int
    id_condutor: int
    tipo: str
    descricao: str
    valor: float
    data: str
    kilometragem: Optional[float] = None
    valor_litro: Optional[float] = None
    quantidade_litros: Optional[float] = None
    observacao: Optional[str] = None

@router.post("")
def criar_despesa(data: DespesaInput, usuario=Depends(requer_supervisor)):
    db = get_db()
    agora = brt()

    # Insere no IXC
    ixc_id = None
    try:
        ixc_id = ixc_insert("""
            INSERT INTO ixcprovedor.veiculos_despesas
                (id_veiculo, descricao, tipo, valor, data, kilometragem,
                 valor_litro, quantidade_litros, id_condutor, observacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data.id_veiculo, data.descricao, data.tipo, data.valor, data.data,
            data.kilometragem or 0, data.valor_litro or 0,
            data.quantidade_litros or 0, data.id_condutor,
            data.observacao or ''
        ))
    except Exception as e:
        print(f"[WARN] Erro ao inserir despesa no IXC: {e}")

    db.execute("""
        INSERT INTO ht_despesas
            (ixc_despesa_id, id_veiculo, id_condutor, id_tecnico, tipo, descricao,
             valor, data, kilometragem, valor_litro, quantidade_litros,
             observacao, sincronizado_ixc, criado_em)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ixc_id, data.id_veiculo, data.id_condutor, usuario["id"],
        data.tipo, data.descricao, data.valor, data.data,
        data.kilometragem, data.valor_litro, data.quantidade_litros,
        data.observacao, 1 if ixc_id else 0, agora
    ))
    db.commit()
    local_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    db.close()
    return {"ok": True, "id": local_id, "ixc_id": ixc_id}

@router.delete("/{id}")
def deletar_despesa(id: int, usuario=Depends(requer_supervisor)):
    db = get_db()
    desp = db.execute("SELECT * FROM ht_despesas WHERE id=?", (id,)).fetchone()
    if not desp:
        db.close()
        raise HTTPException(404, "Despesa não encontrada")

    if desp["ixc_despesa_id"]:
        try:
            ixc_insert(
                "DELETE FROM ixcprovedor.veiculos_despesas WHERE id=%s",
                (desp["ixc_despesa_id"],)
            )
        except Exception as e:
            print(f"[WARN] Erro ao deletar despesa no IXC: {e}")

    db.execute("DELETE FROM ht_despesas WHERE id=?", (id,))
    db.commit()
    db.close()
    return {"ok": True}
