import sqlite3
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone, timedelta
from app.services.auth import requer_tecnico, get_db

router = APIRouter(prefix="/api/km-avulso", tags=["km_avulso"])

TIPOS_DESLOCAMENTO = [
    {"id": "retorno_garagem",    "label": "🏠 Retorno à garagem",         "emoji": "🏠"},
    {"id": "busca_materiais",    "label": "🛒 Busca de materiais",         "emoji": "🛒"},
    {"id": "escritorio",         "label": "🏢 Escritório / Sede",          "emoji": "🏢"},
    {"id": "suporte_interno",    "label": "🔧 Suporte técnico interno",    "emoji": "🔧"},
    {"id": "intercidade",        "label": "🏙️ Deslocamento intercidade",   "emoji": "🏙️"},
    {"id": "abastecimento",      "label": "⛽ Abastecimento",              "emoji": "⛽"},
    {"id": "apoio_tecnico",      "label": "🔄 Apoio a outro técnico",      "emoji": "🔄"},
    {"id": "entrega_equipamento","label": "📦 Entrega de equipamento",     "emoji": "📦"},
]

def brt():
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

def _get_veiculo_atual(tecnico_id: int, db):
    row = db.execute("""
        SELECT p.id_veiculo, v.placa, v.marca_modelo
        FROM ht_veiculo_posse p
        JOIN ht_veiculos v ON v.id = p.id_veiculo
        WHERE p.id_tecnico = ? AND p.entregue_em IS NULL
        ORDER BY p.id DESC LIMIT 1
    """, (tecnico_id,)).fetchone()
    if row:
        return {"veiculo_id": row["id_veiculo"], "placa": row["placa"], "modelo": row["marca_modelo"]}
    return None

def _get_ultimo_km_veiculo(veiculo_id: int, db) -> float:
    row = db.execute("""
        SELECT MAX(km) as km FROM (
            SELECT km_chegada AS km FROM ht_km_os WHERE veiculo_id=? AND km_chegada IS NOT NULL
            UNION ALL
            SELECT km_final AS km FROM ht_km_avulso WHERE veiculo_id=? AND km_final IS NOT NULL
            UNION ALL
            SELECT km_saida AS km FROM ht_km_os WHERE veiculo_id=? AND km_saida IS NOT NULL
            UNION ALL
            SELECT km_inicial AS km FROM ht_km_avulso WHERE veiculo_id=? AND km_inicial IS NOT NULL
        )
    """, (veiculo_id, veiculo_id, veiculo_id, veiculo_id)).fetchone()
    return row["km"] if row and row["km"] else 0

@router.get("/tipos")
def listar_tipos(usuario=Depends(requer_tecnico)):
    return TIPOS_DESLOCAMENTO

@router.get("/ativo")
def deslocamento_ativo(usuario=Depends(requer_tecnico)):
    """Retorna deslocamento avulso em andamento do técnico."""
    db = get_db()
    try:
        row = db.execute("""
            SELECT k.*, v.placa, v.marca_modelo
            FROM ht_km_avulso k
            LEFT JOIN ht_veiculos v ON v.id = k.veiculo_id
            WHERE k.id_tecnico = ? AND k.status = 'em_andamento'
            ORDER BY k.id DESC LIMIT 1
        """, (usuario["id"],)).fetchone()
        if row:
            return dict(row)
        return None
    finally:
        db.close()

class IniciarInput(BaseModel):
    tipo: str
    km_inicial: float

@router.post("/iniciar")
def iniciar_deslocamento_avulso(data: IniciarInput, usuario=Depends(requer_tecnico)):
    db = get_db()
    try:
        # Verifica se ja tem deslocamento ativo
        ativo = db.execute(
            "SELECT id FROM ht_km_avulso WHERE id_tecnico=? AND status='em_andamento'",
            (usuario["id"],)
        ).fetchone()
        if ativo:
            raise HTTPException(400, "Já existe um deslocamento avulso em andamento. Finalize antes de iniciar outro.")

        # Valida tipo
        tipos_ids = [t["id"] for t in TIPOS_DESLOCAMENTO]
        if data.tipo not in tipos_ids:
            raise HTTPException(400, "Tipo de deslocamento inválido")

        # Busca veículo atual
        veiculo = _get_veiculo_atual(usuario["id"], db)
        veiculo_id = veiculo["veiculo_id"] if veiculo else None

        # Valida KM crescente
        if veiculo_id:
            ultimo = _get_ultimo_km_veiculo(veiculo_id, db)
            if data.km_inicial < ultimo:
                raise HTTPException(400, f"KM inválido. Último KM registrado: {ultimo:.0f}")

        db.execute("""
            INSERT INTO ht_km_avulso
                (id_tecnico, veiculo_id, tipo, km_inicial, dt_inicio, status)
            VALUES (?,?,?,?,?,'em_andamento')
        """, (usuario["id"], veiculo_id, data.tipo, data.km_inicial, brt()))
        db.commit()

        return {
            "ok": True,
            "veiculo": veiculo["placa"] if veiculo else None,
            "km_inicial": data.km_inicial,
            "tipo": data.tipo
        }
    finally:
        db.close()

class FinalizarInput(BaseModel):
    km_final: float

@router.post("/finalizar")
def finalizar_deslocamento_avulso(data: FinalizarInput, usuario=Depends(requer_tecnico)):
    db = get_db()
    try:
        ativo = db.execute(
            "SELECT * FROM ht_km_avulso WHERE id_tecnico=? AND status='em_andamento' ORDER BY id DESC LIMIT 1",
            (usuario["id"],)
        ).fetchone()
        if not ativo:
            raise HTTPException(404, "Nenhum deslocamento avulso em andamento")

        if data.km_final < ativo["km_inicial"]:
            raise HTTPException(400, f"KM final não pode ser menor que KM inicial ({ativo['km_inicial']:.0f})")

        km_deslocamento = data.km_final - ativo["km_inicial"]

        db.execute("""
            UPDATE ht_km_avulso
            SET km_final=?, dt_fim=?, km_deslocamento=?, status='finalizado'
            WHERE id=?
        """, (data.km_final, brt(), km_deslocamento, ativo["id"]))
        db.commit()

        return {
            "ok": True,
            "km_deslocamento": round(km_deslocamento, 1),
            "tipo": ativo["tipo"]
        }
    finally:
        db.close()

@router.get("/historico")
def historico_avulso(inicio: str = None, fim: str = None, usuario=Depends(requer_tecnico)):
    db = get_db()
    try:
        if not inicio: inicio = datetime.now().strftime("%Y-%m-%d")
        if not fim: fim = inicio
        rows = db.execute("""
            SELECT k.*, v.placa
            FROM ht_km_avulso k
            LEFT JOIN ht_veiculos v ON v.id = k.veiculo_id
            WHERE k.id_tecnico = ?
              AND DATE(k.criado_em) BETWEEN ? AND ?
            ORDER BY k.id DESC
        """, (usuario["id"], inicio, fim)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()
