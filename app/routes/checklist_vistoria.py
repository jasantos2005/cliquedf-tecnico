import sqlite3
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from app.services.auth import requer_tecnico, requer_supervisor, get_db

router = APIRouter(prefix="/api/vistoria", tags=["vistoria"])

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

@router.get("/itens")
def listar_itens(usuario=Depends(requer_tecnico)):
    db = get_db()
    try:
        rows = db.execute("""
            SELECT id, codigo, descricao, categoria, urgente
            FROM ht_checklist_itens WHERE ativo=1 ORDER BY codigo
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@router.get("/hoje")
def vistoria_hoje(usuario=Depends(requer_tecnico)):
    """Verifica se já fez vistoria hoje."""
    db = get_db()
    try:
        hoje = brt()[:10]
        row = db.execute("""
            SELECT v.*, ve.placa
            FROM ht_checklist_vistoria v
            LEFT JOIN ht_veiculos ve ON ve.id = v.veiculo_id
            WHERE v.id_tecnico = ? AND v.data = ?
            ORDER BY v.id DESC LIMIT 1
        """, (usuario["id"], hoje)).fetchone()
        if row:
            anomalias = db.execute("""
                SELECT a.*, i.descricao, i.urgente, i.categoria
                FROM ht_checklist_anomalias a
                JOIN ht_checklist_itens i ON i.id = a.id_item
                WHERE a.id_vistoria = ?
            """, (row["id"],)).fetchall()
            return {**dict(row), "anomalias": [dict(a) for a in anomalias]}
        return None
    finally:
        db.close()

class AnomaliaInput(BaseModel):
    id_item: int
    obs: Optional[str] = None

class VistoriaInput(BaseModel):
    km_atual: float = 0
    sem_anomalias: bool = False
    anomalias: List[AnomaliaInput] = []
    obs_geral: Optional[str] = None

@router.post("/registrar")
def registrar_vistoria(data: VistoriaInput, usuario=Depends(requer_tecnico)):
    db = get_db()
    try:
        hoje = brt()[:10]

        # Verifica se já fez hoje
        existente = db.execute(
            "SELECT id FROM ht_checklist_vistoria WHERE id_tecnico=? AND data=?",
            (usuario["id"], hoje)
        ).fetchone()
        if existente:
            raise HTTPException(400, "Vistoria já realizada hoje")

        veiculo = _get_veiculo_atual(usuario["id"], db)
        veiculo_id = veiculo["veiculo_id"] if veiculo else None
        tem_anomalia = 0 if data.sem_anomalias else (1 if data.anomalias else 0)

        db.execute("""
            INSERT INTO ht_checklist_vistoria
                (id_tecnico, veiculo_id, data, km_atual, tem_anomalia, obs_geral, status, criado_em)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            usuario["id"], veiculo_id, hoje,
            data.km_atual, tem_anomalia,
            data.obs_geral,
            'ok' if data.sem_anomalias else ('anomalia' if data.anomalias else 'ok'),
            brt()
        ))
        id_vistoria = db.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Salva anomalias
        urgentes = []
        for a in data.anomalias:
            item = db.execute("SELECT * FROM ht_checklist_itens WHERE id=?", (a.id_item,)).fetchone()
            if not item: continue
            db.execute("""
                INSERT INTO ht_checklist_anomalias (id_vistoria, id_item, obs)
                VALUES (?,?,?)
            """, (id_vistoria, a.id_item, a.obs))
            if item["urgente"]:
                urgentes.append(item["descricao"])

        db.commit()

        # Notifica gestor se tiver anomalias urgentes
        if urgentes:
            try:
                import urllib.request, json as _json
                BOT  = "8246203939:AAEFRu8dQiGk0qrIfbb9-qyHYO1wkczbj7Q"
                CHAT = "-5176265124"
                placa = veiculo["placa"] if veiculo else "?"
                msg = (
                    f"⚠️ *Anomalia urgente na vistoria*\n"
                    f"Técnico: {usuario['nome']}\n"
                    f"Veículo: {placa}\n"
                    f"Itens críticos:\n" +
                    "\n".join(f"• {u}" for u in urgentes)
                )
                req = urllib.request.Request(
                    f"https://api.telegram.org/bot{BOT}/sendMessage",
                    data=_json.dumps({"chat_id": CHAT, "text": msg, "parse_mode": "Markdown"}).encode(),
                    headers={"Content-Type": "application/json"}
                )
                urllib.request.urlopen(req, timeout=5)
            except Exception as e:
                print(f"[WARN] Telegram vistoria: {e}")

        return {
            "ok": True,
            "id_vistoria": id_vistoria,
            "tem_anomalia": tem_anomalia,
            "urgentes": len(urgentes)
        }
    finally:
        db.close()

@router.get("/listar")
def listar_vistorias(
    inicio: str = None, fim: str = None,
    id_veiculo: int = None,
    usuario=Depends(requer_supervisor)
):
    db = get_db()
    try:
        if not inicio: inicio = brt()[:10]
        if not fim: fim = inicio
        query = """
            SELECT v.*, u.nome as tecnico_nome, ve.placa, ve.marca_modelo
            FROM ht_checklist_vistoria v
            JOIN ht_usuarios u ON u.id = v.id_tecnico
            LEFT JOIN ht_veiculos ve ON ve.id = v.veiculo_id
            WHERE v.data BETWEEN ? AND ?
        """
        params = [inicio, fim]
        if id_veiculo:
            query += " AND v.veiculo_id = ?"
            params.append(id_veiculo)
        query += " ORDER BY v.criado_em DESC"
        rows = db.execute(query, params).fetchall()
        resultado = []
        for r in rows:
            anomalias = db.execute("""
                SELECT a.*, i.descricao, i.urgente, i.categoria
                FROM ht_checklist_anomalias a
                JOIN ht_checklist_itens i ON i.id = a.id_item
                WHERE a.id_vistoria = ?
            """, (r["id"],)).fetchall()
            resultado.append({**dict(r), "anomalias": [dict(a) for a in anomalias]})
        return resultado
    finally:
        db.close()

class AvaliacaoInput(BaseModel):
    obs_avaliacao: str
    ids_resolvidos: List[int] = []

@router.post("/{id_vistoria}/avaliar")
def avaliar_vistoria(id_vistoria: int, data: AvaliacaoInput, usuario=Depends(requer_supervisor)):
    db = get_db()
    try:
        db.execute("""
            UPDATE ht_checklist_vistoria
            SET avaliado_por=?, avaliado_em=?, obs_avaliacao=?, status='avaliado'
            WHERE id=?
        """, (usuario["id"], brt(), data.obs_avaliacao, id_vistoria))
        for id_anomalia in data.ids_resolvidos:
            db.execute(
                "UPDATE ht_checklist_anomalias SET resolvido=1 WHERE id=? AND id_vistoria=?",
                (id_anomalia, id_vistoria)
            )
        db.commit()
        return {"ok": True}
    finally:
        db.close()
