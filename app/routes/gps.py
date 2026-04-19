import json
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from datetime import datetime
from app.services.auth import requer_tecnico, requer_supervisor, get_db

router = APIRouter(prefix="/api/gps", tags=["gps"])

# Conexoes ativas: {id_tecnico: websocket}
_conexoes = {}

@router.websocket("/tecnico/{id_tecnico}")
async def ws_tecnico(ws: WebSocket, id_tecnico: int):
    await ws.accept()
    _conexoes[id_tecnico] = ws
    try:
        while True:
            data = await ws.receive_json()
            db = get_db()
            db.execute("""
                INSERT INTO ht_gps_track
                    (id_tecnico, lat, lon, velocidade, status_tecnico, ixc_os_id)
                VALUES (?,?,?,?,?,?)
            """, (id_tecnico, data.get("lat"), data.get("lon"),
                  data.get("velocidade", 0), data.get("status", "livre"),
                  data.get("ixc_os_id")))
            db.commit()
            db.close()
    except WebSocketDisconnect:
        _conexoes.pop(id_tecnico, None)

@router.get("/posicoes")
def posicoes_atuais(usuario=Depends(requer_supervisor)):
    db = get_db()
    rows = db.execute("""
        SELECT g.id_tecnico, g.lat, g.lon, g.velocidade,
               g.status_tecnico, g.ixc_os_id, g.registrado_em,
               u.nome AS tecnico_nome
        FROM ht_gps_track g
        JOIN ht_usuarios u ON u.id = g.id_tecnico
        WHERE g.id IN (
            SELECT MAX(id) FROM ht_gps_track GROUP BY id_tecnico
        )
    """).fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.get("/historico/{id_tecnico}")
def historico(id_tecnico: int, usuario=Depends(requer_supervisor)):
    db = get_db()
    rows = db.execute("""
        SELECT lat, lon, velocidade, status_tecnico, registrado_em
        FROM ht_gps_track
        WHERE id_tecnico=?
          AND registrado_em >= datetime('now','-3 hours','-8 hours')
        ORDER BY registrado_em ASC
    """, (id_tecnico,)).fetchall()
    db.close()
    return [dict(r) for r in rows]

from pydantic import BaseModel
from typing import Optional

class PosicaoInput(BaseModel):
    id_tecnico: int
    lat: float
    lon: float
    velocidade: Optional[float] = 0
    status: Optional[str] = 'livre'
    ixc_os_id: Optional[int] = None

@router.post("/posicao")
def salvar_posicao(data: PosicaoInput):
    db = get_db()
    db.execute("""
        INSERT INTO ht_gps_track
            (id_tecnico, lat, lon, velocidade, status_tecnico, ixc_os_id)
        VALUES (?,?,?,?,?,?)
    """, (data.id_tecnico, data.lat, data.lon,
          data.velocidade, data.status, data.ixc_os_id))
    db.commit()
    db.close()
    return {"ok": True}
