"""
retiradas.py — Endpoints de Agendamento de Retiradas
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from datetime import date
from app.services.auth import requer_supervisor
from app.engines.agenda_retiradas_engine import (
    gerar_rotas_retirada, confirmar_rota_retirada, tecnicos_retirada,
)

router = APIRouter(prefix="/api/retiradas", tags=["retiradas"])

class ConfirmarRetiradaBody(BaseModel):
    data_rota: str
    tecnico_id: int
    tecnico_nome: str
    os_ids: List[dict]
    tempo_est: int
    bairro_ref: str

@router.get("/rotas-sugeridas")
async def get_rotas_retirada(data: Optional[str] = Query(None), _user = Depends(requer_supervisor)):
    try:
        data_alvo = data or date.today().isoformat()
        rotas = gerar_rotas_retirada(data_alvo)
        return {
            "data": data_alvo, "total_rotas": len(rotas),
            "total_os": sum(r['total_os'] for r in rotas),
            "rotas": [{
                "rota_num": r['rota_num'], "bairro_ref": r['bairro_ref'],
                "total_os": r['total_os'], "tempo_est": r['tempo_est'],
                "tempo_fmt": r['tempo_fmt'], "distancia_km": r['distancia_km'],
                "garagem": r['garagem'],
                "os": [{
                    "id": o['id'], "ixc_os_id": o['ixc_os_id'],
                    "cliente_nome": o['cliente_nome'], "endereco": o['endereco'],
                    "bairro": o.get('bairro',''), "cidade": o.get('cidade',''),
                    "assunto_nome": o['assunto_nome'], "tempo_min": o['tempo_min'],
                    "horas_abertas": o.get('horas_abertas',0), "sla_estourado": o['sla_estourado'],
                    "lat": o['lat'], "lon": o['lon'],
                    "hora_prevista": o.get('hora_prevista',''),
                    "hora_prevista_fmt": o.get('hora_prevista_fmt',''),
                    "dist_anterior_km": o.get('dist_anterior_km',0),
                } for o in r['os']]
            } for r in rotas]
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))

@router.post("/confirmar")
async def post_confirmar_retirada(body: ConfirmarRetiradaBody, _user = Depends(requer_supervisor)):
    try:
        rota_id = confirmar_rota_retirada(
            data=body.data_rota, tecnico_id=body.tecnico_id,
            tecnico_nome=body.tecnico_nome, os_ids=body.os_ids,
            tempo_est=body.tempo_est, bairro_ref=body.bairro_ref,
        )
        return {"ok": True, "rota_id": rota_id, "msg": f"Rota #{rota_id} confirmada para {body.tecnico_nome}"}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

@router.get("/tecnicos")
async def get_tecnicos(_user = Depends(requer_supervisor)):
    try:
        return {"tecnicos": tecnicos_retirada()}
    except Exception as e:
        raise HTTPException(500, detail=str(e))
