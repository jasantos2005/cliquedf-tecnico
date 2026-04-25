"""
agenda.py — Endpoints do Motor de Agendamento
Caminho: /opt/automacoes/cliquedf/tecnico/app/routes/agenda.py
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from datetime import date

from app.services.auth import requer_supervisor, get_db
from app.engines.agenda_engine import (
    gerar_rotas,
    confirmar_rota,
    listar_rotas_do_dia,
    tecnicos_disponiveis,
)

router = APIRouter(prefix="/api/agenda", tags=["agenda"])


# ── Modelos ───────────────────────────────────────────────────────────────────
class ConfirmarRotaBody(BaseModel):
    data_rota:    str
    tecnico_id:   int
    tecnico_nome: str
    os_ids:       List[dict]   # lista de {id, pontos}
    pontos:       int
    tempo_est:    int
    bairro_ref:   str


# ── GET /api/agenda/rotas-sugeridas ──────────────────────────────────────────
@router.get("/rotas-sugeridas")
async def get_rotas_sugeridas(
    data: Optional[str] = Query(None, description="YYYY-MM-DD — padrão: hoje"),
    _user = Depends(requer_supervisor)
):
    """
    Gera sugestão de rotas para a data informada.
    Não salva nada — apenas retorna para o operador avaliar.
    """
    try:
        data_alvo = data or date.today().isoformat()
        rotas = gerar_rotas(data_alvo)

        return {
            "data": data_alvo,
            "total_rotas": len(rotas),
            "total_os": sum(r['total_os'] for r in rotas),
            "rotas": [
                {
                    "rota_num":   r['rota_num'],
                    "bairro_ref": r['bairro_ref'],
                    "total_os":   r['total_os'],
                    "pontos":     r['pontos'],
                    "tempo_est":  r['tempo_est'],
                    "tempo_fmt":  f"{r['tempo_est']//60}h{r['tempo_est']%60:02d}min",
                    "garagem":     r.get('garagem', ''),
                    "distancia_km": r.get('distancia_km', 0),
                    "os": [
                        {
                            "id":              o['id'],
                            "ixc_os_id":       o['ixc_os_id'],
                            "cliente_nome":    o['cliente_nome'],
                            "endereco":        o['endereco'],
                            "bairro":          o.get('bairro', ''),
                            "cidade":          o.get('cidade', ''),
                            "assunto_nome":    o['assunto_nome'],
                            "pontos":          o['pontos'],
                            "tempo_min":       o['tempo_min'],
                            "sla_estourado":   o['sla_estourado'],
                            "horas_abertas":   o.get('horas_abertas', 0),
                            "lat":             o['lat'],
                            "lon":             o['lon'],
                            "hora_prevista":     o.get('hora_prevista', ''),
                            "hora_prevista_fmt": o.get('hora_prevista_fmt', ''),
                            "dist_anterior_km":  o.get('dist_anterior_km', 0),
                        }
                        for o in r['os']
                    ]
                }
                for r in rotas
            ]
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── POST /api/agenda/confirmar ────────────────────────────────────────────────
@router.post("/confirmar")
async def post_confirmar_rota(
    body: ConfirmarRotaBody,
    _user = Depends(requer_supervisor)
):
    """
    Operador confirma uma rota atribuindo a um técnico.
    Salva em ht_rotas + ht_rotas_os e atualiza id_tecnico nas OS.
    """
    try:
        rota_id = confirmar_rota(
            data        = body.data_rota,
            tecnico_id  = body.tecnico_id,
            tecnico_nome= body.tecnico_nome,
            os_ids      = body.os_ids,
            pontos      = body.pontos,
            tempo_est   = body.tempo_est,
            bairro_ref  = body.bairro_ref,
        )
        return {
            "ok": True,
            "rota_id": rota_id,
            "msg": f"Rota #{rota_id} confirmada para {body.tecnico_nome}"
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── GET /api/agenda/dia ───────────────────────────────────────────────────────
@router.get("/dia")
async def get_rotas_do_dia(
    data: Optional[str] = Query(None, description="YYYY-MM-DD — padrão: hoje"),
    _user = Depends(requer_supervisor)
):
    """Retorna rotas confirmadas do dia com OS detalhadas."""
    try:
        data_alvo = data or date.today().isoformat()
        rotas = listar_rotas_do_dia(data_alvo)
        return {
            "data": data_alvo,
            "total_rotas": len(rotas),
            "rotas": rotas
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── GET /api/agenda/tecnicos ──────────────────────────────────────────────────
@router.get("/tecnicos")
async def get_tecnicos_disponiveis(
    data: Optional[str] = Query(None, description="YYYY-MM-DD — padrão: hoje"),
    _user = Depends(requer_supervisor)
):
    """Retorna técnicos com pontos alocados vs capacidade (80 pts)."""
    try:
        data_alvo = data or date.today().isoformat()
        tecnicos = tecnicos_disponiveis(data_alvo)
        return {
            "data": data_alvo,
            "capacidade_max": 80,
            "tecnicos": tecnicos
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# RETIRADAS
# ══════════════════════════════════════════════════════════════════════════════
from app.engines.agenda_retiradas_engine import (
    gerar_rotas_retirada,
    confirmar_rota_retirada,
    tecnicos_retirada,
)

class ConfirmarRetiradaBody(BaseModel):
    data_rota:    str
    tecnico_id:   int
    tecnico_nome: str
    os_ids:       List[dict]
    tempo_est:    int
    bairro_ref:   str

@router.get("/retiradas/rotas-sugeridas")
async def get_rotas_retirada(
    data: Optional[str] = Query(None),
    _user = Depends(requer_supervisor)
):
    try:
        data_alvo = data or date.today().isoformat()
        rotas = gerar_rotas_retirada(data_alvo)
        return {
            "data": data_alvo,
            "total_rotas": len(rotas),
            "total_os": sum(r['total_os'] for r in rotas),
            "rotas": [_serializar_rota(r) for r in rotas]
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))

@router.post("/retiradas/confirmar")
async def post_confirmar_retirada(
    body: ConfirmarRetiradaBody,
    _user = Depends(requer_supervisor)
):
    try:
        rota_id = confirmar_rota_retirada(
            data=body.data_rota, tecnico_id=body.tecnico_id,
            tecnico_nome=body.tecnico_nome, os_ids=body.os_ids,
            tempo_est=body.tempo_est, bairro_ref=body.bairro_ref,
        )
        return {"ok": True, "rota_id": rota_id, "msg": f"Rota retirada #{rota_id} confirmada para {body.tecnico_nome}"}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

@router.get("/retiradas/tecnicos")
async def get_tecnicos_retirada(_user = Depends(requer_supervisor)):
    try:
        return {"tecnicos": tecnicos_retirada()}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# INFRA
# ══════════════════════════════════════════════════════════════════════════════
from app.engines.agenda_infra_engine import (
    gerar_rotas_infra,
    confirmar_rota_infra,
    tecnicos_infra,
)

class ConfirmarInfraBody(BaseModel):
    data_rota:    str
    tecnico_id:   int
    tecnico_nome: str
    os_ids:       List[dict]
    tempo_est:    int
    bairro_ref:   str

@router.get("/infra/rotas-sugeridas")
async def get_rotas_infra(
    data: Optional[str] = Query(None),
    _user = Depends(requer_supervisor)
):
    try:
        data_alvo = data or date.today().isoformat()
        rotas = gerar_rotas_infra(data_alvo)
        return {
            "data": data_alvo,
            "total_rotas": len(rotas),
            "total_os": sum(r['total_os'] for r in rotas),
            "rotas": [_serializar_rota(r) for r in rotas]
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))

@router.post("/infra/confirmar")
async def post_confirmar_infra(
    body: ConfirmarInfraBody,
    _user = Depends(requer_supervisor)
):
    try:
        rota_id = confirmar_rota_infra(
            data=body.data_rota, tecnico_id=body.tecnico_id,
            tecnico_nome=body.tecnico_nome, os_ids=body.os_ids,
            tempo_est=body.tempo_est, bairro_ref=body.bairro_ref,
        )
        return {"ok": True, "rota_id": rota_id, "msg": f"Rota infra #{rota_id} confirmada para {body.tecnico_nome}"}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

@router.get("/infra/tecnicos")
async def get_tecnicos_infra(_user = Depends(requer_supervisor)):
    try:
        return {"tecnicos": tecnicos_infra()}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── helper interno ─────────────────────────────────────────────────────────
def _serializar_rota(r: dict) -> dict:
    return {
        "rota_num":     r['rota_num'],
        "bairro_ref":   r['bairro_ref'],
        "total_os":     r['total_os'],
        "tempo_est":    r['tempo_est'],
        "tempo_fmt":    r.get('tempo_fmt', f"{r['tempo_est']//60}h{r['tempo_est']%60:02d}min"),
        "garagem":      r.get('garagem', ''),
        "distancia_km": r.get('distancia_km', 0),
        "os": [
            {
                "id":              o['id'],
                "ixc_os_id":       o['ixc_os_id'],
                "cliente_nome":    o['cliente_nome'],
                "endereco":        o['endereco'],
                "bairro":          o.get('bairro', ''),
                "cidade":          o.get('cidade', ''),
                "assunto_nome":    o['assunto_nome'],
                "pontos":          o.get('pontos', 0),
                "tempo_min":       o.get('tempo_min', 30),
                "sla_estourado":   o['sla_estourado'],
                "horas_abertas":   o.get('horas_abertas', 0),
                "lat":             o['lat'],
                "lon":             o['lon'],
                "hora_prevista":     o.get('hora_prevista', ''),
                "hora_prevista_fmt": o.get('hora_prevista_fmt', ''),
                "dist_anterior_km":  o.get('dist_anterior_km', 0),
            }
            for o in r['os']
        ]
    }
