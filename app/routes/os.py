import sqlite3, os, json
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from app.services.auth import requer_tecnico, requer_supervisor, get_db
from app.services.ixc_db import ixc_insert

router = APIRouter(prefix="/api/os", tags=["os"])

def brt(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@router.get("/minhas")
def minhas_os(usuario=Depends(requer_tecnico)):
    db = get_db()
    rows = db.execute("""
        SELECT o.*, e.checklist_json, e.iniciada_em, e.finalizada_em
        FROM ht_os o
        LEFT JOIN ht_os_execucao e ON e.ixc_os_id = o.ixc_os_id
        WHERE o.id_tecnico = ? AND o.status_hub != 'finalizada'
        ORDER BY o.data_agenda ASC, o.data_abertura ASC
    """, (usuario["id"],)).fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.get("/pendentes")
def os_pendentes(usuario=Depends(requer_supervisor)):
    db = get_db()
    rows = db.execute("""
        SELECT * FROM ht_os
        WHERE status_hub = 'pendente' AND (id_tecnico IS NULL OR id_tecnico = 0)
        ORDER BY data_abertura ASC
    """).fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.get("/hoje")
def os_hoje(usuario=Depends(requer_supervisor)):
    db = get_db()
    rows = db.execute("""
        SELECT o.*, u.nome AS tecnico_nome
        FROM ht_os o
        LEFT JOIN ht_usuarios u ON u.id = o.id_tecnico
        WHERE DATE(o.data_abertura) = DATE('now','-3 hours')
           OR DATE(o.data_agenda) = DATE('now','-3 hours')
           OR o.status_hub IN ('pendente','deslocamento','execucao')
        ORDER BY o.status_hub, o.data_agenda ASC
    """).fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.get("/{ixc_os_id}")
def detalhe_os(ixc_os_id: int, usuario=Depends(requer_tecnico)):
    db = get_db()
    os = db.execute("SELECT * FROM ht_os WHERE ixc_os_id=?", (ixc_os_id,)).fetchone()
    if not os: raise HTTPException(404, "OS não encontrada")
    exec = db.execute("SELECT * FROM ht_os_execucao WHERE ixc_os_id=?", (ixc_os_id,)).fetchone()
    mats = db.execute("SELECT * FROM ht_os_materiais WHERE ixc_os_id=?", (ixc_os_id,)).fetchall()
    db.close()
    return {
        **dict(os),
        "execucao": dict(exec) if exec else None,
        "materiais": [dict(m) for m in mats]
    }

@router.post("/{ixc_os_id}/iniciar-deslocamento")
def iniciar_deslocamento(ixc_os_id: int, usuario=Depends(requer_tecnico)):
    db = get_db()
    db.execute("UPDATE ht_os SET status_hub='deslocamento', id_tecnico=? WHERE ixc_os_id=?",
               (usuario["id"], ixc_os_id))
    db.commit()
    db.close()
    return {"ok": True}

@router.post("/{ixc_os_id}/iniciar-execucao")
def iniciar_execucao(ixc_os_id: int, data: dict, usuario=Depends(requer_tecnico)):
    db = get_db()
    db.execute("UPDATE ht_os SET status_hub='execucao' WHERE ixc_os_id=?", (ixc_os_id,))
    db.execute("""
        INSERT OR IGNORE INTO ht_os_execucao (ixc_os_id, iniciada_em, lat_chegada, lon_chegada)
        VALUES (?,?,?,?)
    """, (ixc_os_id, brt(), data.get("lat"), data.get("lon")))
    db.commit()
    db.close()
    return {"ok": True}

class FinalizarInput(BaseModel):
    checklist: list
    fotos_antes: list
    fotos_depois: list
    assinatura: Optional[str] = None
    solucao: str
    obs: Optional[str] = ""
    materiais: Optional[list] = []
    lat: Optional[float] = None
    lon: Optional[float] = None

@router.post("/{ixc_os_id}/finalizar")
def finalizar_os(ixc_os_id: int, data: FinalizarInput, usuario=Depends(requer_tecnico)):
    db = get_db()
    os_row = db.execute("SELECT * FROM ht_os WHERE ixc_os_id=?", (ixc_os_id,)).fetchone()
    if not os_row: raise HTTPException(404, "OS não encontrada")

    # Atualiza execucao
    db.execute("""
        INSERT OR REPLACE INTO ht_os_execucao
            (ixc_os_id, checklist_json, fotos_antes_json, fotos_depois_json,
             assinatura_base64, solucao_registrada, obs_tecnico,
             finalizada_em, lat_chegada, lon_chegada)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        ixc_os_id,
        json.dumps(data.checklist, ensure_ascii=False),
        json.dumps(data.fotos_antes, ensure_ascii=False),
        json.dumps(data.fotos_depois, ensure_ascii=False),
        data.assinatura,
        data.solucao,
        data.obs,
        brt(),
        data.lat, data.lon
    ))

    # Materiais
    for mat in data.materiais:
        db.execute("""
            INSERT INTO ht_os_materiais
                (ixc_os_id, id_tecnico, id_produto, produto_nome,
                 quantidade, unidade, numero_serie, tipo_uso)
            VALUES (?,?,?,?,?,?,?,?)
        """, (ixc_os_id, usuario["id"],
              mat.get("id_produto"), mat.get("nome"),
              mat.get("quantidade"), mat.get("unidade","un"),
              mat.get("numero_serie",""), mat.get("tipo_uso","consumivel_os")))
        # Baixa estoque do tecnico
        db.execute("""
            UPDATE ht_estoque_tecnico
            SET quantidade = quantidade - ?, ultima_atualizacao = ?
            WHERE id_tecnico=? AND id_produto=?
        """, (mat.get("quantidade",0), brt(), usuario["id"], mat.get("id_produto")))

    # Atualiza status
    db.execute("UPDATE ht_os SET status_hub='finalizada' WHERE ixc_os_id=?", (ixc_os_id,))
    db.commit()

    # Sincroniza com IXC
    try:
        corpo = f"=== CHECKLIST ===\n"
        for item in data.checklist:
            if item.get("marcado"): corpo += f"✅ {item.get('texto','')}\n"
        corpo += f"\n=== SOLUÇÃO ===\n{data.solucao}"
        if data.obs: corpo += f"\n\n=== OBSERVAÇÕES ===\n{data.obs}"

        ixc_insert("""
            UPDATE ixcprovedor.su_oss_chamado SET
                status='F', data_fechamento=%s,
                mensagem_resposta=%s
            WHERE id=%s
        """, (brt(), corpo, ixc_os_id))
        db.execute("UPDATE ht_os_execucao SET sincronizado_ixc=1 WHERE ixc_os_id=?", (ixc_os_id,))
        db.commit()
    except Exception as e:
        print(f"[WARN] Erro sync IXC OS {ixc_os_id}: {e}")

    db.close()
    return {"ok": True}

@router.post("/{ixc_os_id}/atribuir")
def atribuir_os(ixc_os_id: int, data: dict, usuario=Depends(requer_supervisor)):
    db = get_db()
    db.execute("UPDATE ht_os SET id_tecnico=?, status_hub='pendente' WHERE ixc_os_id=?",
               (data["id_tecnico"], ixc_os_id))
    db.commit()
    db.close()
    return {"ok": True}
