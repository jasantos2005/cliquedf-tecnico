import sqlite3, os, json
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from app.services.auth import requer_tecnico, requer_supervisor, get_db
from app.services.ixc_db import ixc_insert

router = APIRouter(prefix="/api/os", tags=["os"])

def brt():
    from datetime import timezone, timedelta
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

@router.get("/minhas")
def minhas_os(usuario=Depends(requer_tecnico)):
    db = get_db()
    rows = db.execute("""
        SELECT o.*, e.checklist_json, e.iniciada_em, e.finalizada_em
        FROM ht_os o
        LEFT JOIN ht_os_execucao e ON e.ixc_os_id = o.ixc_os_id
        WHERE o.id_tecnico = (
            SELECT id FROM ht_usuarios WHERE ixc_funcionario_id = ?
        ) AND o.status_hub != 'finalizada'
        ORDER BY o.data_agenda ASC, o.data_abertura ASC
    """, (usuario["ixc_funcionario_id"],)).fetchall()
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

@router.get("/historico")
def historico_os(inicio: Optional[str] = None, fim: Optional[str] = None, usuario=Depends(requer_tecnico)):
    db = get_db()
    if not inicio:
        inicio = datetime.now().strftime("%Y-%m-%d")
    if not fim:
        fim = inicio
    rows = db.execute("""
        SELECT o.*, e.solucao_registrada, e.finalizada_em,
               e.fotos_depois_json, k.km_deslocamento
        FROM ht_os o
        LEFT JOIN ht_os_execucao e ON e.ixc_os_id = o.ixc_os_id
        LEFT JOIN ht_km_os k ON k.ixc_os_id = o.ixc_os_id AND k.id_tecnico = o.id_tecnico
        WHERE o.id_tecnico = ?
          AND o.status_hub = 'finalizada'
          AND DATE(e.finalizada_em) BETWEEN ? AND ?
        ORDER BY e.finalizada_em DESC
    """, (usuario["id"], inicio, fim)).fetchall()
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
    checklist: list = []
    fotos: list = []
    fotos_antes: list = []
    fotos_depois: list = []
    assinatura: Optional[str] = None
    solucao: str = ''
    obs: str = ''
    materiais: list = []
    lat: Optional[float] = None
    lon: Optional[float] = None

@router.post("/{ixc_os_id}/finalizar")
def finalizar_os(ixc_os_id: int, data: FinalizarInput, usuario=Depends(requer_tecnico)):
    db = get_db()
    os_row = db.execute("SELECT * FROM ht_os WHERE ixc_os_id=?", (ixc_os_id,)).fetchone()
    if not os_row: raise HTTPException(404, "OS não encontrada")

    # Atualiza execucao
    fotos = data.fotos if data.fotos else (data.fotos_antes + data.fotos_depois)
    db.execute("""
        INSERT OR REPLACE INTO ht_os_execucao
            (ixc_os_id, checklist_json, fotos_json,
             assinatura_base64, solucao_registrada, obs_tecnico,
             finalizada_em, lat_chegada, lon_chegada)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        ixc_os_id,
        json.dumps(data.checklist, ensure_ascii=False),
        json.dumps(fotos, ensure_ascii=False),
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
    id_tecnico = data.get("id_tecnico")
    data_reservada = data.get("data_reservada")  # formato: YYYY-MM-DD HH:MM

    # Atualiza SQLite
    db = get_db()
    # Buscar ixc_funcionario_id do tecnico
    db2 = get_db()
    tec = db2.execute("SELECT ixc_funcionario_id FROM ht_usuarios WHERE id=?", (id_tecnico,)).fetchone()
    db2.close()
    ixc_func_id = tec["ixc_funcionario_id"] if tec else id_tecnico
    db.execute(
        "UPDATE ht_os SET id_tecnico=?, status_hub='agendada' WHERE ixc_os_id=?",
        (id_tecnico, ixc_os_id)
    )
    db.commit()
    db.close()

    # Atualiza IXC
    try:
        if data_reservada:
            ixc_insert(
                "UPDATE ixcprovedor.su_oss_chamado SET id_tecnico=%s, data_reservada=%s WHERE id=%s",
                (ixc_func_id, data_reservada, ixc_os_id)
            )
        else:
            ixc_insert(
                "UPDATE ixcprovedor.su_oss_chamado SET id_tecnico=%s WHERE id=%s",
                (ixc_func_id, ixc_os_id)
            )
    except Exception as e:
        print(f"[WARN] Erro ao atribuir no IXC OS {ixc_os_id}: {e}")

    return {"ok": True}

# ── KM + DESLOCAMENTO ────────────────────────────────────────

class KmInput(BaseModel):
    km: float
    lat: Optional[float] = None
    lon: Optional[float] = None

@router.post("/{ixc_os_id}/iniciar-deslocamento-km")
def iniciar_deslocamento_km(ixc_os_id: int, data: KmInput, usuario=Depends(requer_tecnico)):
    db = get_db()
    id_tecnico = usuario["id"]
    agora = brt()
    hoje = agora[:10]

    # Validar KM crescente
    ultimo = db.execute("""
        SELECT MAX(km_saida) as ultimo FROM ht_km_os WHERE id_tecnico=?
    """, (id_tecnico,)).fetchone()
    ultimo_km = ultimo["ultimo"] or 0
    if data.km < ultimo_km:
        db.close()
        raise HTTPException(400, f"KM inválido. Último KM registrado: {ultimo_km:.0f}")

    # Verificar se ja tem OS ativa
    ativa = db.execute("""
        SELECT ixc_os_id FROM ht_os
        WHERE id_tecnico=? AND status_hub IN ('deslocamento','execucao')
        AND ixc_os_id != ?
    """, (id_tecnico, ixc_os_id)).fetchone()
    if ativa:
        db.close()
        raise HTTPException(400, f"Você tem OS #{ativa['ixc_os_id']} em andamento. Finalize ou reagende primeiro.")

    # Registrar KM saida
    db.execute("""
        INSERT INTO ht_km_os (ixc_os_id, id_tecnico, km_saida, dt_saida)
        VALUES (?,?,?,?)
    """, (ixc_os_id, id_tecnico, data.km, agora))

    # KM diario
    db.execute("""
        INSERT INTO ht_km_diario (id_tecnico, data, km_inicial)
        VALUES (?,?,?)
        ON CONFLICT(id_tecnico, data) DO NOTHING
    """, (id_tecnico, hoje, data.km))

    # Atualizar OS
    db.execute("UPDATE ht_os SET status_hub='deslocamento' WHERE ixc_os_id=?", (ixc_os_id,))
    db.commit()
    db.close()

    # IXC: status Assumida
    try:
        ixc_insert("UPDATE ixcprovedor.su_oss_chamado SET status=%s WHERE id=%s", ('AS', ixc_os_id))
    except Exception as e:
        print(f"[WARN] IXC status AS: {e}")

    return {"ok": True}

@router.post("/{ixc_os_id}/iniciar-execucao-km")
def iniciar_execucao_km(ixc_os_id: int, data: KmInput, usuario=Depends(requer_tecnico)):
    db = get_db()
    id_tecnico = usuario["id"]
    agora = brt()

    # Buscar KM saida
    km_os = db.execute(
        "SELECT km_saida FROM ht_km_os WHERE ixc_os_id=? AND id_tecnico=?",
        (ixc_os_id, id_tecnico)
    ).fetchone()
    km_saida = km_os["km_saida"] if km_os else 0

    if data.km < km_saida:
        db.close()
        raise HTTPException(400, f"KM inválido. KM de saída foi {km_saida:.0f}")

    km_desloc = data.km - km_saida

    # Atualizar KM
    db.execute("""
        UPDATE ht_km_os SET km_chegada=?, dt_chegada=?, km_deslocamento=?
        WHERE ixc_os_id=? AND id_tecnico=?
    """, (data.km, agora, km_desloc, ixc_os_id, id_tecnico))

    # Atualizar OS
    db.execute("""
        INSERT OR IGNORE INTO ht_os_execucao (ixc_os_id, iniciada_em, lat_chegada, lon_chegada)
        VALUES (?,?,?,?)
    """, (ixc_os_id, agora, data.lat, data.lon))
    db.execute("UPDATE ht_os SET status_hub='execucao' WHERE ixc_os_id=?", (ixc_os_id,))
    db.commit()
    db.close()

    # IXC: status A (em execucao)
    try:
        ixc_insert("UPDATE ixcprovedor.su_oss_chamado SET status=%s WHERE id=%s", ('A', ixc_os_id))
    except Exception as e:
        print(f"[WARN] IXC status A: {e}")

    return {"ok": True, "km_deslocamento": km_desloc}

@router.get("/{id_tecnico}/ultimo-km")
def ultimo_km(id_tecnico: int, usuario=Depends(requer_tecnico)):
    db = get_db()
    r = db.execute(
        "SELECT MAX(km_saida) as km FROM ht_km_os WHERE id_tecnico=?", (id_tecnico,)
    ).fetchone()
    db.close()
    return {"ultimo_km": r["km"] or 0}

class ReagendarInput(BaseModel):
    motivo: str

@router.post("/{ixc_os_id}/reagendar")
def reagendar_os(ixc_os_id: int, data: ReagendarInput, usuario=Depends(requer_tecnico)):
    if not data.motivo or len(data.motivo.strip()) < 5:
        raise HTTPException(400, "Motivo obrigatório (mínimo 5 caracteres)")

    db = get_db()
    # Remove tecnico e volta para base
    db.execute("""
        UPDATE ht_os SET status_hub='reagendada', id_tecnico=NULL,
        motivo_reagendamento=? WHERE ixc_os_id=?
    """, (data.motivo, ixc_os_id))
    db.commit()
    db.close()

    # IXC: status RAG + limpa tecnico
    try:
        ixc_insert(
            "UPDATE ixcprovedor.su_oss_chamado SET status=%s, id_tecnico=0, mensagem_resposta=%s WHERE id=%s",
            ('RAG', f"Reagendado: {data.motivo}", ixc_os_id)
        )
    except Exception as e:
        print(f"[WARN] IXC reagendar: {e}")

    return {"ok": True}


@router.post("/sync-fotos")
def sync_fotos_ixc(usuario=Depends(requer_tecnico)):
    import base64, os, requests as req
    from pathlib import Path
    hub_url = os.getenv("HUB_URL", "https://tecnico.iatechhub.com.br")
    ixc_url = os.getenv("IXC_API_URL", "https://sistema.cliquedf.com.br")
    ixc_user = os.getenv("IXC_API_USER", "64")
    ixc_token = os.getenv("IXC_API_TOKEN", "")
    auth = base64.b64encode(f"{ixc_user}:{ixc_token}".encode()).decode()
    uploads_dir = Path(__file__).resolve().parent.parent.parent / "uploads" / "os"
    uploads_dir.mkdir(parents=True, exist_ok=True)

    db = get_db()
    try:
        rows = db.execute("""
            SELECT e.ixc_os_id, e.fotos_json FROM ht_os_execucao e
            JOIN ht_os o ON o.ixc_os_id = e.ixc_os_id
            WHERE o.id_tecnico = ? AND e.fotos_enviadas_ixc IS NULL AND e.fotos_json != '[]'
        """, (usuario["id"],)).fetchall()

        enviadas = 0
        for row in rows:
            fotos = json.loads(row["fotos_json"] or "[]")
            for i, foto in enumerate(fotos):
                if not foto.startswith("data:image"):
                    continue
                header, b64data = foto.split(",", 1)
                ext = "jpg"
                nome = f"os_{row['ixc_os_id']}_{i+1}.{ext}"
                filepath = uploads_dir / nome
                with open(filepath, "wb") as f:
                    f.write(base64.b64decode(b64data))
                url_foto = f"{hub_url}/uploads/os/{nome}"
                try:
                    resp = req.post(
                        f"{ixc_url}/webservice/v1/su_oss_chamado_arquivos",
                        headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"},
                        json={"id_oss_chamado": str(row["ixc_os_id"]),
                              "descricao": f"Foto OS #{row['ixc_os_id']} - {i+1}",
                              "classificacao_arquivo": "O",
                              "tipo": "imagem",
                              "local_arquivo": b64data,
                              "nome_arquivo": nome},
                        timeout=30
                    )
                    if resp.ok and resp.json().get("type") == "success":
                        enviadas += 1
                    else:
                        print(f"[WARN] IXC recusou foto: {resp.text[:100]}")
                except Exception as e:
                    print(f"[WARN] Erro foto IXC API: {e}")

            db.execute("UPDATE ht_os_execucao SET fotos_enviadas_ixc=1 WHERE ixc_os_id=?", (row["ixc_os_id"],))
        db.commit()
        return {"ok": True, "msg": f"{enviadas} fotos enviadas"}
    finally:
        db.close()
