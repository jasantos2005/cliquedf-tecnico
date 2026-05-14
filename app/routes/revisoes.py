from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime, timedelta, timezone
import sqlite3

router = APIRouter()

DB = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"

def get_db():
    con = sqlite3.connect(DB, timeout=30)
    con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()

def brt():
    return datetime.now(timezone.utc) - timedelta(hours=3)

# ── MODELOS ────────────────────────────────────────────────────────────────────
class RevisaoCreate(BaseModel):
    ht_veiculo_id: int
    item: str
    data_prevista: Optional[str] = None
    km_previsto: Optional[int] = None
    intervalo_dias: Optional[int] = 0
    intervalo_km: Optional[int] = 0
    ultima_data: Optional[str] = None
    ultimo_km: Optional[int] = None
    obs: Optional[str] = ""

class RevisaoUpdate(BaseModel):
    item: Optional[str] = None
    data_prevista: Optional[str] = None
    km_previsto: Optional[int] = None
    intervalo_dias: Optional[int] = None
    intervalo_km: Optional[int] = None
    ultima_data: Optional[str] = None
    ultimo_km: Optional[int] = None
    obs: Optional[str] = None
    ativo: Optional[int] = None

class RealizarRevisao(BaseModel):
    data_realizada: str
    km_realizado: Optional[int] = None
    obs: Optional[str] = ""

# ── LISTAR PROXIMAS REVISOES (relatorio automatico) ────────────────────────────
@router.get("/api/frota/revisoes/proximas")
def proximas_revisoes(dias: int = 30, db: sqlite3.Connection = Depends(get_db)):
    """Retorna revisoes que vencem nos proximos X dias (padrao 30)."""
    hoje = brt().date()
    limite = hoje + timedelta(days=dias)

    cur = db.cursor()
    cur.execute("""
        SELECT r.*, v.placa, v.marca_modelo, v.tipo
        FROM ht_revisoes r
        JOIN ht_veiculos v ON v.id = r.ht_veiculo_id
        WHERE r.ativo = 1
        ORDER BY r.data_prevista ASC, v.placa ASC
    """)
    rows = cur.fetchall()

    resultado = []
    for row in rows:
        r = dict(row)
        dias_restantes = None
        vencido = False
        urgencia = "ok"  # ok / aviso / urgente / vencido

        if r["data_prevista"]:
            try:
                dp = date.fromisoformat(r["data_prevista"])
                dias_restantes = (dp - hoje).days
                vencido = dias_restantes < 0

                if vencido:
                    urgencia = "vencido"
                elif dias_restantes <= 7:
                    urgencia = "urgente"
                elif dias_restantes <= 15:
                    urgencia = "aviso"
                else:
                    urgencia = "ok"

                # So inclui se vencido ou dentro do limite
                if not vencido and dp > limite:
                    continue
            except:
                pass

        r["dias_restantes"] = dias_restantes
        r["vencido"] = vencido
        r["urgencia"] = urgencia
        resultado.append(r)

    # Ordena: vencidos primeiro, depois por dias_restantes
    resultado.sort(key=lambda x: (
        0 if x["urgencia"] == "vencido" else
        1 if x["urgencia"] == "urgente" else
        2 if x["urgencia"] == "aviso" else 3,
        x["dias_restantes"] if x["dias_restantes"] is not None else 9999
    ))

    # Stats resumo
    stats = {
        "total": len(resultado),
        "vencidos": sum(1 for r in resultado if r["urgencia"] == "vencido"),
        "urgentes": sum(1 for r in resultado if r["urgencia"] == "urgente"),
        "avisos": sum(1 for r in resultado if r["urgencia"] == "aviso"),
        "ok": sum(1 for r in resultado if r["urgencia"] == "ok"),
        "hoje": str(hoje),
        "limite": str(limite),
        "dias_filtro": dias
    }

    return {"revisoes": resultado, "stats": stats}

# ── LISTAR TODAS AS REVISOES DE UM VEICULO ─────────────────────────────────────
@router.get("/api/frota/revisoes/veiculo/{ht_veiculo_id}")
def revisoes_veiculo(ht_veiculo_id: int, db: sqlite3.Connection = Depends(get_db)):
    cur = db.cursor()
    cur.execute("""
        SELECT r.*, v.placa, v.marca_modelo
        FROM ht_revisoes r
        JOIN ht_veiculos v ON v.id = r.ht_veiculo_id
        WHERE r.ht_veiculo_id = ? AND r.ativo = 1
        ORDER BY r.data_prevista ASC
    """, (ht_veiculo_id,))
    return [dict(r) for r in cur.fetchall()]

# ── CRIAR ITEM DE REVISAO ──────────────────────────────────────────────────────
@router.post("/api/frota/revisoes")
def criar_revisao(data: RevisaoCreate, db: sqlite3.Connection = Depends(get_db)):
    cur = db.cursor()
    # Verifica veiculo existe
    cur.execute("SELECT id FROM ht_veiculos WHERE id=? AND ativo=1", (data.ht_veiculo_id,))
    if not cur.fetchone():
        raise HTTPException(404, "Veiculo nao encontrado")
    agora = brt().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
        INSERT INTO ht_revisoes
        (ht_veiculo_id, item, data_prevista, km_previsto, intervalo_dias,
         intervalo_km, ultima_data, ultimo_km, obs, criado_em, atualizado_em)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (data.ht_veiculo_id, data.item, data.data_prevista, data.km_previsto,
          data.intervalo_dias or 0, data.intervalo_km or 0,
          data.ultima_data, data.ultimo_km, data.obs or "", agora, agora))
    db.commit()
    return {"ok": True, "id": cur.lastrowid}

# ── ATUALIZAR ITEM DE REVISAO ──────────────────────────────────────────────────
@router.put("/api/frota/revisoes/{id}")
def atualizar_revisao(id: int, data: RevisaoUpdate, db: sqlite3.Connection = Depends(get_db)):
    cur = db.cursor()
    cur.execute("SELECT id FROM ht_revisoes WHERE id=?", (id,))
    if not cur.fetchone():
        raise HTTPException(404, "Revisao nao encontrada")
    campos = []
    vals = []
    for campo, val in data.dict(exclude_none=True).items():
        campos.append(f"{campo}=?")
        vals.append(val)
    if not campos:
        raise HTTPException(400, "Nenhum campo para atualizar")
    agora = brt().strftime("%Y-%m-%d %H:%M:%S")
    campos.append("atualizado_em=?"); vals.append(agora)
    vals.append(id)
    cur.execute(f"UPDATE ht_revisoes SET {','.join(campos)} WHERE id=?", vals)
    db.commit()
    return {"ok": True}

# ── REALIZAR REVISAO (registra e calcula proxima) ──────────────────────────────
@router.post("/api/frota/revisoes/{id}/realizar")
def realizar_revisao(id: int, data: RealizarRevisao, db: sqlite3.Connection = Depends(get_db)):
    cur = db.cursor()
    cur.execute("SELECT * FROM ht_revisoes WHERE id=?", (id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Revisao nao encontrada")
    r = dict(row)

    # Calcula proxima data
    proxima_data = None
    if r["intervalo_dias"] and r["intervalo_dias"] > 0:
        try:
            base = date.fromisoformat(data.data_realizada)
            proxima_data = str(base + timedelta(days=r["intervalo_dias"]))
        except:
            pass

    # Calcula proximo km
    proximo_km = None
    if r["intervalo_km"] and r["intervalo_km"] > 0 and data.km_realizado:
        proximo_km = data.km_realizado + r["intervalo_km"]

    agora = brt().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
        UPDATE ht_revisoes SET
            ultima_data=?, ultimo_km=?,
            data_prevista=?, km_previsto=?,
            obs=?, atualizado_em=?
        WHERE id=?
    """, (data.data_realizada, data.km_realizado,
          proxima_data, proximo_km,
          data.obs or r["obs"], agora, id))
    db.commit()
    return {"ok": True, "proxima_data": proxima_data, "proximo_km": proximo_km}

# ── EXCLUIR (desativar) ────────────────────────────────────────────────────────
@router.delete("/api/frota/revisoes/{id}")
def excluir_revisao(id: int, db: sqlite3.Connection = Depends(get_db)):
    cur = db.cursor()
    cur.execute("UPDATE ht_revisoes SET ativo=0 WHERE id=?", (id,))
    db.commit()
    return {"ok": True}

# ── LISTAR VEICULOS ATIVOS ─────────────────────────────────────────────────────
@router.get("/api/frota/veiculos")
def listar_veiculos(db: sqlite3.Connection = Depends(get_db)):
    cur = db.cursor()
    cur.execute("SELECT id, placa, marca_modelo, tipo, ano_fab, cor FROM ht_veiculos WHERE ativo=1 ORDER BY placa")
    return [dict(r) for r in cur.fetchall()]

# ── HISTÓRICO DE REALIZAÇÕES ──────────────────────────────────
from pydantic import BaseModel as _BM
from typing import List as _List, Optional as _Opt

class ItemExtra(_BM):
    descricao: str
    custo: float = 0.0

class InformarRevisao(_BM):
    data_realizada: str
    km_realizado: _Opt[int] = None
    itens_realizados: _List[str] = []
    itens_extras: _List[dict] = []
    custo_total: float = 0.0
    observacao: str = ''

@router.post("/api/frota/revisoes/{id}/informar")
def informar_revisao(id: int, data: InformarRevisao, db: sqlite3.Connection = Depends(get_db)):
    import json
    cur = db.cursor()
    cur.execute("SELECT * FROM ht_revisoes WHERE id=?", (id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Revisao nao encontrada")
    r = dict(row)

    # Calcular proximas datas
    from datetime import date, timedelta
    proxima_data = None
    proximo_km = None
    if r["intervalo_dias"] and r["intervalo_dias"] > 0:
        try:
            base = date.fromisoformat(data.data_realizada)
            proxima_data = str(base + timedelta(days=r["intervalo_dias"]))
        except: pass
    if r["intervalo_km"] and r["intervalo_km"] > 0 and data.km_realizado:
        proximo_km = data.km_realizado + r["intervalo_km"]

    agora = brt().strftime("%Y-%m-%d %H:%M:%S")

    # Atualizar revisao principal
    cur.execute("""
        UPDATE ht_revisoes SET
            ultima_data=?, ultimo_km=?,
            data_prevista=?, km_previsto=?,
            obs=?, atualizado_em=?
        WHERE id=?
    """, (data.data_realizada, data.km_realizado,
          proxima_data, proximo_km,
          data.observacao or r["obs"], agora, id))

    # Registrar no histórico
    cur.execute("""
        INSERT INTO ht_revisoes_historico
            (ht_veiculo_id, revisao_id, data_realizada, km_realizado,
             itens_realizados, itens_extras, custo_total, observacao, criado_em)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (r["ht_veiculo_id"], id, data.data_realizada, data.km_realizado,
          json.dumps(data.itens_realizados, ensure_ascii=False),
          json.dumps(data.itens_extras, ensure_ascii=False),
          data.custo_total, data.observacao, agora))

    # Registrar custo como despesa se houver
    if data.custo_total and data.custo_total > 0:
        cur.execute("""
            INSERT INTO ht_despesas
                (id_veiculo, tipo, descricao, valor, data, kilometragem, observacao)
            VALUES (?,?,?,?,?,?,?)
        """, (r["ht_veiculo_id"], 'revisao',
              f"Revisao: {r['item']}",
              data.custo_total, data.data_realizada,
              data.km_realizado, data.observacao))

    db.commit()
    return {"ok": True, "proxima_data": proxima_data, "proximo_km": proximo_km}

@router.get("/api/frota/revisoes/{id}/historico")
def historico_revisao(id: int, db: sqlite3.Connection = Depends(get_db)):
    import json
    cur = db.cursor()
    cur.execute("""
        SELECT * FROM ht_revisoes_historico
        WHERE revisao_id=? ORDER BY data_realizada DESC
    """, (id,))
    rows = cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['itens_realizados'] = json.loads(d.get('itens_realizados') or '[]')
        d['itens_extras'] = json.loads(d.get('itens_extras') or '[]')
        result.append(d)
    return result
