import sqlite3
from fastapi import APIRouter, Depends
from app.services.auth import requer_supervisor, get_db
from datetime import datetime, timezone, timedelta

router = APIRouter(prefix="/api/raiox", tags=["raiox"])

def brt():
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

TIPO_LABEL = {
    'C': 'Combustível', 'M': 'Manutenção', 'O': 'Óleo',
    'P': 'Pneu', 'MT': 'Manutenção', 'OT': 'Outros'
}

@router.get("/frota")
def raiox_frota_geral(
    inicio: str = None, fim: str = None,
    usuario=Depends(requer_supervisor)
):
    """Dashboard geral — todos os veículos com semáforo de consumo."""
    if not inicio: inicio = "2026-01-01"
    if not fim: fim = brt()[:10]

    db = get_db()
    try:
        veiculos = db.execute("""
            SELECT v.id, v.ixc_veiculo_id, v.placa, v.marca_modelo, v.ano_fab,
                   b.consumo_esperado_kml, b.tipo AS tipo_combustivel,
                   b.consumo_cidade_kml, b.consumo_estrada_kml, b.fonte
            FROM ht_veiculos v
            LEFT JOIN ht_veiculo_benchmark b ON b.ixc_veiculo_id = v.ixc_veiculo_id
            WHERE v.ativo = 1
              AND v.placa NOT IN ('GERADOR1','GERADOR2','MOTOPODA')
            ORDER BY v.placa
        """).fetchall()

        resultado = []
        for v in veiculos:
            vid = v["id"]
            ixc_vid = v["ixc_veiculo_id"]

            # Despesas no período
            despesas = db.execute("""
                SELECT tipo, SUM(valor) as total_valor,
                       SUM(quantidade_litros) as total_litros,
                       COUNT(*) as qtd,
                       MAX(kilometragem) as km_max,
                       MIN(kilometragem) as km_min
                FROM ht_despesas
                WHERE id_veiculo = ? AND data BETWEEN ? AND ?
                  AND kilometragem > 0
            """, (vid, inicio, fim)).fetchall()

            total_valor = 0
            total_litros = 0
            km_max = 0
            km_min = 0
            despesas_por_tipo = {}

            for d in despesas:
                total_valor += d["total_valor"] or 0
                if d["tipo"] == "C":
                    total_litros += d["total_litros"] or 0
                    km_max = max(km_max, d["km_max"] or 0)
                    km_min = d["km_min"] if km_min == 0 else min(km_min, d["km_min"] or 0)
                despesas_por_tipo[d["tipo"]] = {
                    "label": TIPO_LABEL.get(d["tipo"], d["tipo"]),
                    "total": round(d["total_valor"] or 0, 2),
                    "qtd": d["qtd"]
                }

            # KM rodado no período (via ht_km_os)
            km_os = db.execute("""
                SELECT SUM(k.km_deslocamento) as km_total, COUNT(k.id) as os_total
                FROM ht_km_os k
                JOIN ht_veiculo_posse p ON p.id_veiculo = ? 
                    AND p.id_tecnico = k.id_tecnico
                    AND DATE(k.criado_em) BETWEEN ? AND ?
                WHERE k.km_deslocamento IS NOT NULL
            """, (vid, inicio, fim)).fetchone()

            # KM calculado pelo odômetro das despesas
            km_rodado_odo = (km_max - km_min) if km_max > km_min else 0

            # KM/L real
            kml_real = None
            if total_litros > 0 and km_rodado_odo > 0:
                kml_real = round(km_rodado_odo / total_litros, 1)

            # Semáforo
            esperado = v["consumo_esperado_kml"]
            tipo_comb = v["tipo_combustivel"] or "gasolina"
            semaforo = "cinza"
            variacao_pct = None

            if tipo_comb == "eletrico":
                semaforo = "azul"
            elif kml_real and esperado and esperado > 0:
                variacao_pct = round(((kml_real - esperado) / esperado) * 100, 1)
                if variacao_pct >= -10:
                    semaforo = "verde"
                elif variacao_pct >= -20:
                    semaforo = "amarelo"
                else:
                    semaforo = "vermelho"

            # Condutor atual
            condutor = db.execute("""
                SELECT u.nome FROM ht_veiculo_posse p
                JOIN ht_usuarios u ON u.id = p.id_tecnico
                WHERE p.id_veiculo = ? AND p.entregue_em IS NULL
                ORDER BY p.id DESC LIMIT 1
            """, (vid,)).fetchone()

            # Score de OS no período
            os_info = db.execute("""
                SELECT COUNT(DISTINCT k.ixc_os_id) as total_os,
                       SUM(k.km_deslocamento) as km_os
                FROM ht_km_os k
                WHERE k.veiculo_id = ?
                  AND DATE(k.criado_em) BETWEEN ? AND ?
            """, (vid, inicio, fim)).fetchone()

            resultado.append({
                "id": vid,
                "ixc_veiculo_id": ixc_vid,
                "placa": v["placa"],
                "marca_modelo": v["marca_modelo"],
                "ano_fab": v["ano_fab"],
                "tipo_combustivel": tipo_comb,
                "condutor_atual": condutor["nome"] if condutor else None,
                "semaforo": semaforo,
                "consumo_esperado_kml": esperado,
                "consumo_real_kml": kml_real,
                "variacao_pct": variacao_pct,
                "km_rodado": km_rodado_odo,
                "total_litros": round(total_litros, 2),
                "total_gasto": round(total_valor, 2),
                "total_os": os_info["total_os"] or 0,
                "km_por_os": round((os_info["km_os"] or 0) / os_info["total_os"], 1) if os_info["total_os"] else 0,
                "despesas_por_tipo": despesas_por_tipo,
                "fonte_benchmark": v["fonte"],
                "periodo": {"inicio": inicio, "fim": fim}
            })

        # Ordenar: vermelho → amarelo → verde → cinza → azul
        ordem = {"vermelho": 0, "amarelo": 1, "verde": 2, "cinza": 3, "azul": 4}
        resultado.sort(key=lambda x: ordem.get(x["semaforo"], 5))

        return resultado
    finally:
        db.close()


@router.get("/veiculo/{veiculo_id}")
def raiox_veiculo(
    veiculo_id: int,
    inicio: str = None, fim: str = None,
    usuario=Depends(requer_supervisor)
):
    """Raio-X detalhado de um veículo."""
    if not inicio: inicio = "2026-01-01"
    if not fim: fim = brt()[:10]

    db = get_db()
    try:
        v = db.execute("""
            SELECT v.*, b.consumo_esperado_kml, b.consumo_cidade_kml,
                   b.consumo_estrada_kml, b.tipo AS tipo_comb, b.fonte
            FROM ht_veiculos v
            LEFT JOIN ht_veiculo_benchmark b ON b.ixc_veiculo_id = v.ixc_veiculo_id
            WHERE v.id = ?
        """, (veiculo_id,)).fetchone()

        if not v:
            return {"erro": "Veículo não encontrado"}

        # Histórico de abastecimentos com km/l calculado
        abastecimentos = db.execute("""
            SELECT data, valor, quantidade_litros, kilometragem,
                   valor_litro, descricao, observacao, id_condutor
            FROM ht_despesas
            WHERE id_veiculo = ? AND tipo = 'C'
              AND data BETWEEN ? AND ?
              AND kilometragem > 0
            ORDER BY data ASC, kilometragem ASC
        """, (veiculo_id, inicio, fim)).fetchall()

        hist_abast = []
        prev_km = None
        for a in abastecimentos:
            kml = None
            if prev_km and a["kilometragem"] > prev_km and a["quantidade_litros"] > 0:
                kml = round((a["kilometragem"] - prev_km) / a["quantidade_litros"], 1)
            hist_abast.append({
                "data": a["data"],
                "km": a["kilometragem"],
                "litros": float(a["quantidade_litros"] or 0),
                "valor": float(a["valor"] or 0),
                "valor_litro": float(a["valor_litro"] or 0),
                "kml_calculado": kml,
                "id_condutor": a["id_condutor"]
            })
            prev_km = a["kilometragem"]

        # Mensal: km/l por mês
        mensal = db.execute("""
            SELECT strftime('%Y-%m', data) as mes,
                   SUM(quantidade_litros) as litros,
                   SUM(valor) as gasto,
                   MAX(kilometragem) as km_max,
                   MIN(kilometragem) as km_min
            FROM ht_despesas
            WHERE id_veiculo = ? AND tipo = 'C'
              AND data BETWEEN ? AND ?
              AND kilometragem > 0
            GROUP BY strftime('%Y-%m', data)
            ORDER BY mes
        """, (veiculo_id, inicio, fim)).fetchall()

        hist_mensal = []
        for m in mensal:
            km_mes = (m["km_max"] - m["km_min"]) if m["km_max"] > m["km_min"] else 0
            kml = round(km_mes / m["litros"], 1) if m["litros"] and m["litros"] > 0 and km_mes > 0 else None
            hist_mensal.append({
                "mes": m["mes"],
                "litros": round(float(m["litros"] or 0), 2),
                "gasto": round(float(m["gasto"] or 0), 2),
                "km_rodado": km_mes,
                "kml": kml
            })

        # Despesas por tipo
        desp_tipo = db.execute("""
            SELECT tipo, SUM(valor) as total, COUNT(*) as qtd,
                   SUM(quantidade_litros) as litros
            FROM ht_despesas
            WHERE id_veiculo = ? AND data BETWEEN ? AND ?
            GROUP BY tipo ORDER BY total DESC
        """, (veiculo_id, inicio, fim)).fetchall()

        # Score por condutor
        condutores = db.execute("""
            SELECT d.id_condutor, u.nome,
                   COUNT(*) as qtd_abast,
                   SUM(d.quantidade_litros) as litros,
                   SUM(d.valor) as gasto,
                   MAX(d.kilometragem) as km_max,
                   MIN(d.kilometragem) as km_min
            FROM ht_despesas d
            LEFT JOIN ht_usuarios u ON u.ixc_funcionario_id = d.id_condutor
            WHERE d.id_veiculo = ? AND d.tipo = 'C'
              AND d.data BETWEEN ? AND ?
              AND d.kilometragem > 0
            GROUP BY d.id_condutor
            ORDER BY gasto DESC
        """, (veiculo_id, inicio, fim)).fetchall()

        score_condutores = []
        for c in condutores:
            km_c = (c["km_max"] - c["km_min"]) if c["km_max"] > c["km_min"] else 0
            kml_c = round(km_c / c["litros"], 1) if c["litros"] and c["litros"] > 0 and km_c > 0 else None
            esperado = v["consumo_esperado_kml"]
            status = "ok"
            if kml_c and esperado:
                pct = ((kml_c - esperado) / esperado) * 100
                status = "bom" if pct >= -10 else ("atencao" if pct >= -20 else "critico")
            score_condutores.append({
                "condutor": c["nome"] or f"ID {c['id_condutor']}",
                "abastecimentos": c["qtd_abast"],
                "litros": round(float(c["litros"] or 0), 2),
                "gasto": round(float(c["gasto"] or 0), 2),
                "km_rodado": km_c,
                "kml": kml_c,
                "status": status
            })

        # OS atendidas com este veículo
        os_stats = db.execute("""
            SELECT COUNT(DISTINCT k.ixc_os_id) as total_os,
                   SUM(k.km_deslocamento) as km_total,
                   AVG(k.km_deslocamento) as km_medio_os
            FROM ht_km_os k
            WHERE k.veiculo_id = ?
              AND DATE(k.criado_em) BETWEEN ? AND ?
        """, (veiculo_id, inicio, fim)).fetchone()

        # Histórico de posse
        posses = db.execute("""
            SELECT p.assumido_em, p.entregue_em, u.nome as tecnico
            FROM ht_veiculo_posse p
            JOIN ht_usuarios u ON u.id = p.id_tecnico
            WHERE p.id_veiculo = ?
            ORDER BY p.id DESC LIMIT 10
        """, (veiculo_id,)).fetchall()

        return {
            "veiculo": dict(v),
            "periodo": {"inicio": inicio, "fim": fim},
            "abastecimentos": hist_abast,
            "mensal": hist_mensal,
            "despesas_por_tipo": [
                {
                    "tipo": d["tipo"],
                    "label": TIPO_LABEL.get(d["tipo"], d["tipo"]),
                    "total": round(float(d["total"] or 0), 2),
                    "qtd": d["qtd"],
                    "litros": round(float(d["litros"] or 0), 2) if d["litros"] else 0
                }
                for d in desp_tipo
            ],
            "score_condutores": score_condutores,
            "os_stats": {
                "total_os": os_stats["total_os"] or 0,
                "km_total": round(float(os_stats["km_total"] or 0), 1),
                "km_medio_por_os": round(float(os_stats["km_medio_os"] or 0), 1)
            },
            "posses": [dict(p) for p in posses]
        }
    finally:
        db.close()


@router.get("/score-condutores")
def score_condutores_geral(
    inicio: str = None, fim: str = None,
    usuario=Depends(requer_supervisor)
):
    """Ranking de condutores por eficiência de combustível."""
    if not inicio: inicio = "2026-01-01"
    if not fim: fim = brt()[:10]

    db = get_db()
    try:
        rows = db.execute("""
            SELECT d.id_condutor, u.nome,
                   COUNT(*) as qtd_abast,
                   SUM(d.quantidade_litros) as total_litros,
                   SUM(d.valor) as total_gasto,
                   COUNT(DISTINCT d.id_veiculo) as veiculos_usados
            FROM ht_despesas d
            LEFT JOIN ht_usuarios u ON u.ixc_funcionario_id = d.id_condutor
            WHERE d.tipo = 'C' AND d.data BETWEEN ? AND ?
              AND d.quantidade_litros > 0
            GROUP BY d.id_condutor
            ORDER BY total_litros DESC
        """, (inicio, fim)).fetchall()

        return [
            {
                "condutor": r["nome"] or f"ID {r['id_condutor']}",
                "id_condutor": r["id_condutor"],
                "abastecimentos": r["qtd_abast"],
                "total_litros": round(float(r["total_litros"] or 0), 2),
                "total_gasto": round(float(r["total_gasto"] or 0), 2),
                "veiculos_usados": r["veiculos_usados"]
            }
            for r in rows
        ]
    finally:
        db.close()
