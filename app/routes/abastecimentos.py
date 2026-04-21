from fastapi import APIRouter, Depends
from typing import Optional
from app.services.auth import requer_supervisor, get_db

router = APIRouter(prefix="/api/abastecimentos", tags=["abastecimentos"])

@router.get("")
def listar(
    id_veiculo: Optional[int] = None,
    mes: Optional[str] = None,
    ano: Optional[str] = None,
    usuario=Depends(requer_supervisor)
):
    db = get_db()
    sql = """
        WITH ord AS (
            SELECT d.id, d.id_veiculo, d.data, d.valor,
                   d.kilometragem, d.quantidade_litros, d.valor_litro,
                   d.observacao, d.ixc_despesa_id,
                   v.placa, v.marca_modelo,
                   LAG(d.kilometragem) OVER (
                       PARTITION BY d.id_veiculo
                       ORDER BY d.data, d.id
                   ) AS km_ant
            FROM ht_despesas d
            JOIN ht_veiculos v ON v.ixc_veiculo_id = d.id_veiculo
            WHERE d.tipo = 'Abastecimento'
              AND d.quantidade_litros > 0
              AND d.kilometragem > 0
        ),
        efic AS (
            SELECT *,
                CASE WHEN km_ant IS NOT NULL AND kilometragem > km_ant
                     THEN ROUND((kilometragem - km_ant) / quantidade_litros, 2)
                     ELSE NULL END AS km_por_litro,
                CASE WHEN km_ant IS NOT NULL AND kilometragem > km_ant
                     THEN kilometragem - km_ant
                     ELSE NULL END AS km_rodados
            FROM ord
        ),
        medias AS (
            SELECT id_veiculo, ROUND(AVG(km_por_litro), 2) AS media_veiculo
            FROM efic WHERE km_por_litro IS NOT NULL
            GROUP BY id_veiculo
        )
        SELECT e.*,
               m.media_veiculo AS media_km_litro,
               CASE WHEN e.km_por_litro IS NOT NULL
                         AND m.media_veiculo IS NOT NULL
                         AND e.km_por_litro < m.media_veiculo * 0.80
                    THEN 1 ELSE 0 END AS alerta_consumo
        FROM efic e
        LEFT JOIN medias m ON m.id_veiculo = e.id_veiculo
        WHERE 1=1
    """
    params = []
    if id_veiculo:
        sql += " AND e.id_veiculo = ?"
        params.append(id_veiculo)
    if mes:
        sql += " AND strftime('%m', e.data) = ?"
        params.append(mes.zfill(2))
    if ano:
        sql += " AND strftime('%Y', e.data) = ?"
        params.append(str(ano))
    sql += " ORDER BY e.data DESC, e.id DESC LIMIT 300"
    rows = db.execute(sql, params).fetchall()
    db.close()
    return [dict(r) for r in rows]

@router.get("/resumo")
def resumo(usuario=Depends(requer_supervisor)):
    """Resumo por veiculo: total gasto, media km/L, custo/km."""
    db = get_db()
    sql = """
        WITH ord AS (
            SELECT d.id_veiculo, d.valor, d.kilometragem, d.quantidade_litros,
                   LAG(d.kilometragem) OVER (
                       PARTITION BY d.id_veiculo ORDER BY d.data, d.id
                   ) AS km_ant
            FROM ht_despesas d
            WHERE d.tipo = 'Abastecimento' AND d.quantidade_litros > 0 AND d.kilometragem > 0
        ),
        calc AS (
            SELECT id_veiculo, valor, quantidade_litros,
                CASE WHEN km_ant IS NOT NULL AND kilometragem > km_ant
                     THEN (kilometragem - km_ant) / quantidade_litros ELSE NULL END AS kml,
                CASE WHEN km_ant IS NOT NULL AND kilometragem > km_ant
                     THEN CAST(kilometragem - km_ant AS REAL) ELSE NULL END AS km_rod
            FROM ord
        )
        SELECT c.id_veiculo, v.placa, v.marca_modelo, v.tipo AS tipo_veiculo,
               COUNT(*) AS total_abast,
               ROUND(SUM(c.valor), 2) AS total_gasto,
               ROUND(SUM(c.quantidade_litros), 2) AS total_litros,
               ROUND(AVG(c.kml), 2) AS media_km_litro,
               ROUND(SUM(c.km_rod), 0) AS km_total,
               ROUND(SUM(c.valor) / NULLIF(SUM(c.km_rod), 0), 4) AS custo_por_km
        FROM calc c
        JOIN ht_veiculos v ON v.ixc_veiculo_id = c.id_veiculo
        GROUP BY c.id_veiculo, v.placa, v.marca_modelo, v.tipo
        ORDER BY total_gasto DESC
    """
    rows = db.execute(sql).fetchall()
    db.close()
    return [dict(r) for r in rows]
