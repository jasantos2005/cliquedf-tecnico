from fastapi import APIRouter, Depends
from typing import Optional
from app.services.auth import requer_supervisor, get_db

router = APIRouter(prefix="/api/custos", tags=["custos"])

TIPOS_COMB = ("Abastecimento",)
TIPOS_MANUT = ("Manutencao", "Manutenção", "Troca de Oleo", "Filtro de Ar",
               "Alinhamento/Balanceamento", "Troca de Pneus", "Revisao Geral")

@router.get("")
def consolidado(
    mes: Optional[str] = None,
    ano: Optional[str] = None,
    usuario=Depends(requer_supervisor)
):
    db = get_db()
    filtro = "WHERE 1=1"
    params = []
    if mes:
        filtro += " AND strftime('%m', d.data) = ?"
        params.append(mes.zfill(2))
    if ano:
        filtro += " AND strftime('%Y', d.data) = ?"
        params.append(str(ano))

    tipos_comb_str = ",".join("?" * len(TIPOS_COMB))
    tipos_manut_str = ",".join("?" * len(TIPOS_MANUT))

    sql = f"""
        SELECT
            v.id AS vid, v.ixc_veiculo_id, v.placa, v.marca_modelo, v.tipo AS tipo_veiculo,
            ROUND(SUM(CASE WHEN d.tipo IN ({tipos_comb_str}) THEN d.valor ELSE 0 END), 2) AS combustivel,
            ROUND(SUM(CASE WHEN d.tipo IN ({tipos_manut_str}) THEN d.valor ELSE 0 END), 2) AS manutencao,
            ROUND(SUM(CASE WHEN d.tipo NOT IN ({tipos_comb_str},{tipos_manut_str}) THEN d.valor ELSE 0 END), 2) AS outros,
            ROUND(SUM(d.valor), 2) AS total,
            COUNT(*) AS lancamentos
        FROM ht_despesas d
        JOIN ht_veiculos v ON v.ixc_veiculo_id = d.id_veiculo
        {filtro}
        GROUP BY v.id, v.ixc_veiculo_id, v.placa, v.marca_modelo, v.tipo
        ORDER BY total DESC
    """
    all_params = list(TIPOS_COMB) + list(TIPOS_MANUT) + list(TIPOS_COMB) + list(TIPOS_MANUT) + params
    rows = db.execute(sql, all_params).fetchall()
    db.close()
    return [dict(r) for r in rows]
