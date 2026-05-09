from fastapi import APIRouter, Depends
from app.services.auth import requer_supervisor, get_db
from app.services.ixc_db import ixc_select
from math import radians, sin, cos, sqrt, atan2

router = APIRouter(prefix="/api/despacho", tags=["despacho"])

def distancia_km(lat1, lon1, lat2, lon2):
    if not all([lat1, lon1, lat2, lon2]): return 999
    R = 6371
    dlat = radians(lat2-lat1); dlon = radians(lon2-lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

@router.get("/mapa")
def dados_mapa(usuario=Depends(requer_supervisor)):
    db = get_db()

    # OS do dia por status
    os_rows = db.execute("""
        SELECT o.*, u.nome AS tecnico_nome
        FROM ht_os o
        LEFT JOIN ht_usuarios u ON u.id = o.id_tecnico
        WHERE o.status_hub IN ('pendente','deslocamento','execucao','agendada','reagendada')
           OR (o.status_hub = 'finalizada' AND DATE(o.data_abertura) = DATE('now','-3 hours'))
        ORDER BY o.data_abertura ASC
    """).fetchall()

    # Posicoes GPS atuais + stats do dia
    gps_rows = db.execute("""
        SELECT g.id_tecnico, g.lat, g.lon, g.velocidade,
               g.status_tecnico, g.ixc_os_id, g.registrado_em,
               u.nome AS tecnico_nome,
               -- Veiculo com posse ativa
               v.placa, v.marca_modelo, v.tipo AS veiculo_tipo,
               -- Stats GPS do dia (BRT = UTC-3)
               (SELECT COUNT(*) FROM ht_gps_track g2
                WHERE g2.id_tecnico = g.id_tecnico
                AND g2.registrado_em >= strftime('%Y-%m-%d',datetime('now','-3 hours'))||' 00:00:00'
                AND g2.registrado_em <= strftime('%Y-%m-%d',datetime('now','-3 hours'))||' 23:59:59'
               ) AS pontos_hoje,
               (SELECT ROUND(MAX(g2.velocidade),1) FROM ht_gps_track g2
                WHERE g2.id_tecnico = g.id_tecnico
                AND g2.registrado_em >= strftime('%Y-%m-%d',datetime('now','-3 hours'))||' 00:00:00'
                AND g2.registrado_em <= strftime('%Y-%m-%d',datetime('now','-3 hours'))||' 23:59:59'
               ) AS vel_max_hoje,
               (SELECT g2.registrado_em FROM ht_gps_track g2
                WHERE g2.id_tecnico = g.id_tecnico
                AND g2.registrado_em >= strftime('%Y-%m-%d',datetime('now','-3 hours'))||' 00:00:00'
                ORDER BY g2.id ASC LIMIT 1) AS primeiro_gps_hoje,
               -- OS do dia
               (SELECT COUNT(*) FROM ht_os o2
                WHERE o2.id_tecnico = g.id_tecnico
                AND o2.status_hub = 'finalizada'
                AND o2.data_abertura >= strftime('%Y-%m-%d',datetime('now','-3 hours'))||' 00:00:00'
               ) AS os_finalizadas,
               (SELECT COUNT(*) FROM ht_os o2
                WHERE o2.id_tecnico = g.id_tecnico
                AND o2.status_hub IN ('agendada','reagendada')) AS os_agendadas,
               (SELECT COUNT(*) FROM ht_os o2
                WHERE o2.id_tecnico = g.id_tecnico
                AND o2.status_hub = 'execucao') AS os_execucao
        FROM ht_gps_track g
        JOIN ht_usuarios u ON u.id = g.id_tecnico
        LEFT JOIN ht_veiculo_posse p ON p.id_tecnico = g.id_tecnico AND p.entregue_em IS NULL
        LEFT JOIN ht_veiculos v ON v.id = p.id_veiculo
        WHERE g.id IN (
            SELECT MAX(id) FROM ht_gps_track GROUP BY id_tecnico
        )
    """).fetchall()

    # Stats
    stats = db.execute("""
        SELECT
            SUM(CASE WHEN status_hub='pendente' THEN 1 ELSE 0 END) as pendentes,
            SUM(CASE WHEN status_hub='deslocamento' THEN 1 ELSE 0 END) as deslocamento,
            SUM(CASE WHEN status_hub='execucao' THEN 1 ELSE 0 END) as execucao,
            SUM(CASE WHEN status_hub IN ('agendada','reagendada') THEN 1 ELSE 0 END) as agendadas,
            SUM(CASE WHEN status_hub='finalizada'
                AND DATE(data_abertura)=DATE('now','-3 hours') THEN 1 ELSE 0 END) as finalizadas
        FROM ht_os
    """).fetchone()

    db.close()
    return {
        "os": [dict(o) for o in os_rows],
        "tecnicos": [dict(g) for g in gps_rows],
        "stats": dict(stats) if stats else {}
    }

@router.get("/sugerir-tecnico/{ixc_os_id}")
def sugerir_tecnico(ixc_os_id: int, usuario=Depends(requer_supervisor)):
    db = get_db()
    os_row = db.execute("SELECT * FROM ht_os WHERE ixc_os_id=?", (ixc_os_id,)).fetchone()
    if not os_row: return []

    tecnicos = db.execute("""
        SELECT u.id, u.nome,
               COUNT(CASE WHEN o.status_hub IN ('pendente','deslocamento','execucao')
                     THEN 1 END) as os_ativas,
               g.lat, g.lon, g.status_tecnico
        FROM ht_usuarios u
        LEFT JOIN ht_os o ON o.id_tecnico = u.id
        LEFT JOIN ht_gps_track g ON g.id IN (
            SELECT MAX(id) FROM ht_gps_track WHERE id_tecnico=u.id
        )
        WHERE u.nivel = 10 AND u.ativo = 1
        GROUP BY u.id
    """).fetchall()
    db.close()

    result = []
    for t in tecnicos:
        dist = distancia_km(
            os_row["lat"], os_row["lon"],
            t["lat"], t["lon"]
        )
        result.append({
            **dict(t),
            "distancia_km": round(dist, 1),
            "score": round(dist + (t["os_ativas"] * 2), 1)
        })

    return sorted(result, key=lambda x: x["score"])
