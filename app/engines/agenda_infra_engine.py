"""
agenda_infra_engine.py — Motor de Agendamento de Infra v1
"""
import sqlite3, math, logging
from datetime import datetime, date, timedelta
from typing import Optional

log = logging.getLogger(__name__)

DB_PATH        = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"
JORNADA_INICIO = 8
TEMPO_INFRA    = 120
DESLOCAMENTO   = 20
RAIO_MAX_KM    = 8.0

ID_ASSUNTOS_INFRA = {
    3,7,16,53,138,142,143,145,146,148,151,152,153,154,
    155,156,157,158,159,160,161,162,163,164,165,166,167,168,
    169,170,171,172,173,174,175,176,177,178,179,180,181,182,
    183,185,186,187,188,221,222,232,242,243,244,247
}

GARAGENS = {
    1: {'nome': 'Neópolis',        'lat': -10.321895, 'lon': -36.579450},
    2: {'nome': 'Ilha das Flores', 'lat': -10.436325, 'lon': -36.534847},
}

LAT_MIN, LAT_MAX = -11.6, -9.5
LON_MIN, LON_MAX = -38.3, -36.3

def coord_valida(lat, lon) -> bool:
    try:
        return LAT_MIN <= float(lat) <= LAT_MAX and LON_MIN <= float(lon) <= LON_MAX
    except:
        return False

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(a))

def carregar_os_infra(data: str, db) -> list:
    ids = ','.join(str(i) for i in ID_ASSUNTOS_INFRA)
    cur = db.cursor()
    cur.execute(f"""
        SELECT o.id, o.ixc_os_id, o.id_tecnico, o.id_assunto, o.assunto_nome,
               o.status_ixc, o.status_hub, o.cliente_nome, o.endereco,
               o.bairro, o.cidade, o.lat, o.lon, o.data_abertura,
               o.data_agenda, o.horas_abertas, o.sla_estourado
        FROM ht_os o
        WHERE o.status_ixc IN ('A','AG','RAG')
          AND o.status_hub IN ('pendente','reagendada')
          AND o.id_assunto IN ({ids})
          AND o.lat IS NOT NULL AND o.lat != 0
          AND o.lon IS NOT NULL AND o.lon != 0
        ORDER BY o.horas_abertas DESC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    return [r for r in rows if coord_valida(r.get('lat'), r.get('lon'))]

def clusterizar(lista_os: list) -> dict:
    por_cidade = {}
    for os in lista_os:
        cidade = (os.get('cidade') or 'SEM_CIDADE').strip().upper()
        por_cidade.setdefault(cidade, []).append(os)
    clusters = {}
    for cidade, os_cidade in por_cidade.items():
        if len(os_cidade) == 1:
            bairro = (os_cidade[0].get('bairro') or 'SEM_BAIRRO').strip().upper()
            clusters[f"{cidade} — {bairro}"] = os_cidade
            continue
        nao_agrupados = os_cidade[:]
        sub_clusters = []
        while nao_agrupados:
            semente = nao_agrupados.pop(0)
            grupo = [semente]
            ainda_nao = []
            for os in nao_agrupados:
                if not os.get('lat'): ainda_nao.append(os); continue
                todas_proximas = all(
                    haversine(os['lat'], os['lon'], g['lat'], g['lon']) <= RAIO_MAX_KM
                    for g in grupo if g.get('lat')
                )
                if todas_proximas:
                    grupo.append(os)
                else:
                    ainda_nao.append(os)
            sub_clusters.append(grupo)
            nao_agrupados = ainda_nao
        for idx, grupo in enumerate(sub_clusters):
            bairros = [o.get('bairro') or 'SEM_BAIRRO' for o in grupo]
            bairro_ref = max(set(bairros), key=bairros.count).strip().upper()
            sufixo = f" {idx+1}" if len(sub_clusters) > 1 else ""
            clusters[f"{cidade} — {bairro_ref}{sufixo}"] = grupo
    return clusters

def sequenciar(lista_os: list, lat_ini: float, lon_ini: float) -> list:
    if len(lista_os) <= 1:
        return lista_os
    restantes = lista_os[:]
    pos = (lat_ini, lon_ini)
    ordenadas = []
    while restantes:
        mp = min(restantes, key=lambda o: haversine(pos[0], pos[1], o['lat'], o['lon']))
        ordenadas.append(mp)
        pos = (mp['lat'], mp['lon'])
        restantes.remove(mp)
    return ordenadas

def calcular_horarios_e_distancias(os_list: list, data: str, garagem: dict) -> list:
    hora_atual = datetime.strptime(f"{data} {JORNADA_INICIO:02d}:00", "%Y-%m-%d %H:%M")
    lat_prev, lon_prev = garagem['lat'], garagem['lon']
    for os in os_list:
        os['hora_prevista']      = hora_atual.strftime("%Y-%m-%d %H:%M")
        os['hora_prevista_fmt']  = hora_atual.strftime("%H:%M")
        os['tempo_min']          = TEMPO_INFRA
        os['dist_anterior_km']   = round(haversine(lat_prev, lon_prev, os['lat'], os['lon']), 1)
        os['pontos']             = 20
        lat_prev, lon_prev = os['lat'], os['lon']
        hora_atual += timedelta(minutes=TEMPO_INFRA + DESLOCAMENTO)
    return os_list

def gerar_rotas_infra(data: Optional[str] = None) -> list:
    if not data:
        data = date.today().isoformat()
    db = get_db()
    try:
        os_list = carregar_os_infra(data, db)
        if not os_list:
            return []
        clusters = clusterizar(os_list)
        rotas_finais = []
        rota_num = 1
        for chave, os_cluster in clusters.items():
            lat_c = sum(o['lat'] for o in os_cluster) / len(os_cluster)
            lon_c = sum(o['lon'] for o in os_cluster) / len(os_cluster)
            garagem = min(GARAGENS.values(), key=lambda g: haversine(lat_c, lon_c, g['lat'], g['lon']))
            os_seq = sequenciar(os_cluster, garagem['lat'], garagem['lon'])
            os_seq = calcular_horarios_e_distancias(os_seq, data, garagem)
            dist_total = haversine(garagem['lat'], garagem['lon'], os_seq[0]['lat'], os_seq[0]['lon'])
            for i in range(len(os_seq) - 1):
                dist_total += haversine(os_seq[i]['lat'], os_seq[i]['lon'], os_seq[i+1]['lat'], os_seq[i+1]['lon'])
            tempo_total = len(os_seq) * TEMPO_INFRA + (len(os_seq) - 1) * DESLOCAMENTO
            rotas_finais.append({
                'rota_num': rota_num, 'bairro_ref': chave,
                'total_os': len(os_seq), 'tempo_est': tempo_total,
                'tempo_fmt': f"{tempo_total//60}h{tempo_total%60:02d}min",
                'distancia_km': round(dist_total, 1),
                'garagem': garagem['nome'], 'os': os_seq,
            })
            rota_num += 1
        return rotas_finais
    finally:
        db.close()

def confirmar_rota_infra(data: str, tecnico_id: int, tecnico_nome: str,
                          os_ids: list, tempo_est: int, bairro_ref: str) -> int:
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("""
            INSERT INTO ht_rotas (data_rota, tecnico_id, tecnico_nome, pontos, tempo_est,
                                  total_os, bairro_ref, status, confirmado_em)
            VALUES (?, ?, ?, 0, ?, ?, ?, 'confirmada', datetime('now','-3 hours'))
        """, (data, tecnico_id, tecnico_nome, tempo_est, len(os_ids), f"[INFRA] {bairro_ref}"))
        rota_id = cur.lastrowid
        tec = db.execute("SELECT ixc_funcionario_id FROM ht_usuarios WHERE id=?", (tecnico_id,)).fetchone()
        ixc_func_id = tec["ixc_funcionario_id"] if tec and tec["ixc_funcionario_id"] else tecnico_id
        for ordem, os_item in enumerate(os_ids, start=1):
            cur.execute("INSERT INTO ht_rotas_os (rota_id, os_id, ordem, pontos) VALUES (?, ?, ?, 20)",
                        (rota_id, os_item['id'], ordem))
            hora_prevista = os_item.get('hora_prevista') or f"{data} {JORNADA_INICIO:02d}:00"
            if hora_prevista and len(hora_prevista) == 10:
                hora_prevista = f"{hora_prevista} {JORNADA_INICIO:02d}:00"
            cur.execute("UPDATE ht_os SET id_tecnico=?, ordem_execucao=?, status_hub='agendada' WHERE id=?",
                        (tecnico_id, ordem, os_item['id']))
            row = db.execute("SELECT ixc_os_id FROM ht_os WHERE id=?", (os_item['id'],)).fetchone()
            if row and row["ixc_os_id"]:
                try:
                    from app.services.ixc_db import ixc_insert
                    ixc_insert(
                        "UPDATE ixcprovedor.su_oss_chamado SET id_tecnico=%s, status='AG', data_reservada=%s, data_agenda=%s WHERE id=%s",
                        (ixc_func_id, hora_prevista[:10], hora_prevista, row["ixc_os_id"])
                    )
                except Exception as e:
                    log.warning(f"Erro ao agendar OS {row['ixc_os_id']} no IXC: {e}")
        db.commit()
        return rota_id
    finally:
        db.close()

def tecnicos_infra() -> list:
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("SELECT id, nome, nivel, ixc_funcionario_id FROM ht_usuarios WHERE nivel IN (10,20) AND ativo=1 ORDER BY nome")
        return [dict(r) for r in cur.fetchall()]
    finally:
        db.close()
