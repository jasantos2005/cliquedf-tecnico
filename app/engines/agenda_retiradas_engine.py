"""
agenda_retiradas_engine.py — Motor de Agendamento de Retiradas v1
"""
import sqlite3, math, logging
from datetime import datetime, date, timedelta
from typing import Optional

log = logging.getLogger(__name__)

DB_PATH        = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"
JORNADA_INICIO = 8
TEMPO_RETIRADA = 10
DESLOCAMENTO   = 15
RAIO_MAX_KM    = 8.0
MAX_OS_ROTA    = 8
ID_ASSUNTOS_RETIRADA = {22, 39, 89, 111}
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

def carregar_os_retirada(data: str, db) -> list:
    ids = ','.join(str(i) for i in ID_ASSUNTOS_RETIRADA)
    cur = db.cursor()
    cur.execute(f"""
        SELECT o.id, o.ixc_os_id, o.id_tecnico, o.id_assunto, o.assunto_nome,
               o.status_ixc, o.status_hub, o.cliente_nome, o.endereco,
               o.bairro, o.cidade, o.lat, o.lon, o.data_abertura,
               o.data_agenda, o.horas_abertas, o.sla_estourado
        FROM ht_os o
        WHERE o.status_ixc IN ('A','AG','RAG')
          AND o.id_assunto IN ({ids})
          AND o.lat IS NOT NULL AND o.lat != 0
          AND o.lon IS NOT NULL AND o.lon != 0
        ORDER BY o.horas_abertas DESC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    return [r for r in rows if coord_valida(r.get('lat'), r.get('lon'))]

def clusterizar(lista_os: list) -> dict:
    """
    Agrupa apenas por proximidade geografica (complete-linkage, raio RAIO_MAX_KM).
    Nao agrupa por cidade — evita que cidades distintas mas proximas fiquem separadas
    e que OS distantes da mesma cidade fiquem juntas.
    """
    nao_agrupados = [o for o in lista_os if o.get('lat') and o.get('lon')]
    sem_coord     = [o for o in lista_os if not o.get('lat') or not o.get('lon')]
    sub_clusters  = []

    while nao_agrupados:
        semente = nao_agrupados.pop(0)
        grupo   = [semente]
        ainda_nao = []
        for os in nao_agrupados:
            todas_proximas = all(
                haversine(os['lat'], os['lon'], g['lat'], g['lon']) <= RAIO_MAX_KM
                for g in grupo
            )
            if todas_proximas:
                grupo.append(os)
            else:
                ainda_nao.append(os)
        sub_clusters.append(grupo)
        nao_agrupados = ainda_nao

    # OS sem coordenadas ficam em cluster proprio
    if sem_coord:
        sub_clusters.append(sem_coord)

    clusters = {}
    for idx, grupo in enumerate(sub_clusters):
        cidades = [o.get('cidade') or 'SEM_CIDADE' for o in grupo]
        bairros = [o.get('bairro') or 'SEM_BAIRRO' for o in grupo]
        cidade_ref = max(set(cidades), key=cidades.count).strip().upper()
        bairro_ref = max(set(bairros), key=bairros.count).strip().upper()
        sufixo = f" {idx+1}" if len(sub_clusters) > 1 else ""
        clusters[f"{cidade_ref} — {bairro_ref}{sufixo}"] = grupo
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
        os['tempo_min']          = TEMPO_RETIRADA
        os['dist_anterior_km']   = round(haversine(lat_prev, lon_prev, os['lat'], os['lon']), 1)
        lat_prev, lon_prev = os['lat'], os['lon']
        hora_atual += timedelta(minutes=TEMPO_RETIRADA + DESLOCAMENTO)
    return os_list

def max_dist_cluster(os1, os2):
    """Distancia maxima entre qualquer par de OS dos dois grupos."""
    md = 0
    for a in os1:
        for b in os2:
            if a.get('lat') and b.get('lat'):
                d = haversine(a['lat'], a['lon'], b['lat'], b['lon'])
                if d > md: md = d
    return md

def mesclar_retiradas(rotas: list, raio_km: float = RAIO_MAX_KM, data: str = None) -> list:
    def min_dist(os1, os2):
        md = float('inf')
        for a in os1:
            for b in os2:
                if a.get('lat') and b.get('lat'):
                    d = haversine(a['lat'], a['lon'], b['lat'], b['lon'])
                    if d < md: md = d
        return md if md != float('inf') else 999
    modificou = True
    while modificou:
        modificou = False
        novas = []
        usadas = set()
        for i, r1 in enumerate(rotas):
            if i in usadas: continue
            melhor_j, melhor_d = None, raio_km
            for j, r2 in enumerate(rotas):
                if j <= i or j in usadas: continue
                if r1.get('garagem') != r2.get('garagem'): continue
                # So mescla se distancia MAXIMA entre todos os pares <= raio
                if max_dist_cluster(r1['os'], r2['os']) > raio_km: continue
                d = min_dist(r1['os'], r2['os'])
                if d < melhor_d: melhor_d = d; melhor_j = j
            if melhor_j is not None and len(r1['os']) + len(rotas[melhor_j]['os']) <= MAX_OS_ROTA:
                r2 = rotas[melhor_j]
                g = next((g for g in GARAGENS.values() if g['nome'] == r1['garagem']), None)
                os_m = sequenciar(r1['os'] + r2['os'], g['lat'] if g else r1['os'][0]['lat'], g['lon'] if g else r1['os'][0]['lon'])
                bairros = [o.get('bairro') or 'SEM_BAIRRO' for o in os_m]
                cidades = [o.get('cidade') or 'SEM_CIDADE' for o in os_m]
                bairro_ref = max(set(bairros), key=bairros.count).strip().upper()
                cidade_ref = max(set(cidades), key=cidades.count).strip().upper()
                tempo = sum(o.get('tempo_min', TEMPO_RETIRADA) for o in os_m) + (len(os_m)-1)*DESLOCAMENTO
                novas.append({
                    'os': os_m, 'garagem': r1['garagem'],
                    'bairro_ref': f"{cidade_ref} — {bairro_ref}",
                    'total_os': len(os_m), 'tempo_est': tempo,
                    'tempo_fmt': f"{tempo//60}h{tempo%60:02d}min",
                    'distancia_km': r1.get('distancia_km', 0),
                })
                usadas.add(i); usadas.add(melhor_j); modificou = True
            else:
                novas.append(r1); usadas.add(i)
        rotas = novas
    # Recalcular horarios e distancias apos mescla
    for r in rotas:
        g = next((g for g in GARAGENS.values() if g['nome'] == r.get('garagem')), None)
        if not g: continue
        r['os'] = sequenciar(r['os'], g['lat'], g['lon'])
        r['os'] = calcular_horarios_e_distancias(r['os'], data or date.today().isoformat(), g)
        dist = haversine(g['lat'], g['lon'], r['os'][0]['lat'], r['os'][0]['lon'])
        for i in range(len(r['os'])-1):
            dist += haversine(r['os'][i]['lat'], r['os'][i]['lon'], r['os'][i+1]['lat'], r['os'][i+1]['lon'])
        r['distancia_km'] = round(dist, 1)
    return rotas


def agrupar_por_endereco(lista_os: list) -> list:
    """
    Agrupa OS do mesmo cliente/endereco em uma unica parada.
    Concatena os assuntos e soma os tempos.
    """
    grupos = {}
    for os in lista_os:
        chave = (round(os['lat'], 4), round(os['lon'], 4))
        if chave not in grupos:
            grupos[chave] = dict(os)
            grupos[chave]['assuntos'] = [os['assunto_nome']]
        else:
            grupos[chave]['assuntos'].append(os['assunto_nome'])
            grupos[chave]['tempo_min'] = grupos[chave].get('tempo_min', TEMPO_RETIRADA) + TEMPO_RETIRADA
    result = []
    for os in grupos.values():
        assuntos = os.pop('assuntos', [os['assunto_nome']])
        if len(assuntos) > 1:
            os['assunto_nome'] = ' + '.join(dict.fromkeys(assuntos))
        result.append(os)
    return result


def gerar_rotas_retirada(data: Optional[str] = None) -> list:
    if not data:
        data = date.today().isoformat()
    db = get_db()
    try:
        os_list = carregar_os_retirada(data, db)
        if not os_list:
            return []
        os_list = agrupar_por_endereco(os_list)
        clusters = clusterizar(os_list)
        # Agrupar por garagem primeiro
        por_garagem = {gid: [] for gid in GARAGENS}
        for os in os_list:
            gid = min(GARAGENS, key=lambda g: haversine(os['lat'], os['lon'], GARAGENS[g]['lat'], GARAGENS[g]['lon']))
            por_garagem[gid].append(os)
        rotas_finais = []
        rota_num = 1
        for gid, os_garagem in por_garagem.items():
            if not os_garagem:
                continue
            garagem = GARAGENS[gid]
            sub_clusters = clusterizar(os_garagem)
            for chave, os_cluster in sub_clusters.items():
                os_seq = sequenciar(os_cluster, garagem['lat'], garagem['lon'])
                # Subdividir por MAX_OS_ROTA
                fatias = [os_seq[i:i+MAX_OS_ROTA] for i in range(0, len(os_seq), MAX_OS_ROTA)]
                for fatia in fatias:
                    fatia = calcular_horarios_e_distancias(fatia, data, garagem)
                    dist_total = haversine(garagem['lat'], garagem['lon'], fatia[0]['lat'], fatia[0]['lon'])
                    for i in range(len(fatia) - 1):
                        dist_total += haversine(fatia[i]['lat'], fatia[i]['lon'], fatia[i+1]['lat'], fatia[i+1]['lon'])
                    tempo_total = sum(o.get('tempo_min', TEMPO_RETIRADA) for o in fatia) + (len(fatia) - 1) * DESLOCAMENTO
                    rotas_finais.append({
                        'rota_num': rota_num, 'bairro_ref': chave,
                        'total_os': len(fatia), 'tempo_est': tempo_total,
                        'tempo_fmt': f"{tempo_total//60}h{tempo_total%60:02d}min",
                        'distancia_km': round(dist_total, 1),
                        'garagem': garagem['nome'], 'os': fatia,
                    })
                    rota_num += 1
        # Mesclar rotas pequenas da mesma garagem
        rotas_finais = mesclar_retiradas(rotas_finais, data=data)
        for idx, r in enumerate(rotas_finais, start=1):
            r['rota_num'] = idx
        return rotas_finais
    finally:
        db.close()

def confirmar_rota_retirada(data: str, tecnico_id: int, tecnico_nome: str,
                             os_ids: list, tempo_est: int, bairro_ref: str) -> int:
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("""
            INSERT INTO ht_rotas (data_rota, tecnico_id, tecnico_nome, pontos, tempo_est,
                                  total_os, bairro_ref, status, confirmado_em)
            VALUES (?, ?, ?, 0, ?, ?, ?, 'confirmada', datetime('now','-3 hours'))
        """, (data, tecnico_id, tecnico_nome, tempo_est, len(os_ids), f"[RETIRADA] {bairro_ref}"))
        rota_id = cur.lastrowid
        tec = db.execute("SELECT ixc_funcionario_id FROM ht_usuarios WHERE id=?", (tecnico_id,)).fetchone()
        ixc_func_id = tec["ixc_funcionario_id"] if tec and tec["ixc_funcionario_id"] else tecnico_id
        for ordem, os_item in enumerate(os_ids, start=1):
            cur.execute("INSERT INTO ht_rotas_os (rota_id, os_id, ordem, pontos) VALUES (?, ?, ?, 0)",
                        (rota_id, os_item['id'], ordem))
            hora_prevista = os_item.get('hora_prevista', f"{data} {JORNADA_INICIO:02d}:00")
            cur.execute("UPDATE ht_os SET id_tecnico=?, ordem_execucao=?, status_hub='agendada' WHERE id=?",
                        (tecnico_id, ordem, os_item['id']))
            row = db.execute("SELECT ixc_os_id FROM ht_os WHERE id=?", (os_item['id'],)).fetchone()
            if row and row["ixc_os_id"]:
                try:
                    from app.services.ixc_db import ixc_insert
                    ixc_insert(
                        "UPDATE ixcprovedor.su_oss_chamado SET id_tecnico=%s, status='AG', data_reservada=%s WHERE id=%s",
                        (ixc_func_id, hora_prevista, row["ixc_os_id"])
                    )
                except Exception as e:
                    log.warning(f"Erro ao agendar OS {row['ixc_os_id']} no IXC: {e}")
        db.commit()
        return rota_id
    finally:
        db.close()

def tecnicos_retirada() -> list:
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("SELECT id, nome, nivel, ixc_funcionario_id FROM ht_usuarios WHERE nivel IN (10,20) AND ativo=1 ORDER BY nome")
        return [dict(r) for r in cur.fetchall()]
    finally:
        db.close()
