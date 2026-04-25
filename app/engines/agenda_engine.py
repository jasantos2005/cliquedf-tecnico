"""
agenda_engine.py — Motor de Agendamento Automático v2
Melhorias:
  - Agrupamento por cidade + bairro
  - Extração de cidade do endereço
  - Otimização de rota por distância real
  - Cálculo de horário estimado por OS
  - Agendamento no IXC com data/hora
  - Pontos e tempos atualizados por tipo de OS
"""

import sqlite3
import math
import re
import logging
from datetime import datetime, date, timedelta
from typing import Optional

log = logging.getLogger(__name__)

# ── Configurações ─────────────────────────────────────────────────────────────
CAPACIDADE_PONTOS = 80
TOLERANCIA_PONTOS = 5
PONTOS_PADRAO     = 10
TEMPO_PADRAO_MIN  = 45
JORNADA_INICIO    = 8  # hora de início (8h)
DB_PATH = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"

# ── Mapa de pontos por assunto ─────────────────────────────────────────────────
PONTOS_ASSUNTO = {
    'ATIVACAO FIBRA': 10,
    '[OPC] ATIVAÇÃO FIBRA - NOVO': 10,
    'REATIVAÇÃO': 10,
    'MANUTENÇÃO': 10,
    'SEM ACESSO': 10,
    'INTERNET LENTA': 10,
    '[DF] VERIFICAR CONEXÃO': 10,
    'TROCA DE SENHA': 10,
    'RECOLHIMENTO DE EQUIPAMENTO': 10,
    'RETIRADA DE EQUIPAMENTO': 10,
    'RETIRAR FIBRA': 10,
    'RECOLHILMENTO DE EQUIPAMENTO( M. TITULARIDADE)': 10,
    'MUDANÇA DE ENDEREÇO': 20,
    'MUDANÇA DE EQUIPAMENTO DE CÔMODO': 20,
    'MUDANÇA DE TITULARIDADE ( TECNICO)': 20,
    '[INFCDF] INFRA/PROJETOS': 20,
}

# ── Mapa de tempos por assunto (minutos) ──────────────────────────────────────
TEMPO_ASSUNTO = {
    'ATIVACAO FIBRA': 90,
    '[OPC] ATIVAÇÃO FIBRA - NOVO': 90,
    'REATIVAÇÃO': 90,
    'MANUTENÇÃO': 60,
    'SEM ACESSO': 60,
    'INTERNET LENTA': 30,
    '[DF] VERIFICAR CONEXÃO': 30,
    'TROCA DE SENHA': 30,
    'RECOLHIMENTO DE EQUIPAMENTO': 30,
    'RETIRADA DE EQUIPAMENTO': 30,
    'RETIRAR FIBRA': 30,
    'RECOLHILMENTO DE EQUIPAMENTO( M. TITULARIDADE)': 30,
    'MUDANÇA DE ENDEREÇO': 45,
    'MUDANÇA DE EQUIPAMENTO DE CÔMODO': 45,
    'MUDANÇA DE TITULARIDADE ( TECNICO)': 45,
    '[INFCDF] INFRA/PROJETOS': 120,
}

# ── Utilitários ───────────────────────────────────────────────────────────────
def get_db():
    import glob, os
    path = DB_PATH
    if not os.path.exists(path):
        dbs = glob.glob("/opt/automacoes/cliquedf/tecnico/*.db")
        path = dbs[0] if dbs else DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def extrair_cidade(endereco: str) -> str:
    """Extrai cidade do campo endereco. Ex: 'SE Neópolis 49980-000 CENTRO...' → 'Neópolis'"""
    if not endereco:
        return 'SEM_CIDADE'
    # Padrão: SE <Cidade> <CEP>
    m = re.search(r'SE\s+([A-Za-zÀ-ÿ\s]+?)\s+\d{5}-\d{3}', endereco)
    if m:
        return m.group(1).strip().upper()
    return 'SEM_CIDADE'


def calcular_horarios(os_list: list, data: str, garagem: dict = None) -> list:
    """Calcula horário estimado e distância entre OS da rota."""
    hora_atual = datetime.strptime(f"{data} {JORNADA_INICIO:02d}:00", "%Y-%m-%d %H:%M")
    DESLOCAMENTO_MIN = 15

    lat_prev = garagem['lat'] if garagem else None
    lon_prev = garagem['lon'] if garagem else None

    for os in os_list:
        os['hora_prevista']     = hora_atual.strftime("%Y-%m-%d %H:%M")
        os['hora_prevista_fmt'] = hora_atual.strftime("%H:%M")
        if lat_prev and os.get('lat'):
            os['dist_anterior_km'] = round(haversine(lat_prev, lon_prev, os['lat'], os['lon']), 1)
        else:
            os['dist_anterior_km'] = 0
        lat_prev = os.get('lat')
        lon_prev = os.get('lon')
        tempo = os.get('tempo_min', TEMPO_PADRAO_MIN)
        hora_atual += timedelta(minutes=tempo + DESLOCAMENTO_MIN)

    return os_list


# ── Etapa 1: Carregar OS ──────────────────────────────────────────────────────
# IDs de assunto excluídos da rota normal (infra + retirada)
IDS_INFRA = {
    3,7,16,53,138,142,143,145,146,148,151,152,153,154,
    155,156,157,158,159,160,161,162,163,164,165,166,167,168,
    169,170,171,172,173,174,175,176,177,178,179,180,181,182,
    183,185,186,187,188,221,222,232,242,243,244,247
}
IDS_RETIRADA = {6,22,39,40,89,111,127}
IDS_EXCLUIDOS = IDS_INFRA | IDS_RETIRADA

def carregar_os(data: str, db, incluir_excluidos: bool = False) -> list:
    cur = db.cursor()
    cur.execute("""
        SELECT
            o.id, o.ixc_os_id, o.id_tecnico, o.id_assunto, o.assunto_nome,
            o.status_ixc, o.status_hub, o.cliente_nome, o.endereco,
            o.bairro, o.cidade, o.lat, o.lon, o.data_abertura,
            o.data_agenda, o.sla_horas, o.horas_abertas, o.sla_estourado
        FROM ht_os o
        WHERE
            o.status_ixc IN ('A', 'AG', 'RAG')
            AND o.lat IS NOT NULL AND o.lat != 0
            AND o.lon IS NOT NULL AND o.lon != 0
            AND (
                DATE(o.data_agenda) = ?
                OR (o.data_agenda IS NULL OR o.data_agenda = '' OR o.data_agenda = '0000-00-00 00:00:00')
            )
        ORDER BY o.sla_estourado DESC, o.horas_abertas DESC
    """, (data,))
    rows = [dict(r) for r in cur.fetchall()]
    # Filtra coordenadas fora de Sergipe
    rows = [r for r in rows if coord_valida(r.get('lat'), r.get('lon'))]
    # Filtra por ID de assunto — robusto independente do nome
    if not incluir_excluidos:
        rows = [r for r in rows if (r.get('id_assunto') or 0) not in IDS_EXCLUIDOS]
    log.info(f"OS carregadas para {data}: {len(rows)} (excluidos={'nao' if incluir_excluidos else 'sim'})")
    return rows


# ── Etapa 2: Enriquecer com pontos e cidade ───────────────────────────────────
def enriquecer_pontos(lista_os: list, db) -> list:
    cur = db.cursor()
    cur.execute("SELECT id_assunto, pontos, tempo_min, categoria FROM ht_assunto_pontos")
    mapa_db = {r['id_assunto']: dict(r) for r in cur.fetchall()}

    for os in lista_os:
        assunto = os.get('assunto_nome', '')
        info = mapa_db.get(os['id_assunto'])
        if info:
            os['pontos']    = info['pontos']
            os['tempo_min'] = info['tempo_min']
            os['categoria'] = info['categoria']
        else:
            os['pontos']    = PONTOS_ASSUNTO.get(assunto, PONTOS_PADRAO)
            os['tempo_min'] = TEMPO_ASSUNTO.get(assunto, TEMPO_PADRAO_MIN)
            os['categoria'] = 'outros'

        # Extrai cidade do endereço se não estiver preenchida
        if not os.get('cidade'):
            os['cidade'] = extrair_cidade(os.get('endereco', ''))
        else:
            os['cidade'] = os['cidade'].strip().upper()

    return lista_os


# ── Etapa 3: Priorizar ────────────────────────────────────────────────────────
def priorizar(lista_os: list, data_alvo: str) -> list:
    def score(os):
        p1 = 0 if os.get('sla_estourado') else 1
        p2 = 0 if (os.get('data_agenda') or '')[:10] == data_alvo else 1
        p3 = -(os.get('horas_abertas') or 0)
        return (p1, p2, p3)
    return sorted(lista_os, key=score)


# ── Etapa 4: Clusterizar por cidade + proximidade ────────────────────────────
def clusterizar_por_cidade_bairro(lista_os: list, raio_max_km: float = 8.0) -> dict:
    """
    Agrupa por cidade primeiro, depois por proximidade geográfica dentro da cidade.
    OS com distância > raio_max_km entre si formam clusters separados.
    """
    # 1. Agrupar por cidade
    por_cidade = {}
    for os in lista_os:
        cidade = (os.get('cidade') or 'SEM_CIDADE').strip().upper()
        if cidade not in por_cidade:
            por_cidade[cidade] = []
        por_cidade[cidade].append(os)

    clusters = {}
    for cidade, os_cidade in por_cidade.items():
        if len(os_cidade) == 1:
            bairro = (os_cidade[0].get('bairro') or 'SEM_BAIRRO').strip().upper()
            chave = f"{cidade} — {bairro}"
            clusters[chave] = os_cidade
            continue

        # 2. Dentro da cidade, agrupar por proximidade (single-linkage clustering)
        nao_agrupados = os_cidade[:]
        sub_clusters = []

        while nao_agrupados:
            semente = nao_agrupados.pop(0)
            grupo = [semente]
            ainda_nao = []
            for os in nao_agrupados:
                # Verifica se está próximo de qualquer OS já no grupo
                proximo = any(
                    haversine(os['lat'], os['lon'], g['lat'], g['lon']) <= raio_max_km
                    for g in grupo
                    if g.get('lat') and os.get('lat')
                )
                if proximo:
                    grupo.append(os)
                else:
                    ainda_nao.append(os)
            sub_clusters.append(grupo)
            nao_agrupados = ainda_nao

        # 3. Nomear cada sub-cluster pela cidade + bairro mais frequente
        for idx, grupo in enumerate(sub_clusters):
            bairros = [o.get('bairro') or 'SEM_BAIRRO' for o in grupo]
            bairro_ref = max(set(bairros), key=bairros.count).strip().upper()
            sufixo = f" {idx+1}" if len(sub_clusters) > 1 else ""
            chave = f"{cidade} — {bairro_ref}{sufixo}"
            clusters[chave] = grupo

    log.info(f"Clusters por cidade+proximidade: {len(clusters)} — {list(clusters.keys())}")
    return clusters


# ── Etapa 5: Subdividir por capacidade ───────────────────────────────────────
def subdividir_por_pontos(lista_os: list, capacidade: int = CAPACIDADE_PONTOS + TOLERANCIA_PONTOS) -> list:
    rotas = []
    atual = []
    pts   = 0
    tempo = 0

    for os in lista_os:
        p = os.get('pontos', PONTOS_PADRAO)
        t = os.get('tempo_min', TEMPO_PADRAO_MIN)
        if pts + p <= capacidade:
            atual.append(os)
            pts   += p
            tempo += t
        else:
            if atual:
                rotas.append({'os': atual, 'pontos': pts, 'tempo_est': tempo})
            atual = [os]
            pts   = p
            tempo = t

    if atual:
        rotas.append({'os': atual, 'pontos': pts, 'tempo_est': tempo})

    return rotas


# ── Etapa 6: Sequenciar por proximidade ──────────────────────────────────────
GARAGENS = {
    1: {'nome': 'Neópolis',       'lat': -10.321895, 'lon': -36.579450},
    2: {'nome': 'Ilha das Flores', 'lat': -10.436325, 'lon': -36.534847},
}

# Bounding box de Sergipe — filtra coordenadas absurdas
LAT_MIN, LAT_MAX = -11.6, -9.5
LON_MIN, LON_MAX = -38.3, -36.3

def coord_valida(lat, lon) -> bool:
    try:
        return LAT_MIN <= float(lat) <= LAT_MAX and LON_MIN <= float(lon) <= LON_MAX
    except:
        return False

# Técnicos por garagem (id_usuario: garagem_id)
def get_garagem_tecnico(tecnico_id: int) -> dict:
    db = get_db()
    row = db.execute("SELECT garagem_id FROM ht_usuarios WHERE id=?", (tecnico_id,)).fetchone()
    db.close()
    gid = row["garagem_id"] if row and row["garagem_id"] else 1
    return GARAGENS.get(gid, GARAGENS[1])

def sequenciar_por_proximidade(lista_os: list, lat_inicio: float = None, lon_inicio: float = None) -> list:
    if len(lista_os) <= 1:
        return lista_os

    restantes = lista_os[:]

    # Ponto de partida: garagem ou centroide
    if lat_inicio and lon_inicio:
        pos = (lat_inicio, lon_inicio)
    else:
        lat_c = sum(o['lat'] for o in restantes) / len(restantes)
        lon_c = sum(o['lon'] for o in restantes) / len(restantes)
        pos = (lat_c, lon_c)

    ordenadas = []

    while restantes:
        mais_proximo = min(
            restantes,
            key=lambda o: haversine(pos[0], pos[1], o['lat'], o['lon'])
        )
        ordenadas.append(mais_proximo)
        pos = (mais_proximo['lat'], mais_proximo['lon'])
        restantes.remove(mais_proximo)

    return ordenadas


# ── Etapa 5b: Mesclar rotas pequenas próximas ────────────────────────────────
def mesclar_rotas_proximas(rotas: list, capacidade: int = CAPACIDADE_PONTOS + TOLERANCIA_PONTOS, raio_km: float = 8.0) -> list:
    def max_dist_entre_rotas(os1, os2):
        max_d = 0
        for a in os1:
            for b in os2:
                if a.get('lat') and b.get('lat') and a['lat'] and b['lat']:
                    d = haversine(a['lat'], a['lon'], b['lat'], b['lon'])
                    if d > max_d:
                        max_d = d
        return max_d

    modificou = True
    while modificou:
        modificou = False
        novas = []
        usadas = set()
        for i, r1 in enumerate(rotas):
            if i in usadas:
                continue
            melhor_j = None
            melhor_dist = raio_km
            for j, r2 in enumerate(rotas):
                if j <= i or j in usadas:
                    continue
                if r1['pontos'] + r2['pontos'] > capacidade:
                    continue
                dist = max_dist_entre_rotas(r1['os'], r2['os'])
                if dist < melhor_dist:
                    melhor_dist = dist
                    melhor_j = j
            if melhor_j is not None:
                r2 = rotas[melhor_j]
                os_merged = sequenciar_por_proximidade(r1['os'] + r2['os'])
                novas.append({
                    'os':        os_merged,
                    'pontos':    r1['pontos'] + r2['pontos'],
                    'tempo_est': r1['tempo_est'] + r2['tempo_est'],
                    'bairro_ref': r1['bairro_ref'] + ' + ' + r2['bairro_ref'],
                    'total_os':  r1['total_os'] + r2['total_os'],
                })
                usadas.add(i)
                usadas.add(melhor_j)
                modificou = True
            else:
                novas.append(r1)
                usadas.add(i)
        rotas = novas
    return rotas


# ── Gerar rotas ───────────────────────────────────────────────────────────────
def gerar_rotas(data: Optional[str] = None) -> list:
    if not data:
        data = date.today().isoformat()

    db = get_db()
    try:
        os_list = carregar_os(data, db)
        if not os_list:
            return []

        os_list = enriquecer_pontos(os_list, db)
        os_list = priorizar(os_list, data)
        clusters = clusterizar_por_cidade_bairro(os_list)

        rotas_finais = []
        rota_num = 1

        # Garagem mais próxima do cluster para definir ponto de partida
        for chave, os_cluster in clusters.items():
            sub_rotas = subdividir_por_pontos(os_cluster)
            # Centroide do cluster
            lat_c = sum(o['lat'] for o in os_cluster) / len(os_cluster)
            lon_c = sum(o['lon'] for o in os_cluster) / len(os_cluster)
            # Garagem mais próxima
            garagem = min(GARAGENS.values(), key=lambda g: haversine(lat_c, lon_c, g['lat'], g['lon']))
            for sr in sub_rotas:
                sr['os'] = sequenciar_por_proximidade(sr['os'], garagem['lat'], garagem['lon'])
                sr['os'] = calcular_horarios(sr['os'], data, garagem)
                sr['rota_num']   = rota_num
                sr['bairro_ref'] = chave
                sr['total_os']   = len(sr['os'])
                sr['garagem']    = garagem['nome']
                # Distância total da rota
                dist = haversine(garagem['lat'], garagem['lon'], sr['os'][0]['lat'], sr['os'][0]['lon'])
                for i in range(len(sr['os'])-1):
                    dist += haversine(sr['os'][i]['lat'], sr['os'][i]['lon'], sr['os'][i+1]['lat'], sr['os'][i+1]['lon'])
                sr['distancia_km'] = round(dist, 1)
                rota_num += 1
                rotas_finais.append(sr)

        rotas_finais = mesclar_rotas_proximas(rotas_finais)

        for idx, r in enumerate(rotas_finais, start=1):
            r['rota_num'] = idx

        log.info(f"Rotas geradas: {len(rotas_finais)}")
        return rotas_finais

    finally:
        db.close()


# ── Confirmar rota ────────────────────────────────────────────────────────────
def confirmar_rota(data: str, tecnico_id: int, tecnico_nome: str, os_ids: list, pontos: int, tempo_est: int, bairro_ref: str) -> int:
    db = get_db()
    try:
        cur = db.cursor()

        cur.execute("""
            INSERT INTO ht_rotas (data_rota, tecnico_id, tecnico_nome, pontos, tempo_est, total_os, bairro_ref, status, confirmado_em)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'confirmada', datetime('now','-3 hours'))
        """, (data, tecnico_id, tecnico_nome, pontos, tempo_est, len(os_ids), bairro_ref))

        rota_id = cur.lastrowid

        # Buscar ixc_funcionario_id do tecnico
        tec = db.execute("SELECT ixc_funcionario_id FROM ht_usuarios WHERE id=?", (tecnico_id,)).fetchone()
        ixc_func_id = tec["ixc_funcionario_id"] if tec and tec["ixc_funcionario_id"] else tecnico_id

        for ordem, os_item in enumerate(os_ids, start=1):
            p = PONTOS_ASSUNTO.get(os_item.get('assunto_nome', ''), os_item.get('pontos', PONTOS_PADRAO))
            cur.execute("""
                INSERT INTO ht_rotas_os (rota_id, os_id, ordem, pontos)
                VALUES (?, ?, ?, ?)
            """, (rota_id, os_item['id'], ordem, p))

            # Calcula horário estimado
            hora_prevista = os_item.get('hora_prevista', f"{data} {JORNADA_INICIO:02d}:00")

            cur.execute("""
                UPDATE ht_os SET id_tecnico=?, ordem_execucao=?, status_hub='agendada' WHERE id=?
            """, (tecnico_id, ordem, os_item['id']))

            # Atualiza IXC com técnico, status AG e data/hora prevista
            row = db.execute("SELECT ixc_os_id FROM ht_os WHERE id=?", (os_item['id'],)).fetchone()
            if row and row["ixc_os_id"]:
                try:
                    from app.services.ixc_db import ixc_insert
                    ixc_insert(
                        "UPDATE ixcprovedor.su_oss_chamado SET id_tecnico=%s, status='AG', data_reservada=%s WHERE id=%s",
                        (ixc_func_id, hora_prevista, row["ixc_os_id"])
                    )
                    log.info(f"OS {row['ixc_os_id']} agendada no IXC para {hora_prevista} — técnico {ixc_func_id}")
                except Exception as e:
                    log.warning(f"Erro ao agendar OS {row['ixc_os_id']} no IXC: {e}")

        db.commit()
        log.info(f"Rota {rota_id} confirmada — técnico {tecnico_nome} — {len(os_ids)} OS")
        return rota_id

    finally:
        db.close()


# ── Listar rotas do dia ───────────────────────────────────────────────────────
def listar_rotas_do_dia(data: str) -> list:
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT r.*, COUNT(ro.id) as qtd_os
            FROM ht_rotas r
            LEFT JOIN ht_rotas_os ro ON ro.rota_id = r.id
            WHERE r.data_rota = ?
            GROUP BY r.id
            ORDER BY r.tecnico_nome, r.id
        """, (data,))
        rotas = [dict(r) for r in cur.fetchall()]

        for rota in rotas:
            cur.execute("""
                SELECT ro.ordem, ro.pontos,
                       o.ixc_os_id, o.cliente_nome, o.endereco, o.bairro, o.cidade,
                       o.assunto_nome, o.lat, o.lon, o.sla_estourado
                FROM ht_rotas_os ro
                JOIN ht_os o ON o.id = ro.os_id
                WHERE ro.rota_id = ?
                ORDER BY ro.ordem
            """, (rota['id'],))
            rota['os'] = [dict(r) for r in cur.fetchall()]

        return rotas
    finally:
        db.close()


# ── Técnicos disponíveis ──────────────────────────────────────────────────────
def tecnicos_disponiveis(data: str) -> list:
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT id, nome, nivel FROM ht_usuarios
            WHERE nivel IN (10, 20) AND ativo = 1
            ORDER BY nome
        """)
        tecnicos = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT tecnico_id, SUM(pontos) as pontos_alocados
            FROM ht_rotas
            WHERE data_rota = ? AND status = 'confirmada'
            GROUP BY tecnico_id
        """, (data,))
        alocados_agenda = {r['tecnico_id']: r['pontos_alocados'] for r in cur.fetchall()}

        cur.execute("""
            SELECT o.id_tecnico,
                   COUNT(o.id) as qtd_os,
                   SUM(COALESCE(ap.pontos, ?)) as pontos_os
            FROM ht_os o
            LEFT JOIN ht_assunto_pontos ap ON ap.id_assunto = o.id_assunto
            WHERE o.status_ixc IN ('A', 'AG', 'RAG')
              AND o.id_tecnico IS NOT NULL AND o.id_tecnico != 0
              AND (DATE(o.data_agenda) = ? OR DATE(o.data_agenda) IS NULL)
            GROUP BY o.id_tecnico
        """, (PONTOS_PADRAO, data))
        alocados_ixc = {r['id_tecnico']: r['pontos_os'] for r in cur.fetchall()}

        for t in tecnicos:
            pts_agenda = alocados_agenda.get(t['id'], 0)
            pts_ixc    = alocados_ixc.get(t['id'], 0)
            t['pontos_alocados']    = pts_agenda + pts_ixc
            t['pontos_agenda']      = pts_agenda
            t['pontos_ixc']         = pts_ixc
            t['pontos_disponiveis'] = max(0, CAPACIDADE_PONTOS - t['pontos_alocados'])

        return tecnicos
    finally:
        db.close()
