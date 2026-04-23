"""
agenda_engine.py — Motor de Agendamento Automático v1
Caminho: /opt/automacoes/cliquedf/tecnico/app/engines/agenda_engine.py

Fluxo:
  1. Carregar OS abertas/agendadas para a data
  2. Enriquecer com pontos do assunto
  3. Priorizar (SLA estourado > agendada hoje > mais antiga)
  4. Clusterizar por bairro
  5. Subdividir por capacidade (80 pts)
  6. Sequenciar por proximidade (nearest-neighbor)
  7. Retornar rotas prontas para o operador confirmar
"""

import sqlite3
import math
import logging
from datetime import datetime, date
from typing import Optional

log = logging.getLogger(__name__)

# ── Configurações ─────────────────────────────────────────────────────────────
CAPACIDADE_PONTOS = 80
TOLERANCIA_PONTOS = 5        # permite até 85
PONTOS_PADRAO     = 10       # assuntos não mapeados
TEMPO_PADRAO_MIN  = 45       # minutos estimados padrão
DB_PATH = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"


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
    """Distância em km entre dois pontos geográficos."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


# ── Etapa 1: Carregar OS ──────────────────────────────────────────────────────
def carregar_os(data: str, db) -> list:
    """
    Carrega OS abertas ou agendadas para a data informada.
    Inclui OS sem técnico definido (id_tecnico IS NULL ou 0).
    """
    cur = db.cursor()
    cur.execute("""
        SELECT
            o.id,
            o.ixc_os_id,
            o.id_tecnico,
            o.id_assunto,
            o.assunto_nome,
            o.status_ixc,
            o.status_hub,
            o.cliente_nome,
            o.endereco,
            o.bairro,
            o.cidade,
            o.lat,
            o.lon,
            o.data_abertura,
            o.data_agenda,
            o.sla_horas,
            o.horas_abertas,
            o.sla_estourado
        FROM ht_os o
        WHERE
            o.status_ixc IN ('A', 'AG', 'RAG')
            AND o.lat IS NOT NULL
            AND o.lat != 0
            AND o.lon IS NOT NULL
            AND o.lon != 0
            AND (
                DATE(o.data_agenda) = ?
                OR (o.data_agenda IS NULL OR o.data_agenda = '' OR o.data_agenda = '0000-00-00 00:00:00')
            )
        ORDER BY o.sla_estourado DESC, o.horas_abertas DESC
    """, (data,))
    rows = [dict(r) for r in cur.fetchall()]
    log.info(f"OS carregadas para {data}: {len(rows)}")
    return rows


# ── Etapa 2: Enriquecer com pontos ───────────────────────────────────────────
def enriquecer_pontos(lista_os: list, db) -> list:
    """Adiciona pontos e tempo_estimado a cada OS pelo id_assunto."""
    cur = db.cursor()
    cur.execute("SELECT id_assunto, pontos, tempo_min, categoria FROM ht_assunto_pontos")
    mapa = {r['id_assunto']: dict(r) for r in cur.fetchall()}

    for os in lista_os:
        info = mapa.get(os['id_assunto'])
        if info:
            os['pontos']        = info['pontos']
            os['tempo_min']     = info['tempo_min']
            os['categoria']     = info['categoria']
        else:
            os['pontos']        = PONTOS_PADRAO
            os['tempo_min']     = TEMPO_PADRAO_MIN
            os['categoria']     = 'outros'

    return lista_os


# ── Etapa 3: Priorizar ────────────────────────────────────────────────────────
def priorizar(lista_os: list, data_alvo: str) -> list:
    """
    Ordena por:
      1. SLA estourado (prioridade máxima)
      2. Agendada para o dia alvo
      3. Mais horas abertas
    """
    def score(os):
        p1 = 0 if os.get('sla_estourado') else 1
        p2 = 0 if (os.get('data_agenda') or '')[:10] == data_alvo else 1
        p3 = -(os.get('horas_abertas') or 0)
        return (p1, p2, p3)

    return sorted(lista_os, key=score)


# ── Etapa 4: Clusterizar por bairro ──────────────────────────────────────────
def clusterizar_por_bairro(lista_os: list) -> dict:
    """
    Agrupa OS pelo campo bairro.
    OS sem bairro vão para cluster 'SEM_BAIRRO'.
    """
    clusters = {}
    for os in lista_os:
        bairro = (os.get('bairro') or 'SEM_BAIRRO').strip().upper()
        if bairro not in clusters:
            clusters[bairro] = []
        clusters[bairro].append(os)

    log.info(f"Clusters por bairro: {len(clusters)} — {list(clusters.keys())}")
    return clusters


# ── Etapa 5: Subdividir por capacidade ───────────────────────────────────────
def subdividir_por_pontos(lista_os: list, capacidade: int = CAPACIDADE_PONTOS + TOLERANCIA_PONTOS) -> list:
    """
    Algoritmo greedy: preenche rota até o limite de pontos,
    depois abre nova rota.
    Retorna lista de dicts: {os: [...], pontos: N, tempo_est: N}
    """
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
def sequenciar_por_proximidade(lista_os: list) -> list:
    """
    Nearest-neighbor greedy.
    Ponto inicial = centroide do cluster.
    Sem dependência externa.
    """
    if len(lista_os) <= 1:
        return lista_os

    restantes = lista_os[:]

    # centroide como ponto de partida
    lat_c = sum(o['lat'] for o in restantes) / len(restantes)
    lon_c = sum(o['lon'] for o in restantes) / len(restantes)

    ordenadas = []
    pos = (lat_c, lon_c)

    while restantes:
        mais_proximo = min(
            restantes,
            key=lambda o: haversine(pos[0], pos[1], o['lat'], o['lon'])
        )
        ordenadas.append(mais_proximo)
        pos = (mais_proximo['lat'], mais_proximo['lon'])
        restantes.remove(mais_proximo)

    return ordenadas


# ── Motor principal ───────────────────────────────────────────────────────────
def gerar_rotas(data: Optional[str] = None) -> list:
    """
    Gera sugestão de rotas para a data informada (padrão: hoje).
    Retorna lista de rotas prontas para o operador confirmar.
    """
    if not data:
        data = date.today().isoformat()

    db = get_db()
    try:
        # 1. Carregar
        os_list = carregar_os(data, db)
        if not os_list:
            log.info(f"Nenhuma OS encontrada para {data}")
            return []

        # 2. Enriquecer
        os_list = enriquecer_pontos(os_list, db)

        # 3. Priorizar
        os_list = priorizar(os_list, data)

        # 4. Clusterizar
        clusters = clusterizar_por_bairro(os_list)

        # 5 + 6. Subdividir e sequenciar
        rotas_finais = []
        rota_num = 1

        for bairro, os_cluster in clusters.items():
            sub_rotas = subdividir_por_pontos(os_cluster)
            for sr in sub_rotas:
                sr['os'] = sequenciar_por_proximidade(sr['os'])
                sr['rota_num']   = rota_num
                sr['bairro_ref'] = bairro
                sr['total_os']   = len(sr['os'])
                rota_num += 1
                rotas_finais.append(sr)

        log.info(f"Rotas geradas: {len(rotas_finais)}")
        return rotas_finais

    finally:
        db.close()


# ── Confirmar rota ────────────────────────────────────────────────────────────
def confirmar_rota(data: str, tecnico_id: int, tecnico_nome: str, os_ids: list, pontos: int, tempo_est: int, bairro_ref: str) -> int:
    """
    Salva rota confirmada em ht_rotas + ht_rotas_os.
    Retorna o id da rota criada.
    """
    db = get_db()
    try:
        cur = db.cursor()

        cur.execute("""
            INSERT INTO ht_rotas (data_rota, tecnico_id, tecnico_nome, pontos, tempo_est, total_os, bairro_ref, status, confirmado_em)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'confirmada', datetime('now','-3 hours'))
        """, (data, tecnico_id, tecnico_nome, pontos, tempo_est, len(os_ids), bairro_ref))

        rota_id = cur.lastrowid

        for ordem, os_item in enumerate(os_ids, start=1):
            cur.execute("""
                INSERT INTO ht_rotas_os (rota_id, os_id, ordem, pontos)
                VALUES (?, ?, ?, ?)
            """, (rota_id, os_item['id'], ordem, os_item.get('pontos', PONTOS_PADRAO)))

            # Atualiza técnico na ht_os
            cur.execute("""
                UPDATE ht_os SET id_tecnico = ?, ordem_execucao = ? WHERE id = ?
            """, (tecnico_id, ordem, os_item['id']))

        db.commit()
        log.info(f"Rota {rota_id} confirmada — técnico {tecnico_nome} — {len(os_ids)} OS")
        return rota_id

    finally:
        db.close()


# ── Listar rotas do dia ───────────────────────────────────────────────────────
def listar_rotas_do_dia(data: str) -> list:
    """Retorna rotas confirmadas do dia com OS detalhadas."""
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("""
            SELECT r.*, 
                   COUNT(ro.id) as qtd_os
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
                       o.ixc_os_id, o.cliente_nome, o.endereco, o.bairro,
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
    """Retorna técnicos com pontos já alocados vs capacidade."""
    db = get_db()
    try:
        cur = db.cursor()

        # Técnicos do sistema (nível técnico = 20)
        cur.execute("""
            SELECT id, nome, nivel FROM ht_usuarios
            WHERE nivel = 20 AND ativo = 1
            ORDER BY nome
        """)
        tecnicos = [dict(r) for r in cur.fetchall()]

        # Pontos já alocados no dia
        cur.execute("""
            SELECT tecnico_id, SUM(pontos) as pontos_alocados
            FROM ht_rotas
            WHERE data_rota = ? AND status = 'confirmada'
            GROUP BY tecnico_id
        """, (data,))
        alocados = {r['tecnico_id']: r['pontos_alocados'] for r in cur.fetchall()}

        for t in tecnicos:
            t['pontos_alocados']   = alocados.get(t['id'], 0)
            t['pontos_disponiveis'] = max(0, CAPACIDADE_PONTOS - t['pontos_alocados'])

        return tecnicos
    finally:
        db.close()
