from fastapi import APIRouter
import requests
from datetime import datetime, timezone, timedelta
import sqlite3, math

router = APIRouter()
DB = "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db"

# ── CONFIGURACOES ─────────────────────────────────────────────
GARAGEM_LAT   = -10.321962
GARAGEM_LON   = -36.579507
RAIO_OPERACAO = 25000   # 25km em metros
RAIO_GARAGEM  = 500     # 500m da garagem = "em casa"
VEL_MAX       = 80      # km/h
HORA_FIM      = 19      # apos 19h = modo noturno
PARADO_DESLOC = 15      # min parado em deslocamento
PARADO_EXEC   = 90      # min parado em execucao
GPS_MAX_HORAS = 8       # GPS mais antigo que isso = offline

def brt():
    return datetime.now(timezone.utc) - timedelta(hours=3)

def get_db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

def dist_metros(lat1, lon1, lat2, lon2):
    """Distancia em metros entre dois pontos."""
    R = 6371000
    f1, f2 = math.radians(lat1), math.radians(lat2)
    df = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(df/2)**2 + math.cos(f1)*math.cos(f2)*math.sin(dl/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def bearing(lat1, lon1, lat2, lon2):
    """Angulo de direcao em graus (0=norte, 90=leste)."""
    f1, f2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(f2)
    y = math.cos(f1)*math.sin(f2) - math.sin(f1)*math.cos(f2)*math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360) % 360

def min_desde(dt_str):
    """Minutos desde um datetime string (BRT sem timezone)."""
    if not dt_str:
        return 9999
    try:
        dt = datetime.fromisoformat(dt_str)
        # Remove timezone do agora para comparar com dt naive
        agora = brt().replace(tzinfo=None)
        return (agora - dt).total_seconds() / 60
    except:
        return 9999

def registrar_evento(cur, db, id_tecnico, tipo, lat, lon, vel, agora):
    """Registra evento se nao houver um recente (30min) do mesmo tipo."""
    cur.execute("""
        SELECT id FROM ht_eventos_frota
        WHERE id_tecnico=? AND tipo_evento=?
        AND registrado_em > datetime('now','-3 hours','-30 minutes')
    """, (id_tecnico, tipo))
    if not cur.fetchone():
        cur.execute("""
            INSERT INTO ht_eventos_frota
            (id_tecnico, tipo_evento, lat, lon, velocidade, registrado_em, alertado)
            VALUES (?,?,?,?,?,?,0)
        """, (id_tecnico, tipo, lat, lon, vel, agora.strftime("%Y-%m-%d %H:%M:%S")))
        db.commit()


import threading, functools

@functools.lru_cache(maxsize=512)
def _reverse_geo(lat, lon):
    """Retorna nome da rua via Nominatim (cache em memória)."""
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lon, "format": "json", "zoom": 17, "addressdetails": 1},
            headers={"User-Agent": "HubTecnico/1.0"},
            timeout=3
        )
        d = r.json()
        addr = d.get("address", {})
        rua  = addr.get("road") or addr.get("pedestrian") or addr.get("path") or ""
        bairro = addr.get("suburb") or addr.get("neighbourhood") or ""
        return f"{rua}, {bairro}".strip(", ") if rua else d.get("display_name","")[:60]
    except Exception:
        return ""

def _geo_async(lat, lon, resultado_list, idx):
    resultado_list[idx]["endereco_gps"] = _reverse_geo(round(lat,4), round(lon,4))

@router.get("/api/tv/status")
def tv_status():
    db  = get_db()
    cur = db.cursor()
    agora    = brt()
    modo_noturno = agora.hour >= HORA_FIM

    # ── OS ativas por tecnico ─────────────────────────────────
    cur.execute("""
        SELECT o.id_tecnico, o.ixc_os_id, o.status_hub,
               o.cliente_nome, o.assunto_nome, o.endereco,
               o.data_reservada, o.data_agenda
        FROM ht_os o
        WHERE o.status_hub IN ('deslocamento','execucao','agendada','reagendada','pendente')
        AND o.id_tecnico IS NOT NULL
        AND (
            o.data_reservada = date('now','-3 hours')
            OR o.status_hub IN ('deslocamento','execucao')
        )
    """)
    os_rows = cur.fetchall()
    os_por_tecnico = {}
    for o in os_rows:
        tid = o["id_tecnico"]
        if tid not in os_por_tecnico:
            os_por_tecnico[tid] = []
        os_por_tecnico[tid].append(dict(o))

    # OS finalizadas hoje por tecnico
    cur.execute("""
        SELECT o.id_tecnico, COUNT(*) as qtd
        FROM ht_os o
        WHERE o.status_hub = 'finalizada'
        AND date(o.data_agenda) = date('now','-3 hours')
        AND o.id_tecnico IS NOT NULL
        GROUP BY o.id_tecnico
    """)
    finalizadas = {r["id_tecnico"]: r["qtd"] for r in cur.fetchall()}

    # KM do dia por tecnico
    cur.execute("""
        SELECT id_tecnico, SUM(km_deslocamento) as km_total
        FROM ht_km_os
        WHERE date(criado_em) = date('now','-3 hours')
        GROUP BY id_tecnico
    """)
    km_dia = {r["id_tecnico"]: round(r["km_total"] or 0, 1) for r in cur.fetchall()}

    # ── Ultima posicao + penultima (para bearing) ─────────────
    cur.execute("""
        SELECT g.id_tecnico, g.lat, g.lon, g.velocidade,
               g.status_tecnico, g.registrado_em,
               u.nome as tecnico_nome,
               tv.id_veiculo, v.placa, v.marca_modelo, v.tipo
        FROM ht_gps_track g
        JOIN ht_usuarios u ON u.id = g.id_tecnico
        LEFT JOIN ht_tecnico_veiculo tv
            ON tv.id_tecnico = g.id_tecnico
            AND tv.data = date('now','-3 hours')
        LEFT JOIN ht_veiculos v ON v.id = tv.id_veiculo
        WHERE g.id = (
            SELECT MAX(g2.id) FROM ht_gps_track g2
            WHERE g2.id_tecnico = g.id_tecnico
        )
        AND u.ativo = 1
        ORDER BY u.nome
    """)
    rows = cur.fetchall()

    # Penultimas posicoes para bearing
    cur.execute("""
        SELECT id_tecnico, lat, lon
        FROM ht_gps_track g
        WHERE id = (
            SELECT g2.id FROM ht_gps_track g2
            WHERE g2.id_tecnico = g.id_tecnico
            ORDER BY g2.id DESC LIMIT 1 OFFSET 1
        )
    """)
    penultimas = {r["id_tecnico"]: (r["lat"], r["lon"]) for r in cur.fetchall()}

    # Rastro das ultimas 8h
    cur.execute("""
        SELECT id_tecnico, lat, lon, velocidade, registrado_em
        FROM ht_gps_track
        WHERE registrado_em >= datetime('now','-3 hours','-8 hours')
        ORDER BY id_tecnico, id ASC
    """)
    rastros = {}
    for r in cur.fetchall():
        tid = r["id_tecnico"]
        if tid not in rastros:
            rastros[tid] = []
        rastros[tid].append({"lat": r["lat"], "lon": r["lon"], "vel": r["velocidade"]})

    resultado = []
    stats = {"total":0,"em_movimento":0,"parados":0,"offline":0,"alertas":0}

    for row in rows:
        r = dict(row)
        tid  = r["id_tecnico"]
        vel  = float(r["velocidade"] or 0)
        lat  = r["lat"]
        lon  = r["lon"]
        min_gps = min_desde(r["registrado_em"])

        # Sem veiculo = ignora completamente
        if not r["id_veiculo"]:
            continue

        # Offline se GPS > 8h E nao tem OS em execucao ativa
        # Se tecnico esta em execucao, GPS pode estar inativo (app fechado)
        os_exec_check = os_por_tecnico.get(tid, [])
        tem_execucao = any(o["status_hub"] == "execucao" for o in os_exec_check)
        offline = min_gps > GPS_MAX_HORAS * 60 and not tem_execucao
        gps_inativo = min_gps > 60 and not (vel > 2)  # GPS parado ha mais de 1h
        em_movimento = vel > 2 and not offline

        # OS ativas do tecnico
        os_list    = os_por_tecnico.get(tid, [])
        os_desloc  = next((o for o in os_list if o["status_hub"] == "deslocamento"), None)
        os_exec    = next((o for o in os_list if o["status_hub"] == "execucao"), None)
        os_ativa   = os_exec or os_desloc
        # Proxima OS = agendada para hoje com horario futuro (nao reagendada)
        from datetime import datetime as _dt
        agora_str = agora.strftime("%Y-%m-%d %H:%M")
        data_hoje = agora.strftime("%Y-%m-%d")
        candidatas = [
            o for o in os_list
            if o["status_hub"] == "agendada"
            and o != os_ativa
            and o.get("data_reservada","") == data_hoje
            and o.get("data_agenda","0000-00-00") not in ("0000-00-00 00:00","0000-00-00","","None",None)
        ]
        # Ordena por horario de agenda
        candidatas.sort(key=lambda o: o.get("data_agenda",""))
        os_proxima = candidatas[0] if candidatas else None
        os_fin     = finalizadas.get(tid, 0)

        # Distancias
        d_garagem  = dist_metros(lat, lon, GARAGEM_LAT, GARAGEM_LON)
        d_operacao = d_garagem  # simplificado: distancia da garagem
        fora_area  = d_operacao > RAIO_OPERACAO
        na_garagem = d_garagem <= RAIO_GARAGEM

        # Bearing para rotacao
        ang = 0
        if em_movimento and tid in penultimas:
            plat, plon = penultimas[tid]
            if plat and plon:
                ang = bearing(plat, plon, lat, lon)

        # ── REGRAS DE ALERTA ──────────────────────────────────
        status     = "normal"
        cor        = "#22c55e"
        alerta_txt = None
        nivel      = "info"

        if offline:
            status = "offline"
            cor    = "#475569"

        elif modo_noturno:
            # Apos 19h: so alerta se sair da garagem
            if not na_garagem:
                status     = "critico"
                cor        = "#ef4444"
                alerta_txt = f"Fora da garagem apos 19h ({d_garagem/1000:.1f}km)"
                nivel      = "critico"
                registrar_evento(cur, db, tid, "fora_garagem", lat, lon, vel, agora)
            else:
                status = "normal"
                cor    = "#334155"  # cinza escuro noturno

        else:
            # ── Horario comercial ──────────────────────────────
            # 1. Velocidade
            if vel > VEL_MAX:
                status     = "critico"
                cor        = "#ef4444"
                alerta_txt = f"Velocidade {vel:.0f} km/h"
                nivel      = "critico"
                registrar_evento(cur, db, tid, "velocidade", lat, lon, vel, agora)

            # 2. Fora da area de operacao
            elif fora_area:
                status     = "atencao"
                cor        = "#f59e0b"
                alerta_txt = f"Fora da area ({d_operacao/1000:.1f}km da base)"
                nivel      = "atencao"
                registrar_evento(cur, db, tid, "fora_area", lat, lon, vel, agora)

            # 3. Em deslocamento + parado > 15min
            elif os_desloc and not em_movimento and min_gps > PARADO_DESLOC:
                status     = "atencao"
                cor        = "#f59e0b"
                alerta_txt = f"Parado em deslocamento ha {int(min_gps)}min"
                nivel      = "atencao"
                registrar_evento(cur, db, tid, "parado_deslocamento", lat, lon, vel, agora)

            # 4. Em execucao + parado > 90min
            elif os_exec and not em_movimento and min_gps > PARADO_EXEC:
                status     = "critico"
                cor        = "#ef4444"
                alerta_txt = f"Execucao acima do tempo ({int(min_gps)}min)"
                nivel      = "critico"
                registrar_evento(cur, db, tid, "execucao_longa", lat, lon, vel, agora)

            # 5. Sem OS + parado = vermelho visual sem alarme
            elif not os_ativa and not em_movimento:
                status = "parado_sem_os"
                cor    = "#dc2626"
                # SEM alerta_txt = nao gera alarme

            # 6. Normal em movimento
            elif em_movimento:
                status = "normal"
                cor    = "#22c55e"

        # Stats
        stats["total"] += 1
        if offline:
            stats["offline"] += 1
        elif em_movimento:
            stats["em_movimento"] += 1
        else:
            stats["parados"] += 1
        if alerta_txt:
            stats["alertas"] += 1

        resultado.append({
            "id_tecnico":    tid,
            "lat":           lat,
            "lon":           lon,
            "velocidade":    round(vel, 1),
            "registrado_em": r["registrado_em"],
            "tecnico_nome":  r["tecnico_nome"],
            "placa":         r["placa"],
            "marca_modelo":  r["marca_modelo"],
            "tipo":          r["tipo"] or "carro",
            "id_veiculo":    r["id_veiculo"],
            "em_movimento":  em_movimento,
            "offline":       offline,
            "min_parado":    round(min_gps) if not em_movimento and not offline else 0,
            "gps_inativo":   gps_inativo and not em_movimento,
            "bearing":       round(ang, 1),
            "status":        status,
            "cor":           cor,
            "alerta_txt":    alerta_txt,
            "nivel":         nivel,
            "modo_noturno":  modo_noturno,
            "na_garagem":    na_garagem,
            "d_garagem_km":  round(d_garagem/1000, 1),
            # OS
            "os_ativa":      os_ativa,
            "os_desloc":     os_desloc,
            "os_exec":       os_exec,
            "os_proxima":    os_proxima,
            "os_total":      len(os_list),
            "os_finalizadas":os_fin,
            "km_dia":        km_dia.get(tid, 0),
            "rastro":        rastros.get(tid, []),
        })

    # Alertas recentes (ultimas 2h)
    cur.execute("""
        SELECT e.*, u.nome as tecnico_nome,
               COALESCE(v.placa,'') as placa
        FROM ht_eventos_frota e
        JOIN ht_usuarios u ON u.id = e.id_tecnico
        LEFT JOIN ht_tecnico_veiculo tv
            ON tv.id_tecnico = e.id_tecnico
            AND tv.data = date('now','-3 hours')
        LEFT JOIN ht_veiculos v ON v.id = tv.id_veiculo
        WHERE e.registrado_em > datetime('now','-3 hours','-2 hours')
        ORDER BY e.id DESC LIMIT 30
    """)
    alertas = [dict(a) for a in cur.fetchall()]
    db.close()

    # Geocoding reverso em paralelo (apenas parados com GPS recente)
    threads = []
    for i, t in enumerate(resultado):
        if not t.get("offline") and not t.get("em_movimento") and t.get("lat") and t.get("lon"):
            t["endereco_gps"] = ""
            th = threading.Thread(target=_geo_async, args=(t["lat"], t["lon"], resultado, i), daemon=True)
            threads.append(th)
            th.start()
        else:
            t["endereco_gps"] = ""
    for th in threads:
        th.join(timeout=4)

    return {
        "tecnicos":       resultado,
        "stats":          stats,
        "alertas":        alertas,
        "modo_noturno":   modo_noturno,
        "atualizado_em":  agora.strftime("%d/%m/%Y %H:%M:%S"),
        "garagem":        {"lat": GARAGEM_LAT, "lon": GARAGEM_LON, "raio": RAIO_GARAGEM},
        "area_operacao":  {"lat": GARAGEM_LAT, "lon": GARAGEM_LON, "raio": RAIO_OPERACAO},
    }

@router.get("/api/tv/alertas")
def tv_alertas():
    db  = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT e.*, u.nome as tecnico_nome
        FROM ht_eventos_frota e
        JOIN ht_usuarios u ON u.id = e.id_tecnico
        WHERE e.alertado = 0
        AND e.registrado_em > datetime('now','-3 hours','-1 hours')
        ORDER BY e.id DESC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    if rows:
        ids = [r["id"] for r in rows]
        cur.execute(
            f"UPDATE ht_eventos_frota SET alertado=1 WHERE id IN ({','.join('?'*len(ids))})",
            ids
        )
        db.commit()
    db.close()
    return rows

@router.get("/api/tv/historico")
def tv_historico(id_veiculo: int, horas: int = 24):
    """Rastro historico de um veiculo nas ultimas X horas."""
    db  = get_db()
    cur = db.cursor()
    # Busca tecnicos que usaram esse veiculo
    cur.execute("""
        SELECT DISTINCT tv.id_tecnico
        FROM ht_tecnico_veiculo tv
        WHERE tv.id_veiculo = ?
        AND tv.data >= date('now','-3 hours','-7 days')
    """, (id_veiculo,))
    tids = [r["id_tecnico"] for r in cur.fetchall()]
    if not tids:
        db.close()
        return {"pontos": [], "resumo": {}}

    placeholders = ','.join('?'*len(tids))
    cur.execute(f"""
        SELECT g.lat, g.lon, g.velocidade, g.registrado_em, u.nome as tecnico_nome
        FROM ht_gps_track g
        JOIN ht_usuarios u ON u.id = g.id_tecnico
        WHERE g.id_tecnico IN ({placeholders})
        AND g.registrado_em >= datetime('now','-3 hours','-{horas} hours')
        ORDER BY g.id ASC
    """, tids)
    pontos = [dict(r) for r in cur.fetchall()]

    # Resumo + paradas enriquecidas
    paradas_lista = []
    if pontos:
        vels = [p["velocidade"] or 0 for p in pontos]
        vel_max = max(vels)

        # Detecta paradas com duração
        em_parada = False
        parada_ini_idx = 0
        for i, p in enumerate(pontos):
            if (p["velocidade"] or 0) < 2:
                if not em_parada:
                    em_parada = True
                    parada_ini_idx = i
            else:
                if em_parada:
                    ini = pontos[parada_ini_idx]
                    fim_p = pontos[i-1]
                    from datetime import datetime as _dt
                    try:
                        t0 = _dt.fromisoformat(ini["registrado_em"])
                        t1 = _dt.fromisoformat(fim_p["registrado_em"])
                        dur_min = int((t1-t0).total_seconds()/60)
                    except:
                        dur_min = 0
                    if dur_min >= 2:  # ignora microparadas
                        paradas_lista.append({
                            "lat": ini["lat"],
                            "lon": ini["lon"],
                            "inicio": ini["registrado_em"],
                            "fim": fim_p["registrado_em"],
                            "dur_min": dur_min,
                            "endereco": _reverse_geo(round(ini["lat"],4), round(ini["lon"],4)),
                        })
                    em_parada = False
        # Parada ainda ativa no fim
        if em_parada:
            ini = pontos[parada_ini_idx]
            fim_p = pontos[-1]
            from datetime import datetime as _dt
            try:
                t0 = _dt.fromisoformat(ini["registrado_em"])
                t1 = _dt.fromisoformat(fim_p["registrado_em"])
                dur_min = int((t1-t0).total_seconds()/60)
            except:
                dur_min = 0
            if dur_min >= 2:
                paradas_lista.append({
                    "lat": ini["lat"],
                    "lon": ini["lon"],
                    "inicio": ini["registrado_em"],
                    "fim": fim_p["registrado_em"],
                    "dur_min": dur_min,
                    "endereco": _reverse_geo(round(ini["lat"],4), round(ini["lon"],4)),
                })

        # Busca OS próximas a cada parada (raio ~200m)
        cur.execute("""
            SELECT ixc_os_id, cliente_nome, assunto_nome, endereco,
                   status_hub, data_agenda, id_tecnico
            FROM ht_os
            WHERE id_tecnico IN ({})
            AND date(data_agenda) >= date('now','-3 hours','-7 days')
        """.format(placeholders), tids)
        os_rows = [dict(r) for r in cur.fetchall()]

        import math
        def _dist(la1,lo1,la2,lo2):
            R=6371000
            dlat=math.radians(la2-la1); dlon=math.radians(lo2-lo1)
            a=math.sin(dlat/2)**2+math.cos(math.radians(la1))*math.cos(math.radians(la2))*math.sin(dlon/2)**2
            return R*2*math.atan2(math.sqrt(a),math.sqrt(1-a))

        for par in paradas_lista:
            os_prox = None
            menor_dist = 999999
            menor_tempo = 999999
            for os in os_rows:
                try:
                    # 1. Associação por distância GPS (prioritária, raio 300m)
                    if os.get("lat") and os.get("lon") and os["lat"] != 0 and os["lon"] != 0:
                        d = _dist(par["lat"], par["lon"], os["lat"], os["lon"])
                        if d < 300 and d < menor_dist:
                            menor_dist = d
                            os_prox = os
                            continue
                    # 2. Fallback: associação por horário (dentro de 2h)
                    if os_prox is None:
                        from datetime import datetime as _dt2
                        t_par = _dt2.fromisoformat(par["inicio"])
                        t_os  = _dt2.fromisoformat(os["data_agenda"]) if os.get("data_agenda") and os["data_agenda"] not in ("0000-00-00 00:00","","None") else None
                        if t_os:
                            diff = abs((t_par - t_os).total_seconds()/60)
                            if diff < menor_tempo and diff < 120:
                                menor_tempo = diff
                                os_prox = os
                except:
                    pass
            par["os"] = os_prox
            par["os_dist_m"] = round(menor_dist) if menor_dist < 999999 else None

        resumo = {
            "total_pontos": len(pontos),
            "vel_max": round(vel_max, 1),
            "paradas": len(paradas_lista),
            "inicio": pontos[0]["registrado_em"],
            "fim": pontos[-1]["registrado_em"],
        }
    else:
        resumo = {}

    db.close()
    return {"pontos": pontos, "resumo": resumo, "paradas": paradas_lista}

@router.get("/api/tv/veiculos-ativos")
def veiculos_ativos():
    """Veiculos que rodaram nos ultimos 7 dias."""
    db  = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT DISTINCT v.id, v.placa, v.marca_modelo, v.tipo,
               MAX(g.registrado_em) as ultimo_gps,
               u.nome as tecnico_nome
        FROM ht_gps_track g
        JOIN ht_tecnico_veiculo tv ON tv.id_tecnico = g.id_tecnico
            AND tv.data >= date('now','-3 hours','-7 days')
        JOIN ht_veiculos v ON v.id = tv.id_veiculo
        JOIN ht_usuarios u ON u.id = g.id_tecnico
        WHERE g.registrado_em >= datetime('now','-3 hours','-7 days')
        AND v.ativo = 1
        GROUP BY v.id
        ORDER BY ultimo_gps DESC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    db.close()
    return rows
