# ============================================================
# PATCH: KM por Veículo — app/routes/os.py
# Substituir as funções: ultimo_km, iniciar_deslocamento_km,
# iniciar_execucao_km
# HubTécnico CliqueDf · IaTechHub · 2026
# ============================================================

# --------------- helper: busca veículo atual do técnico ----

def _get_veiculo_atual(tecnico_id: int, db) -> dict | None:
    """Retorna o veículo atualmente atribuído ao técnico."""
    row = db.execute("""
        SELECT tv.veiculo_id, v.placa, v.modelo, v.marca
        FROM ht_tecnico_veiculo tv
        JOIN ht_veiculos v ON v.id = tv.veiculo_id
        WHERE tv.tecnico_id = ?
          AND tv.data_fim IS NULL
        ORDER BY tv.data_inicio DESC
        LIMIT 1
    """, (tecnico_id,)).fetchone()
    if row:
        return {"veiculo_id": row[0], "placa": row[1],
                "modelo": row[2], "marca": row[3]}
    return None


def _get_ultimo_km_veiculo(veiculo_id: int, db) -> int:
    """Retorna o maior KM já registrado para um veículo."""
    # Busca em ht_km_os (km_chegada e km_saida)
    row = db.execute("""
        SELECT MAX(km) FROM (
            SELECT km_chegada AS km FROM ht_km_os
            WHERE veiculo_id = ? AND km_chegada IS NOT NULL
            UNION ALL
            SELECT km_saida AS km FROM ht_km_os
            WHERE veiculo_id = ? AND km_saida IS NOT NULL
        )
    """, (veiculo_id, veiculo_id)).fetchone()
    return row[0] if row and row[0] else 0


# --------------- GET /api/os/{id}/ultimo-km ----------------
# SUBSTITUIR a função atual por esta:

@router.get("/{os_id}/ultimo-km")
async def ultimo_km(os_id: int, usuario=Depends(requer_tecnico)):
    db = get_db()
    try:
        # 1. Busca veículo atual do técnico
        veiculo = _get_veiculo_atual(usuario["id"], db)

        if veiculo:
            ultimo = _get_ultimo_km_veiculo(veiculo["veiculo_id"], db)
            return {
                "ultimo_km": ultimo,
                "veiculo_id": veiculo["veiculo_id"],
                "placa": veiculo["placa"],
                "modelo": veiculo["modelo"],
                "marca": veiculo["marca"],
                "fonte": "veiculo"
            }
        else:
            # Fallback: sem veículo atribuído → busca por técnico (legado)
            row = db.execute("""
                SELECT MAX(km) FROM (
                    SELECT km_chegada AS km FROM ht_km_os
                    WHERE tecnico_id = ? AND km_chegada IS NOT NULL
                    UNION ALL
                    SELECT km_saida AS km FROM ht_km_os
                    WHERE tecnico_id = ? AND km_saida IS NOT NULL
                )
            """, (usuario["id"], usuario["id"])).fetchone()
            return {
                "ultimo_km": row[0] if row and row[0] else 0,
                "veiculo_id": None,
                "placa": None,
                "modelo": None,
                "marca": None,
                "fonte": "tecnico_legado"
            }
    finally:
        db.close()


# --------------- POST /api/os/{id}/iniciar-deslocamento-km -
# SUBSTITUIR a função atual por esta:

@router.post("/{os_id}/iniciar-deslocamento-km")
async def iniciar_deslocamento_km(
    os_id: int,
    payload: dict,
    usuario=Depends(requer_tecnico)
):
    km_saida = payload.get("km_saida")
    if km_saida is None:
        raise HTTPException(400, "km_saida obrigatório")

    db = get_db()
    try:
        # Verifica OS pertence ao técnico
        os_row = db.execute(
            "SELECT id, ixc_os_id FROM ht_os WHERE id = ? AND id_tecnico = ?",
            (os_id, usuario["id"])
        ).fetchone()
        if not os_row:
            raise HTTPException(404, "OS não encontrada")

        # Busca veículo atual
        veiculo = _get_veiculo_atual(usuario["id"], db)
        veiculo_id = veiculo["veiculo_id"] if veiculo else None

        # Validação: KM deve ser >= último KM do veículo (ou técnico se sem veículo)
        if veiculo_id:
            ultimo = _get_ultimo_km_veiculo(veiculo_id, db)
        else:
            row = db.execute("""
                SELECT MAX(km) FROM (
                    SELECT km_chegada AS km FROM ht_km_os
                    WHERE tecnico_id = ? AND km_chegada IS NOT NULL
                    UNION ALL
                    SELECT km_saida AS km FROM ht_km_os
                    WHERE tecnico_id = ? AND km_saida IS NOT NULL
                )
            """, (usuario["id"], usuario["id"])).fetchone()
            ultimo = row[0] if row and row[0] else 0

        if ultimo and km_saida < ultimo:
            raise HTTPException(
                400,
                f"KM inválido. Último KM registrado: {ultimo}km "
                f"{'(veículo ' + veiculo['placa'] + ')' if veiculo else ''}"
            )

        # Salva em ht_km_os com veiculo_id
        # Verifica se já existe registro para essa OS
        existing = db.execute(
            "SELECT id FROM ht_km_os WHERE os_id = ?", (os_id,)
        ).fetchone()

        brt = datetime.now(timezone.utc) - timedelta(hours=3)

        if existing:
            db.execute("""
                UPDATE ht_km_os
                SET km_saida = ?, veiculo_id = ?, updated_at = ?
                WHERE os_id = ?
            """, (km_saida, veiculo_id, brt.isoformat(), os_id))
        else:
            db.execute("""
                INSERT INTO ht_km_os (os_id, tecnico_id, veiculo_id, km_saida, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (os_id, usuario["id"], veiculo_id, km_saida, brt.isoformat()))

        # Atualiza status da OS
        db.execute("""
            UPDATE ht_os SET status_hub = 'deslocamento' WHERE id = ?
        """, (os_id,))
        db.commit()

        # Atualiza IXC → status EN (Encaminhada/Deslocamento)
        try:
            from app.services.ixc_db import ixc_insert
            ixc_insert(
                "UPDATE su_oss_chamado SET status = 'EN' WHERE id = %s",
                (os_row[1],)
            )
        except Exception:
            pass  # IXC não bloqueia fluxo local

        return {
            "ok": True,
            "km_saida": km_saida,
            "veiculo_id": veiculo_id,
            "placa": veiculo["placa"] if veiculo else None
        }
    finally:
        db.close()


# --------------- POST /api/os/{id}/iniciar-execucao-km -----
# SUBSTITUIR a função atual por esta:

@router.post("/{os_id}/iniciar-execucao-km")
async def iniciar_execucao_km(
    os_id: int,
    payload: dict,
    usuario=Depends(requer_tecnico)
):
    km_chegada = payload.get("km_chegada")
    if km_chegada is None:
        raise HTTPException(400, "km_chegada obrigatório")

    db = get_db()
    try:
        os_row = db.execute(
            "SELECT id, ixc_os_id FROM ht_os WHERE id = ? AND id_tecnico = ?",
            (os_id, usuario["id"])
        ).fetchone()
        if not os_row:
            raise HTTPException(404, "OS não encontrada")

        # Busca km_saida já registrado para calcular deslocamento
        km_row = db.execute(
            "SELECT km_saida, veiculo_id FROM ht_km_os WHERE os_id = ?",
            (os_id,)
        ).fetchone()

        km_saida = km_row[0] if km_row else None
        veiculo_id = km_row[1] if km_row else None

        # Validação crescente
        if km_saida and km_chegada < km_saida:
            raise HTTPException(
                400,
                f"KM chegada ({km_chegada}) não pode ser menor que KM saída ({km_saida})"
            )

        km_deslocamento = (km_chegada - km_saida) if km_saida else None

        brt = datetime.now(timezone.utc) - timedelta(hours=3)

        if km_row:
            db.execute("""
                UPDATE ht_km_os
                SET km_chegada = ?, km_deslocamento = ?, updated_at = ?
                WHERE os_id = ?
            """, (km_chegada, km_deslocamento, brt.isoformat(), os_id))
        else:
            veiculo = _get_veiculo_atual(usuario["id"], db)
            veiculo_id = veiculo["veiculo_id"] if veiculo else None
            db.execute("""
                INSERT INTO ht_km_os (os_id, tecnico_id, veiculo_id, km_chegada, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (os_id, usuario["id"], veiculo_id, km_chegada, brt.isoformat()))

        db.execute("""
            UPDATE ht_os SET status_hub = 'execucao' WHERE id = ?
        """, (os_id,))
        db.commit()

        # IXC → status A (em execução)
        try:
            from app.services.ixc_db import ixc_insert
            ixc_insert(
                "UPDATE su_oss_chamado SET status = 'A' WHERE id = %s",
                (os_row[1],)
            )
        except Exception:
            pass

        return {
            "ok": True,
            "km_chegada": km_chegada,
            "km_deslocamento": km_deslocamento,
            "veiculo_id": veiculo_id
        }
    finally:
        db.close()
