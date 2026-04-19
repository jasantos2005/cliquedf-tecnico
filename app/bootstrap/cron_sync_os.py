import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
import sqlite3
from datetime import datetime
from app.services.ixc_db import ixc_select

DB = os.path.join(os.path.dirname(__file__), "../../hub_tecnico.db")

IXC_TECNICOS_IDS = [13,17,32,35,47,50,55,56,60,46]

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def run():
    print(f"[{datetime.now().strftime('%d/%m/%Y %H:%M')}] sync_os iniciado")
    ids_str = ",".join(map(str, IXC_TECNICOS_IDS))
    os_rows = ixc_select(f"""
        SELECT o.id, o.status, o.id_assunto, o.id_tecnico,
               o.id_cliente, o.id_contrato_kit,
               o.data_abertura, o.data_agenda,
               o.mensagem, o.latitude, o.longitude,
               o.endereco, o.bairro, o.referencia,
               cl.razao AS cliente_nome,
               cl.telefone_celular AS telefone,
               a.assunto AS assunto_nome,
               TIMESTAMPDIFF(HOUR, o.data_abertura, NOW()) AS horas_abertas
        FROM su_oss_chamado o
        JOIN cliente cl ON cl.id = o.id_cliente
        JOIN su_oss_assunto a ON a.id = o.id_assunto
        WHERE o.status IN ('A','EN','AG','RAG')
          AND (o.id_tecnico = 0 OR o.id_tecnico IN (13,17,32,35,47,50,55,56,60,46))
          AND o.id_assunto IN (15,16,17,18,19,20,21,22,39,49,53,89,110,111,226,227)

    """)

    db = get_db()
    novos = 0
    atualizados = 0

    for o in os_rows:
        # Status IXC:
        # A = Aberta (sem técnico = pendente)
        # AG = Agendada (tem técnico + data)
        # EN = Encaminhada (técnico atribuído, a caminho)
        # RAG = Reagendada
        # F = Finalizada
        tem_tecnico = o['id_tecnico'] and int(o['id_tecnico']) > 0
        status_ixc = o['status']
        if status_ixc == 'F':
            status_hub = 'finalizada'
        elif status_ixc == 'AG':
            status_hub = 'agendada'
        elif status_ixc == 'RAG':
            status_hub = 'reagendada'
        elif status_ixc == 'EN' and tem_tecnico:
            status_hub = 'deslocamento'
        elif status_ixc == 'A' and tem_tecnico:
            status_hub = 'execucao'
        else:
            status_hub = 'pendente'

        existente = db.execute(
            "SELECT id, status_hub FROM ht_os WHERE ixc_os_id=?", (o['id'],)
        ).fetchone()

        if not existente:
            db.execute("""
                INSERT INTO ht_os (
                    ixc_os_id, ixc_tecnico_id, ixc_cliente_id, id_contrato_kit,
                    id_assunto, assunto_nome, status_ixc, status_hub,
                    cliente_nome, endereco, bairro, referencia, telefone,
                    lat, lon, data_abertura, data_agenda,
                    obs_abertura, sincronizado_em, horas_abertas, sla_estourado
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                o['id'], o['id_tecnico'], o['id_cliente'], o['id_contrato_kit'],
                o['id_assunto'], o['assunto_nome'], o['status'], status_hub,
                o['cliente_nome'], o['endereco'], o['bairro'], o['referencia'],
                o['telefone'],
                float(o['latitude']) if o['latitude'] else None,
                float(o['longitude']) if o['longitude'] else None,
                str(o['data_abertura'])[:16] if o['data_abertura'] else None,
                str(o['data_agenda'])[:16] if o['data_agenda'] else None,
                o['mensagem'],
                datetime.now().strftime("%d/%m/%Y %H:%M"),
                float(o.get('horas_abertas') or 0),
                1 if float(o.get('horas_abertas') or 0) > 48 else 0
            ))
            novos += 1
        else:
            if existente['status_hub'] not in ('execucao', 'finalizada'):
                db.execute("""
                    UPDATE ht_os SET status_ixc=?, status_hub=?,
                    ixc_tecnico_id=?, sincronizado_em=?
                    WHERE ixc_os_id=?
                """, (o['status'], status_hub, o['id_tecnico'],
                      datetime.now().strftime("%d/%m/%Y %H:%M"), o['id']))
                atualizados += 1

    db.commit()
    db.close()
    print(f"  Novas: {novos} | Atualizadas: {atualizados} | Total IXC: {len(os_rows)}")

if __name__ == "__main__":
    run()
