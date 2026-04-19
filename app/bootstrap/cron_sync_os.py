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
               a.assunto AS assunto_nome
        FROM su_oss_chamado o
        JOIN cliente cl ON cl.id = o.id_cliente
        JOIN su_oss_assunto a ON a.id = o.id_assunto
        WHERE o.status IN ('A','D','E')
          AND (o.id_tecnico IN ({ids_str}) OR o.id_tecnico = 0 OR o.id_tecnico IS NULL)
          AND DATE(o.data_abertura) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    """)

    db = get_db()
    novos = 0
    atualizados = 0

    for o in os_rows:
        status_hub = {
            'A': 'pendente',
            'D': 'deslocamento',
            'E': 'execucao',
            'F': 'finalizada'
        }.get(o['status'], 'pendente')

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
                    obs_abertura, sincronizado_em
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                datetime.now().strftime("%d/%m/%Y %H:%M")
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
