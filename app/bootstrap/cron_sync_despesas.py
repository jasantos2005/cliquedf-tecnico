#!/usr/bin/env python3
"""
Cron: Sincroniza despesas e abastecimentos do IXC → Hub
Executar: */30 * * * * (a cada 30 min)
"""
import sqlite3, sys, os
sys.path.insert(0, '/opt/automacoes/cliquedf/tecnico')
from datetime import datetime, timezone, timedelta
from app.services.ixc_db import ixc_conn

DB = '/opt/automacoes/cliquedf/tecnico/hub_tecnico.db'

def brt():
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

def sync_despesas():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row

    try:
        with ixc_conn() as c:
            cur = c.cursor()
            cur.execute("""
                SELECT d.id, d.data, d.tipo, d.descricao, d.valor,
                       d.kilometragem, d.valor_litro, d.quantidade_litros,
                       d.id_condutor, d.observacao, d.id_veiculo
                FROM ixcprovedor.veiculos_despesas d
                WHERE d.data >= '2026-01-01'
                ORDER BY d.data ASC
            """)
            despesas = cur.fetchall()

        inseridas = 0
        atualizadas = 0
        for d in despesas:
            v = db.execute("SELECT id FROM ht_veiculos WHERE ixc_veiculo_id=?",
                           (d['id_veiculo'],)).fetchone()
            id_veiculo_local = v['id'] if v else None

            tec = db.execute("SELECT id FROM ht_usuarios WHERE ixc_funcionario_id=?",
                             (d['id_condutor'],)).fetchone() if d['id_condutor'] else None
            id_tecnico = tec['id'] if tec else None

            existing = db.execute(
                "SELECT id FROM ht_despesas WHERE ixc_despesa_id=?", (d['id'],)
            ).fetchone()

            if existing:
                db.execute("""
                    UPDATE ht_despesas SET
                        valor=?, kilometragem=?, valor_litro=?,
                        quantidade_litros=?, observacao=?, id_veiculo=?,
                        id_tecnico=?, sincronizado_ixc=1
                    WHERE ixc_despesa_id=?
                """, (
                    float(d['valor'] or 0), float(d['kilometragem'] or 0),
                    float(d['valor_litro'] or 0), float(d['quantidade_litros'] or 0),
                    d['observacao'], id_veiculo_local, id_tecnico, d['id']
                ))
                atualizadas += 1
            else:
                db.execute("""
                    INSERT INTO ht_despesas
                        (ixc_despesa_id, id_veiculo, id_condutor, id_tecnico,
                         tipo, descricao, valor, data, kilometragem,
                         valor_litro, quantidade_litros, observacao,
                         sincronizado_ixc, criado_em)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,?)
                """, (
                    d['id'], id_veiculo_local, d['id_condutor'], id_tecnico,
                    d['tipo'], d['descricao'],
                    float(d['valor'] or 0), str(d['data']),
                    float(d['kilometragem'] or 0),
                    float(d['valor_litro'] or 0),
                    float(d['quantidade_litros'] or 0),
                    d['observacao'], brt()
                ))
                inseridas += 1

        db.commit()
        print(f"[{brt()}] sync_despesas: Novas={inseridas} | Atualizadas={atualizadas} | Total IXC={len(despesas)}")

    except Exception as e:
        print(f"[{brt()}] ERRO sync_despesas: {e}")
        import traceback; traceback.print_exc()
    finally:
        db.close()

if __name__ == '__main__':
    sync_despesas()
