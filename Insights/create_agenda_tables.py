"""
Etapa 1 — Criar tabelas do motor de agendamento
Executar: python3 create_agenda_tables.py
"""
import sqlite3, os

DB_PATH = next(
    (f for f in [
        "/opt/automacoes/cliquedf/tecnico/hub_tecnico.db",
        "/opt/automacoes/cliquedf/tecnico/tecnico.db",
    ] if os.path.exists(f)),
    None
)

if not DB_PATH:
    # Descobrir automaticamente
    import glob
    dbs = glob.glob("/opt/automacoes/cliquedf/tecnico/*.db")
    DB_PATH = dbs[0] if dbs else None

if not DB_PATH:
    print("ERRO: banco .db não encontrado")
    exit(1)

print(f"Banco: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
cur = conn.cursor()

# ── Tabela 1: Pontuação por assunto IXC ──────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS ht_assunto_pontos (
    id_assunto   INTEGER PRIMARY KEY,
    assunto      TEXT,
    pontos       INTEGER DEFAULT 10,
    tempo_min    INTEGER DEFAULT 60,
    categoria    TEXT DEFAULT 'suporte'
)
""")

# Popular com os assuntos reais mapeados
assuntos = [
    # Instalações — 20 pts, 90 min
    (2,   'INSTALAÇÃO FTTH',                          20, 90, 'instalacao'),
    (3,   'INSTALACAO EM REDE DE FIBRA',               20, 90, 'instalacao'),
    (49,  'ATIVACAO FIBRA',                            20, 90, 'instalacao'),
    (68,  'INSTALAÇÃO PONTO A PONTO FIBRA/WIRELESS',   20, 90, 'instalacao'),
    (26,  'INSTALAÇÃO UTP',                            20, 90, 'instalacao'),
    (152, 'CONSTRUÇÃO - ATIVAÇÃO DE FIBRA',            20, 90, 'instalacao'),
    (232, '[INFRA] ATIVAÇÃO FIBRA',                    20, 90, 'instalacao'),
    (227, '[OPC] ATIVAÇÃO FIBRA - NOVO',               20, 90, 'instalacao'),

    # Mudança de endereço — 20 pts, 90 min
    (19,  'MUDANÇA DE ENDEREÇO',                       20, 90, 'mudanca'),
    (4,   'TROCA DE ENDERECO',                         20, 90, 'mudanca'),
    (235, '[SUPF] SUPERVISÃO - MUDANÇA DE ENDEREÇO',   20, 90, 'mudanca'),

    # Fibra rompida — 20 pts, 60 min
    (240, 'FIBRA ROMPIDA',                             20, 60, 'fibra_rompida'),
    (166, 'MANUTENÇÃO - ROMPIMENTO DE FTTH',           20, 60, 'fibra_rompida'),
    (170, 'MANUTENÇÃO - ROMPIMENTO DE RAMAL',          20, 60, 'fibra_rompida'),
    (241, 'FEEDBACK/FIBRA ROMPIDA',                    20, 60, 'fibra_rompida'),

    # Reativação — 15 pts, 60 min
    (15,  'REATIVAÇÃO',                                15, 60, 'reativacao'),
    (75,  'REATIVAÇÃO - NOVO CONTRATO',                15, 60, 'reativacao'),

    # Manutenção — 15 pts, 60 min
    (16,  'MANUTENÇÃO',                                15, 60, 'manutencao'),
    (7,   'MANUTENÇÃO',                                15, 60, 'manutencao'),
    (120, 'MANUTENÇÃO CORRETIVA',                      15, 60, 'manutencao'),
    (145, 'MANUTENÇÃO DE REDES',                       15, 60, 'manutencao'),
    (161, 'MANUTENÇÃO DE REDES',                       15, 60, 'manutencao'),
    (160, 'MANUTENÇÃO DE REDES - REINCIDENCIA',        15, 60, 'manutencao'),
    (148, 'MANUTENÇÃO DE REDES - REINCIDENCIA',        15, 60, 'manutencao'),
    (14,  'MIGRAÇÃO DE TECNOLOGIA',                    15, 60, 'manutencao'),

    # Suporte / internet — 10 pts, 45 min
    (1,   'VERIFICAR INTERNET',                        10, 45, 'suporte'),
    (5,   'INTERNET LENTA',                            10, 45, 'suporte'),
    (9,   'REPARA A INTERNET',                         10, 45, 'suporte'),
    (20,  'SEM ACESSO',                                10, 45, 'suporte'),
    (21,  'INTERNET LENTA',                            10, 45, 'suporte'),
    (44,  'REINCIDENCIA/SEM ACESSO',                   10, 45, 'suporte'),
    (47,  'REINCIDENCIA/INTERNET LENTA',               10, 45, 'suporte'),
    (94,  '[CDF] SUPORTE TECNICO',                     10, 45, 'suporte'),
    (103, 'REICIDENCI/VERIFICAR INTERNET',             10, 45, 'suporte'),
    (245, 'FIBRA BAIXA',                               10, 45, 'suporte'),

    # Configurar/instalar equipamento — 10 pts, 45 min
    (27,  'CONFIGURAR ROTEADOR',                       10, 45, 'equipamento'),
    (30,  'INSTALAR COMODATO',                         10, 45, 'equipamento'),
    (239, 'INSTALAR COMODATO_UP2025',                  10, 45, 'equipamento'),
    (48,  'INSTALAR ROTEADOR COMPRADO NA LOJA',        10, 45, 'equipamento'),
    (113, 'TROCA DE EQUIPAMENTOS',                     10, 45, 'equipamento'),
    (18,  'MUDANÇA DE EQUIPAMENTO DE CÔMODO',          10, 45, 'equipamento'),
    (172, 'MANUTENÇÃO - TROCA DE EQUIPAMENTO',         10, 45, 'equipamento'),

    # Visita / recolhimento — 5 pts, 30 min
    (8,   'VISITA TECNICA',                             5, 30, 'visita'),
    (35,  '[OPERACIONAL]VISITAR CLIENTE',               5, 30, 'visita'),
    (6,   'RECOLHER ONU E PTO',                         5, 30, 'recolhimento'),
    (22,  'RECOLHIMENTO DE EQUIPAMENTO',                5, 30, 'recolhimento'),
    (39,  'RETIRADA DE EQUIPAMENTO',                    5, 30, 'recolhimento'),
    (40,  'RETIRADA DA FIBRA NA CTO',                   5, 30, 'recolhimento'),
    (89,  'RETIRAR FIBRA',                              5, 30, 'recolhimento'),
    (111, 'RECOLHIMENTO DE EQUIPAMENTO (M. TITULARIDADE)', 5, 30, 'recolhimento'),
    (112, 'RECOLHER/DEVOLUÇÃO',                         5, 30, 'recolhimento'),
]

cur.executemany("""
    INSERT OR IGNORE INTO ht_assunto_pontos (id_assunto, assunto, pontos, tempo_min, categoria)
    VALUES (?, ?, ?, ?, ?)
""", assuntos)

print(f"ht_assunto_pontos: {len(assuntos)} assuntos inseridos")

# ── Tabela 2: Rotas geradas ───────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS ht_rotas (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    data_rota    TEXT NOT NULL,
    tecnico_id   INTEGER,
    tecnico_nome TEXT,
    pontos       INTEGER DEFAULT 0,
    tempo_est    INTEGER DEFAULT 0,
    total_os     INTEGER DEFAULT 0,
    bairro_ref   TEXT,
    status       TEXT DEFAULT 'pendente',
    criado_em    TEXT DEFAULT (datetime('now','-3 hours')),
    confirmado_em TEXT
)
""")
print("ht_rotas: criada")

# ── Tabela 3: OS de cada rota ─────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS ht_rotas_os (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    rota_id   INTEGER NOT NULL,
    os_id     INTEGER NOT NULL,
    ordem     INTEGER DEFAULT 0,
    pontos    INTEGER DEFAULT 0,
    FOREIGN KEY (rota_id) REFERENCES ht_rotas(id)
)
""")
print("ht_rotas_os: criada")

conn.commit()
conn.close()
print("\n✅ Etapa 1 concluída — tabelas criadas com sucesso")
print("\nPróximo: rode 'python3 create_agenda_tables.py' no servidor")
