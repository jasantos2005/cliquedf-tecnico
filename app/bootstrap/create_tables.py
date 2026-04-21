import sqlite3, os, hashlib

DB = os.path.join(os.path.dirname(__file__), "../../hub_tecnico.db")

def sha256(s): return hashlib.sha256(s.encode()).hexdigest()

def init():
    conn = sqlite3.connect(DB)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS ht_usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT, login TEXT UNIQUE, senha_hash TEXT,
        nivel INTEGER DEFAULT 10, ixc_funcionario_id INTEGER,
        telefone TEXT, ativo INTEGER DEFAULT 1,
        criado_em TEXT DEFAULT (datetime('now','-3 hours'))
    );
    CREATE TABLE IF NOT EXISTS ht_os (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ixc_os_id INTEGER UNIQUE, id_tecnico INTEGER,
        ixc_tecnico_id INTEGER, ixc_cliente_id INTEGER,
        id_contrato_kit INTEGER, id_assunto INTEGER,
        assunto_nome TEXT, status_ixc TEXT,
        status_hub TEXT DEFAULT 'pendente',
        cliente_nome TEXT, endereco TEXT, bairro TEXT,
        cidade TEXT, telefone TEXT, referencia TEXT,
        lat REAL, lon REAL, data_abertura TEXT,
        data_agenda TEXT, obs_abertura TEXT, sincronizado_em TEXT,
        sla_horas REAL DEFAULT 0, horas_abertas REAL DEFAULT 0,
        sla_estourado INTEGER DEFAULT 0, data_reservada TEXT,
        motivo_reagendamento TEXT, ordem_execucao INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS ht_os_execucao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ixc_os_id INTEGER UNIQUE,
        checklist_json TEXT DEFAULT '[]',
        fotos_antes_json TEXT DEFAULT '[]',
        fotos_depois_json TEXT DEFAULT '[]',
        assinatura_base64 TEXT, obs_tecnico TEXT,
        solucao_registrada TEXT, iniciada_em TEXT,
        finalizada_em TEXT, lat_chegada REAL, lon_chegada REAL,
        km_deslocamento REAL DEFAULT 0,
        sincronizado_ixc INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS ht_os_materiais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ixc_os_id INTEGER, id_tecnico INTEGER,
        id_produto INTEGER, produto_nome TEXT,
        quantidade REAL, unidade TEXT, numero_serie TEXT,
        tipo_uso TEXT DEFAULT 'consumivel_os',
        sincronizado_ixc INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS ht_gps_track (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_tecnico INTEGER, lat REAL, lon REAL,
        velocidade REAL DEFAULT 0,
        status_tecnico TEXT DEFAULT 'livre',
        ixc_os_id INTEGER,
        registrado_em TEXT DEFAULT (datetime('now','-3 hours'))
    );
    CREATE TABLE IF NOT EXISTS ht_eventos_frota (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_tecnico INTEGER, tipo_evento TEXT,
        lat REAL, lon REAL, velocidade REAL,
        ixc_os_id INTEGER, registrado_em TEXT,
        alertado INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS ht_produtos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT, codigo TEXT, tipo TEXT,
        unidade TEXT DEFAULT 'un',
        vincula_contrato_ixc INTEGER DEFAULT 0,
        estoque_minimo REAL DEFAULT 0, ativo INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS ht_estoque_principal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_produto INTEGER UNIQUE, quantidade REAL DEFAULT 0,
        ultima_atualizacao TEXT
    );
    CREATE TABLE IF NOT EXISTS ht_estoque_tecnico (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_tecnico INTEGER, id_produto INTEGER,
        quantidade REAL DEFAULT 0, ultima_atualizacao TEXT,
        UNIQUE(id_tecnico, id_produto)
    );
    CREATE TABLE IF NOT EXISTS ht_requisicoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_tecnico INTEGER, status TEXT DEFAULT 'pendente',
        criada_em TEXT DEFAULT (datetime('now','-3 hours')),
        aprovada_em TEXT, entregue_em TEXT,
        aprovado_por INTEGER, obs TEXT
    );
    CREATE TABLE IF NOT EXISTS ht_requisicao_itens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_requisicao INTEGER, id_produto INTEGER,
        qtd_solicitada REAL, qtd_aprovada REAL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS ht_configuracoes (
        chave TEXT PRIMARY KEY, valor TEXT
    );
    CREATE TABLE IF NOT EXISTS ht_veiculos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ixc_veiculo_id INTEGER UNIQUE,
        marca_modelo TEXT, placa TEXT,
        ano_fab INTEGER DEFAULT 0, cor TEXT DEFAULT '',
        tipo TEXT DEFAULT 'carro', ativo INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS ht_tecnico_veiculo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_tecnico INTEGER NOT NULL,
        id_veiculo INTEGER NOT NULL,
        data TEXT NOT NULL,
        km_inicial REAL DEFAULT 0,
        km_final REAL DEFAULT 0,
        jornada_inicio TEXT,
        UNIQUE(id_tecnico, data)
    );
    CREATE TABLE IF NOT EXISTS ht_despesas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ixc_despesa_id INTEGER,
        id_veiculo INTEGER,
        id_condutor INTEGER,
        id_tecnico INTEGER,
        tipo TEXT,
        descricao TEXT,
        valor REAL,
        data TEXT,
        kilometragem REAL,
        valor_litro REAL,
        quantidade_litros REAL,
        observacao TEXT,
        sincronizado_ixc INTEGER DEFAULT 0,
        criado_em TEXT DEFAULT (datetime('now','-3 hours'))
    );
    """)

    conn.execute("""
        INSERT OR IGNORE INTO ht_usuarios
            (nome, login, senha_hash, nivel, ixc_funcionario_id)
        VALUES (?,?,?,?,?)
    """, ("Admin", "admin", sha256("admin123"), 99, 27))

    tecnicos = [
        (13, "ALEXANDRE",            "alexandre"),
        (17, "DENISON",              "denison"),
        (32, "RODRIGO SANTOS",       "rodrigo"),
        (35, "RODRIGO SANTOS 2",     "rodrigo2"),
        (47, "LEANDRO",              "leandro"),
        (50, "RICARDO ILHA",         "ricardo"),
        (56, "ROGERIO",              "rogerio"),
        (55, "VICTOR FERREIRA",      "victor"),
        (60, "WELINTON SANTOS",      "welinton"),
        (46, "WELLINGTON PIACABUCU", "wellington"),
    ]
    for ixc_id, nome, login in tecnicos:
        conn.execute("""
            INSERT OR IGNORE INTO ht_usuarios
                (nome, login, senha_hash, nivel, ixc_funcionario_id)
            VALUES (?,?,?,?,?)
        """, (nome, login, sha256("tecnico123"), 10, ixc_id))

    configs = [
        ("vel_maxima_kmh","80"),
        ("tempo_parado_alerta_min","15"),
        ("raio_chegada_cliente_m","100"),
        ("intervalo_gps_s","10"),
        ("horario_inicio","07:00"),
        ("horario_fim","18:00"),
    ]
    for k,v in configs:
        conn.execute("INSERT OR IGNORE INTO ht_configuracoes VALUES (?,?)",(k,v))

    conn.commit()
    conn.close()
    print("OK — Tabelas criadas | admin/admin123 | tecnicos/tecnico123")

if __name__ == "__main__":
    init()
