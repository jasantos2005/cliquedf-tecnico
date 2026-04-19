import os, hashlib, sqlite3
from datetime import datetime, timedelta
from fastapi import HTTPException, Header
from jose import jwt, JWTError
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

SECRET = os.getenv("SECRET_KEY", "hubtecnico2026")
DB_PATH = os.path.join(os.path.dirname(__file__), "../../hub_tecnico.db")

def sha256(s): return hashlib.sha256(s.encode()).hexdigest()

def criar_token(usuario_id, nivel):
    return jwt.encode(
        {"sub": str(usuario_id), "nivel": nivel,
         "exp": datetime.utcnow() + timedelta(days=30)},
        SECRET, algorithm="HS256"
    )

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _usuario(token):
    if not token or not token.startswith("Bearer "):
        raise HTTPException(401, "Token ausente")
    try:
        payload = jwt.decode(token.split(" ", 1)[1], SECRET, algorithms=["HS256"])
    except JWTError:
        raise HTTPException(401, "Token inválido")
    db = get_db()
    u = db.execute("SELECT * FROM ht_usuarios WHERE id=?", (payload["sub"],)).fetchone()
    db.close()
    if not u or not u["ativo"]: raise HTTPException(401, "Usuário inativo")
    return dict(u)

def requer_tecnico(authorization: str = Header(...)):
    u = _usuario(authorization)
    if u["nivel"] < 10: raise HTTPException(403, "Acesso negado")
    return u

def requer_supervisor(authorization: str = Header(...)):
    u = _usuario(authorization)
    if u["nivel"] < 50: raise HTTPException(403, "Acesso negado")
    return u

def requer_admin(authorization: str = Header(...)):
    u = _usuario(authorization)
    if u["nivel"] < 99: raise HTTPException(403, "Acesso negado")
    return u
