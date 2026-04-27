import sqlite3, os, hashlib
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.services.auth import criar_token, get_db, sha256

router = APIRouter(prefix="/api/auth", tags=["auth"])

class LoginInput(BaseModel):
    login: str
    senha: str

@router.post("/login")
def login(data: LoginInput):
    db = get_db()
    u = db.execute(
        "SELECT * FROM ht_usuarios WHERE login=? AND ativo=1", (data.login,)
    ).fetchone()
    db.close()
    if not u or u["senha_hash"] != sha256(data.senha):
        raise HTTPException(401, "Login ou senha incorretos")
    return {
        "token": criar_token(u["id"], u["nivel"]),
        "usuario": {
            "id": u["id"], "nome": u["nome"],
            "login": u["login"],
            "nivel": u["nivel"],
            "ixc_funcionario_id": u["ixc_funcionario_id"]
        }
    }

@router.post("/trocar-senha")
def trocar_senha(data: dict):
    db = get_db()
    u = db.execute("SELECT * FROM ht_usuarios WHERE id=?", (data["id"],)).fetchone()
    if not u or u["senha_hash"] != sha256(data["senha_atual"]):
        raise HTTPException(400, "Senha atual incorreta")
    db.execute("UPDATE ht_usuarios SET senha_hash=? WHERE id=?",
               (sha256(data["nova_senha"]), data["id"]))
    db.commit()
    db.close()
    return {"ok": True}
