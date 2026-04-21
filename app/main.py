import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

app = FastAPI(title="Hub Técnico Cliquedf", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

from app.routes import auth, os as os_routes, gps, despacho, estoque, admin, despesas
from app.bootstrap.create_tables import init as init_tables

init_tables()

app.include_router(auth.router)
app.include_router(os_routes.router)
app.include_router(gps.router)
app.include_router(despacho.router)
app.include_router(estoque.router)
app.include_router(admin.router)
app.include_router(despesas.router)

STATIC_DIR = BASE_DIR / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/", response_class=HTMLResponse)
async def root():
    p = STATIC_DIR / "login.html"
    return p.read_text() if p.exists() else "<h2>Hub Técnico — em construção</h2>"

@app.get("/app", response_class=HTMLResponse)
async def app_tecnico():
    p = STATIC_DIR / "app.html"
    return HTMLResponse(p.read_text())

@app.get("/hub", response_class=HTMLResponse)
async def hub_page():
    p = STATIC_DIR / "hub.html"
    return HTMLResponse(p.read_text())

@app.get("/login", response_class=HTMLResponse)
async def login_page():
    p = STATIC_DIR / "login.html"
    return HTMLResponse(p.read_text())

@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    p = STATIC_DIR / "admin.html"
    return HTMLResponse(p.read_text())

@app.get("/painel", response_class=HTMLResponse)
async def despacho_page():
    p = STATIC_DIR / "painel.html"
    return p.read_text() if p.exists() else "<h2>Despacho — em construção</h2>"

@app.get("/health")
async def health():
    return {"status": "ok", "operacao": os.getenv("OPERACAO"), "versao": "1.0.0"}
