# ============================================================
# GUIA DE DEPLOY — Patch KM por Veículo
# HubTécnico CliqueDf · IaTechHub · 2026
# ============================================================

## ORDEM DE EXECUÇÃO

### PASSO 1 — Backup do banco (OBRIGATÓRIO antes de tudo)
```bash
cd /opt/automacoes/cliquedf/tecnico
cp hub_tecnico.db hub_tecnico_backup_$(date +%Y%m%d_%H%M).db
```

### PASSO 2 — Rodar migration SQL
```bash
sqlite3 hub_tecnico.db < migration_km_veiculo.sql
```

Verificar se funcionou:
```bash
sqlite3 hub_tecnico.db ".schema ht_km_os"
# Deve mostrar: veiculo_id INTEGER REFERENCES ht_veiculos(id)

sqlite3 hub_tecnico.db "SELECT name FROM sqlite_master WHERE type='view';"
# Deve listar: vw_ultimo_km_veiculo, vw_km_por_veiculo
```

### PASSO 3 — Aplicar patch em os.py

No arquivo `app/routes/os.py`:

1. Adicionar as funções helper logo após os imports:
   - `_get_veiculo_atual(tecnico_id, db)`
   - `_get_ultimo_km_veiculo(veiculo_id, db)`

2. Substituir as 3 funções:
   - `ultimo_km` (GET /{os_id}/ultimo-km)
   - `iniciar_deslocamento_km` (POST /{os_id}/iniciar-deslocamento-km)
   - `iniciar_execucao_km` (POST /{os_id}/iniciar-execucao-km)

3. Adicionar novo endpoint:
   ```python
   @router.get("/veiculo-atual")
   async def get_veiculo_atual_tecnico(usuario=Depends(requer_tecnico)):
       db = get_db()
       try:
           veiculo = _get_veiculo_atual(usuario["id"], db)
           if not veiculo:
               return {"placa": None, "modelo": None, "marca": None,
                       "veiculo_id": None, "ultimo_km": 0}
           ultimo = _get_ultimo_km_veiculo(veiculo["veiculo_id"], db)
           return {**veiculo, "ultimo_km": ultimo}
       finally:
           db.close()
   ```

### PASSO 4 — Aplicar patch em app.html

No arquivo `static/app.html`:

1. Adicionar `HTML_CARD_VEICULO` no dashboard (após os KPIs)
2. Substituir funções: `abrirModalKmSaida`, `confirmarKmSaida`
3. Substituir funções: `abrirModalKmChegada`, `confirmarKmChegada`
4. Adicionar chamada `await carregarVeiculoAtual()` dentro de `carregarDashboard()`

### PASSO 5 — Reiniciar serviço
```bash
systemctl restart hubtecnico_cliquedf
sleep 3
curl -s https://tecnico.iatechhub.com.br/health
```

---

## COMO TESTAR

### Teste 1 — API veículo atual
```bash
TOKEN=$(curl -s -X POST http://127.0.0.1:8008/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"login":"admin","senha":"@!wt0n123"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s http://127.0.0.1:8008/api/os/veiculo-atual \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

Deve retornar: `{ "placa": "TNV1C40", "modelo": "...", "ultimo_km": 12345, ... }`

### Teste 2 — Views de KM
```bash
sqlite3 hub_tecnico.db "SELECT * FROM vw_ultimo_km_veiculo;"
sqlite3 hub_tecnico.db "SELECT * FROM vw_km_por_veiculo ORDER BY data DESC LIMIT 10;"
```

---

## RELATÓRIO FROTA — Query pronta

Para saber quantos KM um veículo rodou em um período:
```sql
SELECT
    placa, modelo,
    SUM(km_total_dia) AS km_total,
    COUNT(DISTINCT data) AS dias_trabalhados,
    COUNT(DISTINCT tecnico_id) AS tecnicos_distintos
FROM vw_km_por_veiculo
WHERE data BETWEEN '2026-04-01' AND '2026-04-30'
GROUP BY veiculo_id
ORDER BY km_total DESC;
```

---

## O QUE MUDA NO COMPORTAMENTO

| Situação | Antes | Depois |
|---|---|---|
| Técnico com mesmo veículo | Valida por técnico | Valida por veículo ✅ |
| Técnico troca de veículo | ❌ Erro de KM retroativo | ✅ Busca último KM do novo veículo |
| Dois técnicos no mesmo veículo (turnos) | ❌ Conflito | ✅ Continua do último KM do veículo |
| App mostra veículo atual | ❌ Não mostrava | ✅ Card no dashboard |
| Relatório KM por veículo | ❌ Não tinha | ✅ View vw_km_por_veiculo |
