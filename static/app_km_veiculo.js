/* ============================================================
   PATCH app.html — KM por Veículo
   HubTécnico CliqueDf · IaTechHub · 2026

   ONDE APLICAR:
   Substituir a função `abrirModalKmSaida` e `abrirModalKmChegada`
   no app.html. Também adicionar o card de veículo no dashboard.
   ============================================================ */


/* ── 1. CARD VEÍCULO NO DASHBOARD ──────────────────────────
   Adicionar logo após o bloco de KPIs no dashboard,
   dentro de <div id="secao-dashboard"> */

const HTML_CARD_VEICULO = `
<div id="card-veiculo" style="
    background: var(--card-bg, #1e1e2e);
    border: 1px solid var(--border, #333);
    border-radius: 10px;
    padding: 12px 16px;
    margin: 8px 0 12px 0;
    display: flex;
    align-items: center;
    gap: 12px;
">
    <span style="font-size:1.6rem">🚗</span>
    <div id="veiculo-info" style="flex:1">
        <div style="font-size:.75rem; color:#aaa; margin-bottom:2px">Veículo atual</div>
        <div id="veiculo-nome" style="font-weight:600; font-size:.95rem">Carregando...</div>
        <div id="veiculo-km" style="font-size:.78rem; color:#aaa"></div>
    </div>
</div>
`;


/* ── 2. FUNÇÃO: Carregar veículo atual ──────────────────────
   Chamar dentro de carregarDashboard() */

async function carregarVeiculoAtual() {
    try {
        // Reutiliza o endpoint ultimo-km que agora retorna dados do veículo
        // Passa os_id=0 apenas para obter o veículo — ou use endpoint dedicado
        const res = await fetch('/api/os/veiculo-atual', {
            headers: { 'Authorization': 'Bearer ' + token }
        });
        if (!res.ok) return;
        const data = await res.json();

        const nomeEl = document.getElementById('veiculo-nome');
        const kmEl   = document.getElementById('veiculo-km');

        if (data.placa) {
            nomeEl.textContent = data.placa + ' · ' + (data.modelo || '');
            kmEl.textContent   = data.ultimo_km
                ? 'Último KM: ' + data.ultimo_km.toLocaleString('pt-BR') + ' km'
                : 'Sem KM registrado';
            // Guarda globalmente para usar nos modais
            window._veiculoAtual = data;
        } else {
            nomeEl.textContent = 'Nenhum veículo atribuído';
            kmEl.textContent   = 'Solicite ao supervisor';
            window._veiculoAtual = null;
        }
    } catch(e) {
        console.warn('Erro ao carregar veículo:', e);
    }
}


/* ── 3. MODAL KM SAÍDA — substituir abrirModalKmSaida ───────*/

async function abrirModalKmSaida(osId) {
    // Busca último KM do veículo atual
    let ultimoKm = 0;
    let infoVeiculo = '';

    try {
        const res = await fetch('/api/os/' + osId + '/ultimo-km', {
            headers: { 'Authorization': 'Bearer ' + token }
        });
        if (res.ok) {
            const data = await res.json();
            ultimoKm = data.ultimo_km || 0;
            if (data.placa) {
                infoVeiculo = '<div style="font-size:.78rem;color:#aaa;margin-bottom:8px">'
                    + '🚗 ' + data.placa + ' · ' + (data.modelo || '')
                    + (ultimoKm ? ' · Último KM: <b>' + ultimoKm.toLocaleString('pt-BR') + '</b>' : '')
                    + '</div>';
            } else {
                infoVeiculo = '<div style="font-size:.78rem;color:#f59e0b;margin-bottom:8px">'
                    + '⚠️ Sem veículo atribuído — KM salvo por técnico'
                    + '</div>';
            }
        }
    } catch(e) {}

    // Monta modal (adaptar ao padrão visual do seu app.html)
    const modal = document.getElementById('modal-km') || criarModalKm();
    modal.innerHTML = `
        <div class="modal-content">
            <h3>📍 KM de Saída</h3>
            ${infoVeiculo}
            <label>Informe o KM atual do veículo:</label>
            <input type="number" id="input-km-saida"
                   value="${ultimoKm || ''}"
                   min="${ultimoKm}"
                   placeholder="Ex: ${ultimoKm || 45000}"
                   style="width:100%;padding:10px;font-size:1.1rem;
                          margin:8px 0;border-radius:8px;border:1px solid #444;
                          background:#2a2a3e;color:#fff;text-align:center">
            ${ultimoKm ? '<div style="font-size:.75rem;color:#aaa">Mínimo aceito: ' + ultimoKm.toLocaleString('pt-BR') + ' km</div>' : ''}
            <div style="display:flex;gap:8px;margin-top:14px">
                <button onclick="fecharModalKm()"
                        style="flex:1;padding:10px;border-radius:8px;
                               background:#444;border:none;color:#fff;cursor:pointer">
                    Cancelar
                </button>
                <button onclick="confirmarKmSaida(${osId}, ${ultimoKm})"
                        style="flex:2;padding:10px;border-radius:8px;
                               background:#3b82f6;border:none;color:#fff;
                               font-weight:600;cursor:pointer">
                    ✅ Confirmar Saída
                </button>
            </div>
        </div>
    `;
    modal.style.display = 'flex';
}


/* ── 4. CONFIRMAR KM SAÍDA ───────────────────────────────── */

async function confirmarKmSaida(osId, ultimoKm) {
    const input = document.getElementById('input-km-saida');
    const km = parseInt(input.value);

    if (!km || isNaN(km)) {
        alert('Informe o KM atual.');
        return;
    }
    if (ultimoKm && km < ultimoKm) {
        alert('KM inválido!\nO KM deve ser maior ou igual a ' + ultimoKm.toLocaleString('pt-BR') + ' km.');
        return;
    }

    try {
        const res = await fetch('/api/os/' + osId + '/iniciar-deslocamento-km', {
            method: 'POST',
            headers: {
                'Authorization': 'Bearer ' + token,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ km_saida: km })
        });
        const data = await res.json();
        if (!res.ok) {
            alert('Erro: ' + (data.detail || 'Tente novamente'));
            return;
        }
        fecharModalKm();
        mostrarToast('✅ Saída registrada · ' + km.toLocaleString('pt-BR') + ' km');
        await carregarMinhasOS();
        await carregarVeiculoAtual(); // Atualiza card do veículo
    } catch(e) {
        alert('Erro de conexão. Tente novamente.');
    }
}


/* ── 5. MODAL KM CHEGADA — substituir abrirModalKmChegada ── */

async function abrirModalKmChegada(osId, kmSaida) {
    let infoVeiculo = '';
    if (window._veiculoAtual && window._veiculoAtual.placa) {
        infoVeiculo = '<div style="font-size:.78rem;color:#aaa;margin-bottom:8px">'
            + '🚗 ' + window._veiculoAtual.placa
            + (kmSaida ? ' · KM saída: <b>' + kmSaida.toLocaleString('pt-BR') + '</b>' : '')
            + '</div>';
    }

    const modal = document.getElementById('modal-km') || criarModalKm();
    modal.innerHTML = `
        <div class="modal-content">
            <h3>📍 KM de Chegada</h3>
            ${infoVeiculo}
            <label>Informe o KM na chegada ao cliente:</label>
            <input type="number" id="input-km-chegada"
                   value=""
                   min="${kmSaida || 0}"
                   placeholder="Ex: ${(kmSaida || 45000) + 10}"
                   style="width:100%;padding:10px;font-size:1.1rem;
                          margin:8px 0;border-radius:8px;border:1px solid #444;
                          background:#2a2a3e;color:#fff;text-align:center">
            ${kmSaida ? '<div style="font-size:.75rem;color:#aaa">Mínimo aceito: ' + kmSaida.toLocaleString('pt-BR') + ' km</div>' : ''}
            <div style="display:flex;gap:8px;margin-top:14px">
                <button onclick="fecharModalKm()"
                        style="flex:1;padding:10px;border-radius:8px;
                               background:#444;border:none;color:#fff;cursor:pointer">
                    Cancelar
                </button>
                <button onclick="confirmarKmChegada(${osId}, ${kmSaida || 0})"
                        style="flex:2;padding:10px;border-radius:8px;
                               background:#22c55e;border:none;color:#fff;
                               font-weight:600;cursor:pointer">
                    ✅ Confirmar Chegada
                </button>
            </div>
        </div>
    `;
    modal.style.display = 'flex';
}


/* ── 6. CONFIRMAR KM CHEGADA ────────────────────────────── */

async function confirmarKmChegada(osId, kmSaida) {
    const input = document.getElementById('input-km-chegada');
    const km = parseInt(input.value);

    if (!km || isNaN(km)) {
        alert('Informe o KM de chegada.');
        return;
    }
    if (kmSaida && km < kmSaida) {
        alert('KM chegada não pode ser menor que KM saída (' + kmSaida.toLocaleString('pt-BR') + ')');
        return;
    }

    try {
        const res = await fetch('/api/os/' + osId + '/iniciar-execucao-km', {
            method: 'POST',
            headers: {
                'Authorization': 'Bearer ' + token,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ km_chegada: km })
        });
        const data = await res.json();
        if (!res.ok) {
            alert('Erro: ' + (data.detail || 'Tente novamente'));
            return;
        }
        const deslocamento = kmSaida ? (km - kmSaida) : null;
        fecharModalKm();
        mostrarToast('✅ Chegada registrada'
            + (deslocamento ? ' · ' + deslocamento + ' km rodados' : ''));
        await carregarMinhasOS();
        await carregarVeiculoAtual();
    } catch(e) {
        alert('Erro de conexão. Tente novamente.');
    }
}


/* ── 7. NOVO ENDPOINT BACKEND necessário ────────────────────
   Adicionar em os.py (ou criar frota.py):

   @router.get("/veiculo-atual")
   async def get_veiculo_atual_tecnico(usuario=Depends(requer_tecnico)):
       db = get_db()
       try:
           veiculo = _get_veiculo_atual(usuario["id"], db)
           if not veiculo:
               return {"placa": None, "modelo": None, "ultimo_km": 0}
           ultimo = _get_ultimo_km_veiculo(veiculo["veiculo_id"], db)
           return {**veiculo, "ultimo_km": ultimo}
       finally:
           db.close()
*/
