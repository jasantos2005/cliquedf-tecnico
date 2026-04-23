"""
patch_agenda_frontend.py — Adiciona aba Agenda no painel.html
Caminho: /tmp/patch_agenda_frontend.py
Executar: python3 /tmp/patch_agenda_frontend.py
"""

import re

PAINEL = "/opt/automacoes/cliquedf/tecnico/static/painel.html"

with open(PAINEL, "r", encoding="utf-8") as f:
    html = f.read()

# ── 1. Menu item na sidebar ───────────────────────────────────────────────────
MENU_ANCHOR = '<span class="ni-label">Abastecimentos</span>'
MENU_NEW = '''<span class="ni-label">Abastecimentos</span>
      </div>
      <div class="ni" id="ni-agenda" style="display:none" onclick="navTo(\'agenda\')">
        <div class="ni-icon">📅</div>
        <span class="ni-label">Agenda</span>'''

if 'ni-agenda' not in html:
    html = html.replace(MENU_ANCHOR, MENU_NEW)
    print("✅ Menu item inserido")
else:
    print("⚠️  Menu item já existe")

# ── 2. Seção HTML — inserir antes de secao-usuarios ──────────────────────────
SECAO_ANCHOR = '<div class="secao" id="secao-usuarios"'
SECAO_NEW = '''<div class="secao" id="secao-agenda" style="padding:20px;overflow-y:auto;flex-direction:column">

  <!-- Cabeçalho -->
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:18px;flex-wrap:wrap;gap:10px">
    <div>
      <div style="font-size:1.1rem;font-weight:800">📅 Agenda Automática</div>
      <div style="font-size:.78rem;color:var(--text3)">Rotas sugeridas por pontos e localização</div>
    </div>
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
      <input type="date" id="agenda-data" style="background:var(--surf2);border:1px solid var(--border);border-radius:7px;color:var(--text);padding:6px 10px;font-size:.82rem;outline:none">
      <button onclick="agendaGerarRotas()" style="background:var(--accent);color:#fff;border:none;border-radius:7px;padding:7px 16px;font-size:.82rem;font-weight:700;cursor:pointer">⚡ Gerar Rotas</button>
      <button onclick="agendaVerDia()" style="background:var(--surf2);border:1px solid var(--border);color:var(--text);border-radius:7px;padding:7px 14px;font-size:.82rem;cursor:pointer">📋 Ver Dia</button>
    </div>
  </div>

  <!-- KPIs -->
  <div id="agenda-kpis" style="display:none;gap:10px;margin-bottom:18px;flex-wrap:wrap">
    <div style="background:var(--surf2);border:1px solid var(--border);border-radius:10px;padding:12px 18px;min-width:120px;text-align:center">
      <div style="font-size:1.4rem;font-weight:800;color:var(--accent)" id="kpi-rotas">0</div>
      <div style="font-size:.72rem;color:var(--text3)">Rotas</div>
    </div>
    <div style="background:var(--surf2);border:1px solid var(--border);border-radius:10px;padding:12px 18px;min-width:120px;text-align:center">
      <div style="font-size:1.4rem;font-weight:800;color:var(--green)" id="kpi-os">0</div>
      <div style="font-size:.72rem;color:var(--text3)">OS</div>
    </div>
    <div style="background:var(--surf2);border:1px solid var(--border);border-radius:10px;padding:12px 18px;min-width:120px;text-align:center">
      <div style="font-size:1.4rem;font-weight:800;color:var(--amber)" id="kpi-tecnicos">0</div>
      <div style="font-size:.72rem;color:var(--text3)">Técnicos disponíveis</div>
    </div>
  </div>

  <!-- Loading -->
  <div id="agenda-loading" style="display:none;text-align:center;padding:40px;color:var(--text3)">
    <div style="font-size:1.5rem;margin-bottom:8px">⏳</div>
    <div>Gerando rotas...</div>
  </div>

  <!-- Lista de rotas sugeridas -->
  <div id="agenda-rotas" style="display:flex;flex-direction:column;gap:12px"></div>

  <!-- Modal confirmar técnico -->
  <div id="modal-agenda" class="modal-bg" onclick="if(event.target===this)this.style.display='none'">
    <div style="background:var(--surf);border-radius:14px;padding:28px;width:420px;max-width:95vw">
      <div style="font-size:1rem;font-weight:800;margin-bottom:16px">👷 Atribuir Técnico</div>
      <div id="modal-agenda-info" style="background:var(--surf2);border-radius:8px;padding:12px;margin-bottom:14px;font-size:.82rem;color:var(--text2)"></div>
      <div style="font-size:.78rem;color:var(--text3);margin-bottom:8px">Selecione o técnico:</div>
      <div id="modal-agenda-tecnicos" style="display:flex;flex-direction:column;gap:6px;max-height:260px;overflow-y:auto"></div>
      <div style="display:flex;gap:8px;margin-top:16px">
        <button onclick="document.getElementById('modal-agenda').style.display='none'" style="flex:1;padding:9px;background:var(--surf2);border:1px solid var(--border);color:var(--text);border-radius:7px;cursor:pointer">Cancelar</button>
        <button onclick="agendaConfirmar()" id="btn-confirmar-rota" style="flex:1;padding:9px;background:var(--accent);color:#fff;border:none;border-radius:7px;font-weight:700;cursor:pointer">✅ Confirmar</button>
      </div>
    </div>
  </div>

</div>

<div class="secao" id="secao-usuarios"'''

if 'secao-agenda' not in html:
    html = html.replace(SECAO_ANCHOR, SECAO_NEW)
    print("✅ Seção HTML inserida")
else:
    print("⚠️  Seção já existe")

# ── 3. navTo — exibir menu para supervisores (nivel >= 50) ────────────────────
NAV_ANCHOR = "} else if (secao==='usuarios') {"
NAV_NEW = """} else if (secao==='agenda') {
    agendaIniciar();
  } else if (secao==='usuarios') {"""

if "secao==='agenda'" not in html:
    html = html.replace(NAV_ANCHOR, NAV_NEW)
    print("✅ navTo agenda inserido")
else:
    print("⚠️  navTo agenda já existe")

# ── 4. Mostrar menu agenda para supervisores ──────────────────────────────────
SHOW_ANCHOR = "document.getElementById('ni-usuarios').style.display='flex';"
SHOW_NEW = """document.getElementById('ni-usuarios').style.display='flex';
    if (u.nivel >= 50) { document.getElementById('ni-agenda').style.display='flex'; }"""

if "ni-agenda').style.display='flex'" not in html:
    html = html.replace(SHOW_ANCHOR, SHOW_NEW)
    print("✅ Visibilidade menu agenda inserida")
else:
    print("⚠️  Visibilidade já existe")

# ── 5. JavaScript da agenda ───────────────────────────────────────────────────
JS_ANCHOR = "// fim do script"
JS_NOT_FOUND = JS_ANCHOR not in html

# Tentar ancora alternativa
if JS_NOT_FOUND:
    JS_ANCHOR = "</script>"
    # pegar o último </script>
    pos = html.rfind("</script>")
    JS_INSERT = """

// ═══════════════════════════════════════════════════════
// AGENDA — Motor de Agendamento Automático
// ═══════════════════════════════════════════════════════
var agendaRotasPendentes = [];
var agendaRotaSelecionada = null;
var agendaTecnicosCache = [];

function agendaIniciar() {
  var hoje = new Date().toISOString().slice(0,10);
  document.getElementById('agenda-data').value = hoje;
}

function agendaGerarRotas() {
  var data = document.getElementById('agenda-data').value;
  if (!data) { alert('Selecione uma data'); return; }

  document.getElementById('agenda-loading').style.display = 'block';
  document.getElementById('agenda-rotas').innerHTML = '';
  document.getElementById('agenda-kpis').style.display = 'none';

  // Buscar técnicos e rotas em paralelo
  Promise.all([
    fetch('/api/agenda/rotas-sugeridas?data=' + data, { headers: { Authorization: 'Bearer ' + TOKEN } }).then(r => r.json()),
    fetch('/api/agenda/tecnicos?data=' + data, { headers: { Authorization: 'Bearer ' + TOKEN } }).then(r => r.json())
  ]).then(function(results) {
    var rotasData = results[0];
    var tecData   = results[1];

    document.getElementById('agenda-loading').style.display = 'none';
    agendaRotasPendentes = rotasData.rotas || [];
    agendaTecnicosCache  = tecData.tecnicos || [];

    document.getElementById('kpi-rotas').textContent    = rotasData.total_rotas || 0;
    document.getElementById('kpi-os').textContent       = rotasData.total_os || 0;
    document.getElementById('kpi-tecnicos').textContent = agendaTecnicosCache.filter(function(t){ return t.pontos_disponiveis > 0; }).length;
    document.getElementById('agenda-kpis').style.display = 'flex';

    agendaRenderRotas(agendaRotasPendentes);
  }).catch(function(e) {
    document.getElementById('agenda-loading').style.display = 'none';
    document.getElementById('agenda-rotas').innerHTML = '<div style="color:var(--red);padding:20px">Erro ao gerar rotas: ' + e.message + '</div>';
  });
}

function agendaRenderRotas(rotas) {
  var el = document.getElementById('agenda-rotas');
  if (!rotas.length) {
    el.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">Nenhuma OS disponível para esta data.</div>';
    return;
  }

  el.innerHTML = rotas.map(function(r, idx) {
    var slaCount = r.os.filter(function(o){ return o.sla_estourado; }).length;
    var slaTag   = slaCount ? '<span style="background:#ff4d4d22;color:var(--red);border-radius:5px;padding:2px 7px;font-size:.7rem;font-weight:700">🚨 ' + slaCount + ' SLA</span>' : '';
    var osList   = r.os.map(function(o, i) {
      var sla = o.sla_estourado ? '🚨' : '';
      var pts = '<span style="background:var(--accent);color:#fff;border-radius:4px;padding:1px 6px;font-size:.68rem;font-weight:700">' + o.pontos + 'pts</span>';
      return '<div style="display:flex;align-items:flex-start;gap:8px;padding:7px 0;border-bottom:1px solid var(--border)">' +
        '<div style="color:var(--text3);font-size:.72rem;min-width:18px;padding-top:2px">' + (i+1) + '</div>' +
        '<div style="flex:1">' +
          '<div style="font-size:.8rem;font-weight:600">' + sla + ' ' + (o.cliente_nome||'') + '</div>' +
          '<div style="font-size:.72rem;color:var(--text3)">' + (o.assunto_nome||'') + ' · ' + (o.bairro||'') + '</div>' +
        '</div>' + pts + '</div>';
    }).join('');

    return '<div style="background:var(--surf2);border:1px solid var(--border);border-radius:12px;padding:16px">' +
      '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;flex-wrap:wrap;gap:6px">' +
        '<div>' +
          '<span style="font-weight:800;font-size:.9rem">Rota #' + r.rota_num + '</span>' +
          '<span style="color:var(--text3);font-size:.78rem;margin-left:8px">' + r.bairro_ref + '</span>' +
        '</div>' +
        '<div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">' +
          slaTag +
          '<span style="background:var(--surf);border:1px solid var(--border);border-radius:6px;padding:2px 8px;font-size:.72rem">' + r.total_os + ' OS</span>' +
          '<span style="background:var(--surf);border:1px solid var(--border);border-radius:6px;padding:2px 8px;font-size:.72rem">⚡ ' + r.pontos + ' pts</span>' +
          '<span style="background:var(--surf);border:1px solid var(--border);border-radius:6px;padding:2px 8px;font-size:.72rem">⏱ ' + r.tempo_fmt + '</span>' +
          '<button onclick="agendaAbrirModal(' + idx + ')" style="background:var(--accent);color:#fff;border:none;border-radius:6px;padding:4px 12px;font-size:.75rem;font-weight:700;cursor:pointer">Atribuir</button>' +
        '</div>' +
      '</div>' +
      '<div style="max-height:200px;overflow-y:auto">' + osList + '</div>' +
    '</div>';
  }).join('');
}

function agendaAbrirModal(idx) {
  agendaRotaSelecionada = agendaRotasPendentes[idx];
  var r = agendaRotaSelecionada;
  var data = document.getElementById('agenda-data').value;

  document.getElementById('modal-agenda-info').innerHTML =
    '<b>Rota #' + r.rota_num + '</b> · ' + r.bairro_ref +
    '<br>' + r.total_os + ' OS · ⚡ ' + r.pontos + ' pts · ⏱ ' + r.tempo_fmt;

  var el = document.getElementById('modal-agenda-tecnicos');
  el.innerHTML = agendaTecnicosCache.map(function(t) {
    var livre = t.pontos_disponiveis >= r.pontos;
    var cor   = livre ? 'var(--green)' : 'var(--red)';
    var tag   = livre ? '✅ Disponível' : '⚠️ Sem capacidade';
    return '<div class="tecnico-option" style="' + (!livre ? 'opacity:.5' : '') + '" onclick="' + (livre ? 'agendaSelecionarTecnico(' + t.id + ',\'' + t.nome + '\')' : '') + '">' +
      '<div>' +
        '<div style="font-weight:600;font-size:.85rem">' + t.nome + '</div>' +
        '<div style="font-size:.72rem;color:var(--text3)">' + t.pontos_alocados + ' pts alocados · ' + t.pontos_disponiveis + ' disponíveis</div>' +
      '</div>' +
      '<span style="font-size:.72rem;color:' + cor + ';font-weight:600">' + tag + '</span>' +
    '</div>';
  }).join('');

  document.getElementById('modal-agenda').style.display = 'flex';
  document.getElementById('btn-confirmar-rota').dataset.tecnicoId   = '';
  document.getElementById('btn-confirmar-rota').dataset.tecnicoNome = '';
}

function agendaSelecionarTecnico(id, nome) {
  document.querySelectorAll('.tecnico-option').forEach(function(el){ el.style.background=''; });
  event.currentTarget.style.background = 'var(--accent)22';
  document.getElementById('btn-confirmar-rota').dataset.tecnicoId   = id;
  document.getElementById('btn-confirmar-rota').dataset.tecnicoNome = nome;
}

function agendaConfirmar() {
  var btn  = document.getElementById('btn-confirmar-rota');
  var tId  = parseInt(btn.dataset.tecnicoId);
  var tNome= btn.dataset.tecnicoNome;
  var data = document.getElementById('agenda-data').value;

  if (!tId) { alert('Selecione um técnico'); return; }

  var r = agendaRotaSelecionada;
  btn.textContent = 'Salvando...';
  btn.disabled = true;

  fetch('/api/agenda/confirmar', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + TOKEN },
    body: JSON.stringify({
      data_rota:    data,
      tecnico_id:   tId,
      tecnico_nome: tNome,
      os_ids:       r.os,
      pontos:       r.pontos,
      tempo_est:    r.tempo_est,
      bairro_ref:   r.bairro_ref
    })
  }).then(function(res){ return res.json(); }).then(function(d) {
    document.getElementById('modal-agenda').style.display = 'none';
    btn.textContent = '✅ Confirmar';
    btn.disabled = false;
    alert('✅ ' + d.msg);
    agendaGerarRotas(); // refresh
  }).catch(function(e) {
    btn.textContent = '✅ Confirmar';
    btn.disabled = false;
    alert('Erro: ' + e.message);
  });
}

function agendaVerDia() {
  var data = document.getElementById('agenda-data').value;
  if (!data) { alert('Selecione uma data'); return; }

  fetch('/api/agenda/dia?data=' + data, { headers: { Authorization: 'Bearer ' + TOKEN } })
    .then(function(r){ return r.json(); })
    .then(function(d) {
      var el = document.getElementById('agenda-rotas');
      document.getElementById('agenda-kpis').style.display = 'flex';
      document.getElementById('kpi-rotas').textContent = d.total_rotas;
      document.getElementById('kpi-os').textContent    = d.rotas.reduce(function(a,r){ return a + (r.os||[]).length; }, 0);
      document.getElementById('kpi-tecnicos').textContent = '—';

      if (!d.rotas.length) {
        el.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">Nenhuma rota confirmada para esta data.</div>';
        return;
      }

      el.innerHTML = d.rotas.map(function(r) {
        var osList = (r.os||[]).map(function(o, i) {
          return '<div style="display:flex;gap:8px;padding:6px 0;border-bottom:1px solid var(--border);font-size:.78rem">' +
            '<span style="color:var(--text3);min-width:20px">' + o.ordem + '</span>' +
            '<div><div style="font-weight:600">' + (o.cliente_nome||'') + '</div>' +
            '<div style="color:var(--text3)">' + (o.assunto_nome||'') + ' · ' + (o.bairro||'') + '</div></div>' +
          '</div>';
        }).join('');

        return '<div style="background:var(--surf2);border:1px solid var(--border);border-radius:12px;padding:16px">' +
          '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">' +
            '<div><span style="font-weight:800">👷 ' + (r.tecnico_nome||'—') + '</span>' +
            '<span style="color:var(--text3);font-size:.75rem;margin-left:8px">' + r.bairro_ref + '</span></div>' +
            '<div style="display:flex;gap:6px">' +
              '<span style="background:var(--surf);border:1px solid var(--border);border-radius:6px;padding:2px 8px;font-size:.72rem">⚡ ' + r.pontos + ' pts</span>' +
              '<span style="background:var(--green)22;color:var(--green);border-radius:6px;padding:2px 8px;font-size:.72rem;font-weight:700">' + r.status + '</span>' +
            '</div>' +
          '</div>' + osList + '</div>';
      }).join('');
    });
}
"""
    html = html[:pos] + JS_INSERT + html[pos:]
    print("✅ JavaScript inserido")
else:
    print("⚠️  JavaScript já existe")

# ── Salvar ────────────────────────────────────────────────────────────────────
with open(PAINEL, "w", encoding="utf-8") as f:
    f.write(html)

print("\n✅ Etapa 4 concluída — painel.html atualizado")
print("Acesse: http://seu-dominio/painel.html → menu Agenda")
