#!/usr/bin/env python3
"""
Adiciona suporte PWA ao app.html do HubTecnico
Execute: python3 patch_pwa.py
"""

PATH = '/opt/automacoes/cliquedf/tecnico/static/app.html'

with open(PATH, 'r') as f:
    content = f.read()

# 1. Adicionar manifest e meta tags no <head>
old_head = '<meta name="theme-color" content="#0d0f14">'
new_head = '''<meta name="theme-color" content="#0d0f14">
<link rel="manifest" href="/static/manifest.json">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="CliqueDf Técnico">
<link rel="apple-touch-icon" href="/static/icons/icon-192x192.png">'''

# 2. Adicionar registro do SW antes do </body>
sw_script = '''
<script>
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/static/sw.js')
      .then(reg => {
        console.log('[PWA] SW registrado:', reg.scope);
        // Notifica atualização disponível
        reg.addEventListener('updatefound', () => {
          const newSW = reg.installing;
          newSW.addEventListener('statechange', () => {
            if (newSW.state === 'installed' && navigator.serviceWorker.controller) {
              showToast('🔄 Atualização disponível! Feche e abra o app.', 'ok');
            }
          });
        });
      })
      .catch(err => console.warn('[PWA] SW erro:', err));
  });
}

// Banner de instalação Android
let deferredPrompt;
window.addEventListener('beforeinstallprompt', e => {
  e.preventDefault();
  deferredPrompt = e;
  // Mostra botão de instalar se não estiver instalado
  if (!window.matchMedia('(display-mode: standalone)').matches) {
    setTimeout(() => {
      const banner = document.createElement('div');
      banner.id = 'pwa-banner';
      banner.style.cssText = 'position:fixed;bottom:90px;left:50%;transform:translateX(-50%);background:#1c2230;border:1px solid #e8521a;border-radius:12px;padding:12px 16px;z-index:9999;display:flex;align-items:center;gap:12px;font-size:.85rem;box-shadow:0 4px 20px rgba(0,0,0,.5);max-width:320px;width:90%';
      banner.innerHTML = '<img src="/static/icons/icon-72x72.png" style="width:36px;height:36px;border-radius:8px"><div style="flex:1"><div style="font-weight:700;color:#e8eaf0">Instalar CliqueDf Técnico</div><div style="color:#6b7590;font-size:.75rem">Adicionar à tela inicial</div></div><button onclick="instalarPWA()" style="background:#e8521a;color:#fff;border:none;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.8rem">Instalar</button><button onclick="document.getElementById(\'pwa-banner\').remove()" style="background:transparent;border:none;color:#6b7590;cursor:pointer;padding:4px;font-size:1.2rem">✕</button>';
      document.body.appendChild(banner);
    }, 3000);
  }
});

function instalarPWA() {
  if (!deferredPrompt) return;
  deferredPrompt.prompt();
  deferredPrompt.userChoice.then(r => {
    if (r.outcome === 'accepted') showToast('✅ App instalado!', 'ok');
    deferredPrompt = null;
    const b = document.getElementById('pwa-banner');
    if (b) b.remove();
  });
}
</script>
</body>'''

if old_head in content:
    content = content.replace(old_head, new_head)
    print("OK: manifest e meta tags adicionados")
else:
    print("ERRO: meta theme-color não encontrado")

if '</body>' in content:
    content = content.replace('</body>', sw_script)
    print("OK: service worker registrado")
else:
    print("ERRO: </body> não encontrado")

with open(PATH, 'w') as f:
    f.write(content)

print("\nPWA aplicado com sucesso!")
print("Próximos passos:")
print("  1. Copiar icons para /opt/automacoes/cliquedf/tecnico/static/icons/")
print("  2. Copiar sw.js para /opt/automacoes/cliquedf/tecnico/static/")
print("  3. Copiar manifest.json para /opt/automacoes/cliquedf/tecnico/static/")
print("  4. Adicionar rota /static/sw.js e /static/manifest.json no main.py")
