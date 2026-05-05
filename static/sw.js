const CACHE = 'cliquedf-tec-v2';
const ASSETS = [
  '/',
  '/app',
  '/static/app.html',
  '/static/icons/icon-192x192.png',
  '/static/icons/icon-512x512.png',
];

// Instala e faz cache dos assets estáticos
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(ASSETS)).then(() => self.skipWaiting())
  );
});

// Limpa caches antigos
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

// Estratégia: Network First para API, Cache First para assets
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // API — sempre tenta rede primeiro
  if (url.pathname.startsWith('/api/')) {
    e.respondWith(
      fetch(e.request)
        .catch(() => new Response(JSON.stringify({ erro: 'Sem conexão' }), {
          headers: { 'Content-Type': 'application/json' }
        }))
    );
    return;
  }

  // Assets estáticos — cache first
  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;
      return fetch(e.request).then(resp => {
        if (resp.ok) {
          const clone = resp.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
        }
        return resp;
      });
    })
  );
});

// Recebe mensagem para forçar atualização do cache
self.addEventListener('message', e => {
  if (e.data === 'skipWaiting') self.skipWaiting();
});
