const CACHE = 'hubtecnico-v1';
const ASSETS = ['/', '/api/os/minhas'];

// Instalar — cachear assets estáticos
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(['/']))
  );
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys =>
    Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
  ));
  self.clients.claim();
});

// Fetch — estratégia por tipo de request
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // GET /api/os/minhas — cache first, atualiza em background
  if (url.pathname === '/api/os/minhas' && e.request.method === 'GET') {
    e.respondWith(
      caches.open(CACHE).then(async cache => {
        const cached = await cache.match(e.request);
        const fetchPromise = fetch(e.request).then(resp => {
          if (resp.ok) cache.put(e.request, resp.clone());
          return resp;
        }).catch(() => null);
        return cached || fetchPromise;
      })
    );
    return;
  }

  // POST requests — salvar em fila se offline
  if (e.request.method === 'POST') {
    e.respondWith(
      fetch(e.request.clone()).catch(async () => {
        // Offline: salvar na fila do IndexedDB via mensagem
        const body = await e.request.clone().text();
        const clients = await self.clients.matchAll();
        clients.forEach(c => c.postMessage({
          type: 'QUEUE_REQUEST',
          url: url.pathname,
          body,
          method: 'POST'
        }));
        return new Response(JSON.stringify({ok:true, offline:true}), {
          headers: {'Content-Type':'application/json'}
        });
      })
    );
    return;
  }

  // App shell — cache first
  if (url.pathname === '/' || url.pathname.startsWith('/static')) {
    e.respondWith(
      caches.match(e.request).then(r => r || fetch(e.request))
    );
    return;
  }

  // Default — network first
  e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
});

// Sincronizar fila quando voltar online
self.addEventListener('sync', e => {
  if (e.tag === 'sync-queue') {
    e.waitUntil(syncQueue());
  }
});

async function syncQueue() {
  const clients = await self.clients.matchAll();
  clients.forEach(c => c.postMessage({type: 'SYNC_NOW'}));
}
