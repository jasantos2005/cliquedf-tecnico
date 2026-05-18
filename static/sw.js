// SW v7 - força desregistro e atualização
const CACHE = 'cliquedf-tec-v7';

self.addEventListener('install', e => {
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

// Sem cache - sempre busca do servidor
self.addEventListener('fetch', e => {
  e.respondWith(fetch(e.request));
});
