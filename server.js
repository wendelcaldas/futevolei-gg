// Servidor estático do FUTEVOLEI.GG (sem dependências). Mais tarde recebe o WebSocket do modo online.
const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

const PORT = process.env.PORT || 3000;
const ROOT = path.join(__dirname, 'public');
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.glb': 'model/gltf-binary', '.json': 'application/json', '.svg': 'image/svg+xml', '.ico': 'image/x-icon' };
const cache = new Map();

function load(file) {
  if (!cache.has(file)) {
    const raw = fs.readFileSync(file);
    cache.set(file, { raw, gz: zlib.gzipSync(raw, { level: 9 }), br: zlib.brotliCompressSync(raw) });
  }
  return cache.get(file);
}

http.createServer((req, res) => {
  const url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  let file = path.normalize(path.join(ROOT, url === '/' ? 'index.html' : url));
  if (!file.startsWith(ROOT) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) file = path.join(ROOT, 'index.html');
  const body = load(file);
  const ae = req.headers['accept-encoding'] || '';
  const headers = { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream', 'Cache-Control': file.endsWith('.html') ? 'no-cache' : 'public, max-age=86400', 'Vary': 'Accept-Encoding' };
  let data = body.raw;
  if (/\bbr\b/.test(ae)) { headers['Content-Encoding'] = 'br'; data = body.br; }
  else if (/\bgzip\b/.test(ae)) { headers['Content-Encoding'] = 'gzip'; data = body.gz; }
  headers['Content-Length'] = data.length;
  res.writeHead(200, headers);
  res.end(req.method === 'HEAD' ? undefined : data);
}).listen(PORT, () => console.log(`FUTEVOLEI.GG no ar na porta ${PORT}`));
