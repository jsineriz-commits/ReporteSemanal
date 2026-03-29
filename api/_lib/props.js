// api/_lib/props.js
// Reemplaza PropertiesService.getScriptProperties() de Apps Script.
// Persiste en memoria (se pierde en cold start — aceptable para R12_VER y SSGN).

const _store = new Map();

function setProp(key, val) { _store.set(key, String(val)); }
function getProp(key) { return _store.has(key) ? _store.get(key) : null; }
function delProp(key) { _store.delete(key); }
function clearProps() { _store.clear(); }

// R12_VER: versión del cache de reportes.
// Inicia con Date.now() en cada cold start, lo que invalida reportes viejos.
let _r12ver = String(Date.now());

function getR12Ver() { return _r12ver; }
function resetR12Ver() { _r12ver = String(Date.now()); }

module.exports = { setProp, getProp, delProp, clearProps, getR12Ver, resetR12Ver };
