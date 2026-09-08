// ============================================================
// VIGILANTE PUMP.FUN → N8N
// ------------------------------------------------------------
// Este programa hace 2 cosas:
// 1. Escucha las monedas nuevas de PumpPortal y avisa a tu n8n
//    (esto ya lo tenías, aquí solo se reenvía tal cual).
// 2. De las monedas que NO son mayhem, vigila sus compras/ventas
//    y, en cuanto suben el % que hayas configurado, avisa a n8n
//    con todos los datos que necesita tu rama "momentum_trigger".
// ============================================================

import WebSocket from 'ws';
import 'dotenv/config';

const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL;
const GAIN_THRESHOLD_PCT = Number(process.env.GAIN_THRESHOLD_PCT || 200);
const PUMPPORTAL_API_KEY = process.env.PUMPPORTAL_API_KEY || '';
const MIN_COMPRAS = Number(process.env.MIN_COMPRAS || 10);

if (!N8N_WEBHOOK_URL) {
  console.error('❌ Falta la variable N8N_WEBHOOK_URL. Configúrala y reinicia.');
  process.exit(1);
}

console.log(`Umbral de subida configurado: ${GAIN_THRESHOLD_PCT}%`);
console.log(`Mínimo de compras exigido: más de ${MIN_COMPRAS}`);

// Aquí guardamos, en memoria, los datos de cada moneda que estamos vigilando.
// Cuando el programa se reinicia, esta lista se vacía (es normal).
const monedasVigiladas = new Map();

let ws;

function conectar() {
  const url = PUMPPORTAL_API_KEY
    ? `wss://pumpportal.fun/api/data?api-key=${PUMPPORTAL_API_KEY}`
    : 'wss://pumpportal.fun/api/data';
  ws = new WebSocket(url);

  ws.on('open', () => {
    console.log('✅ Conectado a PumpPortal. Escuchando monedas nuevas...');
    ws.send(JSON.stringify({ method: 'subscribeNewToken' }));
  });

  ws.on('message', (data) => {
    let evento;
    try {
      evento = JSON.parse(data.toString());
    } catch (err) {
      return; // mensaje que no es JSON válido, lo ignoramos
    }
    manejarEvento(evento);
  });

  ws.on('close', () => {
    console.log('⚠️ Conexión cerrada. Reintentando en 5 segundos...');
    setTimeout(conectar, 5000);
  });

  ws.on('error', (err) => {
    console.error('❌ Error de conexión:', err.message);
  });
}

let contadorEventosDesconocidos = 0;

function manejarEvento(evento) {
  if (evento.txType === 'create') {
    manejarCreacion(evento);
  } else if (evento.txType === 'buy' || evento.txType === 'sell') {
    manejarTrade(evento);
  } else if (contadorEventosDesconocidos < 15) {
    // Modo diagnóstico: mostramos los primeros 15 eventos "raros" para ver
    // cómo son de verdad los datos que manda PumpPortal.
    contadorEventosDesconocidos++;
    console.log('❓ Evento no reconocido:', JSON.stringify(evento).slice(0, 300));
  }
}

async function manejarCreacion(evento) {
  // 1. Avisamos SIEMPRE a n8n de la moneda nueva (tu IF de mayhem decide qué hacer)
  await enviarAN8n(evento);

  // 2. Si es mayhem, no la vigilamos más (ahorra datos y evita señales falsas)
  if (evento.is_mayhem_mode) {
    console.log(`⏭️  ${evento.symbol} es mayhem, no se vigila su subida.`);
    return;
  }

  // 3. Guardamos sus datos iniciales y empezamos a vigilar sus compras/ventas
  monedasVigiladas.set(evento.mint, {
    nombre: evento.name,
    simbolo: evento.symbol,
    creador: evento.traderPublicKey,
    marketCapInicial: evento.marketCapSol,
    marketCapActual: evento.marketCapSol,
    compras: 0,
    ventas: 0,
    compradores: new Set(),
    vendedores: new Set(),
    volumenComprasSol: 0,
    volumenVentasSol: 0,
    volumenPorComprador: new Map(),
    creadorVendio: false,
    avisada: false,
    creadoEn: Date.now(),
  });

  ws.send(JSON.stringify({ method: 'subscribeTokenTrade', keys: [evento.mint] }));
  console.log(`👀 Vigilando: ${evento.symbol} (market cap inicial: ${evento.marketCapSol} SOL)`);
}

function manejarTrade(evento) {
  const moneda = monedasVigiladas.get(evento.mint);
  if (!moneda || moneda.avisada) return;

  if (typeof evento.marketCapSol === 'number') {
    moneda.marketCapActual = evento.marketCapSol;
  }

  if (evento.txType === 'buy') {
    moneda.compras++;
    moneda.compradores.add(evento.traderPublicKey);
    moneda.volumenComprasSol += evento.solAmount || 0;
    const previo = moneda.volumenPorComprador.get(evento.traderPublicKey) || 0;
    moneda.volumenPorComprador.set(evento.traderPublicKey, previo + (evento.solAmount || 0));
  } else if (evento.txType === 'sell') {
    moneda.ventas++;
    moneda.vendedores.add(evento.traderPublicKey);
    moneda.volumenVentasSol += evento.solAmount || 0;
    if (evento.traderPublicKey === moneda.creador) {
      moneda.creadorVendio = true;
    }
  }

  const subidaPorcentaje =
    ((moneda.marketCapActual - moneda.marketCapInicial) / moneda.marketCapInicial) * 100;

  // Log de progreso: para ver en los Registros que las monedas se están moviendo de verdad
  console.log(`📊 ${moneda.simbolo}: ${subidaPorcentaje.toFixed(1)}% (mcap actual ${moneda.marketCapActual.toFixed(2)} SOL, inicial ${moneda.marketCapInicial.toFixed(2)} SOL)`);

  if (subidaPorcentaje >= GAIN_THRESHOLD_PCT && moneda.compras > MIN_COMPRAS) {
    moneda.avisada = true; // para no avisar dos veces de la misma moneda
    avisarMomentum(evento.mint, moneda, subidaPorcentaje);

    // Dejamos de vigilarla (si el método no existe en la API no pasa nada, se ignora)
    try {
      ws.send(JSON.stringify({ method: 'unsubscribeTokenTrade', keys: [evento.mint] }));
    } catch (_) {}
  }
}

async function enviarAN8n(payload) {
  try {
    await fetch(N8N_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    console.error('❌ Error avisando a n8n:', err.message);
  }
}

function avisarMomentum(mint, moneda, subidaPorcentaje) {
  const mayorComprador = moneda.volumenPorComprador.size
    ? Math.max(...moneda.volumenPorComprador.values())
    : 0;
  const mayorCompradorPct = moneda.volumenComprasSol > 0
    ? (mayorComprador / moneda.volumenComprasSol) * 100
    : 0;

  const payload = {
    event_type: 'momentum_trigger',
    mint,
    name: moneda.nombre,
    symbol: moneda.simbolo,
    creator: moneda.creador,
    initialMarketCapSol: moneda.marketCapInicial,
    currentMarketCapSol: moneda.marketCapActual,
    gainPct: subidaPorcentaje,
    multiple: moneda.marketCapActual / moneda.marketCapInicial,
    buyCount: moneda.compras,
    sellCount: moneda.ventas,
    uniqueBuyers: moneda.compradores.size,
    uniqueSellers: moneda.vendedores.size,
    buyVolumeSol: moneda.volumenComprasSol,
    sellVolumeSol: moneda.volumenVentasSol,
    buySellRatio: moneda.ventas > 0 ? moneda.compras / moneda.ventas : moneda.compras,
    topBuyerSharePct: mayorCompradorPct,
    creatorSold: moneda.creadorVendio,
  };

  console.log(`🚀 ${moneda.simbolo} ha subido ${subidaPorcentaje.toFixed(0)}%. Avisando a n8n...`);
  enviarAN8n(payload);
}

// Limpieza: cada 30 min borramos monedas que llevan más de 1h sin llegar al umbral,
// para que la memoria del programa no crezca sin límite.
setInterval(() => {
  const ahora = Date.now();
  for (const [mint, moneda] of monedasVigiladas) {
    if (ahora - moneda.creadoEn > 60 * 60 * 1000) {
      monedasVigiladas.delete(mint);
    }
  }
}, 30 * 60 * 1000);

conectar();
