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
import express from 'express';
import { VersionedTransaction, Connection, Keypair } from '@solana/web3.js';
import bs58 from 'bs58';
import 'dotenv/config';

const N8N_WEBHOOK_URL = process.env.N8N_WEBHOOK_URL;
const GAIN_THRESHOLD_PCT = Number(process.env.GAIN_THRESHOLD_PCT || 200);
const PUMPPORTAL_API_KEY = process.env.PUMPPORTAL_API_KEY || '';
const MIN_COMPRAS = Number(process.env.MIN_COMPRAS || 10);

// --- Copiar a otros traders (opcional) ---
// Varias direcciones separadas por comas, ej: "direccion1,direccion2,direccion3"
const TRADER_WALLETS = (process.env.TRADER_WALLETS || '')
  .split(',')
  .map((w) => w.trim())
  .filter((w) => w.length > 0);
const walletsAVigilar = new Set(TRADER_WALLETS);

// --- Configuración para comprar de verdad ---
const RPC_URL = process.env.RPC_URL || ''; // la dirección de Helius
const WALLET_PRIVATE_KEY = process.env.WALLET_PRIVATE_KEY || ''; // tu clave privada, exportada de tu cartera
const BUY_AMOUNT_SOL = Number(process.env.BUY_AMOUNT_SOL || 0.02); // cuánto SOL gastar en cada compra
const SLIPPAGE_PCT = Number(process.env.SLIPPAGE_PCT || 15);
const PRIORITY_FEE_SOL = Number(process.env.PRIORITY_FEE_SOL || 0.0005);

// --- Estrategia de venta (valores de partida, cámbialos cuando quieras) ---
const STOP_LOSS_PCT = Number(process.env.STOP_LOSS_PCT || 15);            // vender si baja esto desde que compraste (antes de despegar)
const TRAILING_ACTIVATION_PCT = Number(process.env.TRAILING_ACTIVATION_PCT || 20); // a partir de aquí, se considera "en marcha"
const TRAILING_STOP_PCT = Number(process.env.TRAILING_STOP_PCT || 20);    // una vez en marcha, vender si cae esto desde su máximo
const MAX_HOLD_MINUTES = Number(process.env.MAX_HOLD_MINUTES || 10);      // solo aplica MIENTRAS no está "en marcha"

// Aquí guardamos las monedas que hemos comprado y aún no hemos vendido
const posicionesAbiertas = new Map();

let connection;
let signerKeypair;
if (RPC_URL && WALLET_PRIVATE_KEY) {
  connection = new Connection(RPC_URL, 'confirmed');
  signerKeypair = Keypair.fromSecretKey(bs58.decode(WALLET_PRIVATE_KEY));
  console.log(`💰 Compra activada. Cartera: ${signerKeypair.publicKey.toBase58()}`);
} else {
  console.log('⚠️  Compra DESACTIVADA (faltan RPC_URL o WALLET_PRIVATE_KEY). El vigilante sigue funcionando igual.');
}

if (!N8N_WEBHOOK_URL) {
  console.error('❌ Falta la variable N8N_WEBHOOK_URL. Configúrala y reinicia.');
  process.exit(1);
}

console.log(`Umbral de subida configurado: ${GAIN_THRESHOLD_PCT}%`);
console.log(`Mínimo de compras exigido: más de ${MIN_COMPRAS}`);
if (walletsAVigilar.size > 0) {
  console.log(`🕵️  Copiando a ${walletsAVigilar.size} wallet(s): ${[...walletsAVigilar].join(', ')}`);
} else {
  console.log('🕵️  Copia de traders DESACTIVADA (no hay ninguna wallet en TRADER_WALLETS).');
}
console.log(`Venta: stop loss -${STOP_LOSS_PCT}% / trailing desde +${TRAILING_ACTIVATION_PCT}% cayendo -${TRAILING_STOP_PCT}% desde el máximo / tiempo máx ${MAX_HOLD_MINUTES} min (solo antes de despegar)`);

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
    if (walletsAVigilar.size > 0) {
      ws.send(JSON.stringify({ method: 'subscribeAccountTrade', keys: [...walletsAVigilar] }));
    }
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
  // ¿Es una compra de una de las wallets que estamos copiando? Avisamos a n8n aparte,
  // sea o no una moneda que ya conocíamos.
  if (evento.txType === 'buy' && walletsAVigilar.has(evento.traderPublicKey)) {
    console.log(`🕵️  Wallet copiada ha comprado ${evento.mint}. Avisando a n8n...`);
    enviarAN8n({
      event_type: 'trader_compra',
      mint: evento.mint,
      wallet: evento.traderPublicKey,
      sol_gastado: evento.solAmount,
      market_cap_sol: evento.marketCapSol,
    });
  }

  // Si esta moneda es una posición que hemos comprado, comprobamos si toca vender
  const posicion = posicionesAbiertas.get(evento.mint);
  if (posicion && !posicion.vendiendo && typeof evento.marketCapSol === 'number') {
    comprobarSalida(evento.mint, posicion, evento.marketCapSol);
  }

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

async function comprarToken(mint) {
  if (!connection || !signerKeypair) {
    throw new Error('La compra no está activada (faltan RPC_URL o WALLET_PRIVATE_KEY)');
  }

  // 1. Le pedimos a PumpPortal la orden de compra, sin firmar todavía
  const respuesta = await fetch('https://pumpportal.fun/api/trade-local', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      publicKey: signerKeypair.publicKey.toBase58(),
      action: 'buy',
      mint,
      denominatedInSol: 'true',
      amount: BUY_AMOUNT_SOL,
      slippage: SLIPPAGE_PCT,
      priorityFee: PRIORITY_FEE_SOL,
      pool: 'auto',
    }),
  });

  if (!respuesta.ok) {
    const texto = await respuesta.text();
    throw new Error(`PumpPortal rechazó la orden: ${texto}`);
  }

  // 2. La firmamos nosotros mismos, con nuestra propia clave (nunca sale de aquí)
  const bufferTx = await respuesta.arrayBuffer();
  const tx = VersionedTransaction.deserialize(new Uint8Array(bufferTx));
  tx.sign([signerKeypair]);

  // 3. La enviamos a la red de Solana a través de Helius
  const firma = await connection.sendTransaction(tx);

  // 3b. Esperamos a que la red confirme que de verdad se ejecutó (no solo que se envió)
  const confirmacion = await connection.confirmTransaction(firma, 'confirmed');
  if (confirmacion.value.err) {
    throw new Error(`La compra se envió pero la red la rechazó: ${JSON.stringify(confirmacion.value.err)}`);
  }
  console.log(`✅ Compra confirmada para ${mint}. Firma: ${firma}`);

  // 4. Registramos la posición para poder decidir cuándo vender,
  //    y nos aseguramos de seguir recibiendo sus trades (por si ya la habíamos dejado de vigilar)
  const monedaConocida = monedasVigiladas.get(mint);
  posicionesAbiertas.set(mint, {
    entradaMcap: monedaConocida ? monedaConocida.marketCapActual : null,
    mcapMaximoAlcanzado: monedaConocida ? monedaConocida.marketCapActual : null,
    ultimoMcapConocido: monedaConocida ? monedaConocida.marketCapActual : null,
    horaCompra: Date.now(),
    vendiendo: false,
  });
  try {
    ws.send(JSON.stringify({ method: 'subscribeTokenTrade', keys: [mint] }));
  } catch (_) {}

  return firma;
}

async function venderToken(mint) {
  if (!connection || !signerKeypair) {
    throw new Error('La venta no está activada (faltan RPC_URL o WALLET_PRIVATE_KEY)');
  }

  const respuesta = await fetch('https://pumpportal.fun/api/trade-local', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      publicKey: signerKeypair.publicKey.toBase58(),
      action: 'sell',
      mint,
      denominatedInSol: 'false',
      amount: '100%', // vendemos todo lo que tengamos de esta moneda
      slippage: SLIPPAGE_PCT,
      priorityFee: PRIORITY_FEE_SOL,
      pool: 'auto',
    }),
  });

  if (!respuesta.ok) {
    const texto = await respuesta.text();
    throw new Error(`PumpPortal rechazó la venta: ${texto}`);
  }

  const bufferTx = await respuesta.arrayBuffer();
  const tx = VersionedTransaction.deserialize(new Uint8Array(bufferTx));
  tx.sign([signerKeypair]);

  const firma = await connection.sendTransaction(tx);
  const confirmacion = await connection.confirmTransaction(firma, 'confirmed');
  if (confirmacion.value.err) {
    throw new Error(`La venta se envió pero la red la rechazó: ${JSON.stringify(confirmacion.value.err)}`);
  }
  console.log(`✅ Venta confirmada para ${mint}. Firma: ${firma}`);
  return firma;
}

function comprobarSalida(mint, posicion, marketCapActual) {
  if (marketCapActual == null) return; // todavía no tenemos ningún precio para esta posición
  if (posicion.entradaMcap == null) {
    posicion.entradaMcap = marketCapActual; // no teníamos precio de referencia, usamos el primero que llega
    posicion.mcapMaximoAlcanzado = marketCapActual;
  }
  posicion.ultimoMcapConocido = marketCapActual;
  if (marketCapActual > posicion.mcapMaximoAlcanzado) {
    posicion.mcapMaximoAlcanzado = marketCapActual;
  }

  const cambioDesdeEntrada = ((marketCapActual - posicion.entradaMcap) / posicion.entradaMcap) * 100;
  const cambioDesdeElMaximo = ((marketCapActual - posicion.mcapMaximoAlcanzado) / posicion.mcapMaximoAlcanzado) * 100;
  const gananciaMaximaAlcanzada = ((posicion.mcapMaximoAlcanzado - posicion.entradaMcap) / posicion.entradaMcap) * 100;
  const minutosEnPosicion = (Date.now() - posicion.horaCompra) / 60000;

  let motivo = null;

  if (gananciaMaximaAlcanzada >= TRAILING_ACTIVATION_PCT) {
    // Ya ha demostrado ser una ganadora: dejamos que corra, solo protegemos el máximo alcanzado.
    // Sin límite de tiempo aquí a propósito, para no cortar una subida grande a medias.
    if (cambioDesdeElMaximo <= -TRAILING_STOP_PCT) {
      motivo = `trailing stop (bajó ${Math.abs(cambioDesdeElMaximo).toFixed(0)}% desde el máximo, resultado final +${cambioDesdeEntrada.toFixed(0)}%)`;
    }
  } else {
    // Todavía no ha despegado: protegemos con stop loss normal y con el tiempo máximo
    if (cambioDesdeEntrada <= -STOP_LOSS_PCT) {
      motivo = `stop loss (${cambioDesdeEntrada.toFixed(0)}%)`;
    } else if (minutosEnPosicion >= MAX_HOLD_MINUTES) {
      motivo = `tiempo máximo sin despegar (${minutosEnPosicion.toFixed(0)} min, ${cambioDesdeEntrada.toFixed(0)}%)`;
    }
  }

  if (!motivo) return;

  posicion.vendiendo = true; // evita vender dos veces si llegan varios trades seguidos
  console.log(`💸 Vendiendo ${mint} por ${motivo}...`);

  venderToken(mint)
    .then((firma) => {
      posicionesAbiertas.delete(mint);
      enviarAN8n({
        event_type: 'venta_ejecutada',
        mint,
        motivo,
        cambioPct: cambioDesdeEntrada,
        signature: firma,
      });
    })
    .catch((err) => {
      // Este error significa que en realidad no tenemos nada de esta moneda
      // (la compra nunca llegó a confirmarse de verdad). No tiene sentido reintentar.
      const esPosicionFantasma = err.message.includes('SellZeroAmount') || err.message.includes('0x1786');
      if (esPosicionFantasma) {
        console.error(`⚠️  ${mint}: la compra nunca se confirmó (no hay nada que vender). Se descarta esta posición.`);
        posicionesAbiertas.delete(mint);
        return;
      }

      posicion.intentosVenta = (posicion.intentosVenta || 0) + 1;
      console.error(`❌ Error vendiendo ${mint} (intento ${posicion.intentosVenta}/5):`, err.message);

      if (posicion.intentosVenta >= 5) {
        console.error(`⚠️  ${mint}: han fallado 5 intentos de venta seguidos. Se abandona esta posición.`);
        posicionesAbiertas.delete(mint);
        return;
      }

      posicion.vendiendo = false; // lo intentaremos otra vez en el siguiente trade
    });
}

// Red de seguridad: comprobamos el tiempo máximo aunque no lleguen más trades
// (por ejemplo, si una moneda deja de tener actividad después de comprarla)
setInterval(() => {
  for (const [mint, posicion] of posicionesAbiertas) {
    if (!posicion.vendiendo) {
      comprobarSalida(mint, posicion, posicion.ultimoMcapConocido);
    }
  }
}, 60 * 1000);

// --- Servidor pequeño: n8n llama aquí cuando decide comprar ---
const app = express();
app.use(express.json());

app.post('/comprar', async (req, res) => {
  const { mint } = req.body || {};
  if (!mint) {
    return res.status(400).json({ ok: false, error: 'Falta el mint en la petición' });
  }
  try {
    const firma = await comprarToken(mint);
    res.json({ ok: true, signature: firma, url: `https://solscan.io/tx/${firma}` });
  } catch (err) {
    console.error('❌ Error al comprar:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/vender', async (req, res) => {
  const { mint } = req.body || {};
  if (!mint) {
    return res.status(400).json({ ok: false, error: 'Falta el mint en la petición' });
  }
  try {
    const firma = await venderToken(mint);
    posicionesAbiertas.delete(mint);
    res.json({ ok: true, signature: firma, url: `https://solscan.io/tx/${firma}` });
  } catch (err) {
    console.error('❌ Error al vender:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/salud', (req, res) => res.json({
  ok: true,
  compraActivada: !!signerKeypair,
  posicionesAbiertas: posicionesAbiertas.size,
}));

const PUERTO = process.env.PORT || 3000;
app.listen(PUERTO, () => console.log(`🌐 Servidor de compras escuchando en el puerto ${PUERTO}`));

conectar();
