require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const fs = require('fs');

// === CONFIGURACIONES GLOBALES Y UMBRALES ===
const token = process.env.TELEGRAM_TOKEN;
const chatId = process.env.CHAT_ID;
const bot = new TelegramBot(token, { polling: true });

const patternLength = 6;
let BASE_PROB_THRESHOLD = 54; 
let MIN_AI_SCORE = 65; 
let lastSignalTime = 0;
const SIGNAL_COOLDOWN = 60 * 1000; 

const CONFIG = {
    BE: 54.94,
    BINANCE_API: 'https://api.binance.com/api/v3',
    BACKEND_URL: 'https://quant-backend-lhue.onrender.com/api', 
    REQUEST_LIMIT: 50000,
    MARKETS: [
        { id: 'BTC/USD', symbolBinance: 'BTCUSDT' }, 
        { id: 'ETH/USD', symbolBinance: 'ETHUSDT' },
        { id: 'ADA/USD', symbolBinance: 'ADAUSDT' }, 
        { id: 'SOL/USD', symbolBinance: 'SOLUSDT' },
        { id: 'XRP/USD', symbolBinance: 'XRPUSDT' },
        { id: 'DOGE/USD', symbolBinance: 'DOGEUSDT' },
        { id: 'BNB/USD', symbolBinance: 'BNBUSDT' }
    ]
};

// 🧠 PERFILES POR ACTIVO (Basado en el análisis de tu CSV)
const ASSET_PROFILES = {
    "ETH/USD": { minScoreMultiplier: 1.3, strictLOB: true, aiPenalty: -5 }, // ETH falla mucho, somos ultra estrictos
    "BTC/USD": { minScoreMultiplier: 1.1, strictLOB: false, aiPenalty: 0 }, 
    "DOGE/USD": { minScoreMultiplier: 0.9, strictLOB: false, aiPenalty: 5 }, // DOGE funciona bien, flexibilidad
    "XRP/USD": { minScoreMultiplier: 0.9, strictLOB: false, aiPenalty: 5 },  // XRP funciona bien, flexibilidad
    "SOL/USD": { minScoreMultiplier: 1.0, strictLOB: false, aiPenalty: 0 },
    "ADA/USD": { minScoreMultiplier: 1.1, strictLOB: true, aiPenalty: 0 },
    "BNB/USD": { minScoreMultiplier: 1.1, strictLOB: false, aiPenalty: 0 }
};

const DEFAULT_ZONES = {
    "BTC/USD": { prob: {min: 62, max: 66}, lob: {min: -0.60, max: -0.20}, alpha: {min: 0.025, max: 0.045} },
    "ETH/USD": { prob: {min: 63, max: 70}, lob: {min: -0.20, max: 0.20}, alpha: {min: 0.030, max: 0.060} },
    "XRP/USD": { prob: {min: 60, max: 64}, lob: {min: -0.30, max: -0.05}, alpha: {min: 0.025, max: 0.045} },
    "SOL/USD": { prob: {min: 63, max: 67}, lob: {min: -0.15, max: 0.15}, alpha: {min: 0.030, max: 0.050} },
    "ADA/USD": { prob: {min: 63, max: 68}, lob: {min: -0.30, max: -0.05}, alpha: {min: 0.025, max: 0.045} },
    "DOGE/USD": { prob: {min: 64, max: 72}, lob: {min: 0.10, max: 0.50}, alpha: {min: 0.040, max: 0.080} },
    "BNB/USD": { prob: {min: 62, max: 68}, lob: {min: -0.50, max: 0.50}, alpha: {min: 0.030, max: 0.060} }
};

let CURRENT_WINNING_ZONES = JSON.parse(JSON.stringify(DEFAULT_ZONES));
const SIGNAL_CACHE = new Map();
let signalCounter = 0;
const PENDING_AUDITS = [];

let globalPauseUntil = 0; 
let circuitBreakerLevel = 0;
let lastCircuitBreakerTradeCount = 0;
let autoLearningStats = { winrate: 0.5, total: 0 }; 
let assetPerformance = {}; 
const GLOBAL_CANDLE_CACHE = new Map(); 

// === SINCRONIZACIÓN DE TIEMPO ===
let timeOffset = 0;
async function syncTimeWithBinance() {
    try {
        const res = await axios.get(`${CONFIG.BINANCE_API}/time`);
        timeOffset = res.data.serverTime - Date.now();
        console.log(`[SYS] Reloj sincronizado con Binance. Offset: ${timeOffset}ms`);
    } catch(e) {
        console.error("[SYS] Error al sincronizar reloj. Usando hora local.");
    }
}
syncTimeWithBinance();
setInterval(syncTimeWithBinance, 60 * 60 * 1000); 

function getSyncedTime() { return Date.now() + timeOffset; }
function getLocalTime() { return new Date(getSyncedTime()).toLocaleTimeString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour12: false }); }

function logToCSV(data) {
    const filePath = './auditoria_sniper.csv';
    const now = new Date(getSyncedTime());
    const fecha = now.toLocaleDateString('es-AR');
    const hora = now.toLocaleTimeString('es-AR', { hour12: false });

    if (!fs.existsSync(filePath)) {
        const header = 'Fecha,Hora,Activo,TF,Dir,Prob,LOB,Edge,Alpha,Stability,CWEV,Samples,Veredicto,Open,Close,IA_Verdict,IA_Score,IA_Context,Mode,FinalScore\n';
        fs.writeFileSync(filePath, header);
    }
    const row = `${fecha},${hora},${data.asset || ''},${data.tf || ''},${data.dir || ''},${data.prob || ''},${data.lob || ''},${data.edge || ''},${data.alpha || ''},${data.stab || ''},${data.cwev || ''},${data.samples || ''},${data.veredicto || ''},${data.open || ''},${data.close || ''},${data.iaVerdict || ''},${data.iaScore || ''},${data.iaContext || ''},${data.mode || ''},${data.finalScore || ''}\n`;
    fs.appendFileSync(filePath, row);
}

// === FUNCIONES DE TRADING Y SCORE ===
function formatPrice(val) {
    if (val < 0.01) return val.toFixed(6);
    if (val < 1) return val.toFixed(4);
    if (val < 100) return val.toFixed(3);
    return val.toFixed(2);
}

// 🚀 SCORE FINAL REAL (Unificado + Multiplicador de Activo)
function calculateFinalScore(analysis, assetId) {
    const { edge, stability, n, prob } = analysis;
    const sampleFactor = Math.log(n + 1) / 5; 
    const probFactor = (prob - 50) / 50; 
    let baseScore = (Math.abs(edge) * stability * sampleFactor * probFactor);
    const multiplier = ASSET_PROFILES[assetId] ? ASSET_PROFILES[assetId].minScoreMultiplier : 1.0;
    return baseScore / multiplier; 
}

// 💰 MÓDULO TRADING (Ahora independiente y primario)
function calculateTradeLevels(price, direction, atr, edge, cbLevel, stability, iaContext = null, iaScore = null) {
    const capital = 1000; 
    const leverage = 20;  
    
    let riskPercent = (cbLevel > 0 || autoLearningStats.winrate < 0.45) ? 1.0 : 2.0; 
    if (iaScore !== null && iaScore < 72) riskPercent *= 0.75; 

    const maxRiskPercent = 0.5; 
    let slDistance = atr * 1.2;
    const maxDistance = price * (maxRiskPercent / 100);
    if (slDistance > maxDistance) slDistance = maxDistance;

    let rr = Math.max(1.2, Math.abs(edge) / 2);
    if (iaContext === "CONTINUATION") rr *= 1.4;
    if (iaContext === "REVERSAL") rr *= 0.7;
    if (stability > 0.7) rr *= 1.2;
    if (stability < 0.4) rr *= 0.85;

    let entry = price;
    let sl = direction === "BUY" ? entry - slDistance : entry + slDistance;
    let tp = direction === "BUY" ? entry + (slDistance * rr) : entry - (slDistance * rr);

    if (Math.abs(tp - entry) < price * 0.002) return null; 

    const liquidationDistance = 1 / leverage;
    if ((slDistance / price) > (liquidationDistance * 0.7)) return null;

    const riskAmount = capital * (riskPercent / 100);
    const priceDiff = Math.abs(entry - sl);
    const positionSizeUSDT = (riskAmount / priceDiff) * entry;

    return {
        entry: formatPrice(entry),
        sl: formatPrice(sl),
        tp: formatPrice(tp),
        rr: rr.toFixed(1),
        positionSize: positionSizeUSDT.toFixed(0),
        riskPercent: riskPercent.toFixed(1)
    };
}

// 🧬 THRESHOLD Y CRON DINÁMICOS
function getDynamicThreshold(atr, price) {
    if (!atr || !price) return BASE_PROB_THRESHOLD;
    const volatility = atr / price;
    let threshold = BASE_PROB_THRESHOLD;
    if (volatility > 0.004) threshold = 56;
    else if (volatility < 0.0015) threshold = 52;
    if (autoLearningStats.total >= 20 && autoLearningStats.winrate < 0.48) threshold += 1.5;
    return threshold;
}

// === COMANDOS RESTAURADOS ===
bot.onText(/\/start/, (msg) => {
    if (msg.chat.id.toString() === chatId) {
        bot.sendMessage(chatId, '👋 ¡Hola Ser! El *Quant Sniper V12.5 TACTICAL PRO* está operativo.', { parse_mode: 'Markdown' });
    }
});

bot.onText(/\/scan/, async (msg) => {
    if (msg.chat.id.toString() === chatId) {
        await globalScan('manual');
    }
});

console.log(`🤖 Quant Sniper V12.5 TACTICAL PRO iniciando...`);
bot.sendMessage(chatId, '🟢 *Quant Sniper V12.5 TACTICAL PRO* encendido.\nModo: Trading Previo + IA a Demanda + Perfiles de Activo', { parse_mode: 'Markdown' });

// === MOTOR CUANTITATIVO RESTAURADO ===
function calculateZScore(values) {
    if(values.length === 0) return 0;
    const mean = values.reduce((a,b)=>a+b, 0) / values.length;
    const std = Math.sqrt(values.reduce((s,v)=>s+Math.pow(v-mean, 2), 0) / values.length) || 1;
    return (values[values.length-1] - mean) / std;
}

function marketEntropy(candles) {
    if(candles.length === 0) return 0;
    let up = 0, down = 0;
    for(let c of candles) {
        if(c.c > c.o) up++;
        else down++;
    }
    if(up === 0 || down === 0) return 0; 
    const pUp = up / (up+down);
    const pDown = down / (up+down);
    return -(pUp * Math.log2(pUp) + pDown * Math.log2(pDown));
}

function detectRegime(candles){
    const closes = candles.slice(-50).map(c => c.c);
    const max = Math.max(...closes);
    const min = Math.min(...closes);
    const range = (max - min) / (min || 1);
    const trend = (closes[closes.length-1] - closes[0]) / (closes[0] || 1);
    if(Math.abs(trend) > 0.03) return "TREND";
    if(range < 0.015) return "COMPRESSION";
    return "RANGE";
}

function buildPatternVector(candles, atrs, endIndex) {
    let vec = [];
    for (let k = endIndex - patternLength + 1; k <= endIndex; k++) {
        if (k < 5) continue; 
        const c = candles[k];
        const prevC = candles[k-1] || c;
        const currentATR = atrs[k] || 1;
        const prev5ATR = atrs[k-5] || currentATR;
        const body = (c.c - c.o) / currentATR;
        const range = (c.h - c.l) / currentATR;
        const upperWick = (c.h - Math.max(c.o, c.c)) / currentATR;
        const lowerWick = (Math.min(c.o, c.c) - c.l) / currentATR;
        const direction = c.c > c.o ? 1 : -1;
        const atrSlope = (currentATR - prev5ATR) / currentATR;
        const momentum = (c.c - prevC.c) / currentATR;
        vec.push(body, range, upperWick, lowerWick, direction, atrSlope, momentum);
    }
    return vec;
}

function cosineSimilarity(a, b) {
    let dot = 0, normA = 0, normB = 0;
    for (let i = 0; i < a.length; i++) {
        dot += a[i] * (b[i] || 0);
        normA += a[i] * a[i];
        normB += (b[i] || 0) * (b[i] || 0);
    }
    if (normA === 0 || normB === 0) return 0;
    return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

function timeDecayWeight(ts, now) { 
    return Math.exp(-(now - ts) / (1000 * 60 * 60 * 24 * 90)); 
}

function aggregateCandles(candles, factor) {
    const result = [];
    for (let i = 0; i < candles.length; i += factor) {
        const chunk = candles.slice(i, i + factor);
        if (chunk.length < factor) continue;
        result.push({
            o: chunk[0].o,
            h: Math.max(...chunk.map(c => c.h)),
            l: Math.min(...chunk.map(c => c.l)),
            c: chunk[chunk.length - 1].c
        });
    }
    return result;
}

function precalcATR(candles, period = 14) {
    let atrs = new Array(candles.length).fill(0);
    if (candles.length < period + 1) return atrs;
    let trs = [0];
    for (let i = 1; i < candles.length; i++) {
        trs.push(Math.max(candles[i].h - candles[i].l, Math.abs(candles[i].h - candles[i-1].c), Math.abs(candles[i].l - candles[i-1].c)));
    }
    let sum = 0;
    for (let i = 1; i <= period; i++) sum += trs[i];
    atrs[period] = sum / period;
    for (let i = period + 1; i < candles.length; i++) {
        sum = sum - trs[i - period] + trs[i];
        atrs[i] = sum / period;
    }
    return atrs;
}

function detectStructure(candles) {
    if (!candles || candles.length < 20) return 0;
    let h = candles.slice(-20).map(c => c.h), l = candles.slice(-20).map(c => c.l);
    if (Math.max(...h) === h[h.length-1]) return 1;
    if (Math.min(...l) === l[l.length-1]) return -1;
    return 0;
}

function getRecommendedTimeData(tfLabel, serverTime) {
    const now = new Date(serverTime);
    let minutesToAdd = (tfLabel === '5M') ? 5 : (tfLabel === '15M') ? 15 : (tfLabel === '30M') ? 30 : (tfLabel === '1H') ? 60 : 0;
    if (minutesToAdd === 0) return null;
    
    const currentMs = now.getTime();
    const intervalMs = minutesToAdd * 60 * 1000;
    const startTs = Math.ceil(currentMs / intervalMs) * intervalMs;
    const tStart = new Date(startTs);
    const tEnd = new Date(startTs + intervalMs);

    const format = (d) => d.toLocaleTimeString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour: '2-digit', minute: '2-digit', hour12: false });
    
    return { text: `${format(tStart)} - ${format(tEnd)}`, startTs: tStart.getTime(), endTs: tEnd.getTime() };
}

function runAnalysisElite(candles) {
    if (candles.length < 100) return null;
    const recentCandles = candles.slice(-50);
    
    const entropy = marketEntropy(recentCandles);
    if (entropy > 0.998) return null;
    const zScore = calculateZScore(recentCandles.map(c => c.c));
    if (Math.abs(zScore) > 2.5) return null;

    const regime = detectRegime(candles);
    const atrs = precalcATR(candles);
    const currentATR = atrs[candles.length - 2] || 1;
    const targetVector = buildPatternVector(candles, atrs, candles.length - 2);

    let matches = [];
    let bucketCount = {};
    const startIdx = Math.max(40, candles.length - 10000);

    for (let i = startIdx; i < candles.length - 2 - patternLength - 3; i++) {
        for (let shift = 0; shift <= 2; shift++) {
            const histEndIdx = i + shift + patternLength - 1;
            if (atrs[histEndIdx] / currentATR > 2.5 || atrs[histEndIdx] / currentATR < 0.4) continue; 

            const sim = cosineSimilarity(targetVector, buildPatternVector(candles, atrs, histEndIdx));
            if (sim < 0.80) continue; 

            const ts = Date.now() - ((candles.length - histEndIdx) * 5 * 60 * 1000);
            const bucket = Math.floor(ts / (1000 * 60 * 60));
            bucketCount[bucket] = (bucketCount[bucket] || 0) + 1;
            if (bucketCount[bucket] > 3) continue;

            const next = candles[histEndIdx + 1];
            if (!next) continue;
            
            const futureVolatility = (next.h - next.l) / currentATR;
            if (futureVolatility < 0.5) continue; 

            const moveUp = next.h - candles[histEndIdx].c;
            const moveDown = candles[histEndIdx].c - next.l;
            const win = (moveUp > moveDown) ? 1 : 0; 

            matches.push({ sim, win, timestamp: ts });
        }
    }

    const uniqueMatchesMap = new Map();
    for (const m of matches) {
        if (!uniqueMatchesMap.has(m.timestamp) || uniqueMatchesMap.get(m.timestamp).sim < m.sim) {
            uniqueMatchesMap.set(m.timestamp, m);
        }
    }
    matches = Array.from(uniqueMatchesMap.values());

    if (matches.length < 15) return null; 
    matches.sort((a,b) => b.sim - a.sim);
    
    const eliteCount = Math.min(250, Math.max(15, Math.floor(matches.length * 0.10)));
    const topMatches = matches.slice(0, eliteCount);
    
    let wins = 0; let losses = 0; let totalWeight = 0;
    const now = Date.now();
    
    for (const m of topMatches) {
        const weight = Math.pow(m.sim, 2) * timeDecayWeight(m.timestamp, now);
        totalWeight += weight;
        if (m.win === 1) wins += weight;
        else losses += weight;
    }

    let prob = ((wins + 1) / (wins + losses + 2)) * 100;
    prob = prob * (currentATR > (atrs.reduce((a,b)=>a+b,0)/atrs.length) * 2 ? 0.9 : 1);

    const chunks = 3; const size = Math.floor(topMatches.length / chunks);
    let segs = [];
    for (let j = 0; j < chunks; j++) {
        const s = topMatches.slice(j * size, (j + 1) * size);
        segs.push(s.reduce((a, b) => a + b.win, 0) / s.length);
    }
    const meanWR = segs.reduce((a,b)=>a+b,0) / 3;
    const stability = Math.max(0, 1 - (Math.sqrt(segs.reduce((s, w) => s + Math.pow(w - meanWR, 2), 0) / 3) / (meanWR || 1)));

    const signal = prob > 50 ? "BUY" : "SELL";
    const finalProb = signal === "BUY" ? prob : 100 - prob;
    const edge = finalProb - CONFIG.BE;

    if (edge < 1.2 || (regime === "COMPRESSION" && edge < 6)) return null;

    return { prob: finalProb, direction: signal, edge: (signal === 'BUY' ? edge : -edge), absEdge: edge, stabilityRaw: stability, n: topMatches.length, acs: (edge / 50) * stability * (topMatches.length / 100), cwev: edge * stability, currentPrice: candles[candles.length - 1].c, currentATR: currentATR };
}

function makeProgressBar(percent) {
    const f = Math.min(10, Math.max(0, Math.round(percent / 10)));
    return '█'.repeat(f) + '░'.repeat(10 - f);
}

// === ORQUESTADOR GLOBAL ===
async function globalScan(scanType = 'auto') {
    const currentServerTime = getSyncedTime();
    
    if (currentServerTime < globalPauseUntil) return;
    if (scanType === 'auto' && Date.now() - lastSignalTime < SIGNAL_COOLDOWN) return;

    let statusMsg;
    if (scanType === 'manual') {
        const startTime = getLocalTime();
        try { statusMsg = await bot.sendMessage(chatId, `⏳ *Scan Manual*\n▶️ *Inicio:* ${startTime}\n_Analizando microestructura..._`, { parse_mode: 'Markdown' }); } catch(e) {}
    }

    const now = new Date(currentServerTime);
    const m = now.getMinutes();

    let parsedData = [];
    const candlesByAsset = {}; 

    try {
        if (fs.existsSync('./auditoria_sniper.csv')) {
            const content = fs.readFileSync('./auditoria_sniper.csv', 'utf8');
            const lines = content.split('\n').filter(l => l.trim() !== '');
            if (lines.length > 1) {
                const headers = lines[0].split(',');
                for (let i = 1; i < lines.length; i++) {
                    const vals = lines[i].split(',');
                    let obj = {}; headers.forEach((h, idx) => obj[h.trim()] = vals[idx]);
                    parsedData.push(obj);
                }
                
                CONFIG.MARKETS.forEach(asset => {
                    const assetRows = parsedData.filter(r => r.Activo === asset.id && (r.Veredicto.includes('GANADA') || r.Veredicto.includes('PERDIDA'))).slice(-20);
                    if(assetRows.length > 0) {
                        const wins = assetRows.filter(r => r.Veredicto.includes('GANADA')).length;
                        assetPerformance[asset.id] = wins / assetRows.length;
                    }
                });

                const finished = parsedData.filter(r => r.Veredicto.includes('GANADA') || r.Veredicto.includes('PERDIDA')).slice(-50);
                if (finished.length > 0) {
                    const wins = finished.filter(r => r.Veredicto.includes('GANADA')).length;
                    autoLearningStats = { winrate: wins / finished.length, total: finished.length };
                    if (autoLearningStats.winrate > 0.55) MIN_AI_SCORE = 60;
                    else if (autoLearningStats.winrate < 0.48) MIN_AI_SCORE = 75;
                    else MIN_AI_SCORE = 68;
                }

                const totalFinishedTrades = parsedData.filter(r => r.Veredicto.includes('GANADA') || r.Veredicto.includes('PERDIDA')).length;
                const globalLastTrades = parsedData.filter(r => r.Veredicto.includes('GANADA') || r.Veredicto.includes('PERDIDA')).slice(-4);
                
                if (globalLastTrades.length > 0 && globalLastTrades[globalLastTrades.length - 1].Veredicto.includes('GANADA')) circuitBreakerLevel = 0;

                if (globalLastTrades.length === 4 && globalLastTrades.every(r => r.Veredicto.includes('PERDIDA')) && totalFinishedTrades > lastCircuitBreakerTradeCount) {
                    lastCircuitBreakerTradeCount = totalFinishedTrades;
                    const cooldownMinutes = 30 * Math.pow(2, Math.min(circuitBreakerLevel, 2));
                    globalPauseUntil = currentServerTime + (cooldownMinutes * 60 * 1000);
                    circuitBreakerLevel++;
                    bot.sendMessage(chatId, `🚨 *CIRCUIT BREAKER ACTIVADO (Nivel ${circuitBreakerLevel})*\nEl bot se pausa automáticamente por *${cooldownMinutes} minutos* para proteger el capital.`, { parse_mode: 'Markdown' });
                    return; 
                }
            }
        }
    } catch (e) {}

    let allowedAssets = CONFIG.MARKETS.map(m => m.id);
    const validSignals = [];
    let tfs = [{ tf: '5M', aggregate: 1 }]; 

    if (scanType === 'auto') {
        if ([13, 28, 43, 58].includes(m)) tfs.push({ tf: '15M', aggregate: 3 });
        if ([28, 58].includes(m)) tfs.push({ tf: '30M', aggregate: 6 });
        if (m === 58) tfs.push({ tf: '1H', aggregate: 12 });
    } else {
        tfs = [{ tf: '5M', aggregate: 1 }, { tf: '15M', aggregate: 3 }, { tf: '30M', aggregate: 6 }, { tf: '1H', aggregate: 12 }];
    }

    for (const asset of CONFIG.MARKETS) {
        if (!allowedAssets.includes(asset.id)) continue;

        try {
            let historical = GLOBAL_CANDLE_CACHE.get(asset.id);
            if (!historical) {
                const histRes = await axios.get(`${CONFIG.BACKEND_URL}/candles?symbol=${asset.symbolBinance}&limit=${CONFIG.REQUEST_LIMIT}`);
                historical = histRes.data;
                if (!historical || historical.length < 100) continue;
                GLOBAL_CANDLE_CACHE.set(asset.id, historical);
            } else {
                const recentRes = await axios.get(`${CONFIG.BACKEND_URL}/candles?symbol=${asset.symbolBinance}&limit=100`);
                if (recentRes.data && recentRes.data.length > 0) {
                    const lastCached = historical[historical.length - 1];
                    let overlapIdx = recentRes.data.findIndex(r => r.c === lastCached.c && r.o === lastCached.o);
                    if (overlapIdx !== -1) historical.push(...recentRes.data.slice(overlapIdx + 1));
                    else historical = recentRes.data;
                    if (historical.length > 50000) historical = historical.slice(historical.length - 50000);
                    GLOBAL_CANDLE_CACHE.set(asset.id, historical);
                }
            }
            
            candlesByAsset[asset.id] = historical;

            const [mRes1h, mRes4h] = await Promise.all([
                axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${asset.symbolBinance}&interval=1h&limit=50`),
                axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${asset.symbolBinance}&interval=4h&limit=50`)
            ]);
            const s1h = detectStructure(mRes1h.data.map(v=>({h:parseFloat(v[2]),l:parseFloat(v[3])})));
            const s4h = detectStructure(mRes4h.data.map(v=>({h:parseFloat(v[2]),l:parseFloat(v[3])})));

            let obi = 0;
            try {
                const lobRes = await axios.get(`${CONFIG.BINANCE_API}/depth?symbol=${asset.symbolBinance}&limit=10`);
                let bV=0, aV=0;
                if(lobRes.data.bids && lobRes.data.asks && lobRes.data.bids.length > 0) {
                    const midPrice = (parseFloat(lobRes.data.bids[0][0]) + parseFloat(lobRes.data.asks[0][0])) / 2;
                    lobRes.data.bids.forEach(l => { const price = parseFloat(l[0]); bV += parseFloat(l[1]) * (1 / Math.max(Math.abs(midPrice - price) / midPrice, 0.0001)); });
                    lobRes.data.asks.forEach(l => { const price = parseFloat(l[0]); aV += parseFloat(l[1]) * (1 / Math.max(Math.abs(midPrice - price) / midPrice, 0.0001)); });
                    if (bV + aV > 0) obi = (bV - aV) / (bV + aV);
                }
            } catch(e) {}

            for (const item of tfs) {
                const res = runAnalysisElite(item.aggregate > 1 ? aggregateCandles(historical, item.aggregate) : historical);
                if (res) validSignals.push({ assetId: asset.id, symbolBinance: asset.symbolBinance, tf: item.tf, analysis: res, obi, macro: { h1: s1h, h4: s4h } });
            }
        } catch(e) {}
    } 

    const consensusSignals = [];

    for (const s of validSignals) {
        const isB = s.analysis.direction === "BUY";
        const passMacro = !(isB && s.macro.h4 === -1) && !(!isB && s.macro.h4 === 1);
        
        const historicalForAsset = candlesByAsset[s.assetId];
        let momentumSlope = 0, nearResistance = false, nearSupport = false;

        if (historicalForAsset && historicalForAsset.length >= 10) {
            const recent10 = historicalForAsset.slice(-10).map(c => c.c);
            let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
            for (let k = 0; k < 10; k++) { sumX += k; sumY += recent10[k]; sumXY += k * recent10[k]; sumX2 += k * k; }
            momentumSlope = (10 * sumXY - sumX * sumY) / (10 * sumX2 - sumX * sumX);
        }

        if (historicalForAsset && historicalForAsset.length >= 50) {
            const recent50 = historicalForAsset.slice(-50);
            const highestHigh = Math.max(...recent50.map(c => c.h));
            const lowestLow = Math.min(...recent50.map(c => c.l));
            const avgRange = recent50.slice(-10).reduce((acc, c) => acc + (c.h - c.l), 0) / 10;
            nearResistance = Math.abs(highestHigh - s.analysis.currentPrice) < avgRange;
            nearSupport = Math.abs(s.analysis.currentPrice - lowestLow) < avgRange;
        }

        const regime = detectRegime(historicalForAsset);
        let passRegime = true;
        if (regime === "TREND" && ((isB && momentumSlope < 0) || (!isB && momentumSlope > 0))) passRegime = false;
        if (regime === "COMPRESSION" && s.analysis.absEdge < 4) passRegime = false;
        if (regime === "RANGE" && ((isB && nearResistance) || (!isB && nearSupport))) passRegime = false;

        const currentProbThreshold = getDynamicThreshold(s.analysis.currentATR, s.analysis.currentPrice);
        const finalScore = calculateFinalScore(s.analysis, s.assetId);
        
        const isStrictLOB = ASSET_PROFILES[s.assetId] ? ASSET_PROFILES[s.assetId].strictLOB : false;
        const lobPass = isStrictLOB ? ((isB && s.obi > 0.05) || (!isB && s.obi < -0.05)) : true;

        const passFinalScore = finalScore >= 0.15;
        const recentLossesAsset = parsedData.filter(r => r.Activo === s.assetId).slice(-5).filter(r => r.Veredicto.includes('PERDIDA')).length;

        let isElite = passRegime && passFinalScore && passMacro && s.analysis.cwev >= 1.2 && recentLossesAsset < 3 && s.analysis.prob >= currentProbThreshold && s.analysis.stabilityRaw >= 0.40 && lobPass;
        let isAggressive = passMacro && s.analysis.prob >= (currentProbThreshold - 2) && s.analysis.stabilityRaw >= 0.35 && !((isB && momentumSlope < -0.0001) || (!isB && momentumSlope > 0.0001));

        if (isElite || isAggressive) {
            s.isElite = isElite; s.isAggressive = isAggressive; s.momentumSlope = momentumSlope; 
            s.nearResistance = nearResistance; s.nearSupport = nearSupport; s.passMacro = passMacro; 
            s.finalScore = finalScore;
            consensusSignals.push(s);
        }
    }
    
    consensusSignals.sort((a,b) => b.analysis.cwev - a.analysis.cwev);

    if (statusMsg) bot.editMessageText(`✅ *Scan completado*\n🚀 *${consensusSignals.length} oportunidades.*`, { chat_id: chatId, message_id: statusMsg.message_id, parse_mode: 'Markdown' }).catch(()=>{});

    if (consensusSignals.length > 0) {
        lastSignalTime = Date.now();
        
        for (const s of consensusSignals) {
            const sigId = `sig_${signalCounter++}`;
            SIGNAL_CACHE.set(sigId, s);
            
            const dualScore = (s.isElite ? 0.6 : 0) + (s.isAggressive ? 0.4 : 0);
            let scoreVisual = dualScore === 1.0 ? "1.0 → PERFECTO" : dualScore === 0.6 ? "0.6 → SOLO ELITE" : "0.4 → SOLO AGRESIVO";
            const icon = s.analysis.direction === 'BUY' ? '🟢' : '🔴';
            
            const confBar = makeProgressBar(s.analysis.prob); 
            const anticipationLevel = s.isAggressive ? 90 : 30;
            const antBar = makeProgressBar(anticipationLevel);

            // 💰 TRADING GENERADO ANTES DE LA IA
            const baseTradeData = calculateTradeLevels(s.analysis.currentPrice, s.analysis.direction, s.analysis.currentATR, s.analysis.absEdge, circuitBreakerLevel, s.analysis.stabilityRaw);
            let tradingText = "";
            if (baseTradeData) {
                tradingText = `\n\n💰 *MODO TRADING (Spot/CFD x20)*\n📍 *Entry:* ${baseTradeData.entry} | 🛑 *SL:* ${baseTradeData.sl} | 🎯 *TP:* ${baseTradeData.tp}\n⚖️ *R:R:* 1:${baseTradeData.rr} | 💼 *Volumen sugerido:* ${baseTradeData.positionSize} USDT\n📉 *Riesgo:* ${baseTradeData.riskPercent}% del capital`;
            }

            // 🎯 TARJETA VISUAL COMPLETA CON TRADING
            const msgText = `🎯 *${s.assetId} | ${s.tf}*\n🧠 *MODO DUAL*\n🟢 ELITE    → ${s.isElite ? '✅' : '❌'}\n🟡 AGRESIVO → ${s.isAggressive ? '✅' : '❌'}\n\n🏆 *DUAL SCORE:* ${scoreVisual}\n📈 *Dirección:* ${icon} *${s.analysis.direction}*\n\n⚖️ *BALANCE*\nConfianza:    ${confBar} (${s.analysis.prob.toFixed(0)}%)\nAnticipación: ${antBar} (${anticipationLevel}%)${tradingText}`;

            // Guardamos el texto base para la IA
            s.msgText = msgText;

            const opts = {
                parse_mode: 'Markdown',
                reply_markup: {
                    inline_keyboard: [[ { text: "🧠 Análisis IA de Contexto", callback_data: `ia_${sigId}` } ]]
                }
            };

            const sentMsg = await bot.sendMessage(chatId, msgText, opts);

            const timeData = getRecommendedTimeData(s.tf, currentServerTime);
            PENDING_AUDITS.push({
                sigId, assetId: s.assetId, symbolBinance: s.symbolBinance, tf: s.tf, direction: s.analysis.direction,
                startTs: timeData.startTs, endTs: timeData.endTs, messageId: sentMsg.message_id, retries: 0,
                logData: {
                    prob: s.analysis.prob.toFixed(1), lob: s.obi.toFixed(3), edge: s.analysis.edge.toFixed(2), 
                    alpha: s.analysis.acs.toFixed(3), stab: (s.analysis.stabilityRaw * 100).toFixed(0), cwev: s.analysis.cwev.toFixed(1),
                    samples: s.analysis.n, iaVerdict: 'NO_SOLICITADO', iaScore: 0, iaContext: 'N/A', 
                    mode: s.isElite ? 'ELITE' : 'AGRESIVO', finalScore: s.finalScore.toFixed(2)
                }
            });
        }
    }
}

// === BOTÓN DE ANÁLISIS A DEMANDA (IA) ===
bot.on('callback_query', async (query) => {
    const action = query.data;
    if (action.startsWith('ia_')) {
        const sigId = action.replace('ia_', '');
        const s = SIGNAL_CACHE.get(sigId);
        
        if (!s) return bot.answerCallbackQuery(query.id, { text: "Señal expirada o no encontrada.", show_alert: true });
        bot.answerCallbackQuery(query.id);

        const originalMsg = s.msgText;
        await bot.editMessageText(originalMsg + '\n\n⏳ _Consultando a Gemini AI..._', { chat_id: query.message.chat.id, message_id: query.message.message_id, parse_mode: 'Markdown' });

        const assetPerf = assetPerformance[s.assetId] !== undefined ? `${(assetPerformance[s.assetId]*100).toFixed(0)}%` : 'N/A';
        const aiPenalty = ASSET_PROFILES[s.assetId] ? ASSET_PROFILES[s.assetId].aiPenalty : 0;
        
        const promptText = `Eres un Quant Trader. AI_SCORE (0-100), TRADE_CONTEXT (CONTINUATION, REVERSAL, TRAP) y REASONING en 2 oraciones. 
        Activo: ${s.assetId} | Dir: ${s.analysis.direction} | Prob: ${s.analysis.prob.toFixed(1)}% | Edge: ${s.analysis.absEdge.toFixed(2)}% | LOB: ${s.obi.toFixed(3)} | FinalScore: ${s.finalScore.toFixed(2)}.
        NOTA DEL SISTEMA: El winrate histórico de este activo es ${assetPerf}. Aplica una penalización/bonificación de ${aiPenalty} puntos a tu evaluación de riesgo.`;

        try {
            const result = await axios.post(`https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key=${process.env.GEMINI_API_KEY}`, { contents: [{ parts: [{ text: promptText }] }], generationConfig: { temperature: 0.2 } });
            const text = result.data.candidates[0].content.parts[0].text;
            
            let iaScore = 0, iaCtx = "UNKNOWN", iaReason = "Analizado.";
            const scoreMatch = text.match(/AI_SCORE:\s*(\d+)/i); if (scoreMatch) iaScore = parseInt(scoreMatch[1]) + aiPenalty; 
            const ctxMatch = text.match(/TRADE_CONTEXT:\s*(\w+)/i); if (ctxMatch) iaCtx = ctxMatch[1].toUpperCase();
            const reasonMatch = text.match(/REASONING:\s*([\s\S]*?)$/i); if (reasonMatch) iaReason = reasonMatch[1].trim();

            const aiText = `\n\n🤖 *VEREDICTO IA:* ${iaScore>=MIN_AI_SCORE?'🚀 EXECUTE':'⛔ PASS'} (${iaScore}/100)\n*Contexto:* 🧩 ${iaCtx}\n_📝 ${iaReason}_`;
            
            bot.editMessageText(originalMsg + aiText, { chat_id: query.message.chat.id, message_id: query.message.message_id, parse_mode: 'Markdown' });
            
            const auditEntry = PENDING_AUDITS.find(a => a.sigId === sigId);
            if (auditEntry) {
                auditEntry.logData.iaVerdict = iaScore >= MIN_AI_SCORE ? 'EXECUTE' : 'PASS';
                auditEntry.logData.iaScore = iaScore;
                auditEntry.logData.iaContext = iaCtx;
            }
            
        } catch (e) { bot.editMessageText(originalMsg + '\n\n❌ _Error al contactar IA._', { chat_id: query.message.chat.id, message_id: query.message.message_id, parse_mode: 'Markdown' }); }
    }
});

// === CRONES DINÁMICOS ===
setInterval(async () => { 
    let volSum = 0, count = 0;
    for (const candles of GLOBAL_CANDLE_CACHE.values()) {
        const atrs = precalcATR(candles);
        const atr = atrs[atrs.length - 1];
        const price = candles[candles.length - 1].c;
        if (atr && price) { volSum += atr / price; count++; }
    }
    const avgVol = count > 0 ? volSum / count : 0.002;
    const targetSecond = getDynamicSecond(avgVol);
    const now = new Date(getSyncedTime()); 
    if ((now.getMinutes() % 5 === 3) && Math.abs(now.getSeconds() - targetSecond) <= 1) globalScan('auto'); 
}, 1000);

// 🚀 CRON DE AUDITORÍA
setInterval(async () => {
    for (let i = PENDING_AUDITS.length - 1; i >= 0; i--) {
        const audit = PENDING_AUDITS[i];
        const currentServerTime = getSyncedTime(); 
        if (currentServerTime >= audit.endTs + 10000) {
            try {
                const binanceInterval = audit.tf.toLowerCase();
                const res = await axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${audit.symbolBinance}&interval=${binanceInterval}&limit=3`);
                const klines = res.data;
                const tradeCandle = klines.find(kline => kline[0] === audit.startTs);

                if (!tradeCandle) {
                    audit.retries++;
                    if (audit.retries > 5) PENDING_AUDITS.splice(i, 1); 
                    continue;
                }

                const openPrice = parseFloat(tradeCandle[1]);
                const closePrice = parseFloat(tradeCandle[4]);
                let isWin = (audit.direction === 'BUY' && closePrice > openPrice) || (audit.direction === 'SELL' && closePrice < openPrice);
                const isTie = closePrice === openPrice;
                const iconResult = isTie ? 'EMPATE' : (isWin ? 'GANADA' : 'PERDIDA');

                const auditMsg = `🔍 *AUDITORÍA FINAL (BINANCE)*\n\n*Activo:* ${audit.assetId}\n*Dirección:* ${audit.direction}\n*Modo:* ${audit.logData.mode}\n*Contexto IA:* ${audit.logData.iaContext}\n*Veredicto IA:* ${audit.logData.iaVerdict} (Score: ${audit.logData.iaScore})\n\n🟢 Open: ${openPrice.toFixed(4)}\n🔴 Close: ${closePrice.toFixed(4)}\n\n*Resultado:* ${iconResult}`;
                await bot.sendMessage(chatId, auditMsg, { parse_mode: 'Markdown', reply_to_message_id: audit.messageId });
                
                logToCSV({ 
                    asset: audit.assetId, tf: audit.tf, dir: audit.direction, 
                    prob: audit.logData.prob, lob: audit.logData.lob, edge: audit.logData.edge,
                    alpha: audit.logData.alpha, stab: audit.logData.stab, cwev: audit.logData.cwev,
                    samples: audit.logData.samples, iaVerdict: audit.logData.iaVerdict, iaScore: audit.logData.iaScore, iaContext: audit.logData.iaContext, mode: audit.logData.mode, finalScore: audit.logData.finalScore,
                    veredicto: iconResult, open: openPrice, close: closePrice 
                });
                
                PENDING_AUDITS.splice(i, 1);
            } catch (error) { audit.retries++; }
        }
    }
}, 15000);
