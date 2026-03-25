require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const fs = require('fs');

// ═══════════════════════════════════════════════════════════════════
// QUANT SNIPER V13 — ADAPTIVE TACTICAL PRO (FUSIONADO Y CORREGIDO)
// ═══════════════════════════════════════════════════════════════════

// === CONFIGURACIONES GLOBALES ===
const token = process.env.TELEGRAM_TOKEN;
const chatId = process.env.CHAT_ID;
const bot = new TelegramBot(token, { polling: true });

const patternLength = 6;
const BASE_PROB_THRESHOLD = 54;
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
const GLOBAL_CANDLE_CACHE = new Map();

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 1: SISTEMA DE APRENDIZAJE ADAPTATIVO (CSV INTELLIGENCE)
// ═══════════════════════════════════════════════════════════════════

let ADAPTIVE_PROFILES = {};

function parseCSVResolved() {
    const filePath = './auditoria_sniper.csv';
    if (!fs.existsSync(filePath)) return [];

    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(l => l.trim() !== '');
    if (lines.length < 2) return [];

    const headers = lines[0].split(',').map(h => h.trim());
    const rows = [];

    for (let i = 1; i < lines.length; i++) {
        const vals = lines[i].split(',');
        const obj = {};
        headers.forEach((h, idx) => { obj[h] = (vals[idx] || '').trim(); });

        if (obj.Veredicto === 'GANADA' || obj.Veredicto === 'PERDIDA') {
            obj._prob = parseFloat(obj.Prob) || 0;
            obj._lob = parseFloat(obj.LOB) || 0;
            obj._edge = parseFloat(obj.Edge) || 0;
            obj._stab = parseFloat(obj.Stability) || 0;
            obj._cwev = parseFloat(obj.CWEV) || 0;
            obj._alpha = parseFloat(obj.Alpha) || 0;
            obj._samples = parseFloat(obj.Samples) || 0;
            obj._win = obj.Veredicto === 'GANADA' ? 1 : 0;
            rows.push(obj);
        }
    }
    return rows;
}

function calcWR(subset) {
    if (!subset || subset.length < 3) return { wr: null, n: subset ? subset.length : 0 };
    const wins = subset.reduce((s, r) => s + r._win, 0);
    return { wr: (wins / subset.length) * 100, n: subset.length };
}

function buildAdaptiveProfiles(resolvedRows) {
    const profiles = {};

    for (const market of CONFIG.MARKETS) {
        const assetId = market.id;
        const assetRows = resolvedRows.filter(r => r.Activo === assetId);

        const profile = {
            totalTrades: assetRows.length,
            overallWR: calcWR(assetRows).wr || 50,
            maxCWEV: 10,
            maxAlpha: 0.10,
            maxAbsEdge: 20,
            preferredTFs: ['5M', '15M', '30M', '1H'],
            minStability: 30,
            lobBias: 0,
            lossPatterns: [],
            confidence: 'LOW',
            aiPenalty: 0 
        };

        if (assetRows.length < 10) {
            profiles[assetId] = profile;
            continue;
        }

        profile.confidence = assetRows.length >= 25 ? 'HIGH' : 'MEDIUM';
        
        // Asignamos penalidades a la IA si el activo viene mal en general
        if (profile.overallWR < 45) profile.aiPenalty = -5;
        if (profile.overallWR > 55) profile.aiPenalty = 5;

        // --- CWEV óptimo ---
        const cwevThresholds = [5, 6, 7, 8, 9];
        let bestCWEV = { thresh: 10, score: 0 };
        for (const thresh of cwevThresholds) {
            const sub = assetRows.filter(r => r._cwev < thresh);
            const { wr, n } = calcWR(sub);
            if (wr !== null && n >= 5) {
                const score = wr * Math.log(n + 1);
                if (score > bestCWEV.score) bestCWEV = { thresh, score, wr, n };
            }
        }
        if (bestCWEV.score > 0) profile.maxCWEV = bestCWEV.thresh;

        // --- Alpha óptimo ---
        const alphaThresholds = [0.025, 0.030, 0.035, 0.040, 0.050];
        let bestAlpha = { thresh: 0.10, score: 0 };
        for (const thresh of alphaThresholds) {
            const sub = assetRows.filter(r => r._alpha < thresh);
            const { wr, n } = calcWR(sub);
            if (wr !== null && n >= 5) {
                const score = wr * Math.log(n + 1);
                if (score > bestAlpha.score) bestAlpha = { thresh, score, wr, n };
            }
        }
        if (bestAlpha.score > 0) profile.maxAlpha = bestAlpha.thresh;

        // --- Edge absoluto óptimo ---
        const edgeThresholds = [5, 6, 7, 8, 10, 12];
        let bestEdge = { thresh: 20, score: 0 };
        for (const thresh of edgeThresholds) {
            const sub = assetRows.filter(r => Math.abs(r._edge) < thresh);
            const { wr, n } = calcWR(sub);
            if (wr !== null && n >= 5) {
                const score = wr * Math.log(n + 1);
                if (score > bestEdge.score) bestEdge = { thresh, score, wr, n };
            }
        }
        if (bestEdge.score > 0) profile.maxAbsEdge = bestEdge.thresh;

        // --- TF preferido ---
        const tfScores = {};
        for (const tf of ['5M', '15M', '30M', '1H']) {
            const sub = assetRows.filter(r => r.TF === tf);
            const { wr, n } = calcWR(sub);
            if (wr !== null && n >= 5) {
                tfScores[tf] = { wr, n, score: wr * Math.log(n + 1) };
            }
        }
        const goodTFs = Object.entries(tfScores).filter(([_, v]) => v.wr >= 45).sort((a, b) => b[1].score - a[1].score).map(([tf]) => tf);
        if (goodTFs.length > 0) profile.preferredTFs = goodTFs;

        // --- Patrones de pérdida ---
        const losses = assetRows.filter(r => r._win === 0);
        const wins = assetRows.filter(r => r._win === 1);

        if (losses.length >= 5 && wins.length >= 5) {
            const avgLoss = {
                cwev: losses.reduce((s, r) => s + r._cwev, 0) / losses.length,
                alpha: losses.reduce((s, r) => s + r._alpha, 0) / losses.length,
                absEdge: losses.reduce((s, r) => s + Math.abs(r._edge), 0) / losses.length,
                stab: losses.reduce((s, r) => s + r._stab, 0) / losses.length
            };
            const avgWin = {
                cwev: wins.reduce((s, r) => s + r._cwev, 0) / wins.length,
                alpha: wins.reduce((s, r) => s + r._alpha, 0) / wins.length,
                absEdge: wins.reduce((s, r) => s + Math.abs(r._edge), 0) / wins.length,
                stab: wins.reduce((s, r) => s + r._stab, 0) / wins.length
            };

            if (avgLoss.cwev > avgWin.cwev * 1.1) profile.lossPatterns.push(`CWEV alto (loss: ${avgLoss.cwev.toFixed(1)} vs win: ${avgWin.cwev.toFixed(1)})`);
            if (avgLoss.alpha > avgWin.alpha * 1.1) profile.lossPatterns.push(`Alpha excesivo (loss: ${avgLoss.alpha.toFixed(3)} vs win: ${avgWin.alpha.toFixed(3)})`);
            if (avgLoss.absEdge > avgWin.absEdge * 1.15) profile.lossPatterns.push(`Edge extremo (loss: ${avgLoss.absEdge.toFixed(1)} vs win: ${avgWin.absEdge.toFixed(1)})`);
        }

        // --- LOB bias ---
        const buyRows = assetRows.filter(r => r.Dir === 'BUY');
        const sellRows = assetRows.filter(r => r.Dir === 'SELL');
        const buyAligned = buyRows.filter(r => r._lob > 0);
        const sellAligned = sellRows.filter(r => r._lob < 0);
        const buyAlignedWR = calcWR(buyAligned);
        const sellAlignedWR = calcWR(sellAligned);
        if (buyAlignedWR.wr !== null && sellAlignedWR.wr !== null) {
            profile.lobBias = ((buyAlignedWR.wr + sellAlignedWR.wr) / 2) - profile.overallWR;
        }

        profiles[assetId] = profile;
    }
    return profiles;
}

function applyAdaptiveFilter(signal) {
    const profile = ADAPTIVE_PROFILES[signal.assetId];
    if (!profile || profile.confidence === 'LOW') {
        return { pass: true, reason: 'Sin datos suficientes', adjustedScore: signal.finalScore };
    }

    const reasons = [];
    let penalty = 0;

    if (signal.analysis.cwev >= profile.maxCWEV) { reasons.push(`CWEV ${signal.analysis.cwev.toFixed(1)}>=${profile.maxCWEV}`); penalty += 0.2; }
    if (signal.analysis.acs >= profile.maxAlpha) { reasons.push(`Alpha ${signal.analysis.acs.toFixed(3)}>=${profile.maxAlpha}`); penalty += 0.15; }
    if (Math.abs(signal.analysis.edge) >= profile.maxAbsEdge) { reasons.push(`|Edge| ${Math.abs(signal.analysis.edge).toFixed(1)}>=${profile.maxAbsEdge}`); penalty += 0.15; }
    if (!profile.preferredTFs.includes(signal.tf)) { reasons.push(`TF ${signal.tf} no preferido`); penalty += 0.25; }
    if (profile.overallWR < 42 && profile.totalTrades >= 15) { reasons.push(`WR global bajo: ${profile.overallWR.toFixed(1)}%`); penalty += 0.30; }

    const adjustedScore = signal.finalScore * (1 - penalty);
    // Dejamos pasar si el Score Ajustado sigue superando el umbral base de 0.15
    const pass = adjustedScore >= 0.15;
    
    return { pass, reason: reasons.length > 0 ? reasons.join(' | ') : 'OK', adjustedScore, penalty };
}

function getAssetLearningContext(assetId) {
    const profile = ADAPTIVE_PROFILES[assetId];
    if (!profile || profile.confidence === 'LOW') return 'Sin datos históricos suficientes para este activo.';

    let ctx = `Datos históricos ${assetId} (${profile.totalTrades} trades, WR: ${profile.overallWR.toFixed(1)}%, Confianza: ${profile.confidence}):\n`;
    ctx += `- Filtros óptimos: CWEV<${profile.maxCWEV}, Alpha<${profile.maxAlpha}, |Edge|<${profile.maxAbsEdge}\n`;
    ctx += `- TFs rentables: ${profile.preferredTFs.join(', ')}\n`;

    if (profile.lossPatterns.length > 0) ctx += `- Patrones pérdida: ${profile.lossPatterns.join('; ')}\n`;
    if (profile.lobBias > 3) ctx += `- LOB alineado mejora WR en +${profile.lobBias.toFixed(1)}%\n`;

    return ctx;
}

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 2: SINCRONIZACIÓN DE TIEMPO
// ═══════════════════════════════════════════════════════════════════

let timeOffset = 0;
async function syncTimeWithBinance() {
    try {
        const res = await axios.get(`${CONFIG.BINANCE_API}/time`);
        timeOffset = res.data.serverTime - Date.now();
        console.log(`[SYS] Reloj sincronizado. Offset: ${timeOffset}ms`);
    } catch(e) { console.error("[SYS] Error al sincronizar reloj."); }
}
syncTimeWithBinance();
setInterval(syncTimeWithBinance, 60 * 60 * 1000);

function getSyncedTime() { return Date.now() + timeOffset; }
function getLocalTime() { return new Date(getSyncedTime()).toLocaleTimeString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour12: false }); }

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 3: LOGGING CSV
// ═══════════════════════════════════════════════════════════════════

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

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 4: FUNCIONES DE TRADING Y SCORE
// ═══════════════════════════════════════════════════════════════════

function formatPrice(val) { return val < 1 ? val.toFixed(4) : val.toFixed(2); }

function calculateFinalScore(analysis) {
    const { edge, stability, n, prob } = analysis;
    const sampleFactor = Math.log(n + 1) / 5;
    const probFactor = (prob - 50) / 50;
    return (Math.abs(edge) * stability * sampleFactor * probFactor);
}

function calculateTradeLevels(price, direction, atr, edge, iaContext, cbLevel, stability, iaScore = null) {
    const capital = 1000;
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

    const minMove = price * 0.002;
    if (Math.abs(tp - entry) < minMove) return null;

    const liquidationDistance = 1 / 20; // leverage 20
    if ((slDistance / price) > (liquidationDistance * 0.7)) return null;

    const riskAmount = capital * (riskPercent / 100);
    const positionSizeUSDT = (riskAmount / Math.abs(entry - sl)) * entry;

    return { entry: formatPrice(entry), sl: formatPrice(sl), tp: formatPrice(tp), rr: rr.toFixed(1), positionSize: positionSizeUSDT.toFixed(0), riskPercent: riskPercent.toFixed(1) };
}

function getDynamicThreshold(atr, price) {
    if (!atr || !price) return BASE_PROB_THRESHOLD;
    const volatility = atr / price;
    let threshold = BASE_PROB_THRESHOLD;
    if (volatility > 0.004) threshold = 56;
    else if (volatility < 0.0015) threshold = 52;
    if (autoLearningStats.total >= 20 && autoLearningStats.winrate < 0.48) threshold += 1.5;
    return threshold;
}

function getDynamicSecond(volatility) {
    if (volatility > 0.004) return 5;
    if (volatility < 0.0015) return 35;
    return 15;
}

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 5: MOTOR CUANTITATIVO Y REGLAS BÁSICAS
// ═══════════════════════════════════════════════════════════════════

function buildWinningZonesFromCSV(parsedData) {
    const zones = JSON.parse(JSON.stringify(DEFAULT_ZONES));
    if (!parsedData || parsedData.length === 0) return zones;
    const getRange = (arr) => {
        if (arr.length === 0) return null;
        arr.sort((a,b)=>a-b);
        return { min: arr[Math.floor(arr.length * 0.15)], max: arr[Math.floor(arr.length * 0.85)] };
    };
    CONFIG.MARKETS.forEach(asset => {
        const wins = parsedData.filter(r => r.Activo === asset.id && r.Veredicto === 'GANADA');
        if (wins.length >= 10) {
            const probs = wins.map(r => parseFloat(r.Prob)).filter(n => !isNaN(n));
            const alphas = wins.map(r => parseFloat(r.Alpha)).filter(n => !isNaN(n));
            const lobs = wins.map(r => parseFloat(r.LOB)).filter(n => !isNaN(n));
            if(getRange(probs)) zones[asset.id].prob = getRange(probs);
            if(getRange(alphas)) zones[asset.id].alpha = getRange(alphas);
            if(getRange(lobs)) zones[asset.id].lob = getRange(lobs);
        }
    });
    return zones;
}

function calculateZScore(values) {
    if(values.length === 0) return 0;
    const mean = values.reduce((a,b)=>a+b, 0) / values.length;
    const std = Math.sqrt(values.reduce((s,v)=>s+Math.pow(v-mean, 2), 0) / values.length) || 1;
    return (values[values.length-1] - mean) / std;
}

function marketEntropy(candles) {
    if(candles.length === 0) return 0;
    let up = 0, down = 0;
    for(let c of candles) { v = c.c > c.o ? up++ : down++; }
    if(up === 0 || down === 0) return 0;
    const pUp = up / (up+down), pDown = down / (up+down);
    return -(pUp * Math.log2(pUp) + pDown * Math.log2(pDown));
}

function detectRegime(candles){
    const closes = candles.slice(-50).map(c => c.c);
    const range = (Math.max(...closes) - Math.min(...closes)) / (Math.min(...closes) || 1);
    const trend = (closes[closes.length-1] - closes[0]) / (closes[0] || 1);
    if(Math.abs(trend) > 0.03) return "TREND";
    if(range < 0.015) return "COMPRESSION";
    return "RANGE";
}

function buildPatternVector(candles, atrs, endIndex) {
    let vec = [];
    for (let k = endIndex - patternLength + 1; k <= endIndex; k++) {
        if (k < 5) continue;
        const c = candles[k]; const prevC = candles[k-1] || c;
        const currentATR = atrs[k] || 1; const prev5ATR = atrs[k-5] || currentATR;
        vec.push((c.c - c.o) / currentATR, (c.h - c.l) / currentATR, (c.h - Math.max(c.o, c.c)) / currentATR, (Math.min(c.o, c.c) - c.l) / currentATR, c.c > c.o ? 1 : -1, (currentATR - prev5ATR) / currentATR, (c.c - prevC.c) / currentATR);
    }
    return vec;
}

function cosineSimilarity(a, b) {
    let dot = 0, nA = 0, nB = 0;
    for (let i = 0; i < a.length; i++) { dot += a[i] * (b[i] || 0); nA += a[i]**2; nB += (b[i] || 0)**2; }
    if (nA === 0 || nB === 0) return 0;
    return dot / (Math.sqrt(nA) * Math.sqrt(nB));
}

function timeDecayWeight(ts, now) { return Math.exp(-(now - ts) / (1000 * 60 * 60 * 24 * 90)); }

function aggregateCandles(candles, factor) {
    const result = [];
    for (let i = 0; i < candles.length; i += factor) {
        const chunk = candles.slice(i, i + factor);
        if (chunk.length < factor) continue;
        result.push({ o: chunk[0].o, h: Math.max(...chunk.map(c => c.h)), l: Math.min(...chunk.map(c => c.l)), c: chunk[chunk.length - 1].c });
    }
    return result;
}

function precalcATR(candles, period = 14) {
    let atrs = new Array(candles.length).fill(0);
    if (candles.length < period + 1) return atrs;
    let trs = [0], sum = 0;
    for (let i = 1; i < candles.length; i++) {
        trs.push(Math.max(candles[i].h - candles[i].l, Math.abs(candles[i].h - candles[i-1].c), Math.abs(candles[i].l - candles[i-1].c)));
    }
    for (let i = 1; i <= period; i++) sum += trs[i];
    atrs[period] = sum / period;
    for (let i = period + 1; i < candles.length; i++) {
        sum = sum - trs[i - period] + trs[i];
        atrs[i] = sum / period;
    }
    return atrs;
}

function getRecommendedTimeData(tfLabel, serverTime) {
    const now = new Date(serverTime);
    let minutesToAdd = (tfLabel === '5M') ? 5 : (tfLabel === '15M') ? 15 : (tfLabel === '30M') ? 30 : (tfLabel === '1H') ? 60 : 0;
    if (minutesToAdd === 0) return null;
    const msPerInterval = minutesToAdd * 60 * 1000;
    const startTs = Math.ceil(now.getTime() / msPerInterval) * msPerInterval;
    const format = (d) => d.toLocaleTimeString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour: '2-digit', minute: '2-digit', hour12: false });
    return { text: `${format(new Date(startTs))} - ${format(new Date(startTs + msPerInterval))}`, startTs: startTs, endTs: startTs + msPerInterval };
}

function runAnalysisElite(candles) {
    if (candles.length < 100) return null;
    const recentCandles = candles.slice(-50);
    if (marketEntropy(recentCandles) > 0.998 || Math.abs(calculateZScore(recentCandles.map(c => c.c))) > 2.5) return null;

    const regime = detectRegime(candles);
    const atrs = precalcATR(candles);
    const currentATR = atrs[candles.length - 2] || 1;
    const targetVector = buildPatternVector(candles, atrs, candles.length - 2);

    let matches = [], bucketCount = {};
    for (let i = Math.max(40, candles.length - 10000); i < candles.length - 11; i++) {
        const sim = cosineSimilarity(targetVector, buildPatternVector(candles, atrs, i));
        if (sim < 0.80) continue;
        const ts = Date.now() - ((candles.length - i) * 5 * 60 * 1000);
        const bucket = Math.floor(ts / (1000 * 60 * 60));
        bucketCount[bucket] = (bucketCount[bucket] || 0) + 1;
        if (bucketCount[bucket] > 3) continue;
        const next = candles[i + 1];
        if (!next) continue;
        matches.push({ sim, win: (next.h - candles[i].c > candles[i].c - next.l) ? 1 : 0, timestamp: ts });
    }

    if (matches.length < 15) return null;
    matches.sort((a,b) => b.sim - a.sim);
    const topMatches = matches.slice(0, Math.min(250, Math.max(15, Math.floor(matches.length * 0.10))));
    
    let wins = 0, totalWeight = 0;
    const now = Date.now();
    for (const m of topMatches) {
        const weight = Math.pow(m.sim, 2) * timeDecayWeight(m.timestamp, now);
        totalWeight += weight;
        if (m.win === 1) wins += weight;
    }

    let prob = ((wins + 1) / (totalWeight + 2)) * 100;
    prob = prob * (currentATR > (atrs.reduce((a,b)=>a+b,0)/atrs.length) * 2 ? 0.9 : 1);

    const size = Math.floor(topMatches.length / 3);
    let segs = [];
    for (let j = 0; j < 3; j++) {
        const s = topMatches.slice(j * size, (j + 1) * size);
        segs.push(s.reduce((a, b) => a + b.win, 0) / s.length);
    }
    const meanWR = segs.reduce((a,b)=>a+b,0) / 3;
    const stability = Math.max(0, 1 - (Math.sqrt(segs.reduce((s, w) => s + Math.pow(w - meanWR, 2), 0) / 3) / (meanWR || 1)));

    const signal = prob > 50 ? "BUY" : "SELL";
    const finalProb = signal === "BUY" ? prob : 100 - prob;
    const edge = finalProb - CONFIG.BE;

    if (edge < 1.2 || (regime === "COMPRESSION" && edge < 6) || (regime === "TREND" && edge < 0.3)) return null;

    return { prob: finalProb, direction: signal, edge: (signal === 'BUY' ? edge : -edge), absEdge: edge, stability, n: topMatches.length, acs: (edge / 50) * stability * (topMatches.length / 100), cwev: edge * stability, currentPrice: candles[candles.length - 1].c, currentATR: currentATR };
}

function makeProgressBar(percent) {
    const f = Math.min(10, Math.max(0, Math.round(percent / 10)));
    return '█'.repeat(f) + '░'.repeat(10 - f);
}

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 6: ORQUESTADOR GLOBAL (SCAN)
// ═══════════════════════════════════════════════════════════════════

async function globalScan(scanType = 'auto') {
    const currentServerTime = getSyncedTime();

    if (currentServerTime < globalPauseUntil) return;
    if (scanType === 'auto' && Date.now() - lastSignalTime < SIGNAL_COOLDOWN) return;

    const m = new Date(currentServerTime).getMinutes();
    let parsedData = [];

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

                const finished = parsedData.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA').slice(-50);
                if (finished.length > 0) {
                    const wins = finished.filter(r => r.Veredicto === 'GANADA').length;
                    autoLearningStats = { winrate: wins / finished.length, total: finished.length };
                    if (autoLearningStats.winrate > 0.55) MIN_AI_SCORE = 60;
                    else if (autoLearningStats.winrate < 0.48) MIN_AI_SCORE = 75;
                    else MIN_AI_SCORE = 68;
                }

                const totalFinishedTrades = parsedData.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA').length;
                const globalLastTrades = parsedData.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA').slice(-4);
                
                if (globalLastTrades.length > 0 && globalLastTrades[globalLastTrades.length - 1].Veredicto === 'GANADA') circuitBreakerLevel = 0;
                if (globalLastTrades.length === 4 && globalLastTrades.every(r => r.Veredicto === 'PERDIDA') && totalFinishedTrades > lastCircuitBreakerTradeCount) {
                    lastCircuitBreakerTradeCount = totalFinishedTrades;
                    const cooldownMinutes = 30 * Math.pow(2, Math.min(circuitBreakerLevel, 2));
                    globalPauseUntil = currentServerTime + (cooldownMinutes * 60 * 1000);
                    circuitBreakerLevel++;
                    bot.sendMessage(chatId, `🚨 *CIRCUIT BREAKER ACTIVADO (Nivel ${circuitBreakerLevel})*\nPausa de ${cooldownMinutes} minutos.`, { parse_mode: 'Markdown' });
                    return; 
                }
            }
        }
    } catch (e) {}

    const resolvedRows = parseCSVResolved();
    ADAPTIVE_PROFILES = buildAdaptiveProfiles(resolvedRows);
    CURRENT_WINNING_ZONES = buildWinningZonesFromCSV(parsedData);
    const validSignals = [];
    const candlesByAsset = {};

    let tfs = [{ tf: '5M', aggregate: 1 }]; 
    if (scanType === 'auto') {
        if ([13, 28, 43, 58].includes(m)) tfs.push({ tf: '15M', aggregate: 3 });
        if ([28, 58].includes(m)) tfs.push({ tf: '30M', aggregate: 6 });
        if (m === 58) tfs.push({ tf: '1H', aggregate: 12 });
    } else {
        tfs = [{ tf: '5M', aggregate: 1 }, { tf: '15M', aggregate: 3 }, { tf: '30M', aggregate: 6 }, { tf: '1H', aggregate: 12 }];
    }

    for (const asset of CONFIG.MARKETS) {
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

            let obi = 0;
            try {
                const lobRes = await axios.get(`${CONFIG.BINANCE_API}/depth?symbol=${asset.symbolBinance}&limit=5`);
                if(lobRes.data.bids && lobRes.data.asks && lobRes.data.bids.length > 0) {
                    const midPrice = (parseFloat(lobRes.data.bids[0][0]) + parseFloat(lobRes.data.asks[0][0])) / 2;
                    let bV=0, aV=0;
                    lobRes.data.bids.forEach(l => { bV += parseFloat(l[1]) * (1 / Math.max(Math.abs(midPrice - parseFloat(l[0])) / midPrice, 0.0001)); });
                    lobRes.data.asks.forEach(l => { aV += parseFloat(l[1]) * (1 / Math.max(Math.abs(midPrice - parseFloat(l[0])) / midPrice, 0.0001)); });
                    if (bV + aV > 0) obi = (bV - aV) / (bV + aV);
                }
            } catch(e) {}

            for (const item of tfs) {
                const res = runAnalysisElite(item.aggregate > 1 ? aggregateCandles(historical, item.aggregate) : historical);
                if (res) validSignals.push({ assetId: asset.id, symbolBinance: asset.symbolBinance, tf: item.tf, analysis: res, obi });
            }
        } catch(e) {}
    }

    const consensusSignals = [];
    for (const s of validSignals) {
        const isB = s.analysis.direction === "BUY";
        s.analysis.finalScore = calculateFinalScore(s.analysis, s.assetId);

        // --- FILTRO ADAPTATIVO CONTEXTUAL ---
        const adaptiveResult = applyAdaptiveFilter({ ...s, finalScore: s.analysis.finalScore });
        if (!adaptiveResult.pass) continue; // Si no pasa el score ajustado, lo descartamos
        
        s.adaptiveScore = adaptiveResult.adjustedScore;
        const currentProbThreshold = getDynamicThreshold(s.analysis.currentATR, s.analysis.currentPrice);

        // Aceptamos basado en el Adaptive Score en lugar del score rígido viejo
        let isElite = s.adaptiveScore >= 0.20 && s.analysis.prob >= currentProbThreshold && s.analysis.stability >= 0.40;
        let isAggressive = s.adaptiveScore >= 0.15 && s.analysis.prob >= (currentProbThreshold - 2) && s.analysis.stability >= 0.35;

        if (isElite || isAggressive) {
            s.isElite = isElite;
            s.isAggressive = isAggressive;
            consensusSignals.push(s);
        }
    }

    if (consensusSignals.length > 0) {
        lastSignalTime = Date.now();
        consensusSignals.sort((a,b) => b.adaptiveScore - a.adaptiveScore);
        
        for (const s of consensusSignals) {
            const sigId = `sig_${signalCounter++}`;
            SIGNAL_CACHE.set(sigId, s);
            
            const dualScore = (s.isElite ? 0.6 : 0) + (s.isAggressive ? 0.4 : 0);
            let scoreVisual = dualScore === 1.0 ? "1.0 → PERFECTO" : dualScore === 0.6 ? "0.6 → SOLO ELITE" : "0.4 → SOLO AGRESIVO";
            let modeString = dualScore === 1.0 ? "ELITE+AGRESIVO" : dualScore === 0.6 ? "ELITE" : "AGRESIVO";
            
            const confBar = makeProgressBar(s.analysis.prob); 
            const anticipationLevel = s.isAggressive ? 90 : 30;
            const antBar = makeProgressBar(anticipationLevel);

            // 💰 TRADING GENERADO ANTES DE LA IA
            const baseTradeData = calculateTradeLevels(s.analysis.currentPrice, s.analysis.direction, s.analysis.currentATR, s.analysis.absEdge, "CONTINUATION", circuitBreakerLevel, s.analysis.stability);
            let tradingText = "";
            if (baseTradeData) {
                tradingText = `\n\n💰 *MODO TRADING (Spot/CFD x20)*\n📍 *Entry:* ${baseTradeData.entry} | 🛑 *SL:* ${baseTradeData.sl} | 🎯 *TP:* ${baseTradeData.tp}\n⚖️ *R:R:* 1:${baseTradeData.rr} | 💼 *Volumen sugerido:* ${baseTradeData.positionSize} USDT | 📉 *Riesgo:* ${baseTradeData.riskPercent}%`;
            }

            // 🎯 TARJETA VISUAL COMPLETA CON TRADING
            const msgText = `🎯 *${s.assetId} | ${s.tf}*\n🧠 *MODO DUAL*\n🟢 ELITE    → ${s.isElite ? '✅' : '❌'}\n🟡 AGRESIVO → ${s.isAggressive ? '✅' : '❌'}\n\n🏆 *DUAL SCORE:* ${scoreVisual}\n📈 *Dirección:* ${s.analysis.direction === 'BUY' ? '🟢' : '🔴'} *${s.analysis.direction}*\n\n⚖️ *BALANCE*\nConfianza:    ${confBar} (${s.analysis.prob.toFixed(0)}%)\nAnticipación: ${antBar} (${anticipationLevel}%)${tradingText}\n\n*(Presioná el botón de abajo para Análisis IA)*`;

            s.msgText = msgText; // Guardamos el texto base para la IA
            s.modeString = modeString;

            const opts = {
                parse_mode: 'Markdown',
                reply_markup: { inline_keyboard: [[ { text: "🧠 Análisis IA de Contexto", callback_data: `ia_${sigId}` } ]] }
            };

            const sentMsg = await bot.sendMessage(chatId, msgText, opts);

            const timeData = getRecommendedTimeData(s.tf, currentServerTime);
            PENDING_AUDITS.push({
                sigId, assetId: s.assetId, symbolBinance: s.symbolBinance, tf: s.tf, direction: s.analysis.direction,
                startTs: timeData.startTs, endTs: timeData.endTs, messageId: sentMsg.message_id, retries: 0,
                logData: {
                    prob: s.analysis.prob.toFixed(1), lob: s.obi.toFixed(3), edge: s.analysis.edge.toFixed(2), 
                    alpha: s.analysis.acs.toFixed(3), stab: (s.analysis.stability * 100).toFixed(0), cwev: s.analysis.cwev.toFixed(1),
                    samples: s.analysis.n, iaVerdict: 'NO_SOLICITADO', iaScore: 0, iaContext: 'N/A', 
                    mode: modeString, finalScore: s.adaptiveScore.toFixed(2)
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
        try {
            await bot.editMessageText(originalMsg + '\n\n⏳ _Consultando a Gemini AI..._', { chat_id: query.message.chat.id, message_id: query.message.message_id, parse_mode: 'Markdown' });
        } catch(e) {}

        const assetContext = getAssetLearningContext(s.assetId);
        
        const promptText = `Eres un Quant Trader. AI_SCORE (0-100), TRADE_CONTEXT (CONTINUATION, REVERSAL, TRAP) y REASONING en 2 oraciones. 
        Activo: ${s.assetId} | Dir: ${s.analysis.direction} | Prob: ${s.analysis.prob.toFixed(1)}% | Edge: ${s.analysis.absEdge.toFixed(2)}% | LOB: ${s.obi.toFixed(3)} | FinalScore: ${s.adaptiveScore.toFixed(2)}.
        HISTORIAL APRENDIDO: ${assetContext}`;

        try {
            const result = await axios.post(`https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key=${process.env.GEMINI_API_KEY}`, { contents: [{ parts: [{ text: promptText }] }], generationConfig: { temperature: 0.2 } });
            const text = result.data.candidates[0].content.parts[0].text;
            
            let iaScore = 0, iaCtx = "UNKNOWN", iaReason = "Analizado.";
            const scoreMatch = text.match(/AI_SCORE:\s*(\d+)/i); 
            if (scoreMatch) {
                // Aplicamos penalidad de perfil a la respuesta de la IA
                const aiPenalty = ASSET_PROFILES[s.assetId] ? ASSET_PROFILES[s.assetId].aiPenalty : 0;
                iaScore = parseInt(scoreMatch[1]) + aiPenalty; 
            }
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

// === COMANDOS EXTRA RESTAURADOS (Stats y Perfil) ===
bot.onText(/\/profile/, async (msg) => {
    if (msg.chat.id.toString() === chatId) {
        const resolvedRows = parseCSVResolved();
        const profiles = buildAdaptiveProfiles(resolvedRows);
        let text = '📊 *PERFILES ADAPTATIVOS*\n';
        for (const [assetId, p] of Object.entries(profiles)) {
            if (p.totalTrades < 5) continue;
            text += `\n*${assetId}* (${p.totalTrades}t, WR: ${p.overallWR.toFixed(1)}%) | Pen. IA: ${p.aiPenalty}`;
            text += `\nCWEV<${p.maxCWEV} | Alpha<${p.maxAlpha.toFixed(3)} | |Edge|<${p.maxAbsEdge}`;
            text += `\nTFs: ${p.preferredTFs.join(', ')} | ${p.confidence}`;
            if (p.lossPatterns.length > 0) text += `\n⚠️ ${p.lossPatterns[0]}`;
            text += '\n';
        }
        bot.sendMessage(chatId, text, { parse_mode: 'Markdown' });
    }
});

bot.onText(/\/stats/, async (msg) => {
    if (msg.chat.id.toString() === chatId) {
        const resolved = parseCSVResolved();
        if (resolved.length === 0) return bot.sendMessage(chatId, '📊 Sin trades resueltos aún.');
        const totalW = resolved.filter(r => r._win === 1).length;
        const totalL = resolved.filter(r => r._win === 0).length;
        let text = `📊 *ESTADÍSTICAS GLOBALES*\n\nTotal: ${resolved.length} | ✅ ${totalW} | ❌ ${totalL}\nWR Global: ${(totalW / resolved.length * 100).toFixed(1)}%\n\n`;
        for (const market of CONFIG.MARKETS) {
            const ar = resolved.filter(r => r.Activo === market.id);
            if (ar.length === 0) continue;
            const w = ar.filter(r => r._win === 1).length;
            const wr = (w / ar.length * 100).toFixed(1);
            text += `${market.id}: ${makeProgressBar(parseFloat(wr))} ${wr}% (${ar.length})\n`;
        }
        bot.sendMessage(chatId, text, { parse_mode: 'Markdown' });
    }
});

// === CRON DINÁMICO REPARADO (CON PRECISIÓN SNIPER) ===
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
    // Precisión Sniper + Timeframes correctos
    if ((now.getMinutes() % 5 === 3) && Math.abs(now.getSeconds() - targetSecond) <= 1) globalScan('auto'); 
}, 1000);

// === CRON DE AUDITORÍA RESTAURADO ===
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
