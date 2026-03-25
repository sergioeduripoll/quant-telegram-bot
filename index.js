require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const fs = require('fs');

// ═══════════════════════════════════════════════════════════════════
// QUANT SNIPER V13.2 — ADAPTIVE TACTICAL PRO (Con Live Learning)
// ═══════════════════════════════════════════════════════════════════

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

const ASSET_PROFILES = {
    "ETH/USD": { minScoreMultiplier: 1.3, strictLOB: true, aiPenalty: -5 }, 
    "BTC/USD": { minScoreMultiplier: 1.1, strictLOB: false, aiPenalty: 0 }, 
    "DOGE/USD": { minScoreMultiplier: 0.9, strictLOB: false, aiPenalty: 5 }, 
    "XRP/USD": { minScoreMultiplier: 0.9, strictLOB: false, aiPenalty: 5 },  
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

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 0: LIVE LEARNING MEMORY (Memoria Viva)
// ═══════════════════════════════════════════════════════════════════
const LEARNING_MEMORY_FILE = './learning_memory.json';
let liveLearningState = {};

function loadLearningMemory() {
    if (fs.existsSync(LEARNING_MEMORY_FILE)) {
        try {
            liveLearningState = JSON.parse(fs.readFileSync(LEARNING_MEMORY_FILE, 'utf8'));
        } catch(e) { console.error("[SYS] Error cargando memoria viva:", e); }
    }
}
loadLearningMemory();

function saveLearningMemory() {
    fs.writeFileSync(LEARNING_MEMORY_FILE, JSON.stringify(liveLearningState, null, 2));
}

// Crea un hash único para agrupar trades con contextos muy similares
function getPatternKey(cwev, alpha, edge, tf, lob, momentum) {
    const cwevBin = Math.floor(cwev);
    const alphaBin = (Math.floor(alpha * 100) / 100).toFixed(2);
    const edgeBin = Math.floor(Math.abs(edge) / 2) * 2; 
    const lobDir = lob > 0 ? 'POS' : 'NEG';
    const momDir = momentum > 0 ? 'POS' : 'NEG';
    return `${tf}_C${cwevBin}_A${alphaBin}_E${edgeBin}_L${lobDir}_M${momDir}`;
}

function updateLearningMemory(tradeResult) {
    const { asset, isWin, cwev, alpha, edge, tf, lob, momentum } = tradeResult;

    if (!liveLearningState[asset]) {
        liveLearningState[asset] = { totalTrades: 0, wins: 0, losses: 0, lossStreak: 0, patterns: {} };
    }

    const state = liveLearningState[asset];
    state.totalTrades++;
    
    if (isWin) {
        state.wins++;
        state.lossStreak = 0; // Corta la racha negativa
    } else {
        state.losses++;
        state.lossStreak++; // Aumenta la racha negativa
    }

    state.winRate = state.wins / state.totalTrades;

    const patKey = getPatternKey(cwev, alpha, edge, tf, lob, momentum);
    if (!state.patterns[patKey]) state.patterns[patKey] = { wins: 0, losses: 0, total: 0 };
    
    state.patterns[patKey].total++;
    if (isWin) state.patterns[patKey].wins++;
    else state.patterns[patKey].losses++;

    saveLearningMemory(); // Persistir
}

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 1: SISTEMA DE APRENDIZAJE ADAPTATIVO HISTÓRICO (CSV)
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
            aiPenalty: 0,
            currentLossStreak: 0,
            avgLossStats: { cwev: 999, alpha: 999, absEdge: 999, stab: 0 }
        };

        if (assetRows.length < 5) {
            profiles[assetId] = profile;
            continue;
        }

        profile.confidence = assetRows.length >= 25 ? 'HIGH' : 'MEDIUM';
        if (profile.overallWR < 45) profile.aiPenalty = -5;
        if (profile.overallWR > 55) profile.aiPenalty = 5;

        let streak = 0;
        for (let i = assetRows.length - 1; i >= 0; i--) {
            if (assetRows[i]._win === 0) streak++;
            else break;
        }
        profile.currentLossStreak = streak;

        const losses = assetRows.filter(r => r._win === 0);
        const wins = assetRows.filter(r => r._win === 1);

        if (losses.length >= 3) {
            profile.avgLossStats = {
                cwev: losses.reduce((s, r) => s + r._cwev, 0) / losses.length,
                alpha: losses.reduce((s, r) => s + r._alpha, 0) / losses.length,
                absEdge: losses.reduce((s, r) => s + Math.abs(r._edge), 0) / losses.length,
                stab: losses.reduce((s, r) => s + r._stab, 0) / losses.length
            };
        }

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

        const tfScores = {};
        for (const tf of ['5M', '15M', '30M', '1H']) {
            const sub = assetRows.filter(r => r.TF === tf);
            const { wr, n } = calcWR(sub);
            if (wr !== null && n >= 5) { tfScores[tf] = { wr, n, score: wr * Math.log(n + 1) }; }
        }
        const goodTFs = Object.entries(tfScores).filter(([_, v]) => v.wr >= 45).sort((a, b) => b[1].score - a[1].score).map(([tf]) => tf);
        if (goodTFs.length > 0) profile.preferredTFs = goodTFs;

        if (losses.length >= 5 && wins.length >= 5) {
            const avgLoss = profile.avgLossStats;
            const avgWin = {
                cwev: wins.reduce((s, r) => s + r._cwev, 0) / wins.length,
                alpha: wins.reduce((s, r) => s + r._alpha, 0) / wins.length,
                absEdge: wins.reduce((s, r) => s + Math.abs(r._edge), 0) / wins.length
            };
            if (avgLoss.cwev > avgWin.cwev * 1.1) profile.lossPatterns.push(`CWEV alto en losses`);
            if (avgLoss.alpha > avgWin.alpha * 1.1) profile.lossPatterns.push(`Alpha excesivo en losses`);
            if (avgLoss.absEdge > avgWin.absEdge * 1.15) profile.lossPatterns.push(`Edge extremo recurrente`);
        }

        const buyRows = assetRows.filter(r => r.Dir === 'BUY'), sellRows = assetRows.filter(r => r.Dir === 'SELL');
        const buyAligned = buyRows.filter(r => r._lob > 0), sellAligned = sellRows.filter(r => r._lob < 0);
        const buyAlignedWR = calcWR(buyAligned), sellAlignedWR = calcWR(sellAligned);
        if (buyAlignedWR.wr !== null && sellAlignedWR.wr !== null) {
            profile.lobBias = ((buyAlignedWR.wr + sellAlignedWR.wr) / 2) - profile.overallWR;
        }
        profiles[assetId] = profile;
    }
    return profiles;
}

// 🧠 NUEVO CEREBRO: Combina Historical (CSV) con Live Learning (RAM)
function evaluateHiddenRisk(signal, profile) {
    let riskScore = 0;

    // --- 1. MEMORIA A LARGO PLAZO (CSV Histórico) ---
    if (profile.overallWR < 50) riskScore += (50 - profile.overallWR);
    if (profile.currentLossStreak > 0) riskScore += (profile.currentLossStreak * 10);

    let patternMatches = 0;
    if (signal.analysis.cwev >= profile.avgLossStats.cwev * 0.9) patternMatches++;
    if (signal.analysis.acs >= profile.avgLossStats.alpha * 0.9) patternMatches++;
    if (Math.abs(signal.analysis.edge) >= profile.avgLossStats.absEdge * 0.9) patternMatches++;
    
    if (patternMatches >= 2) riskScore += 15; 
    if (patternMatches === 3) riskScore += 15; 

    const isB = signal.analysis.direction === "BUY";
    if ((isB && signal.obi < -0.15) || (!isB && signal.obi > 0.15)) riskScore += 15; 
    if ((isB && signal.momentumSlope < -0.0005) || (!isB && signal.momentumSlope > 0.0005)) riskScore += 10; 

    // --- 2. MEMORIA A CORTO PLAZO VIVA (Live Learning) ---
    const liveState = liveLearningState[signal.assetId];
    if (liveState) {
        // Ponderación extrema a la racha perdedora reciente en vivo
        if (liveState.lossStreak >= 3) {
            riskScore += (liveState.lossStreak * 15); // Ej: 3 seguidas malas = +45 Riesgo (Casi bloqueo seguro)
        } else if (liveState.lossStreak === 2) {
            riskScore += 20;
        }

        // Evaluación del patrón específico de este trade en vivo
        const patKey = getPatternKey(signal.analysis.cwev, signal.analysis.acs, signal.analysis.edge, signal.tf, signal.obi, signal.momentumSlope);
        const recentPattern = liveState.patterns[patKey];
        
        if (recentPattern && recentPattern.total >= 2) {
            const patWR = recentPattern.wins / recentPattern.total;
            if (patWR < 0.40) {
                riskScore += 30; // El bot acaba de perder operando algo muy parecido a esto
            } else if (patWR > 0.60) {
                riskScore -= 15; // Patrón en racha ganadora, reducimos riesgo
            }
        }
        
        if (liveState.totalTrades >= 5 && liveState.winRate < 0.45) riskScore += 20;
    }

    return Math.max(0, Math.min(riskScore, 100)); // Cap entre 0 y 100
}

function applyAdaptiveFilter(signal) {
    const profile = ADAPTIVE_PROFILES[signal.assetId];
    if (!profile || profile.confidence === 'LOW') return { pass: true, reason: 'Sin datos suficientes', adjustedScore: signal.finalScore };

    // Evalúa Riesgo Oculto (Combinando CSV + Memoria Viva)
    const riskScore = evaluateHiddenRisk(signal, profile);
    
    // 🛑 REGLAS DE DECISIÓN ESTRICTAS
    if (riskScore > 60) {
        return { pass: false, reason: `Alto Riesgo Oculto (${riskScore.toFixed(0)}/100)`, adjustedScore: 0, penalty: 1 };
    }

    const reasons = [];
    let penalty = 0;

    // ⚠️ PENALIZACIÓN MODERADA
    if (riskScore >= 40 && riskScore <= 60) {
        reasons.push(`Riesgo Mod. (${riskScore.toFixed(0)})`);
        penalty += (riskScore / 200); 
    }

    if (signal.analysis.cwev >= profile.maxCWEV) { reasons.push(`CWEV>=${profile.maxCWEV}`); penalty += 0.2; }
    if (signal.analysis.acs >= profile.maxAlpha) { reasons.push(`Alpha>=${profile.maxAlpha.toFixed(3)}`); penalty += 0.15; }
    if (Math.abs(signal.analysis.edge) >= profile.maxAbsEdge) { reasons.push(`|Edge|>=${profile.maxAbsEdge}`); penalty += 0.15; }
    if (!profile.preferredTFs.includes(signal.tf)) { reasons.push(`TF no preferido`); penalty += 0.25; }

    const adjustedScore = signal.finalScore * Math.max(0, (1 - penalty));
    const pass = adjustedScore >= 0.15;
    
    return { pass, reason: reasons.length > 0 ? reasons.join(' | ') : 'OK', adjustedScore, penalty };
}

function getAssetLearningContext(assetId) {
    const profile = ADAPTIVE_PROFILES[assetId];
    if (!profile || profile.confidence === 'LOW') return 'Sin datos históricos suficientes.';

    let ctx = `Histórico ${assetId} (${profile.totalTrades}t, WR: ${profile.overallWR.toFixed(1)}%):\n`;
    ctx += `- Límites Óptimos: CWEV<${profile.maxCWEV}, Alpha<${profile.maxAlpha}, |Edge|<${profile.maxAbsEdge}\n`;
    
    const live = liveLearningState[assetId];
    if (live) {
        ctx += `\n🚨 Memoria Viva (Corto Plazo):\n- Racha Actual: ${live.lossStreak} pérdidas seguidas.\n- WR Reciente: ${(live.winRate*100).toFixed(1)}% en ${live.totalTrades} trades.\n`;
    }

    if (profile.lossPatterns.length > 0) ctx += `- Patrones de pérdida: ${profile.lossPatterns.join('; ')}\n`;
    return ctx;
}

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 2: SINCRONIZACIÓN Y LOGGING (Mantenido intacto)
// ═══════════════════════════════════════════════════════════════════

let timeOffset = 0;
async function syncTimeWithBinance() {
    try {
        const res = await axios.get(`${CONFIG.BINANCE_API}/time`);
        timeOffset = res.data.serverTime - Date.now();
    } catch(e) { console.error("[SYS] Error sinc."); }
}
syncTimeWithBinance();
setInterval(syncTimeWithBinance, 60 * 60 * 1000); 

function getSyncedTime() { return Date.now() + timeOffset; }
function getLocalTime() { return new Date(getSyncedTime()).toLocaleTimeString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour12: false }); }

function logToCSV(data) {
    const filePath = './auditoria_sniper.csv';
    const now = new Date(getSyncedTime());
    if (!fs.existsSync(filePath)) {
        const header = 'Fecha,Hora,Activo,TF,Dir,Prob,LOB,Edge,Alpha,Stability,CWEV,Samples,Veredicto,Open,Close,IA_Verdict,IA_Score,IA_Context,Mode,FinalScore\n';
        fs.writeFileSync(filePath, header);
    }
    const row = `${now.toLocaleDateString('es-AR')},${now.toLocaleTimeString('es-AR', { hour12: false })},${data.asset || ''},${data.tf || ''},${data.dir || ''},${data.prob || ''},${data.lob || ''},${data.edge || ''},${data.alpha || ''},${data.stab || ''},${data.cwev || ''},${data.samples || ''},${data.veredicto || ''},${data.open || ''},${data.close || ''},${data.iaVerdict || ''},${data.iaScore || ''},${data.iaContext || ''},${data.mode || ''},${data.finalScore || ''}\n`;
    fs.appendFileSync(filePath, row);
}

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 3: FUNCIONES DE TRADING (CFDs / QUANTFURY)
// ═══════════════════════════════════════════════════════════════════

function formatPrice(val) { return val < 1 ? val.toFixed(4) : val.toFixed(2); }

function calculateFinalScore(analysis, assetId) {
    const { edge, stability, n, prob } = analysis;
    const sampleFactor = Math.log(n + 1) / 5; 
    const probFactor = (prob - 50) / 50; 
    let baseScore = (Math.abs(edge) * stability * sampleFactor * probFactor);
    const multiplier = ASSET_PROFILES[assetId] ? ASSET_PROFILES[assetId].minScoreMultiplier : 1.0;
    return baseScore / multiplier; 
}

function calculateTradeLevels(price, direction, atr, edge, iaContext, cbLevel, stability, iaScore = null) {
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
// MÓDULO 4: MOTOR CUANTITATIVO (Mantenido intacto)
// ═══════════════════════════════════════════════════════════════════

function calculateZScore(values) {
    if(values.length === 0) return 0;
    const mean = values.reduce((a,b)=>a+b, 0) / values.length;
    const std = Math.sqrt(values.reduce((s,v)=>s+Math.pow(v-mean, 2), 0) / values.length) || 1;
    return (values[values.length-1] - mean) / std;
}
function marketEntropy(candles) {
    if(candles.length === 0) return 0;
    let up = 0, down = 0;
    for(let c of candles) { if(c.c > c.o) up++; else down++; }
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
        const c = candles[k], prevC = candles[k-1] || c, currentATR = atrs[k] || 1, prev5ATR = atrs[k-5] || currentATR;
        vec.push((c.c - c.o) / currentATR, (c.h - c.l) / currentATR, (c.h - Math.max(c.o, c.c)) / currentATR, (Math.min(c.o, c.c) - c.l) / currentATR, c.c > c.o ? 1 : -1, (currentATR - prev5ATR) / currentATR, (c.c - prevC.c) / currentATR);
    }
    return vec;
}
function cosineSimilarity(a, b) {
    let dot = 0, normA = 0, normB = 0;
    for (let i = 0; i < a.length; i++) { dot += a[i] * (b[i] || 0); normA += a[i] * a[i]; normB += (b[i] || 0) * (b[i] || 0); }
    if (normA === 0 || normB === 0) return 0;
    return dot / (Math.sqrt(normA) * Math.sqrt(normB));
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
    for (let i = 1; i < candles.length; i++) { trs.push(Math.max(candles[i].h - candles[i].l, Math.abs(candles[i].h - candles[i-1].c), Math.abs(candles[i].l - candles[i-1].c))); }
    for (let i = 1; i <= period; i++) sum += trs[i];
    atrs[period] = sum / period;
    for (let i = period + 1; i < candles.length; i++) { sum = sum - trs[i - period] + trs[i]; atrs[i] = sum / period; }
    return atrs;
}
function getRecommendedTimeData(tfLabel, serverTime) {
    const now = new Date(serverTime);
    let minutesToAdd = (tfLabel === '5M') ? 5 : (tfLabel === '15M') ? 15 : (tfLabel === '30M') ? 30 : (tfLabel === '1H') ? 60 : 0;
    if (minutesToAdd === 0) return null;
    const currentMs = now.getTime();
    const intervalMs = minutesToAdd * 60 * 1000;
    const startTs = Math.ceil(currentMs / intervalMs) * intervalMs;
    const format = (d) => d.toLocaleTimeString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour: '2-digit', minute: '2-digit', hour12: false });
    return { text: `${format(new Date(startTs))} - ${format(new Date(startTs + intervalMs))}`, startTs: startTs, endTs: startTs + intervalMs };
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
    
    let wins = 0, totalWeight = 0, now = Date.now();
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

    if (edge < 1.2 || (regime === "COMPRESSION" && edge < 6)) return null;

    return { prob: finalProb, direction: signal, edge: (signal === 'BUY' ? edge : -edge), absEdge: edge, stabilityRaw: stability, n: topMatches.length, acs: (edge / 50) * stability * (topMatches.length / 100), cwev: edge * stability, currentPrice: candles[candles.length - 1].c, currentATR: currentATR };
}
function makeProgressBar(percent) { return '█'.repeat(Math.min(10, Math.max(0, Math.round(percent / 10)))) + '░'.repeat(10 - Math.min(10, Math.max(0, Math.round(percent / 10)))); }

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 5: COMANDOS
// ═══════════════════════════════════════════════════════════════════

bot.onText(/\/start/, (msg) => { if (msg.chat.id.toString() === chatId) bot.sendMessage(chatId, '👋 *Quant Sniper V13.2 ADAPTIVE* operativo.\n🧠 Memoria Viva + IA on-demand.', { parse_mode: 'Markdown' }); });
bot.onText(/\/scan/, async (msg) => { if (msg.chat.id.toString() === chatId) await globalScan('manual'); });

console.log(`🤖 Quant Sniper V13.2 ADAPTIVE iniciando...`);
bot.sendMessage(chatId, '🟢 *Quant Sniper V13.2 ADAPTIVE* encendido.\nModo: Trading Previo + IA a Demanda + Live Learning', { parse_mode: 'Markdown' });

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 6: ORQUESTADOR GLOBAL (SCAN)
// ═══════════════════════════════════════════════════════════════════

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

    const resolvedRows = parseCSVResolved();
    ADAPTIVE_PROFILES = buildAdaptiveProfiles(resolvedRows);

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
        s.finalScore = finalScore;
        s.momentumSlope = momentumSlope;

        // 🧠 APLICAR EL FILTRO ADAPTATIVO (CON LIVE LEARNING MEMORY)
        const adaptiveResult = applyAdaptiveFilter(s);
        if (!adaptiveResult.pass || !passRegime) continue;

        s.adaptiveScore = adaptiveResult.adjustedScore;

        let isElite = s.adaptiveScore >= 0.20 && s.analysis.prob >= currentProbThreshold && s.analysis.stabilityRaw >= 0.40;
        let isAggressive = s.adaptiveScore >= 0.15 && s.analysis.prob >= (currentProbThreshold - 2) && s.analysis.stabilityRaw >= 0.35 && !((isB && momentumSlope < -0.0001) || (!isB && momentumSlope > 0.0001));

        if (isElite || isAggressive) {
            s.isElite = isElite; 
            s.isAggressive = isAggressive; 
            consensusSignals.push(s);
        }
    }
    
    consensusSignals.sort((a,b) => b.adaptiveScore - a.adaptiveScore);

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

            // 💰 TRADING GENERADO ANTES DE LA IA (Usamos CONTINUATION por defecto como caso base)
            const baseTradeData = calculateTradeLevels(s.analysis.currentPrice, s.analysis.direction, s.analysis.currentATR, s.analysis.absEdge, "CONTINUATION", circuitBreakerLevel, s.analysis.stabilityRaw);
            let tradingText = "";
            if (baseTradeData) {
                tradingText = `\n\n💰 *MODO TRADING (Spot/CFD x20)*\n📍 *Entry:* ${baseTradeData.entry} | 🛑 *SL:* ${baseTradeData.sl} | 🎯 *TP:* ${baseTradeData.tp}\n⚖️ *R:R:* 1:${baseTradeData.rr} | 💼 *Vol:* ${baseTradeData.positionSize} USDT | 📉 *Riesgo:* ${baseTradeData.riskPercent}%`;
            }

            // 🎯 TARJETA VISUAL LIMPIA
            const msgText = `🎯 *${s.assetId} | ${s.tf}*\n🧠 *MODO DUAL*\n🟢 ELITE    → ${s.isElite ? '✅' : '❌'}\n🟡 AGRESIVO → ${s.isAggressive ? '✅' : '❌'}\n\n🏆 *DUAL SCORE:* ${scoreVisual}\n📈 *Dirección:* ${icon} *${s.analysis.direction}*\n\n⚖️ *BALANCE*\nConfianza:    ${confBar} (${s.analysis.prob.toFixed(0)}%)\nAnticipación: ${antBar} (${anticipationLevel}%)${tradingText}\n\n*(Presioná el botón de abajo para Análisis IA)*`;

            s.msgText = msgText;
            s.modeString = dualScore === 1.0 ? "ELITE+AGRESIVO" : dualScore === 0.6 ? "ELITE" : "AGRESIVO";

            const opts = {
                parse_mode: 'Markdown',
                reply_markup: {
                    inline_keyboard: [[ { text: "🧠 Solicitar Análisis IA de Contexto", callback_data: `ia_${sigId}` } ]]
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
                    samples: s.analysis.n, momentum: s.momentumSlope.toFixed(4), iaVerdict: 'NO_SOLICITADO', iaScore: 0, iaContext: 'N/A', 
                    mode: s.modeString, finalScore: s.adaptiveScore.toFixed(2)
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
        const loadingText = '\n\n⏳ _Consultando a Gemini AI..._';
        try {
            await bot.editMessageText(originalMsg + loadingText, { chat_id: query.message.chat.id, message_id: query.message.message_id, parse_mode: 'Markdown' });
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

// ═══════════════════════════════════════════════════════════════════
// MÓDULO 7: CRONES DINÁMICOS (CON PRECISIÓN SNIPER Y ANTI-MISS)
// ═══════════════════════════════════════════════════════════════════

let lastScanExecution = 0; // Control de disparo único

setInterval(async () => { 
    let volSum = 0, count = 0;

    // Calculamos el pulso global del mercado con los datos en memoria
    for (const candles of GLOBAL_CANDLE_CACHE.values()) {
        const atrs = precalcATR(candles);
        const atr = atrs[atrs.length - 1];
        const price = candles[candles.length - 1].c;
        if (atr && price) { 
            volSum += atr / price; 
            count++; 
        }
    }

    const avgVol = count > 0 ? volSum / count : 0.002;
    
    // 🧠 Fallback inteligente: Si la caché está vacía al arrancar, apunta al segundo 15 seguro.
    const targetSecond = count > 0 ? getDynamicSecond(avgVol) : 15;
    
    const now = new Date(getSyncedTime()); 
    const minuteMatch = (now.getMinutes() % 5 === 3);
    
    // Ventana de 2 segundos para tolerar latencia del servidor
    const secondWindow = Math.abs(now.getSeconds() - targetSecond) <= 2;

    // 🚀 ANTI-DUPLICADO + ANTI-MISS
    if (minuteMatch && secondWindow) {
        const nowTs = Date.now();

        // Evita que la ventana de 2 segundos dispare el scan 2 o 3 veces seguidas
        if (nowTs - lastScanExecution > 10000) { 
            lastScanExecution = nowTs;
            console.log(`[SYS] 🚀 AUTO SCAN DISPARADO: ${now.toLocaleTimeString('es-AR')} (Target Sec: ${targetSecond})`);
            globalScan('auto'); 
        }
    }
}, 1000);

// 🚀 CRON DE AUDITORÍA Y APRENDIZAJE
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

                // 🧠 ALIMENTAR LA MEMORIA VIVA
                if (!isTie) {
                    updateLearningMemory({
                        asset: audit.assetId,
                        isWin: isWin,
                        cwev: parseFloat(audit.logData.cwev),
                        alpha: parseFloat(audit.logData.alpha),
                        edge: parseFloat(audit.logData.edge),
                        tf: audit.tf,
                        lob: parseFloat(audit.logData.lob),
                        momentum: parseFloat(audit.logData.momentum) || 0
                    });
                }
                
                PENDING_AUDITS.splice(i, 1);
            } catch (error) { audit.retries++; }
        }
    }
}, 15000);
