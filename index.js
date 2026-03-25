require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const fs = require('fs');

// === CONFIGURACIONES GLOBALES Y UMBRALES ===
const token = process.env.TELEGRAM_TOKEN;
const chatId = process.env.CHAT_ID;
const bot = new TelegramBot(token, { polling: true });

const patternLength = 6;
const PROB_THRESHOLD = 54; 

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

// 🟢 Control Anti-Ruina Progresivo y Cache Global (RESTAURADO)
let globalPauseUntil = 0; 
let circuitBreakerLevel = 0;
let lastCircuitBreakerTradeCount = 0;
const GLOBAL_CANDLE_CACHE = new Map(); 

// === SINCRONIZACIÓN DE TIEMPO PRO ===
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

function getSyncedTime() {
    return Date.now() + timeOffset;
}

function getLocalTime() {
    return new Date(getSyncedTime()).toLocaleTimeString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour12: false });
}

// === SISTEMA DE LOGGING CSV ===
function logToCSV(data) {
    const filePath = './auditoria_sniper.csv';
    const now = new Date(getSyncedTime());
    const fecha = now.toLocaleDateString('es-AR');
    const hora = now.toLocaleTimeString('es-AR', { hour12: false });

    if (!fs.existsSync(filePath)) {
        const header = 'Fecha,Hora,Activo,TF,Dir,Prob,LOB,Edge,Alpha,Stability,CWEV,Samples,Veredicto,Open,Close,IA_Verdict,IA_Score,IA_Context,Mode\n';
        fs.writeFileSync(filePath, header);
    }

    const row = `${fecha},${hora},${data.asset || ''},${data.tf || ''},${data.dir || ''},${data.prob || ''},${data.lob || ''},${data.edge || ''},${data.alpha || ''},${data.stab || ''},${data.cwev || ''},${data.samples || ''},${data.veredicto || ''},${data.open || ''},${data.close || ''},${data.iaVerdict || ''},${data.iaScore || ''},${data.iaContext || ''},${data.mode || ''}\n`;
    fs.appendFileSync(filePath, row);
}

// === FUNCIONES DE TRADING (RESTAURADO) ===
function formatPrice(val) {
    if (val < 0.01) return val.toFixed(6);
    if (val < 1) return val.toFixed(4);
    if (val < 100) return val.toFixed(3);
    return val.toFixed(2);
}

function calculateTradeLevels(price, direction, atr, edge, iaContext, cbLevel) {
    const capital = 1000; 
    const leverage = 20;  
    let riskPercent = cbLevel > 0 ? 1.0 : 2.0; 

    const maxRiskPercent = 0.5; 
    let slDistance = atr * 1.2;
    const maxDistance = price * (maxRiskPercent / 100);
    if (slDistance > maxDistance) slDistance = maxDistance;

    // R:R Dinámico basado en Edge (Absoluto) y Contexto de IA de Liquidez
    let rr = Math.max(1.2, Math.abs(edge) / 2);
    if (iaContext === "INSTITUTIONAL_SWEEP") rr *= 1.3;
    if (iaContext === "TREND_IMPULSE") rr *= 1.1;

    let entry = price;
    let sl, tp;

    if (direction === "BUY") {
        sl = entry - slDistance;
        tp = entry + (slDistance * rr);
    } else {
        sl = entry + slDistance;
        tp = entry - (slDistance * rr);
    }

    const minMove = price * 0.002;
    if (Math.abs(tp - entry) < minMove) return null; 

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
        riskPercent: riskPercent
    };
}

// === LÓGICA DINÁMICA (Aprendizaje del CSV) ===
function buildWinningZonesFromCSV(parsedData) {
    const zones = JSON.parse(JSON.stringify(DEFAULT_ZONES));
    if (!parsedData || parsedData.length === 0) return zones;

    const getRange = (arr) => {
        if (arr.length === 0) return null;
        arr.sort((a,b)=>a-b);
        return {
            min: arr[Math.floor(arr.length * 0.15)],
            max: arr[Math.floor(arr.length * 0.85)]
        };
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

// === COMANDOS ===
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

// === MÓDULO DE IA MANUAL A DEMANDA (RESTAURADO Y ALINEADO AL CSV) ===
bot.on('callback_query', async (query) => {
    if (query.data.startsWith('ai_')) {
        const sigId = query.data.replace('ai_', '');
        const cacheData = SIGNAL_CACHE.get(sigId);

        if (!cacheData) {
            bot.answerCallbackQuery(query.id, { text: "Señal expirada en caché.", show_alert: true });
            return;
        }

        bot.answerCallbackQuery(query.id, { text: "Consultando a Gemini IA..." });

        const { s, msgText, modeString, dualScore } = cacheData;
        const loadingMsg = msgText + `\n\n⏳ _Analizando con IA de Liquidez Institucional..._`;
        await bot.editMessageText(loadingMsg, { chat_id: query.message.chat.id, message_id: query.message.message_id, parse_mode: 'Markdown' }).catch(()=>{});

        const promptText = `Eres un Quant Trader Experto en Liquidez Institucional. Tu tarea es decidir el SCORE y clasificar el contexto basado en Smart Money.
        MODO: ${modeString} (Dual Score: ${dualScore.toFixed(1)}).
        
        DATOS:
        Activo: ${s.assetId} | Dir: ${s.analysis.direction} | Prob: ${s.analysis.prob.toFixed(1)}% | Edge: ${s.analysis.edge.toFixed(2)}%
        Stability: ${(s.analysis.stability * 100).toFixed(0)}% | LOB: ${s.obi.toFixed(3)} | Momentum: ${s.momentumSlope.toFixed(4)} | Macro 4H: ${s.macro.h4}
        
        REGLAS DE ÉXITO (BASADAS EN AUDITORÍA CSV):
        1. Activos Mayores (BTC, ETH, SOL, XRP, BNB, ADA): Funcionan como CONTRARIAN. El éxito ocurre con EDGE NEGATIVO. Si el Momentum parece ir en contra, es un barrido de minoristas.
        2. DOGE: Único que requiere EDGE POSITIVO y Momentum a favor.
        3. Estabilidad > 75% en ADA/ETH indica manipulación previa a fakeout.
        
        TAREA:
        1. Define el TRADE_CONTEXT: (INSTITUTIONAL_SWEEP, TREND_IMPULSE, o TRAP).
        2. Define el AI_SCORE (0 a 100): >65 significa EXECUTE.
        
        Responde EXCLUSIVAMENTE en este formato:
        AI_SCORE: (Número)
        TRADE_CONTEXT: (Contexto)
        REASONING: (Argumento táctico en 2 oraciones).`;

        try {
            const apiKey = process.env.GEMINI_API_KEY;
            const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key=${apiKey}`;
            const result = await axios.post(url, { contents: [{ parts: [{ text: promptText }] }], generationConfig: { temperature: 0.2 } });
            const text = result.data.candidates[0].content.parts[0].text;
            
            const scoreMatch = text.match(/AI_SCORE:\s*(\d+)/i);
            const iaScore = scoreMatch ? parseInt(scoreMatch[1]) : 0;
            const contextMatch = text.match(/TRADE_CONTEXT:\s*(\w+)/i);
            const iaContextText = contextMatch ? contextMatch[1].toUpperCase() : 'UNKNOWN';
            const reasonMatch = text.match(/REASONING:\s*([\s\S]*?)$/i);
            const iaReasoning = reasonMatch ? reasonMatch[1].trim().replace(/[*_[\]]/g, '') : 'Análisis completado.';
            
            const isExecute = iaScore >= 65;
            const verdictIcon = isExecute ? '🚀 *EXECUTE TRADE*' : '⛔ *PASS*';

            let tradingModuleText = "";
            if (isExecute && iaContextText !== "TRAP") {
                const tradeData = calculateTradeLevels(s.analysis.currentPrice, s.analysis.direction, s.analysis.currentATR, s.analysis.edge, iaContextText, circuitBreakerLevel);
                if (tradeData) {
                    tradingModuleText = `\n\n💰 *MODO TRADING (Spot/CFD x20)*\n📍 *Entry:* ${tradeData.entry}\n🛑 *Stop Loss:* ${tradeData.sl}\n🎯 *Take Profit:* ${tradeData.tp}\n⚖️ *R:R:* 1:${tradeData.rr}\n\n💼 *Volumen sugerido:* ${tradeData.positionSize} USDT\n📉 *Riesgo:* ${tradeData.riskPercent}% del capital`;
                }
            }

            const finalMsg = msgText + `\n\n*Veredicto IA:* ${verdictIcon} (Score: ${iaScore}/100)\n*Contexto IA:* 🧩 ${iaContextText}\n_📝 ${iaReasoning}_${tradingModuleText}`;
            bot.editMessageText(finalMsg, { chat_id: query.message.chat.id, message_id: query.message.message_id, parse_mode: 'Markdown' });
        } catch (e) {
            bot.editMessageText(msgText + '\n\n❌ _IA Offline._', { chat_id: query.message.chat.id, message_id: query.message.message_id, parse_mode: 'Markdown' });
        }
    }
});

// === MOTOR CUANTITATIVO (RESTAURADO COMPLETO) ===
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

function aggregateCandles(baseData, factor) {
    let agg = [];
    for (let i = 0; i < baseData.length; i += factor) {
        let chunk = baseData.slice(i, i + factor);
        if (chunk.length < factor) break;
        agg.push({ o: chunk[0].o, h: Math.max(...chunk.map(c => c.h)), l: Math.min(...chunk.map(c => c.l)), c: chunk[chunk.length - 1].c });
    }
    return agg;
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
    const msPerInterval = minutesToAdd * 60 * 1000;
    const currentStart = new Date(Math.floor(now.getTime() / msPerInterval) * msPerInterval);
    const tStart = new Date(currentStart.getTime() + msPerInterval);
    const tEnd = new Date(tStart.getTime() + msPerInterval);
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
    const startIdx = Math.max(40, candles.length - 10000);

    for (let i = startIdx; i < candles.length - 2 - patternLength - 3; i++) {
        for (let shift = 0; shift <= 2; shift++) {
            const histEndIdx = i + shift + patternLength - 1;
            if (atrs[histEndIdx] / currentATR > 2.5 || atrs[histEndIdx] / currentATR < 0.4) continue; 
            const sim = cosineSimilarity(targetVector, buildPatternVector(candles, atrs, histEndIdx));
            if (sim < 0.80) continue; 
            const next = candles[histEndIdx + 1];
            if (!next) continue;
            const moveUp = next.h - candles[histEndIdx].c;
            const moveDown = candles[histEndIdx].c - next.l;
            const win = (moveUp > moveDown) ? 1 : 0; 
            matches.push({ sim, win, timestamp: Date.now() - ((candles.length - histEndIdx) * 5 * 60 * 1000) });
        }
    }

    if (matches.length < 15) return null; 
    matches.sort((a,b) => b.sim - a.sim);
    const eliteCount = Math.min(250, Math.max(15, Math.floor(matches.length * 0.10)));
    const topMatches = matches.slice(0, eliteCount);
    
    let wins = 0; let losses = 0;
    const now = Date.now();
    for (const m of topMatches) {
        const weight = Math.pow(m.sim, 2) * timeDecayWeight(m.timestamp, now);
        if (m.win === 1) wins += weight; else losses += weight;
    }

    let prob = ((wins + 1) / (wins + losses + 2)) * 100;
    const chunks = 3; const size = Math.floor(topMatches.length / chunks);
    let segs = [];
    for (let j = 0; j < chunks; j++) {
        const s = topMatches.slice(j * size, (j + 1) * size);
        segs.push(s.reduce((a, b) => a + b.win, 0) / (s.length || 1));
    }
    const meanWR = segs.reduce((a,b)=>a+b,0) / 3;
    const stability = Math.max(0, 1 - (Math.sqrt(segs.reduce((s, w) => s + Math.pow(w - meanWR, 2), 0) / 3) / (meanWR || 1)));

    const signal = prob > 50 ? "BUY" : "SELL";
    const finalProb = signal === "BUY" ? prob : 100 - prob;
    const edge = finalProb - CONFIG.BE;

    return { prob: finalProb, direction: signal, edge: (signal === 'BUY' ? edge : -edge), absEdge: edge, stability, n: topMatches.length, acs: (edge / 50) * stability, cwev: edge * stability, currentPrice: candles[candles.length - 1].c, currentATR: currentATR };
}

function statisticalStrength(samples, alpha, prob) {
    if(samples < 15) return false; 
    return ((prob*0.5) + (alpha*0.3) + (Math.log(samples)/10*0.2)) > 0.45; 
}

function makeProgressBar(percent) {
    const f = Math.min(10, Math.max(0, Math.round(percent / 10)));
    return '█'.repeat(f) + '░'.repeat(10 - f);
}

// === ORQUESTADOR GLOBAL (RESTAURADO COMPLETO) ===
async function globalScan(scanType = 'auto') {
    const currentServerTime = getSyncedTime();
    if (currentServerTime < globalPauseUntil) return;

    const startTime = getLocalTime();
    let statusMsg;
    try {
        const title = scanType === 'auto' ? '🔄 *Scan Automático*' : '⏳ *Scan Manual*';
        statusMsg = await bot.sendMessage(chatId, `${title}\n▶️ *Inicio:* ${startTime}`, { parse_mode: 'Markdown' });
    } catch(e) {}

    let parsedData = [];
    const assetPerformance = {};
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
                
                // --- CIRCUIT BREAKER LÓGICA (RESTAURADO) ---
                const globalLastTrades = parsedData.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA').slice(-4);
                if (globalLastTrades.length === 4 && globalLastTrades.every(r => r.Veredicto === 'PERDIDA')) {
                    const cooldown = 30 * Math.pow(2, Math.min(circuitBreakerLevel, 2));
                    globalPauseUntil = currentServerTime + (cooldown * 60 * 1000);
                    circuitBreakerLevel++;
                    bot.sendMessage(chatId, `🚨 *CIRCUIT BREAKER ACTIVADO*`);
                    return;
                }
            }
        }
    } catch (e) {}

    const validSignals = [];
    let tfs = scanType === 'auto' ? [{ tf: '5M', aggregate: 1 }] : [{ tf: '5M', aggregate: 1 }, { tf: '15M', aggregate: 3 }];

    for (const asset of CONFIG.MARKETS) {
        try {
            // --- CACHE GLOBAL Y FETCH DE VELAS (RESTAURADO) ---
            let historical = GLOBAL_CANDLE_CACHE.get(asset.id);
            if (!historical) {
                const res = await axios.get(`${CONFIG.BACKEND_URL}/candles?symbol=${asset.symbolBinance}&limit=${CONFIG.REQUEST_LIMIT}`);
                historical = res.data;
                GLOBAL_CANDLE_CACHE.set(asset.id, historical);
            } else {
                const recent = await axios.get(`${CONFIG.BACKEND_URL}/candles?symbol=${asset.symbolBinance}&limit=100`);
                historical = [...historical.slice(-CONFIG.REQUEST_LIMIT), ...recent.data];
            }
            candlesByAsset[asset.id] = historical;

            const [mRes1h, mRes4h] = await Promise.all([
                axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${asset.symbolBinance}&interval=1h&limit=50`),
                axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${asset.symbolBinance}&interval=4h&limit=50`)
            ]);
            const s1h = detectStructure(mRes1h.data.map(v=>({h:parseFloat(v[2]),l:parseFloat(v[3])})));
            const s4h = detectStructure(mRes4h.data.map(v=>({h:parseFloat(v[2]),l:parseFloat(v[3])})));

            // --- LOB PONDERADO (RESTAURADO) ---
            const lobRes = await axios.get(`${CONFIG.BINANCE_API}/depth?symbol=${asset.symbolBinance}&limit=10`);
            const bV = lobRes.data.bids.reduce((a, b) => a + parseFloat(b[1]), 0);
            const aV = lobRes.data.asks.reduce((a, b) => a + parseFloat(b[1]), 0);
            const obi = (bV - aV) / (bV + aV);

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
        
        // --- MOMENTUM Y LIQUIDEZ (RESTAURADO) ---
        const hist = candlesByAsset[s.assetId];
        const recent = hist.slice(-10).map(c => c.c);
        const slope = (recent[9] - recent[0]) / 10;
        const currentPrice = hist[hist.length-1].c;
        const nearRes = Math.abs(Math.max(...hist.slice(-50).map(c=>c.h)) - currentPrice) < s.analysis.currentATR;

        s.momentumSlope = slope;
        s.passMacro = passMacro;

        let isElite = true; let isAggressive = true;
        let eR = [];

        // --- PRECISIÓN CSV FILTROS (INTEGRADO) ---
        if (["BTC/USD", "SOL/USD", "XRP/USD", "BNB/USD", "ADA/USD"].includes(s.assetId)) {
            if (s.analysis.edge > 0) { isElite = false; eR.push('Edge Trampa'); }
            if (isB) { isElite = false; eR.push('Solo SELL'); }
        }
        if (s.assetId === "DOGE/USD") {
            if (s.analysis.edge < 3) { isElite = false; eR.push('Edge Bajo'); }
            if (s.tf === "15M") { isElite = false; eR.push('TF 15M Prohibido'); }
        }
        if (["ADA/USD", "ETH/USD"].includes(s.assetId) && s.analysis.stability > 0.75) {
            isElite = false; eR.push('Stability Fakeout');
        }

        if (!passMacro || s.analysis.prob < 54) { isElite = false; isAggressive = false; }

        if (isElite || isAggressive) {
            s.isElite = isElite; s.isAggressive = isAggressive;
            consensusSignals.push(s);
        }
    }
    
    if (consensusSignals.length > 0) {
        for (const s of consensusSignals) {
            const sigId = `sig_${signalCounter++}`;
            const timeData = getRecommendedTimeData(s.tf, currentServerTime);
            const msgText = `🎯 *${s.assetId} | ${s.tf}*\nDir: ${s.analysis.direction === 'BUY' ? '🟢' : '🔴'} *${s.analysis.direction}*\nProb: ${s.analysis.prob.toFixed(1)}%\nEdge: ${s.analysis.edge.toFixed(2)}%\nStab: ${(s.analysis.stability*100).toFixed(0)}%`;
            
            const keyboard = { inline_keyboard: [[{ text: "🧠 Analizar con IA de Liquidez", callback_data: `ai_${sigId}` }]] };
            const sent = await bot.sendMessage(chatId, msgText, { parse_mode: 'Markdown', reply_markup: keyboard });
            
            SIGNAL_CACHE.set(sigId, { s, msgText, modeString: s.isElite ? "ELITE" : "AGRESIVO", dualScore: s.isElite ? 1.0 : 0.4 });
            
            PENDING_AUDITS.push({ 
                sigId, assetId: s.assetId, symbolBinance: s.symbolBinance, tf: s.tf, direction: s.analysis.direction, 
                startTs: timeData.startTs, endTs: timeData.endTs, messageId: sent.message_id, retries: 0,
                logData: { prob: s.analysis.prob.toFixed(1), edge: s.analysis.edge.toFixed(2), mode: s.isElite ? "ELITE" : "AGRESIVO" } 
            });

            logToCSV({ asset: s.assetId, tf: s.tf, dir: s.analysis.direction, prob: s.analysis.prob.toFixed(1), edge: s.analysis.edge.toFixed(2), stab: (s.analysis.stability*100).toFixed(0), veredicto: 'PENDIENTE' });
        }
    }
}

// === AUDITORÍA FINAL (RESTAURADO) ===
setInterval(async () => {
    const now = getSyncedTime();
    for (let i = PENDING_AUDITS.length - 1; i >= 0; i--) {
        const a = PENDING_AUDITS[i];
        if (now >= a.endTs + 15000) {
            try {
                const res = await axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${a.symbolBinance}&interval=${a.tf.toLowerCase()}&limit=5`);
                const candle = res.data.find(k => k[0] === a.startTs);
                if (candle) {
                    const o = parseFloat(candle[1]), c = parseFloat(candle[4]);
                    const win = (a.direction === 'BUY' && c > o) || (a.direction === 'SELL' && c < o);
                    const resTxt = win ? 'GANADA' : 'PERDIDA';
                    bot.sendMessage(chatId, `🔍 *AUDITORÍA: ${a.assetId}*\nResult: ${resTxt}\nO: ${o} | C: ${c}`, { reply_to_message_id: a.messageId });
                    logToCSV({ asset: a.assetId, tf: a.tf, dir: a.direction, prob: a.logData.prob, edge: a.logData.edge, veredicto: resTxt, open: o, close: c });
                    PENDING_AUDITS.splice(i, 1);
                }
            } catch (e) {}
        }
    }
}, 20000);

setInterval(() => {
    const now = new Date(getSyncedTime());
    if (now.getMinutes() % 5 === 3 && now.getSeconds() === 15) globalScan('auto');
}, 1000);

console.log(`🤖 Quant Sniper V12.5 TACTICAL PRO RESTAURADO Y OPERATIVO.`);
