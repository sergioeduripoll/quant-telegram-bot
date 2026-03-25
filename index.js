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

// 🟢 Control Anti-Ruina Progresivo y Cache Global
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

// === FUNCIONES DE TRADING (NUEVO MÓDULO CFDs / QUANTFURY) ===
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

    let rr = Math.max(1.2, edge / 2);
    if (iaContext === "CONTINUATION") rr *= 1.3;
    if (iaContext === "REVERSAL") rr *= 0.8;

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

console.log(`🤖 Quant Sniper V12.5 TACTICAL PRO iniciando...`);
bot.sendMessage(chatId, '🟢 *Quant Sniper V12.5 TACTICAL PRO* encendido.\nModo: Semáforo Visual + IA a Demanda + Módulo Trading Spot', { parse_mode: 'Markdown' });

// === HANDLER DEL BOTÓN DE IA ===
bot.on('callback_query', async (query) => {
    const data = query.data;
    if (!data.startsWith('analyze_')) return;

    const sigId = data.split('_')[1];
    const s = SIGNAL_CACHE.get(sigId);

    if (!s) {
        return bot.answerCallbackQuery(query.id, { text: "⏳ La señal expiró o ya no está en caché.", show_alert: true });
    }

    bot.answerCallbackQuery(query.id, { text: "Iniciando análisis cuántico..." });

    const loadingMsg = s.msgText.replace('⏳ _Esperando orden para análisis IA..._', '⏳ _Analizando contexto con Gemini AI..._');
    await bot.editMessageText(loadingMsg, { chat_id: chatId, message_id: query.message.message_id, parse_mode: 'Markdown' });

    let iaVerdictText = "PASS"; 
    let iaScore = 0;
    let iaContextText = "UNKNOWN";
    let iaReasoning = "Error al consultar la IA.";
    let tradingModuleText = "";

    const promptText = `Eres un Quant Trader Institucional. Tu tarea es doble: decidir el SCORE de ejecución y CLASIFICAR el contexto de mercado.
    MODO DE LA SEÑAL: ${s.modeString} (Dual Score: ${s.dualScore.toFixed(1)}).
    
    DATOS:
    Activo: ${s.assetId} | Timeframe: ${s.tf} | Dir: ${s.analysis.direction} | Prob: ${s.analysis.prob.toFixed(1)}% | Edge Real: ${s.analysis.edge.toFixed(2)}%
    Stability: ${(s.analysis.stability * 100).toFixed(0)}% | LOB Ponderado: ${s.obi.toFixed(3)} | Momentum: ${s.momentumSlope.toFixed(4)} | Macro 4H: ${s.macro.h4}
    
    REGLAS:
    1. Define el TRADE_CONTEXT basado en el Momentum y Macro H4:
       - CONTINUATION: El Momentum y la Macro están a favor de la dirección.
       - REVERSAL: Entrando en contra de la Macro pero el Momentum reciente es fuerte a favor.
       - TRAP: Divergencia peligrosa, baja probabilidad, o choque de liquidez.
    2. Define el AI_SCORE (0 a 100): Evalúa qué tan buena es la oportunidad basándote en el Edge, el LOB, el modo táctico y el contexto. >65 significa EXECUTE.
    
    Responde EXCLUSIVAMENTE en este formato exacto, respetando las mayúsculas:
    AI_SCORE: (Número del 0 al 100)
    TRADE_CONTEXT: (CONTINUATION, REVERSAL, o TRAP)
    REASONING: (Tu argumento táctico en 2 oraciones).`;

    try {
        const apiKey = process.env.GEMINI_API_KEY;
        const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key=${apiKey}`;
        const result = await axios.post(url, { contents: [{ parts: [{ text: promptText }] }], generationConfig: { temperature: 0.2 } });
        const text = result.data.candidates[0].content.parts[0].text;
        
        const scoreMatch = text.match(/AI_SCORE:\s*(\d+)/i);
        iaScore = scoreMatch ? parseInt(scoreMatch[1]) : 0;
        
        const contextMatch = text.match(/TRADE_CONTEXT:\s*(CONTINUATION|REVERSAL|TRAP)/i);
        iaContextText = contextMatch ? contextMatch[1].toUpperCase() : 'UNKNOWN';
        
        const reasonMatch = text.match(/REASONING:\s*([\s\S]*?)$/i);
        iaReasoning = reasonMatch ? reasonMatch[1].trim().replace(/[*_[\]]/g, '') : 'Análisis completado.';
        
        const isExecute = iaScore >= 65;
        const verdictIcon = isExecute ? '🚀 *EXECUTE TRADE*' : '⛔ *PASS*';
        iaVerdictText = isExecute ? "EXECUTE" : "PASS";

        if (isExecute && iaContextText !== "TRAP") {
            const tradeData = calculateTradeLevels(s.analysis.currentPrice, s.analysis.direction, s.analysis.currentATR, s.analysis.edge, iaContextText, circuitBreakerLevel);
            if (tradeData) {
                tradingModuleText = `\n\n💰 *MODO TRADING (Spot/CFD x20)*\n📍 *Entry:* ${tradeData.entry}\n🛑 *Stop Loss:* ${tradeData.sl}\n🎯 *Take Profit:* ${tradeData.tp}\n⚖️ *R:R:* 1:${tradeData.rr}\n\n💼 *Volumen sugerido:* ${tradeData.positionSize} USDT\n📉 *Riesgo:* ${tradeData.riskPercent}% del capital`;
            }
        }

        const updatedMsg = s.msgText.replace('⏳ _Esperando orden para análisis IA..._', `*Veredicto IA:* ${verdictIcon} (Score: ${iaScore}/100)\n*Contexto IA:* 🧩 ${iaContextText}\n_📝 ${iaReasoning}_${tradingModuleText}`);
        await bot.editMessageText(updatedMsg, { chat_id: chatId, message_id: query.message.message_id, parse_mode: 'Markdown' });

        const auditEntry = PENDING_AUDITS.find(a => a.sigId === sigId);
        if (auditEntry) {
            auditEntry.logData.iaVerdict = iaVerdictText;
            auditEntry.logData.iaScore = iaScore;
            auditEntry.logData.iaContext = iaContextText;
        }

    } catch (e) {
        await bot.editMessageText(s.msgText.replace('⏳ _Esperando orden para análisis IA..._', '❌ _Fallo de conexión IA. Intenta de nuevo._'), { 
            chat_id: chatId, 
            message_id: query.message.message_id, 
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[ { text: '🔄 Reintentar IA', callback_data: `analyze_${sigId}` } ]] }
        });
    }
});

// === MOTOR CUANTITATIVO ===
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
    if (minutesToAdd === 0) return null;
    const msPerInterval = minutesToAdd * 60 * 1000;
    const currentStart = new Date(Math.floor(now.getTime() / msPerInterval) * msPerInterval);
    const tStart = new Date(currentStart.getTime() + msPerInterval);
    const tEnd = new Date(tStart.getTime() + msPerInterval);
    const format = (d) => d.toLocaleTimeString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour: '2-digit', minute: '2-digit', hour12: false });
    
    return {
        text: `${format(tStart)} - ${format(tEnd)}`,
        startTs: tStart.getTime(),
        endTs: tEnd.getTime()
    };
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
            
            const futureVolatility = (next.h - next.l) / currentATR;
            if (futureVolatility < 0.5) continue; 

            const moveUp = next.h - candles[histEndIdx].c;
            const moveDown = candles[histEndIdx].c - next.l;
            const win = (moveUp > moveDown) ? 1 : 0; 

            matches.push({ sim, win, timestamp: Date.now() - ((candles.length - histEndIdx) * 5 * 60 * 1000) });
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

    if (
        edge < 1.2 ||
        (regime === "COMPRESSION" && edge < 6) ||
        (regime === "TREND" && edge < 0.3)
    ) return null;

    return { prob: finalProb, direction: signal, edge: (signal === 'BUY' ? edge : -edge), absEdge: edge, stability, n: topMatches.length, acs: (edge / 50) * stability * (topMatches.length / 100), cwev: edge * stability, currentPrice: candles[candles.length - 1].c, currentATR: currentATR };
}

function statisticalStrength(samples, alpha, prob) {
    if(samples < 15) return false; 
    return ((prob*0.5) + (alpha*0.3) + (Math.log(samples)/10*0.2)) > 0.45; 
}

function makeProgressBar(percent) {
    const f = Math.min(10, Math.max(0, Math.round(percent / 10)));
    return '█'.repeat(f) + '░'.repeat(10 - f);
}

// === ORQUESTADOR GLOBAL ===
async function globalScan(scanType = 'auto') {
    const currentServerTime = getSyncedTime();
    
    if (currentServerTime < globalPauseUntil) {
        console.log(`[PAUSA] 🛡️ Protegiendo capital. Bot en reposo hasta ${new Date(globalPauseUntil).toLocaleTimeString('es-AR')}`);
        return;
    }

    const startTime = getLocalTime();
    console.log(`[${startTime}] 🚀 Iniciando Escaneo TACTICAL PRO...`);

    let statusMsg;
    try {
        const title = scanType === 'auto' ? '🔄 *Scan Automático*' : '⏳ *Scan Manual*';
        statusMsg = await bot.sendMessage(chatId, `${title}\n▶️ *Inicio:* ${startTime}\n_Analizando microestructura..._`, { parse_mode: 'Markdown' });
    } catch(e) {}

    const assetPerformance = {};
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
                    let obj = {};
                    headers.forEach((h, idx) => obj[h.trim()] = vals[idx]);
                    parsedData.push(obj);
                }
                
                CONFIG.MARKETS.forEach(m => {
                    const assetRows = parsedData.filter(r => r.Activo === m.id && (r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA')).slice(-20);
                    if (assetRows.length > 0) {
                        const wins = assetRows.filter(r => r.Veredicto === 'GANADA').length;
                        assetPerformance[m.id] = { wr: (wins / assetRows.length * 100).toFixed(1), count: assetRows.length };
                    }
                });

                const totalFinishedTrades = parsedData.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA').length;
                const globalLastTrades = parsedData.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA').slice(-4);
                
                if (globalLastTrades.length > 0 && globalLastTrades[globalLastTrades.length - 1].Veredicto === 'GANADA') {
                    circuitBreakerLevel = 0;
                }

                if (globalLastTrades.length === 4 && globalLastTrades.every(r => r.Veredicto === 'PERDIDA') && totalFinishedTrades > lastCircuitBreakerTradeCount) {
                    lastCircuitBreakerTradeCount = totalFinishedTrades;
                    const cooldownMinutes = 30 * Math.pow(2, Math.min(circuitBreakerLevel, 2));
                    globalPauseUntil = currentServerTime + (cooldownMinutes * 60 * 1000);
                    circuitBreakerLevel++;
                    
                    bot.sendMessage(chatId, `🚨 *CIRCUIT BREAKER ACTIVADO (Nivel ${circuitBreakerLevel})*\nSe detectaron 4 pérdidas globales consecutivas.\nEl bot se pausa automáticamente por *${cooldownMinutes} minutos* para proteger el capital.`, { parse_mode: 'Markdown' });
                    return; 
                }
            }
        }
    } catch (e) {}

    CURRENT_WINNING_ZONES = buildWinningZonesFromCSV(parsedData);

    let allowedAssets = CONFIG.MARKETS.map(m => m.id);
    const assetsWithData = Object.keys(assetPerformance);
    if (assetsWithData.length >= 3) {
        allowedAssets = assetsWithData
            .sort((a, b) => parseFloat(assetPerformance[b].wr) - parseFloat(assetPerformance[a].wr))
            .slice(0, 3);
    }

    const validSignals = [];
    let tfs = [{ tf: '5M', aggregate: 1 }]; 

    if (scanType === 'auto') {
        const now = new Date(getSyncedTime());
        const currentMinute = now.getMinutes();
        if ([13, 28, 43, 58].includes(currentMinute)) tfs.push({ tf: '15M', aggregate: 3 });
        if ([28, 58].includes(currentMinute)) tfs.push({ tf: '30M', aggregate: 6 });
        if (currentMinute === 58) tfs.push({ tf: '1H', aggregate: 12 });
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
                const recentCandles = recentRes.data;
                
                if (recentCandles && recentCandles.length > 0) {
                    const lastCached = historical[historical.length - 1];
                    let overlapIdx = -1;
                    
                    for (let i = recentCandles.length - 1; i >= 0; i--) {
                        const r = recentCandles[i];
                        if (r.c === lastCached.c && r.o === lastCached.o && r.h === lastCached.h && r.l === lastCached.l) {
                            overlapIdx = i;
                            break;
                        }
                    }
                    
                    if (overlapIdx !== -1 && overlapIdx < recentCandles.length - 1) {
                        historical.push(...recentCandles.slice(overlapIdx + 1));
                    } else if (overlapIdx === -1) {
                        const histRes = await axios.get(`${CONFIG.BACKEND_URL}/candles?symbol=${asset.symbolBinance}&limit=${CONFIG.REQUEST_LIMIT}`);
                        historical = histRes.data;
                    }
                    
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
                
                if(lobRes.data.bids && lobRes.data.asks && lobRes.data.bids.length > 0 && lobRes.data.asks.length > 0) {
                    const bestBid = parseFloat(lobRes.data.bids[0][0]);
                    const bestAsk = parseFloat(lobRes.data.asks[0][0]);
                    const midPrice = (bestBid + bestAsk) / 2;

                    lobRes.data.bids.forEach(l => {
                        const price = parseFloat(l[0]);
                        const vol = parseFloat(l[1]);
                        const distance = Math.max(Math.abs(midPrice - price) / midPrice, 0.0001);
                        bV += vol * (1 / distance);
                    });
                    lobRes.data.asks.forEach(l => {
                        const price = parseFloat(l[0]);
                        const vol = parseFloat(l[1]);
                        const distance = Math.max(Math.abs(midPrice - price) / midPrice, 0.0001);
                        aV += vol * (1 / distance);
                    });
                    if (bV + aV > 0) obi = (bV - aV) / (bV + aV);
                }
            } catch(e) {}

            for (const item of tfs) {
                const res = runAnalysisElite(item.aggregate > 1 ? aggregateCandles(historical, item.aggregate) : historical);
                if (res) validSignals.push({ assetId: asset.id, symbolBinance: asset.symbolBinance, tf: item.tf, analysis: res, obi, macro: { h1: s1h, h4: s4h } });
            }
        } catch(e) { console.error(`Error escaneando ${asset.id}`); }
    } 

    const consensusSignals = [];

    for (const s of validSignals) {
        const isB = s.analysis.direction === "BUY";
        const passMacro = !(isB && s.macro.h4 === -1) && !(!isB && s.macro.h4 === 1);
        
        const historicalForAsset = candlesByAsset[s.assetId];
        let momentumSlope = 0;
        let nearResistance = false;
        let nearSupport = false;

        if (historicalForAsset && historicalForAsset.length >= 10) {
            const recent10 = historicalForAsset.slice(-10).map(c => c.c);
            let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
            for (let k = 0; k < 10; k++) { sumX += k; sumY += recent10[k]; sumXY += k * recent10[k]; sumX2 += k * k; }
            momentumSlope = (10 * sumXY - sumX * sumY) / (10 * sumX2 - sumX * sumX);
        }

        if (historicalForAsset && historicalForAsset.length >= 50) {
            const recent50 = historicalForAsset.slice(-50);
            const highs = recent50.map(c => c.h);
            const lows = recent50.map(c => c.l);
            const highestHigh = Math.max(...highs);
            const lowestLow = Math.min(...lows);
            const currentPrice = historicalForAsset[historicalForAsset.length - 1].c;
            const avgRange = recent50.slice(-10).reduce((acc, c) => acc + (c.h - c.l), 0) / 10;
            nearResistance = Math.abs(highestHigh - currentPrice) < avgRange;
            nearSupport = Math.abs(currentPrice - lowestLow) < avgRange;
        }

        s.momentumSlope = momentumSlope;
        s.nearResistance = nearResistance;
        s.nearSupport = nearSupport;
        s.passMacro = passMacro;

        const lastTradesAsset = parsedData.filter(r => r.Activo === s.assetId).slice(-5);
        const recentLossesAsset = lastTradesAsset.filter(r => r.Veredicto === 'PERDIDA').length;

        // --- 🟢 EVALUACIÓN ELITE ---
        let isElite = true;
        if (!passMacro) isElite = false;
        if (s.analysis.cwev < 1.2) isElite = false;
        if (recentLossesAsset >= 3) isElite = false;
        if (isB && momentumSlope < 0) isElite = false;
        if (!isB && momentumSlope > 0) isElite = false;
        if (isB && nearResistance) isElite = false; 
        if (!isB && nearSupport) isElite = false; 
        if (s.analysis.prob < PROB_THRESHOLD) isElite = false;
        if (s.analysis.acs < 0.015) isElite = false;
        if (s.analysis.stability < 0.40) isElite = false;
        if (s.analysis.n < 15) isElite = false;
        if (!((isB && s.obi > 0) || (!isB && s.obi < 0))) isElite = false;
        if (!statisticalStrength(s.analysis.n, s.analysis.acs, s.analysis.prob)) isElite = false;

        // --- 🟡 EVALUACIÓN AGRESIVA ---
        let isAggressive = true;
        if (!passMacro) isAggressive = false; 
        if (s.analysis.prob < 54) isAggressive = false; 
        if (s.analysis.stability < 0.35) isAggressive = false; 
        if (s.analysis.n < 12) isAggressive = false; 
        
        let passMomentumAgr = true;
        if (isB && momentumSlope < -0.0001) passMomentumAgr = false;
        if (!isB && momentumSlope > 0.0001) passMomentumAgr = false;
        if (!passMomentumAgr) isAggressive = false;

        if (isElite || isAggressive) {
            s.isElite = isElite;
            s.isAggressive = isAggressive;
            consensusSignals.push(s);
        }
    }
    
    consensusSignals.sort((a,b) => b.analysis.cwev - a.analysis.cwev);

    const endTime = getLocalTime();
    if (consensusSignals.length > 0) {
        if (statusMsg) {
            bot.editMessageText(`✅ *Scan TACTICAL completado*\n⏹️ *Fin:* ${endTime}\n🚀 *${consensusSignals.length} oportunidades detectadas.*`, { chat_id: chatId, message_id: statusMsg.message_id, parse_mode: 'Markdown' }).catch(()=>{});
        }

        for (const s of consensusSignals) {
            const sigId = `sig_${signalCounter++}`;
            const icon = s.analysis.direction === 'BUY' ? '🟢' : '🔴';
            const timeData = getRecommendedTimeData(s.tf, currentServerTime);
            const edgeSign = s.analysis.edge > 0 ? '+' : '';
            
            // --- 🎨 INTERFAZ VISUAL TÁCTICA ---
            const eliteCheck = s.isElite ? '✅' : '❌';
            const agrCheck = s.isAggressive ? '✅' : '❌';

            const dualScore = (s.isElite ? 0.6 : 0) + (s.isAggressive ? 0.4 : 0);
            let scoreVisual = "";
            let timingGap = "";
            let modeString = "";

            if (dualScore === 1.0) {
                modeString = "ELITE+AGRESIVO";
                scoreVisual = "1.0 → PERFECTO";
                timingGap = "ELITE: Confirmado\nAGRESIVO: Confirmado\n🔥 Sincronización perfecta. Tendencia confirmada con inercia.";
            } else if (dualScore === 0.6) {
                modeString = "ELITE";
                scoreVisual = "0.6 → SOLO ELITE";
                timingGap = "ELITE: Confirmación tardía\nAGRESIVO: ❌ Falta inercia temprana\n⚠️ Setup seguro pero posiblemente atrasado.";
            } else if (dualScore === 0.4) {
                modeString = "AGRESIVO";
                scoreVisual = "0.4 → SOLO AGRESIVO";
                timingGap = "ELITE: ❌ Sin confirmación de contexto\nAGRESIVO: Entrada anticipada\n⚠️ Gap de timing detectado (Entrada temprana, bajar stake).";
            }

            const confBar = makeProgressBar(s.analysis.prob); 
            const anticipationLevel = s.isAggressive ? 90 : 30;
            const antBar = makeProgressBar(anticipationLevel);

            const isB = s.analysis.direction === 'BUY';
            const momentumAlineado = (isB && s.momentumSlope > 0) || (!isB && s.momentumSlope < 0) ? 'A favor ✅' : 'En contra/Débil ⚠️';
            const liquidez = (s.nearResistance || s.nearSupport) ? 'Zona de choque ⚠️' : 'Neutra / Limpia ✅';
            const macroVal = s.passMacro ? 'OK ✅' : 'Contra tendencia ⚠️';

            const msgText = `🎯 *${s.assetId} | ${s.tf}*

🧠 *MODO DUAL*
🟢 ELITE      → ${eliteCheck}
🟡 AGRESIVO   → ${agrCheck}

🏆 *DUAL SCORE:* ${scoreVisual}

📈 *Dirección:* ${icon} *${s.analysis.direction}*
🎯 *Probabilidad:* ${s.analysis.prob.toFixed(1)}%
⚡ *Edge:* ${edgeSign}${s.analysis.edge.toFixed(2)}%

🧬 *Contexto:*
• Momentum: ${momentumAlineado}
• Macro H4: ${macroVal}
• Liquidez: ${liquidez}

⏱️ *Ventana:* ${timeData.text}

🧠 *DIFERENCIA ENTRE MODOS*
${timingGap}

⚖️ *BALANCE*
Confianza:    ${confBar} (${s.analysis.prob.toFixed(0)}%)
Anticipación: ${antBar} (${anticipationLevel}%)

⏳ _Esperando orden para análisis IA..._`;

            // Guardamos todo en caché para usarlo en el callback del botón
            s.msgText = msgText;
            s.modeString = modeString;
            s.dualScore = dualScore;
            SIGNAL_CACHE.set(sigId, s);

            const sentMsg = await bot.sendMessage(chatId, msgText, { 
                parse_mode: 'Markdown',
                reply_markup: {
                    inline_keyboard: [[
                        { text: '🤖 Analizar con IA', callback_data: `analyze_${sigId}` }
                    ]]
                }
            });
            
            logToCSV({
                asset: s.assetId, tf: s.tf, dir: s.analysis.direction, prob: s.analysis.prob.toFixed(1),
                lob: s.obi.toFixed(3), edge: s.analysis.edge.toFixed(2), alpha: s.analysis.acs.toFixed(3),
                stab: (s.analysis.stability * 100).toFixed(0), cwev: s.analysis.cwev.toFixed(1),
                samples: s.analysis.n, veredicto: 'PENDIENTE', iaVerdict: 'NO_SOLICITADO', iaScore: 0, iaContext: 'N/A', mode: modeString
            });

            PENDING_AUDITS.push({
                sigId, assetId: s.assetId, symbolBinance: s.symbolBinance, tf: s.tf, direction: s.analysis.direction,
                startTs: timeData.startTs, endTs: timeData.endTs, messageId: sentMsg.message_id, retries: 0,
                logData: {
                    prob: s.analysis.prob.toFixed(1), lob: s.obi.toFixed(3), 
                    edge: s.analysis.edge.toFixed(2), alpha: s.analysis.acs.toFixed(3),
                    stab: (s.analysis.stability * 100).toFixed(0), cwev: s.analysis.cwev.toFixed(1),
                    samples: s.analysis.n, iaVerdict: 'NO_SOLICITADO', iaScore: 0, iaContext: 'N/A', mode: modeString
                }
            });
        }
    } else if (statusMsg) {
        bot.editMessageText(`💤 *Scan TACTICAL finalizado*\n⏹️ *Fin:* ${endTime}\n_Sin divergencias de consenso._`, { chat_id: chatId, message_id: statusMsg.message_id, parse_mode: 'Markdown' }).catch(()=>{});
    }
}

// === CRONES ===
let lastScanMinute = -1;
setInterval(() => {
    const now = new Date(getSyncedTime());
    const m = now.getMinutes();
    const s = now.getSeconds();
    if ((m % 5 === 3) && s === 15 && lastScanMinute !== m) {
        lastScanMinute = m;
        globalScan('auto');
    }
}, 1000);

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
                    samples: audit.logData.samples, iaVerdict: audit.logData.iaVerdict, iaScore: audit.logData.iaScore, iaContext: audit.logData.iaContext, mode: audit.logData.mode,
                    veredicto: iconResult, open: openPrice, close: closePrice 
                });
                
                PENDING_AUDITS.splice(i, 1);
            } catch (error) { audit.retries++; }
        }
    }
}, 15000);
