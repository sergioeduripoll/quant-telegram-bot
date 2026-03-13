require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const cron = require('node-cron');
const axios = require('axios');
const express = require('express');

// === CONFIGURACIÓN DE SERVIDOR PARA RENDER ===
const app = express();
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => res.send('🚀 Quant Sniper V12.5 ELITE está operando 24/7 en Render'));

app.listen(PORT, () => {
    console.log(`📡 Servidor HTTP activo en puerto ${PORT}`);
});

// === CONFIGURACIONES GLOBALES ===
const token = process.env.TELEGRAM_TOKEN;
const chatId = process.env.CHAT_ID;
const geminiApiKey = process.env.GEMINI_API_KEY;

const bot = new TelegramBot(token, { polling: true });
const PROB_THRESHOLD = 58; 
const patternLength = 6;

const CONFIG = {
    BE: 54.94,
    BINANCE_API: 'https://api.binance.com/api/v3',
    BACKEND_URL: 'https://quant-backend-lhue.onrender.com/api', 
    MARKETS: [
        { id: 'BTC/USD', symbolBinance: 'BTCUSDT' }, 
        { id: 'ETH/USD', symbolBinance: 'ETHUSDT' },
        { id: 'ADA/USD', symbolBinance: 'ADAUSDT' }, 
        { id: 'LTC/USD', symbolBinance: 'LTCUSDT' }, 
        { id: 'SOL/USD', symbolBinance: 'SOLUSDT' },
        { id: 'XRP/USD', symbolBinance: 'XRPUSDT' },
        { id: 'DOGE/USD', symbolBinance: 'DOGEUSDT' },
        { id: 'BNB/USD', symbolBinance: 'BNBUSDT' }
    ]
};

const SIGNAL_CACHE = new Map();
let signalCounter = 0;

console.log('🤖 Motor Quant Sniper V12.5 ELITE iniciado...');

// === MOTOR CUANTITATIVO CORE ===

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
        const body = c.c - c.o;
        const range = c.h - c.l;
        const upperWick = c.h - Math.max(c.o, c.c);
        const lowerWick = Math.min(c.o, c.c) - c.l;
        const direction = c.c > c.o ? 1 : -1;
        const momentum = c.c - prevC.c;
        vec.push(body, range, upperWick, lowerWick, direction, currentATR, momentum);
    }
    return vec;
}

function normalizeVector(v){
    const max = Math.max(...v.map(x => Math.abs(x))) || 1;
    return v.map(x => x / max);
}

function calculateDistance(vecA, vecB) {
    let sum = 0;
    for(let i=0; i<vecA.length; i++) sum += Math.pow(vecA[i] - (vecB[i] || 0), 2);
    return Math.sqrt(sum);
}

function timeDecayWeight(ts, now) { 
    return Math.exp(-(now - ts) / (1000 * 60 * 60 * 24 * 90)); 
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
    let h = candles.slice(-20).map(c => parseFloat(c[2])), l = candles.slice(-20).map(c => parseFloat(c[3]));
    if (Math.max(...h) === h[h.length-1]) return 1;
    if (Math.min(...l) === l[l.length-1]) return -1;
    return 0;
}

function runAnalysisElite(candles) {
    if (candles.length < 100) return null;
    const regime = detectRegime(candles);
    const atrs = precalcATR(candles);
    const currentATR = atrs[candles.length - 2] || 1;
    const targetEndIdx = candles.length - 2; 
    const targetVector = buildPatternVector(candles, atrs, targetEndIdx);
    const vecTargetNorm = normalizeVector(targetVector);

    let matches = [];
    const startIdx = Math.max(40, candles.length - 20000);

    for (let i = startIdx; i < candles.length - 10; i++) {
        const histEndIdx = i;
        const histVector = buildPatternVector(candles, atrs, histEndIdx);
        const vecHistNorm = normalizeVector(histVector);
        const dist = calculateDistance(vecTargetNorm, vecHistNorm);
        if (dist > 1.2) continue; 
        const win = (candles[histEndIdx + 1].c > candles[histEndIdx].c) ? 1 : 0;
        matches.push({ dist, win, timestamp: Date.now() - ((candles.length - i) * 5 * 60 * 1000) });
    }

    if (matches.length < 20) return null;
    matches.sort((a,b) => a.dist - b.dist);
    const topMatches = matches.slice(0, 40);
    
    let wins = 0; let totalWeight = 0; const now = Date.now();
    for (const m of topMatches) {
        const weight = (1 / (m.dist + 0.01)) * timeDecayWeight(m.timestamp, now);
        totalWeight += weight;
        if (m.win === 1) wins += weight;
    }

    let prob = (wins / totalWeight) * 100;
    const stability = 0.7; 
    const signal = prob > 50 ? "BUY" : "SELL";
    const finalProb = signal === "BUY" ? prob : 100 - prob;
    const edge = Math.abs(prob - 50);

    return { prob: finalProb, direction: signal, edge: edge, stability, n: topMatches.length, acs: (edge/50)*0.7 };
}

// === ORQUESTADOR ===
async function globalScan() {
    console.log(`[${new Date().toLocaleTimeString()}] 🚀 Iniciando Escaneo Global...`);
    const validSignals = [];

    for (const asset of CONFIG.MARKETS) {
        try {
            const histRes = await axios.get(`${CONFIG.BACKEND_URL}/candles?symbol=${asset.symbolBinance}&limit=10000`);
            const historical = histRes.data;
            if (!historical || historical.length < 100) continue;

            const [mRes1h, mRes4h] = await Promise.all([
                axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${asset.symbolBinance}&interval=1h&limit=20`),
                axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${asset.symbolBinance}&interval=4h&limit=20`)
            ]);
            const s1h = detectStructure(mRes1h.data);
            const s4h = detectStructure(mRes4h.data);

            let obi = 0;
            const lobRes = await axios.get(`${CONFIG.BINANCE_API}/depth?symbol=${asset.symbolBinance}&limit=20`);
            let bV=0, aV=0;
            lobRes.data.bids.forEach(l => bV += parseFloat(l[0]) * parseFloat(l[1]));
            lobRes.data.asks.forEach(l => aV += parseFloat(l[0]) * parseFloat(l[1]));
            obi = (bV - aV) / (bV + aV);

            const res = runAnalysisElite(historical);
            if (res && res.prob >= PROB_THRESHOLD) {
                validSignals.push({ assetId: asset.id, tf: '5M', analysis: res, obi, macro: { h1: s1h, h4: s4h } });
            }
        } catch(e) { console.error(`Error en ${asset.id}: ${e.message}`); }
    } 

    if (validSignals.length === 0) console.log("No se encontraron señales élite en este ciclo.");

    for (const s of validSignals) {
        const sigId = `sig_${signalCounter++}`;
        SIGNAL_CACHE.set(sigId, s);
        const icon = s.analysis.direction === 'BUY' ? '🟢' : '🔴';
        const msgText = `
${icon} *${s.assetId} | ${s.tf}*
*DIRECCIÓN:* ${s.analysis.direction}
*PROB:* ${s.analysis.prob.toFixed(1)}%
*LOB:* ${s.obi.toFixed(3)}
*ACS:* ${s.analysis.acs.toFixed(3)}
*EDGE:* ${s.analysis.edge.toFixed(2)}%
`;
        bot.sendMessage(chatId, msgText, {
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '🤖 ANALIZAR CON IA', callback_data: sigId }]] }
        });
    }
}

// === INTERACCIÓN IA ===
bot.on('callback_query', async (query) => {
    const sigId = query.data;
    if (!SIGNAL_CACHE.has(sigId)) return;
    const s = SIGNAL_CACHE.get(sigId);
    
    bot.answerCallbackQuery(query.id, { text: 'IA Analizando datos...' });
    
    const promptText = `Eres un experto cuantitativo. Analiza: ${s.assetId} (${s.tf}). Direccion sugerida: ${s.analysis.direction}. Probabilidad: ${s.analysis.prob.toFixed(1)}%. LOB: ${s.obi.toFixed(3)}. Estructura Macro 1H/4H: ${s.macro.h1}/${s.macro.h4}. Responde EXECUTE o PASS con confianza y un breve motivo.`;

    try {
        const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${geminiApiKey}`;
        const result = await axios.post(url, { contents: [{ parts: [{ text: promptText }] }] });
        const text = result.data.candidates[0].content.parts[0].text;
        bot.sendMessage(chatId, `🧠 *VERDICTO IA PARA ${s.assetId}*\n\n${text}`, { parse_mode: 'Markdown' });
    } catch (e) { 
        bot.sendMessage(chatId, '❌ Error de conexión con la IA.');
    }
});

// === CRONOGRAMA DE ESCANEO (Minuto 3:30 de cada vela 5M) ===
cron.schedule('30 3,8,13,18,23,28,33,38,43,48,53,58 * * * *', () => globalScan());

// Comando manual
bot.onText(/\/scan/, () => globalScan());

// AUTO-DESPERTADOR (Ping cada 10 min)
cron.schedule('*/10 * * * *', async () => {
    try {
        await axios.get(`https://quant-telegram-bot.onrender.com/`);
        console.log('⏰ Auto-despertador: Servidor activo.');
    } catch (e) {
        console.log('Auto-despertador: Despertando...');
    }
});