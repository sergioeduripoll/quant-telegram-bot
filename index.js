require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const cron = require('node-cron');
const axios = require('axios');
const express = require('express');

// === CONFIGURACIÓN DE SERVIDOR PARA RENDER ===
const app = express();
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => res.send('🚀 Quant Sniper V12.5 ELITE está operando 24/7'));

app.listen(PORT, () => {
    console.log(`📡 Servidor HTTP activo en puerto ${PORT}`);
});

// === CONFIGURACIONES GLOBALES ===
const token = process.env.TELEGRAM_TOKEN;
const chatId = process.env.CHAT_ID;
const geminiApiKey = process.env.GEMINI_API_KEY;

// Validar que el token existe antes de iniciar el bot
if (!token) {
    console.error("❌ ERROR CRÍTICO: No se encontró TELEGRAM_TOKEN en las variables de entorno.");
}

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

// === FUNCIONES AUXILIARES (Lógica filtrada para evitar errores) ===

function precalcATR(candles, period = 14) {
    if (!candles || candles.length < period + 1) return new Array(candles ? candles.length : 0).fill(0);
    let atrs = new Array(candles.length).fill(0);
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

function detectRegime(candles){
    if (candles.length < 50) return "RANGE";
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
        if (k < 5 || !candles[k] || !candles[k-1]) continue; 
        const c = candles[k];
        const prevC = candles[k-1];
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
    const len = Math.min(vecA.length, vecB.length);
    for(let i=0; i<len; i++) sum += Math.pow(vecA[i] - vecB[i], 2);
    return Math.sqrt(sum);
}

function timeDecayWeight(ts, now) { 
    return Math.exp(-(now - ts) / (1000 * 60 * 60 * 24 * 90)); 
}

function runAnalysisElite(candles) {
    if (!candles || candles.length < 100) return null;
    const atrs = precalcATR(candles);
    const targetEndIdx = candles.length - 2; 
    const targetVector = buildPatternVector(candles, atrs, targetEndIdx);
    if (targetVector.length === 0) return null;
    const vecTargetNorm = normalizeVector(targetVector);

    let matches = [];
    const startIdx = Math.max(40, candles.length - 15000);

    for (let i = startIdx; i < candles.length - 10; i++) {
        const histVector = buildPatternVector(candles, atrs, i);
        if (histVector.length !== targetVector.length) continue;
        const vecHistNorm = normalizeVector(histVector);
        const dist = calculateDistance(vecTargetNorm, vecHistNorm);
        if (dist > 1.2) continue; 
        const win = (candles[i + 1].c > candles[i].c) ? 1 : 0;
        matches.push({ dist, win, timestamp: Date.now() - ((candles.length - i) * 5 * 60 * 1000) });
    }

    if (matches.length < 15) return null;
    matches.sort((a,b) => a.dist - b.dist);
    const topMatches = matches.slice(0, 30);
    
    let wins = 0; let totalWeight = 0; const now = Date.now();
    for (const m of topMatches) {
        const weight = (1 / (m.dist + 0.01)) * timeDecayWeight(m.timestamp, now);
        totalWeight += weight;
        if (m.win === 1) wins += weight;
    }

    let prob = (wins / totalWeight) * 100;
    const signal = prob > 50 ? "BUY" : "SELL";
    const finalProb = signal === "BUY" ? prob : 100 - prob;
    const edge = Math.abs(prob - 50);

    return { prob: finalProb, direction: signal, edge: edge, n: topMatches.length };
}

// === ORQUESTADOR ===
async function globalScan() {
    console.log(`[${new Date().toLocaleTimeString()}] 🚀 Escaneo iniciado...`);
    const validSignals = [];

    for (const asset of CONFIG.MARKETS) {
        try {
            // Pedir datos al backend con timeout para no quedar trabados
            const histRes = await axios.get(`${CONFIG.BACKEND_URL}/candles?symbol=${asset.symbolBinance}&limit=5000`, { timeout: 10000 });
            const historical = histRes.data;
            if (!historical || !Array.isArray(historical) || historical.length < 100) {
                console.log(`⚠️ Datos insuficientes para ${asset.id}`);
                continue;
            }

            // Pedir LOB a Binance
            const lobRes = await axios.get(`${CONFIG.BINANCE_API}/depth?symbol=${asset.symbolBinance}&limit=20`);
            let bV=0, aV=0;
            lobRes.data.bids.forEach(l => bV += parseFloat(l[0]) * parseFloat(l[1]));
            lobRes.data.asks.forEach(l => aV += parseFloat(l[0]) * parseFloat(l[1]));
            const obi = (bV - aV) / (bV + aV);

            const res = runAnalysisElite(historical);
            if (res && res.prob >= PROB_THRESHOLD) {
                validSignals.push({ assetId: asset.id, tf: '5M', analysis: res, obi });
            }
        } catch(e) { 
            console.error(`❌ Error escaneando ${asset.id}: ${e.message}`); 
        }
    } 

    console.log(`✅ Escaneo completado. Señales encontradas: ${validSignals.length}`);

    for (const s of validSignals) {
        const sigId = `sig_${signalCounter++}`;
        SIGNAL_CACHE.set(sigId, s);
        const icon = s.analysis.direction === 'BUY' ? '🟢' : '🔴';
        const msgText = `*${icon} ${s.assetId} | ${s.tf}*\n*PROB:* ${s.analysis.prob.toFixed(1)}%\n*DIR:* ${s.analysis.direction}\n*LOB:* ${s.obi.toFixed(3)}\n*EDGE:* ${s.analysis.edge.toFixed(2)}%`;
        
        bot.sendMessage(chatId, msgText, {
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '🤖 ANALIZAR CON IA', callback_data: sigId }]] }
        }).catch(e => console.error("Error enviando mensaje a Telegram:", e.message));
    }
}

// === INTERACCIÓN IA ===
bot.on('callback_query', async (query) => {
    const sigId = query.data;
    if (!SIGNAL_CACHE.has(sigId)) return;
    const s = SIGNAL_CACHE.get(sigId);
    
    bot.answerCallbackQuery(query.id, { text: 'Consultando a Gemini...' });
    
    const promptText = `Analiza trading binarias: ${s.assetId}. Direccion: ${s.analysis.direction}. Probabilidad: ${s.analysis.prob.toFixed(1)}%. LOB Imbalance: ${s.obi.toFixed(3)}. Responde corto: EXECUTE o PASS, confianza % y motivo.`;

    try {
        const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${geminiApiKey}`;
        const result = await axios.post(url, { contents: [{ parts: [{ text: promptText }] }] });
        const text = result.data.candidates[0].content.parts[0].text;
        bot.sendMessage(chatId, `🧠 *VERDICTO IA*\n\n${text}`, { parse_mode: 'Markdown' });
    } catch (e) { 
        bot.sendMessage(chatId, '❌ Error con Gemini.');
    }
});

// Comandos
bot.onText(/\/scan/, () => globalScan());

// Cronograma cada 5 min (minuto 3:30)
cron.schedule('30 3,8,13,18,23,28,33,38,43,48,53,58 * * * *', () => globalScan());

// Auto-despertador cada 10 min
cron.schedule('*/10 * * * *', async () => {
    try { await axios.get(`https://quant-telegram-bot.onrender.com/`); } catch (e) {}
});

// Captura de errores globales para que el bot no "explote"
bot.on('polling_error', (error) => {
    if (error.message.includes('404')) {
        console.error("🚨 ERROR: El Token de Telegram no es válido (404).");
    } else {
        console.error("⚠️ Error de conexión Telegram:", error.message);
    }
});
