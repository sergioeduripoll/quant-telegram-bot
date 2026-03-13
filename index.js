require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const cron = require('node-cron');
const axios = require('axios');
const express = require('express');

const app = express();
const PORT = process.env.PORT || 3000;
app.get('/', (req, res) => res.send('🚀 QUANT SNIPER V12.5 ELITE ONLINE'));
app.listen(PORT, () => console.log(`📡 Servidor activo en puerto ${PORT}`));

const token = process.env.TELEGRAM_TOKEN;
const chatId = process.env.CHAT_ID;
const geminiApiKey = process.env.GEMINI_API_KEY;

const bot = new TelegramBot(token, { polling: { interval: 2000, autoStart: true } });

const CONFIG = {
    BE: 54.94,
    PROB_THRESHOLD: 58,
    PATTERN_LENGTH: 6,
    BACKEND_URL: 'https://quant-backend-lhue.onrender.com/api',
    BINANCE_API: 'https://api.binance.com/api/v3',
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

// --- FUNCIONES MATEMÁTICAS TRASPLANTADAS DEL HTML ---

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

function detectRegime(candles) {
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
    for (let k = endIndex - CONFIG.PATTERN_LENGTH + 1; k <= endIndex; k++) {
        if (k < 5) continue;
        const c = candles[k];
        const prevC = candles[k-1] || c;
        const currentATR = atrs[k] || 1;
        const prev5ATR = atrs[k-5] || currentATR;
        vec.push(c.c - c.o, c.h - c.l, c.h - Math.max(c.o, c.c), Math.min(c.o, c.c) - c.l, c.c > c.o ? 1 : -1, (currentATR - prev5ATR) / currentATR, (c.h - c.l) / currentATR, c.c - prevC.c);
    }
    return vec;
}

function normalizeVector(v) {
    const max = Math.max(...v.map(x => Math.abs(x))) || 1;
    return v.map(x => x / max);
}

function calculateDistance(vecA, vecB) {
    let sum = 0;
    for(let i=0; i<vecA.length; i++) sum += Math.pow(vecA[i] - (vecB[i] || 0), 2);
    return Math.sqrt(sum);
}

// MOTOR DE ANÁLISIS ÉLITE (EL MISMO DEL HTML)
function runAnalysisElite(candles) {
    if (candles.length < 100) return null;
    const regime = detectRegime(candles);
    const atrs = precalcATR(candles);
    const currentATR = atrs[candles.length - 2] || 1;
    const targetVector = normalizeVector(buildPatternVector(candles, atrs, candles.length - 2));

    let matches = [];
    const startIdx = Math.max(40, candles.length - 40000);

    for (let i = startIdx; i < candles.length - 10; i++) {
        for (let shift = 0; shift <= 2; shift++) {
            const histEndIdx = i + shift + CONFIG.PATTERN_LENGTH - 1;
            const volRatio = atrs[histEndIdx] / currentATR;
            if (volRatio > 2.5 || volRatio < 0.4) continue;

            const dist = calculateDistance(targetVector, normalizeVector(buildPatternVector(candles, atrs, histEndIdx)));
            if (dist > 1.5) continue;

            const win = (candles[histEndIdx + 1].c > candles[histEndIdx].c) ? 1 : 0;
            matches.push({ dist, win, ts: i });
        }
    }

    if (matches.length < 25) return null;
    matches.sort((a,b) => a.dist - b.dist);
    const topMatches = matches.slice(0, Math.min(50, Math.max(25, Math.floor(matches.length * 0.22))));

    let wins = 0, losses = 0;
    topMatches.forEach(m => {
        const weight = 1 / (m.dist + 0.0001);
        if (m.win === 1) wins += weight; else losses += weight;
    });

    let prob = ((wins + 1) / (wins + losses + 2)) * 100;
    const direction = prob > 50 ? "BUY" : "SELL";
    const finalProb = direction === "BUY" ? prob : 100 - prob;
    const edge = Math.abs(prob - 50);

    if (edge < 4.0) return null;
    return { prob: finalProb, direction, n: topMatches.length, edge };
}

// ESCANEO Y ENVÍO A TELEGRAM
async function globalScan() {
    console.log("🚀 Iniciando Auditoría Institucional...");
    for (const asset of CONFIG.MARKETS) {
        try {
            const res = await axios.get(`${CONFIG.BACKEND_URL}/candles?symbol=${asset.symbolBinance}&limit=50000`);
            const analysis = runAnalysisElite(res.data);

            if (analysis && analysis.prob >= CONFIG.PROB_THRESHOLD) {
                const sigId = `sig_${Date.now()}_${asset.id.replace('/','')}`;
                SIGNAL_CACHE.set(sigId, { asset, analysis });

                const icon = analysis.direction === 'BUY' ? '🟢' : '🔴';
                const msg = `${icon} *SEÑAL ÉLITE: ${asset.id}*\n\n` +
                            `*PROBABILIDAD:* ${analysis.prob.toFixed(1)}%\n` +
                            `*DIRECCIÓN:* ${analysis.direction}\n` +
                            `*MUESTRA:* ${analysis.n} clones\n` +
                            `*EDGE:* ${analysis.edge.toFixed(2)}%`;

                bot.sendMessage(chatId, msg, {
                    parse_mode: 'Markdown',
                    reply_markup: { inline_keyboard: [[{ text: '🤖 VALIDAR CON GEMINI IA', callback_data: sigId }]] }
                });
            }
        } catch (e) { console.log(`❌ Error en ${asset.id}`); }
    }
}

// COMANDOS
bot.onText(/\/scan/, () => globalScan());
cron.schedule('30 3,8,13,18,23,28,33,38,43,48,53,58 * * * *', () => globalScan());

bot.on('callback_query', async (query) => {
    const data = SIGNAL_CACHE.get(query.data);
    if (!data) return;
    bot.answerCallbackQuery(query.id, { text: 'Gemini analizando...' });

    const prompt = `Eres un trader senior. Evalúa: ${data.asset.id}, Dir: ${data.analysis.direction}, Prob: ${data.analysis.prob.toFixed(1)}%, Samples: ${data.analysis.n}. Responde corto: EXECUTE o PASS y por qué.`;
    
    try {
        const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${geminiApiKey}`;
        const res = await axios.post(url, { contents: [{ parts: [{ text: prompt }] }] });
        bot.sendMessage(chatId, `🧠 *VERDICTO IA*\n\n${res.data.candidates[0].content.parts[0].text}`);
    } catch (e) { bot.sendMessage(chatId, '❌ Error en IA'); }
});
