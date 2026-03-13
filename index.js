require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const cron = require('node-cron');
const axios = require('axios');
const express = require('express');

// === SERVIDOR PARA RENDER ===
const app = express();
const PORT = process.env.PORT || 3000;
app.get('/', (req, res) => res.send('🚀 Quant Sniper V12.5 ELITE Online'));
app.listen(PORT, () => console.log(`📡 Servidor HTTP activo en puerto ${PORT}`));

// === CONFIGURACIONES ===
const token = process.env.TELEGRAM_TOKEN;
const chatId = process.env.CHAT_ID;
const geminiApiKey = process.env.GEMINI_API_KEY;

// Polling más lento (2 seg) para evitar el error 409 Conflict
const bot = new TelegramBot(token, { 
    polling: {
        interval: 2000,
        autoStart: true,
        params: { timeout: 10 }
    } 
});

// Limpieza automática al arrancar
bot.deleteWebHook().then(() => console.log('🧹 Webhook limpio.'));

const CONFIG = {
    BINANCE_API: 'https://api.binance.com/api/v3',
    BACKEND_URL: 'https://quant-backend-lhue.onrender.com/api',
    PROB_THRESHOLD: 58,
    PATTERN_LENGTH: 6,
    MARKETS: [
        { id: 'BTC/USD', symbolBinance: 'BTCUSDT' }, 
        { id: 'ETH/USD', symbolBinance: 'ETHUSDT' },
        { id: 'SOL/USD', symbolBinance: 'SOLUSDT' },
        { id: 'BNB/USD', symbolBinance: 'BNBUSDT' },
        { id: 'ADA/USD', symbolBinance: 'ADAUSDT' },
        { id: 'XRP/USD', symbolBinance: 'XRPUSDT' }
    ]
};

const SIGNAL_CACHE = new Map();
let signalCounter = 0;

console.log('🤖 Motor Quant Sniper V12.5 ELITE Iniciado...');

// === LÓGICA DE ANÁLISIS CUANTITATIVO ===

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

function buildPatternVector(candles, atrs, endIndex) {
    let vec = [];
    for (let k = endIndex - CONFIG.PATTERN_LENGTH + 1; k <= endIndex; k++) {
        if (k < 5 || !candles[k] || !candles[k-1]) continue; 
        const c = candles[k];
        vec.push(c.c - c.o, c.h - c.l, c.c > c.o ? 1 : -1, atrs[k] || 1, c.c - candles[k-1].c);
    }
    return vec;
}

function runAnalysisElite(candles) {
    if (!candles || candles.length < 100) return null;
    const atrs = precalcATR(candles);
    const targetVector = buildPatternVector(candles, atrs, candles.length - 2);
    if (targetVector.length === 0) return null;

    let matches = [];
    const startIdx = Math.max(40, candles.length - 8000);

    for (let i = startIdx; i < candles.length - 10; i++) {
        const histVector = buildPatternVector(candles, atrs, i);
        if (histVector.length !== targetVector.length) continue;
        
        let dist = 0;
        for(let j=0; j<targetVector.length; j++) dist += Math.pow(targetVector[j] - histVector[j], 2);
        dist = Math.sqrt(dist);

        if (dist > 1.5) continue; 
        matches.push({ dist, win: (candles[i + 1].c > candles[i].c) ? 1 : 0 });
    }

    if (matches.length < 12) return null;
    matches.sort((a,b) => a.dist - b.dist);
    const top = matches.slice(0, 25);
    const wins = top.reduce((acc, m) => acc + m.win, 0);
    const prob = (wins / top.length) * 100;
    
    const direction = prob > 50 ? "BUY" : "SELL";
    const finalProb = direction === "BUY" ? prob : 100 - prob;

    return { prob: finalProb, direction, n: top.length };
}

// === ESCANEO GLOBAL ===
async function globalScan() {
    console.log(`[${new Date().toLocaleTimeString()}] 🚀 Iniciando Escaneo Global...`);
    
    for (const asset of CONFIG.MARKETS) {
        try {
            // 1. Obtener velas del Backend (IP de Render suele funcionar aquí)
            const res = await axios.get(`${CONFIG.BACKEND_URL}/candles?symbol=${asset.symbolBinance}&limit=5000`, { timeout: 15000 });
            const candles = res.data;
            if (!Array.isArray(candles)) continue;

            // 2. Obtener Order Book de Binance (Con bypass para error 451)
            let obi = 0;
            try {
                const lobRes = await axios.get(`${CONFIG.BINANCE_API}/depth?symbol=${asset.symbolBinance}&limit=20`);
                let bV=0, aV=0;
                lobRes.data.bids.forEach(l => bV += parseFloat(l[0]) * parseFloat(l[1]));
                lobRes.data.asks.forEach(l => aV += parseFloat(l[0]) * parseFloat(l[1]));
                obi = (bV - aV) / (bV + aV);
            } catch (binanceErr) {
                // Si Binance bloquea la IP, seguimos sin el OBI para que el bot no se detenga
                obi = 0; 
                console.log(`⚠️ Binance 451 en ${asset.id} (IP Restringida), continuando sin LOB.`);
            }

            // 3. Ejecutar Análisis
            const result = runAnalysisElite(candles);
            
            if (result && result.prob >= CONFIG.PROB_THRESHOLD) {
                const sigId = `sig_${signalCounter++}`;
                SIGNAL_CACHE.set(sigId, { asset, result, obi });

                const icon = result.direction === 'BUY' ? '🟢' : '🔴';
                const msg = `${icon} *${asset.id} | 5M*\n*PROB:* ${result.prob.toFixed(1)}%\n*DIR:* ${result.direction}\n*LOB:* ${obi === 0 ? 'N/A' : obi.toFixed(3)}`;
                
                bot.sendMessage(chatId, msg, {
                    parse_mode: 'Markdown',
                    reply_markup: { inline_keyboard: [[{ text: '🤖 ANALIZAR CON IA', callback_data: sigId }]] }
                });
            }
        } catch (e) {
            console.log(`❌ Error en ${asset.id}: ${e.message}`);
        }
    }
}

// === INTERACCIÓN CON IA ===
bot.on('callback_query', async (query) => {
    const data = SIGNAL_CACHE.get(query.data);
    if (!data) return;

    bot.answerCallbackQuery(query.id, { text: 'Gemini analizando mercado...' });

    const prompt = `Actúa como experto en trading. Analiza: ${data.asset.id}. Dir: ${data.result.direction}. Prob: ${data.result.prob.toFixed(1)}%. LOB: ${data.obi}. Responde MUY corto: EXECUTE o PASS y el por qué técnico.`;

    try {
        const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${geminiApiKey}`;
        const res = await axios.post(url, { contents: [{ parts: [{ text: prompt }] }] });
        bot.sendMessage(chatId, `🧠 *VERDICTO GEMINI IA*\n\n${res.data.candidates[0].content.parts[0].text}`, { parse_mode: 'Markdown' });
    } catch (e) {
        bot.sendMessage(chatId, '❌ Error: Gemini no pudo procesar el análisis.');
    }
});

// === COMANDOS Y PROGRAMACIÓN ===
bot.onText(/\/scan/, () => globalScan());

// Cron cada 5 minutos
cron.schedule('30 3,8,13,18,23,28,33,38,43,48,53,58 * * * *', () => globalScan());

// Keep-alive cada 10 min
cron.schedule('*/10 * * * *', async () => {
    try { await axios.get('https://quant-telegram-bot.onrender.com/'); } catch (e) {}
});

bot.on('polling_error', (err) => {
    if(!err.message.includes('409')) console.log('⚠️ Polling:', err.message);
});

console.log('✅ Sistema cargado. Esperando comandos...');
