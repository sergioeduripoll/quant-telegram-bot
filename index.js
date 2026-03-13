require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const cron = require('node-cron');
const axios = require('axios');

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
    REQUEST_LIMIT: 50000,
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

// Caché para guardar las señales en memoria y que la IA las pueda leer al tocar el botón
const SIGNAL_CACHE = new Map();
let signalCounter = 0;

console.log('🤖 Quant Sniper V12.5 ELITE iniciando motor Node.js...');
bot.sendMessage(chatId, '🟢 *Quant Sniper V12.5 ELITE* motor Node.js encendido.\n\nEl sistema ejecutará escaneos automáticos en el minuto 3:30 de cada vela de 5M.', { parse_mode: 'Markdown' });

// === MOTOR CUANTITATIVO (V12.5 CORE) ===

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

        const body = c.c - c.o;
        const range = c.h - c.l;
        const upperWick = c.h - Math.max(c.o, c.c);
        const lowerWick = Math.min(c.o, c.c) - c.l;
        const direction = c.c > c.o ? 1 : -1;
        const atrSlope = (currentATR - prev5ATR) / currentATR;
        const rangeComp = range / currentATR;
        const momentum = c.c - prevC.c;

        vec.push(body, range, upperWick, lowerWick, direction, atrSlope, rangeComp, momentum);
    }
    return vec;
}

function normalizeVector(v){
    const max = Math.max(...v.map(x => Math.abs(x))) || 1;
    return v.map(x => x / max);
}

function calculateDistance(vecA, vecB) {
    let sum = 0;
    for(let i=0; i<vecA.length; i++){
        sum += Math.pow(vecA[i] - (vecB[i] || 0), 2);
    }
    return Math.sqrt(sum);
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

function runAnalysisElite(candles) {
    if (candles.length < 100) return null;
    
    const regime = detectRegime(candles);
    const atrs = precalcATR(candles);
    const currentATR = atrs[candles.length - 2] || 1;
    const targetEndIdx = candles.length - 2; 

    const targetVector = buildPatternVector(candles, atrs, targetEndIdx);
    const vecTargetNorm = normalizeVector(targetVector);

    let matches = [];
    const startIdx = Math.max(40, candles.length - 40000);

    for (let i = startIdx; i < candles.length - 2 - patternLength - 3; i++) {
        for (let shift = 0; shift <= 2; shift++) {
            const histEndIdx = i + shift + patternLength - 1;
            const histATR = atrs[histEndIdx] || 1;
            const volRatio = histATR / currentATR;
            
            if (volRatio > 2.5 || volRatio < 0.4) continue; 

            const histVector = buildPatternVector(candles, atrs, histEndIdx);
            const vecHistNorm = normalizeVector(histVector);
            const dist = calculateDistance(vecTargetNorm, vecHistNorm);
            
            if (dist > 1.5) continue; 

            const win = (candles[histEndIdx + 1].c > candles[histEndIdx].c) ? 1 : 0;
            matches.push({ dist, win, timestamp: Date.now() - ((candles.length - histEndIdx) * 5 * 60 * 1000) });
        }
    }

    const uniqueMatchesMap = new Map();
    for (const m of matches) {
        if (!uniqueMatchesMap.has(m.timestamp) || uniqueMatchesMap.get(m.timestamp).dist > m.dist) {
            uniqueMatchesMap.set(m.timestamp, m);
        }
    }
    matches = Array.from(uniqueMatchesMap.values());

    if (matches.length < 25) return null;
    matches.sort((a,b) => a.dist - b.dist);
    
    const eliteCount = Math.min(50, Math.max(25, Math.floor(matches.length * 0.22)));
    const topMatches = matches.slice(0, eliteCount);
    
    let wins = 0; let losses = 0; let totalWeight = 0;
    const now = Date.now();
    
    for (const m of topMatches) {
        const weight = (1 / (m.dist + 0.0001)) * timeDecayWeight(m.timestamp, now);
        totalWeight += weight;
        if (m.win === 1) wins += weight;
        else losses += weight;
    }

    let prob = ((wins + 1) / (wins + losses + 2)) * 100;
    const volPenalty = currentATR > (atrs.reduce((a,b)=>a+b,0)/atrs.length) * 2 ? 0.9 : 1;
    prob = prob * volPenalty;

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
    const edge = Math.abs(prob - 50);

    if (edge < 4.0 || (regime === "COMPRESSION" && Math.abs(edge) < 6)) return null;

    return { prob: finalProb, direction: signal, edge: (signal === 'BUY' ? edge : -edge), absEdge: edge, stability, n: topMatches.length, acs: (edge / 50) * stability * (topMatches.length / 100), cwev: (Math.max(prob, 100-prob) - CONFIG.BE) * edge * stability };
}

function statisticalStrength(samples, alpha, prob) {
    if(samples < 25) return false; 
    return ((prob*0.5) + (alpha*0.3) + (Math.log(samples)/10*0.2)) > 0.45; 
}

// === ORQUESTADOR GLOBAL ===
async function globalScan() {
    console.log(`[${new Date().toLocaleTimeString()}] 🚀 Iniciando Escaneo Global Automático...`);
    const validSignals = [];

    for (const asset of CONFIG.MARKETS) {
        try {
            // 1. Obtener Histórico
            const histRes = await axios.get(`${CONFIG.BACKEND_URL}/candles?symbol=${asset.symbolBinance}&limit=${CONFIG.REQUEST_LIMIT}`);
            const historical = histRes.data;
            if (!historical || historical.length < 100) continue;

            // 2. Obtener Macro Estructura (Binance)
            const [mRes1h, mRes4h] = await Promise.all([
                axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${asset.symbolBinance}&interval=1h&limit=50`),
                axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${asset.symbolBinance}&interval=4h&limit=50`)
            ]);
            const s1h = detectStructure(mRes1h.data.map(v=>({h:parseFloat(v[2]),l:parseFloat(v[3])})));
            const s4h = detectStructure(mRes4h.data.map(v=>({h:parseFloat(v[2]),l:parseFloat(v[3])})));

            // 3. Obtener LOB (Order Book Imbalance)
            let obi = 0;
            try {
                const lobRes = await axios.get(`${CONFIG.BINANCE_API}/depth?symbol=${asset.symbolBinance}&limit=10`);
                let bV=0, aV=0;
                lobRes.data.bids.forEach(l => bV += parseFloat(l[0]) * parseFloat(l[1]));
                lobRes.data.asks.forEach(l => aV += parseFloat(l[0]) * parseFloat(l[1]));
                obi = (bV - aV) / (bV + aV);
            } catch(e) {}

            // 4. Analizar temporalidades
            const tfs = [{ tf: '5M', aggregate: 1 }, { tf: '15M', aggregate: 3 }, { tf: '30M', aggregate: 6 }, { tf: '1H', aggregate: 12 }];
            for (const item of tfs) {
                const res = runAnalysisElite(item.aggregate > 1 ? aggregateCandles(historical, item.aggregate) : historical);
                if (res) validSignals.push({ assetId: asset.id, tf: item.tf, analysis: res, obi, macro: { h1: s1h, h4: s4h } });
            }
        } catch(e) {
            console.error(`Error escaneando ${asset.id}:`, e.message);
        }
    } 

    // Filtrar la Élite absoluta
    const elite = validSignals.filter(s => {
        const isB = s.analysis.direction === "BUY";
        const hLOB = (isB && s.obi >= 0.12) || (!isB && s.obi <= -0.12);
        const mAl = (isB && (s.macro.h1 >= 0 || s.macro.h4 >= 0)) || (!isB && (s.macro.h1 <= 0 || s.macro.h4 <= 0));
        const isS = statisticalStrength(s.analysis.n, s.analysis.acs, s.analysis.prob);
        return s.analysis.prob >= PROB_THRESHOLD && s.analysis.acs >= 0.07 && hLOB && s.analysis.stability >= 0.55 && s.analysis.n >= 25 && isS && mAl;
    });
    
    elite.sort((a,b) => b.analysis.cwev - a.analysis.cwev);
    console.log(`[${new Date().toLocaleTimeString()}] 🏁 Escaneo finalizado. Señales Élite encontradas: ${elite.length}`);

    // ENVIAR A TELEGRAM
    if (elite.length > 0) {
        for (const s of elite) {
            const sigId = `sig_${signalCounter++}`;
            SIGNAL_CACHE.set(sigId, s);

            const isBuy = s.analysis.direction === 'BUY';
            const icon = isBuy ? '🟢' : '🔴';
            const lobText = s.obi > 0 ? `+${s.obi.toFixed(3)}` : s.obi.toFixed(3);

            const msgText = `
${icon} *${s.assetId} | ${s.tf}*
*DIRECCIÓN:* ${s.analysis.direction}
*PROB:* ${s.analysis.prob.toFixed(1)}%

📊 *MÉTRICAS CUANTITATIVAS:*
• Edge: \`${s.analysis.edge > 0 ? '+' : ''}${s.analysis.edge.toFixed(2)}%\`
• Estabilidad: \`${(s.analysis.stability * 100).toFixed(0)}%\`
• LOB Imbalance: \`${lobText}\`
• Alpha Conf: \`${s.analysis.acs.toFixed(3)}\`
• Muestras Élite: \`${s.analysis.n}\`
• Macro 1H/4H: \`${s.macro.h1} / ${s.macro.h4}\`
`;
            const options = {
                parse_mode: 'Markdown',
                reply_markup: {
                    inline_keyboard: [[{ text: '🤖 ANALIZAR CON IA', callback_data: sigId }]]
                }
            };
            bot.sendMessage(chatId, msgText, options);
        }
        
        // Limpieza de memoria (borramos caché viejo para que no explote la RAM del servidor con el tiempo)
        if(SIGNAL_CACHE.size > 100) {
            const keys = Array.from(SIGNAL_CACHE.keys());
            for(let i=0; i < 50; i++) SIGNAL_CACHE.delete(keys[i]);
        }
    }
}

// === INTEGRACIÓN DE IA (GEMINI) AL TOCAR EL BOTÓN ===
bot.on('callback_query', async (query) => {
    const sigId = query.data;
    const msgId = query.message.message_id;

    if (!SIGNAL_CACHE.has(sigId)) {
        bot.answerCallbackQuery(query.id, { text: 'La señal expiró de la memoria.', show_alert: true });
        return;
    }

    const s = SIGNAL_CACHE.get(sigId);
    bot.answerCallbackQuery(query.id, { text: 'Iniciando análisis neuronal...' });
    
    // Editamos el mensaje original para mostrar que está cargando y le quitamos el botón para que no lo toque 2 veces
    const loadingText = query.message.text + '\n\n⏳ _La IA está analizando los datos..._';
    bot.editMessageText(loadingText, { chat_id: chatId, message_id: msgId, parse_mode: 'Markdown' });

    const p_obi = s.obi !== null ? s.obi.toFixed(3) : 'N/A';
    const promptText = `Eres un trader institucional senior especializado en trading cuantitativo y análisis de flujo de órdenes. Evalúa oportunidades de opciones binarias generadas por un modelo cuantitativo. Horizonte máximo 1H. NO recalcules estadísticas, solo valida la señal profesionalmente.

Datos de mercado:
Asset: ${s.assetId}
Timeframe: ${s.tf}

Modelo:
Probability: ${s.analysis.prob.toFixed(1)} %
Edge: ${s.analysis.edge.toFixed(2)} %
Alpha Confidence Score: ${s.analysis.acs.toFixed(3)}
Stability: ${(s.analysis.stability * 100).toFixed(0)}%
Historical Sample Size: ${s.analysis.n}
Expected Value Metric (CWEV): ${s.analysis.cwev.toFixed(1)}

Order Flow:
Order Book Imbalance: ${p_obi}

Macro Structure:
1H Structure: ${s.macro.h1}
4H Structure: ${s.macro.h4}

⚠️ Atención especial:
- Si el asset es BTC o ETH, tu análisis debe ponderar fuertemente el LOB. Señales con |OBI| < 0.40 para BTC/ETH deben reducir significativamente la confianza y el trade debería PASS.
- Para otros assets, el análisis sigue reglas normales.
- El Edge es bidireccional: Edge negativo en SELL significa fuerte ventaja a favor de caída. NUNCA penalices Edge negativo en SELL.

Devuelve EXCLUSIVAMENTE:
TRADE_DECISION: EXECUTE o PASS
CONFIDENCE: [número]
REASONING: [razonamiento breve]`;

    try {
        const url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent";
        const result = await axios.post(url, {
            contents: [{ parts: [{ text: promptText }] }],
            generationConfig: { temperature: 0.0 }
        }, {
            headers: { 'Content-Type': 'application/json', 'X-goog-api-key': geminiApiKey }
        });

        const text = result.data.candidates[0].content.parts[0].text;
        
        const isExecute = text.includes('EXECUTE');
        const verdictIcon = isExecute ? '🚀' : '⛔';
        const decision = isExecute ? '*EXECUTE TRADE*' : '*PASS*';

        const confMatch = text.match(/CONFIDENCE:\s*(\d+)/i);
        const conf = confMatch ? confMatch[1] + '%' : 'N/A';

        const reasonMatch = text.match(/REASONING:\s*([\s\S]*?)(?:\nTRADE_QUALITY_SCORE|$)/i);
        const reason = reasonMatch ? reasonMatch[1].trim() : 'Sin razonamiento procesado.';

        const finalText = query.message.text + `\n\n${verdictIcon} *AI VERDICT: ${decision}*\n*Confianza:* ${conf}\n_📝 "${reason}"_`;
        
        bot.editMessageText(finalText, { chat_id: chatId, message_id: msgId, parse_mode: 'Markdown' });

    } catch (error) {
        console.error('Error Gemini API:', error.message);
        bot.editMessageText(query.message.text + '\n\n❌ _Fallo en conexión IA. Reintentá escaneo manual._', { chat_id: chatId, message_id: msgId, parse_mode: 'Markdown' });
    }
});

// === PROGRAMACIÓN DEL RELOJ (CRON) ===
// Ejecuta exactamente en el Segundo 30, de los Minutos 3, 8, 13, 18... de cada hora.
cron.schedule('30 3,8,13,18,23,28,33,38,43,48,53,58 * * * *', () => {
    globalScan();
});

// Comando manual de emergencia por si querés forzar un escaneo desde Telegram
bot.onText(/\/scan/, (msg) => {
    if (msg.chat.id.toString() === chatId) {
        bot.sendMessage(chatId, '⚙️ Forzando escaneo manual...');
        globalScan();
    }
});