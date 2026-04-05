require('dotenv').config();

const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
// INTEGRATION: Motor adaptativo evolutivo
const adaptive = require('./adaptiveEngine');
// INTEGRATION: Capa de persistencia Supabase (reemplaza CSV)
const db = require('./db');
// INTEGRATION: Persistencia de estado adaptativo
const adaptivePersistence = require('./adaptivePersistence');

// ═══════════════════════════════════════════════════════════════════
// QUANT SNIPER V15 — SUPABASE PERSISTENT (PRODUCTION)
// ═══════════════════════════════════════════════════════════════════

// === CONFIGURACIONES GLOBALES ===
const token = process.env.TELEGRAM_TOKEN;
const chatId = process.env.CHAT_ID;

// FIX 409: Polling resiliente — cancela webhook previo y maneja errores
const bot = new TelegramBot(token, {
    polling: {
        interval: 1000,
        autoStart: true,
        params: { timeout: 30 }
    }
});
bot.on('polling_error', (err) => {
    if (err.code === 'ETELEGRAM' && err.message.includes('409')) {
        console.error('[TG] ⚠️ Error 409: Otra instancia del bot está corriendo. Verificar que no haya duplicados.');
    }
});
// Limpiar webhook por si quedó uno viejo
bot.deleteWebHook().then(() => console.log('[TG] Webhook limpiado')).catch(() => {});

const patternLength = 6;
const PROB_THRESHOLD = 54;

// TEST_MODE: Forzar threshold bajo para pruebas de comunicación con broker
const TEST_MODE = process.env.TEST_MODE === 'true';
if (TEST_MODE) console.log('[CONFIG] ⚡ TEST_MODE activo — threshold forzado a 55');

// LOCK: Evita scans simultáneos
let isScanning = false;

const CONFIG = {
    BE: 54.94,
    BINANCE_API: 'https://api.binance.com/api/v3',
    BACKEND_URL: 'https://quant-backend-lhue.onrender.com/api', // DEPRECATED — ya no se usa
    REQUEST_LIMIT: 1000,
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
// signalCounter eliminado — usamos timestamp único para evitar colisiones en redeploy
const PENDING_AUDITS = [];

// Control Anti-Ruina Progresivo y Cache Global
let globalPauseUntil = 0;
let circuitBreakerLevel = 0;
let lastCircuitBreakerTradeCount = 0;
const GLOBAL_CANDLE_CACHE = new Map();

// ── Memoria Dinámica Adaptativa (3 capas) ──
const LOSS_STREAKS = {};                                // Capa 1: racha actual por activo
// IMPROVEMENT: Set en lugar de Array para O(1) en .has() vs O(n) en .includes()
const RECENT_FAILURE_PATTERNS = new Set();              // Capa 3: patrones fallidos recientes (max 50)
let RESOLVED_ROWS_CACHE = [];                           // Cache de filas resueltas para capa 2

// ── Caché de trades en RAM (reduce egress Supabase) ──
let GLOBAL_TRADES_CACHE = [];
let LAST_TRADES_FETCH = 0;


// ═══════════════════════════════════════════════════════════════════
// MÓDULO 1: SISTEMA DE APRENDIZAJE ADAPTATIVO (SUPABASE INTELLIGENCE)
// ═══════════════════════════════════════════════════════════════════

let ADAPTIVE_PROFILES = {};

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
            // Campos para evaluateHiddenRisk
            currentLossStreak: 0,
            maxLossStreak: 0,
            avgLossStreak: 0,
            streaks3Plus: 0,
            losingCombos: [],
            winningCombos: []
        };

        if (assetRows.length < 10) {
            profiles[assetId] = profile;
            continue;
        }

        profile.confidence = assetRows.length >= 25 ? 'HIGH' : 'MEDIUM';

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
        const goodTFs = Object.entries(tfScores)
            .filter(([_, v]) => v.wr >= 45)
            .sort((a, b) => b[1].score - a[1].score)
            .map(([tf]) => tf);
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

            if (avgLoss.cwev > avgWin.cwev * 1.1) {
                profile.lossPatterns.push(`CWEV alto (pérdida: ${avgLoss.cwev.toFixed(1)} vs ganancia: ${avgWin.cwev.toFixed(1)})`);
            }
            if (avgLoss.alpha > avgWin.alpha * 1.1) {
                profile.lossPatterns.push(`Alpha excesivo (pérdida: ${avgLoss.alpha.toFixed(3)} vs ganancia: ${avgWin.alpha.toFixed(3)})`);
            }
            if (avgLoss.absEdge > avgWin.absEdge * 1.15) {
                profile.lossPatterns.push(`Edge extremo (pérdida: ${avgLoss.absEdge.toFixed(1)} vs ganancia: ${avgWin.absEdge.toFixed(1)})`);
            }
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

        // --- Rachas de pérdidas consecutivas ---
        const allStreaks = [];
        let currentStreak = 0;
        for (const r of assetRows) {
            if (r._win === 0) {
                currentStreak++;
            } else {
                if (currentStreak > 0) allStreaks.push(currentStreak);
                currentStreak = 0;
            }
        }
        if (currentStreak > 0) allStreaks.push(currentStreak);

        profile.maxLossStreak = allStreaks.length > 0 ? Math.max(...allStreaks) : 0;
        profile.avgLossStreak = allStreaks.length > 0 ? allStreaks.reduce((a, b) => a + b, 0) / allStreaks.length : 0;
        profile.streaks3Plus = allStreaks.filter(s => s >= 3).length;

        // Racha actual
        let recentStreak = 0;
        for (let ri = assetRows.length - 1; ri >= 0; ri--) {
            if (assetRows[ri]._win === 0) recentStreak++;
            else break;
        }
        profile.currentLossStreak = recentStreak;

        // --- Fingerprinting de combinaciones ganadoras y perdedoras ---
        const comboStats = {};
        for (const r of assetRows) {
            const cwevBucket = r._cwev >= 7 ? 'hiCWEV' : 'loCWEV';
            const alphaBucket = r._alpha >= 0.035 ? 'hiAlpha' : 'loAlpha';
            const edgeBucket = Math.abs(r._edge) >= 8 ? 'hiEdge' : 'loEdge';
            const lobAligned = (r.Dir === 'BUY' && r._lob > 0) || (r.Dir === 'SELL' && r._lob < 0) ? 'lobOK' : 'lobBAD';
            const stabBucket = r._stab >= 80 ? 'hiStab' : 'loStab';
            const tf = r.TF || '?';

            const fingerprints = [
                `${cwevBucket}+${alphaBucket}`,
                `${cwevBucket}+${lobAligned}`,
                `${edgeBucket}+${alphaBucket}`,
                `${tf}+${cwevBucket}`,
                `${stabBucket}+${cwevBucket}`,
                `${tf}+${lobAligned}`,
                `${cwevBucket}+${alphaBucket}+${lobAligned}`,
                `${tf}+${cwevBucket}+${alphaBucket}`
            ];

            for (const fp of fingerprints) {
                if (!comboStats[fp]) comboStats[fp] = { w: 0, l: 0 };
                if (r._win === 1) comboStats[fp].w++;
                else comboStats[fp].l++;
            }
        }

        for (const [fp, stats] of Object.entries(comboStats)) {
            const total = stats.w + stats.l;
            if (total < 5) continue;
            const wr = (stats.w / total) * 100;
            if (wr < 40) {
                profile.losingCombos.push({ fingerprint: fp, wr: Math.round(wr), n: total });
            } else if (wr >= 60) {
                profile.winningCombos.push({ fingerprint: fp, wr: Math.round(wr), n: total });
            }
        }

        profile.losingCombos.sort((a, b) => a.wr - b.wr);
        profile.winningCombos.sort((a, b) => b.wr - a.wr);

        profiles[assetId] = profile;
    }

    return profiles;
}

/**
 * Actualiza las 3 estructuras de memoria dinámica desde los datos resueltos del CSV.
 */
function updateMemoryFromCSV(resolvedRows) {
    // ── Capa 1: LOSS_STREAKS por activo ──
    for (const market of CONFIG.MARKETS) {
        LOSS_STREAKS[market.id] = 0;
    }
    for (const r of resolvedRows) {
        const asset = r.Activo;
        if (!asset) continue;
        if (r.Veredicto === 'PERDIDA' || r._win === 0) {
            LOSS_STREAKS[asset] = (LOSS_STREAKS[asset] || 0) + 1;
        } else if (r.Veredicto === 'GANADA' || r._win === 1) {
            LOSS_STREAKS[asset] = 0;
        }
    }

    // ── Capa 3: RECENT_FAILURE_PATTERNS desde pérdidas recientes ──
    // IMPROVEMENT: Usar Set — clear() para limpiar, .add() para insertar, .has() para buscar
    RECENT_FAILURE_PATTERNS.clear();
    const recentLosses = resolvedRows.filter(r => r._win === 0).slice(-50);
    for (const r of recentLosses) {
        const cwevRound = Math.round(r._cwev || 0);
        const patternKey = `${r.TF || '?'}_${r.Dir || '?'}_${cwevRound}`;
        RECENT_FAILURE_PATTERNS.add(patternKey);
    }
    // IMPROVEMENT: FIFO con Set — si supera 50, convertir a array, recortar, reconstruir
    if (RECENT_FAILURE_PATTERNS.size > 50) {
        const arr = [...RECENT_FAILURE_PATTERNS];
        RECENT_FAILURE_PATTERNS.clear();
        for (const item of arr.slice(-50)) {
            RECENT_FAILURE_PATTERNS.add(item);
        }
    }

    // Actualizar cache global
    RESOLVED_ROWS_CACHE = resolvedRows;
}

/**
 * Capa 2: Calcula riesgo oculto basado en similitud con trades históricos.
 */
function calculateHiddenRisk(signal, resolvedRows) {
    if (!resolvedRows || resolvedRows.length === 0) return 0;
    if (!signal || !signal.analysis) return 0;

    const cwev = signal.analysis.cwev;
    const alpha = signal.analysis.acs;
    const edge = signal.analysis.edge;

    const similar = resolvedRows.filter(r => {
        if (r.Activo !== signal.assetId) return false;
        if (Math.abs((r._cwev || 0) - cwev) >= 2) return false;
        if (Math.abs((r._alpha || 0) - alpha) >= 0.01) return false;
        if (Math.abs((r._edge || 0) - edge) >= 2) return false;
        return true;
    });

    if (similar.length < 5) return 0;

    const losses = similar.filter(r => r._win === 0).length;
    return (losses / similar.length) * 100;
}

/**
 * Genera las fingerprints de combinación de una señal.
 */
function getSignalFingerprints(signal) {
    const cwevBucket = signal.analysis.cwev >= 7 ? 'hiCWEV' : 'loCWEV';
    const alphaBucket = signal.analysis.acs >= 0.035 ? 'hiAlpha' : 'loAlpha';
    const edgeBucket = Math.abs(signal.analysis.edge) >= 8 ? 'hiEdge' : 'loEdge';
    const lobAligned = (signal.analysis.direction === 'BUY' && signal.obi > 0) ||
                       (signal.analysis.direction === 'SELL' && signal.obi < 0) ? 'lobOK' : 'lobBAD';
    const stabBucket = (signal.analysis.stability * 100) >= 80 ? 'hiStab' : 'loStab';
    const tf = signal.tf || '?';

    return [
        `${cwevBucket}+${alphaBucket}`,
        `${cwevBucket}+${lobAligned}`,
        `${edgeBucket}+${alphaBucket}`,
        `${tf}+${cwevBucket}`,
        `${stabBucket}+${cwevBucket}`,
        `${tf}+${lobAligned}`,
        `${cwevBucket}+${alphaBucket}+${lobAligned}`,
        `${tf}+${cwevBucket}+${alphaBucket}`
    ];
}

/**
 * evaluateHiddenRisk — riskScore interno (0-100)
 */
function evaluateHiddenRisk(signal, profile) {
    const factors = [];
    let riskScore = 0;

    // ── 1. Win Rate histórico del activo ──
    if (profile.overallWR < 42) {
        const wrPenalty = Math.min(25, Math.round((50 - profile.overallWR) * 2));
        riskScore += wrPenalty;
        factors.push(`WR bajo: ${profile.overallWR.toFixed(1)}% (+${wrPenalty})`);
    } else if (profile.overallWR < 48) {
        const wrPenalty = Math.min(12, Math.round((50 - profile.overallWR) * 1.5));
        riskScore += wrPenalty;
        factors.push(`WR mediocre: ${profile.overallWR.toFixed(1)}% (+${wrPenalty})`);
    }

    // ── 2. Rachas de pérdidas consecutivas ──
    if (profile.currentLossStreak >= 4) {
        riskScore += 30;
        factors.push(`Racha activa: ${profile.currentLossStreak} pérdidas (+30)`);
    } else if (profile.currentLossStreak >= 3) {
        riskScore += 20;
        factors.push(`Racha activa: ${profile.currentLossStreak} pérdidas (+20)`);
    } else if (profile.currentLossStreak >= 2) {
        riskScore += 8;
        factors.push(`Racha menor: ${profile.currentLossStreak} pérdidas (+8)`);
    }

    if (profile.streaks3Plus >= 3) {
        riskScore += 12;
        factors.push(`${profile.streaks3Plus} rachas de 3+ pérdidas (+12)`);
    } else if (profile.streaks3Plus >= 2) {
        riskScore += 6;
        factors.push(`${profile.streaks3Plus} rachas de 3+ pérdidas (+6)`);
    }

    if (profile.maxLossStreak >= 5) {
        riskScore += 8;
        factors.push(`Max racha histórica: ${profile.maxLossStreak} (+8)`);
    }

    // ── 3. Coincidencia con patrones perdedores del CSV ──
    const signalFPs = getSignalFingerprints(signal);
    let losingMatchCount = 0;
    let worstLosingWR = 100;
    let losingMatchDetails = [];

    for (const fp of signalFPs) {
        const losingMatch = profile.losingCombos.find(c => c.fingerprint === fp);
        if (losingMatch) {
            losingMatchCount++;
            if (losingMatch.wr < worstLosingWR) {
                worstLosingWR = losingMatch.wr;
            }
            losingMatchDetails.push(`${fp}:${losingMatch.wr}%`);
        }
    }

    if (losingMatchCount >= 5) {
        const comboPenalty = Math.min(35, 15 + (losingMatchCount * 3));
        riskScore += comboPenalty;
        factors.push(`${losingMatchCount} combos perdedores (peor: ${worstLosingWR}%) (+${comboPenalty})`);
    } else if (losingMatchCount >= 3) {
        const comboPenalty = Math.min(22, 8 + (losingMatchCount * 3));
        riskScore += comboPenalty;
        factors.push(`${losingMatchCount} combos perdedores (peor: ${worstLosingWR}%) (+${comboPenalty})`);
    } else if (losingMatchCount >= 1) {
        const comboPenalty = 5 * losingMatchCount;
        riskScore += comboPenalty;
        factors.push(`${losingMatchCount} combo perdedor (${losingMatchDetails.join(', ')}) (+${comboPenalty})`);
    }

    // Bonus negativo si coincide con combos ganadores
    let winningMatchCount = 0;
    for (const fp of signalFPs) {
        const winningMatch = profile.winningCombos.find(c => c.fingerprint === fp);
        if (winningMatch) winningMatchCount++;
    }
    if (winningMatchCount >= 3) {
        const bonus = Math.min(15, winningMatchCount * 3);
        riskScore -= bonus;
        factors.push(`${winningMatchCount} combos ganadores (-${bonus})`);
    }

    // ── 4. Contexto actual ──
    const isB = signal.analysis.direction === 'BUY';
    if (signal.momentumSlope !== undefined) {
        if ((isB && signal.momentumSlope < -0.0005) || (!isB && signal.momentumSlope > 0.0005)) {
            riskScore += 8;
            factors.push(`Momentum fuerte contra dirección (+8)`);
        }
    }

    if ((isB && signal.obi < -0.1) || (!isB && signal.obi > 0.1)) {
        riskScore += 6;
        factors.push(`LOB desalineado (+6)`);
    }

    if (signal.analysis.stability < 0.50 && signal.analysis.cwev >= 8) {
        riskScore += 10;
        factors.push(`Baja estabilidad + CWEV alto (+10)`);
    }

    riskScore = Math.max(0, Math.min(100, riskScore));

    return { riskScore, riskFactors: factors };
}

function applyAdaptiveFilter(signal) {
    const profile = ADAPTIVE_PROFILES[signal.assetId];
    if (!profile || profile.confidence === 'LOW') {
        return { pass: true, reason: 'Sin datos suficientes', adjustedScore: 100, riskScore: 0, riskFactors: [] };
    }

    const reasons = [];
    let penalty = 0;

    // --- Filtros existentes (sin modificar) ---
    if (signal.analysis.cwev >= profile.maxCWEV) {
        reasons.push(`CWEV ${signal.analysis.cwev.toFixed(1)} >= ${profile.maxCWEV}`);
        penalty += 20;
    }

    if (signal.analysis.acs >= profile.maxAlpha) {
        reasons.push(`Alpha ${signal.analysis.acs.toFixed(3)} >= ${profile.maxAlpha}`);
        penalty += 15;
    }

    if (Math.abs(signal.analysis.edge) >= profile.maxAbsEdge) {
        reasons.push(`|Edge| ${Math.abs(signal.analysis.edge).toFixed(1)} >= ${profile.maxAbsEdge}`);
        penalty += 15;
    }

    if (!profile.preferredTFs.includes(signal.tf)) {
        reasons.push(`TF ${signal.tf} no preferido`);
        penalty += 25;
    }

    if (profile.overallWR < 42 && profile.totalTrades >= 15) {
        reasons.push(`WR global bajo: ${profile.overallWR.toFixed(1)}%`);
        penalty += 30;
    }

    // --- Integración de evaluateHiddenRisk ---
    const { riskScore, riskFactors } = evaluateHiddenRisk(signal, profile);

    if (riskScore > 60) {
        reasons.push(`RIESGO OCULTO ALTO: ${riskScore}/100`);
        penalty = 100;
    } else if (riskScore >= 40) {
        const extraPenalty = Math.round((riskScore - 40) * 0.75);
        penalty += extraPenalty;
        reasons.push(`Riesgo moderado: ${riskScore}/100 (+${extraPenalty})`);
    }

    // ── CAPA 1: Memoria de racha por activo ──
    const currentStreak = LOSS_STREAKS[signal.assetId] || 0;
    if (currentStreak >= 4) {
        penalty += 35;
        reasons.push(`Racha crítica: ${currentStreak} pérdidas (+35)`);
    } else if (currentStreak >= 3) {
        penalty += 25;
        reasons.push(`Racha activa: ${currentStreak} pérdidas (+25)`);
    } else if (currentStreak >= 2) {
        penalty += 15;
        reasons.push(`Racha menor: ${currentStreak} pérdidas (+15)`);
    }

    // ── CAPA 2: Riesgo oculto por similitud numérica ──
    const hiddenRisk = calculateHiddenRisk(signal, RESOLVED_ROWS_CACHE);
    if (hiddenRisk > 60) {
        penalty += 40;
        reasons.push(`Similitud con pérdidas: ${hiddenRisk.toFixed(1)}% (+40)`);
    } else if (hiddenRisk > 45) {
        penalty += 20;
        reasons.push(`Similitud moderada: ${hiddenRisk.toFixed(1)}% (+20)`);
    }

    // ── CAPA 3: Memoria de patrones fallidos recientes ──
    const cwevRound = Math.round(signal.analysis.cwev || 0);
    const patternKey = `${signal.tf}_${signal.analysis.direction}_${cwevRound}`;
    // IMPROVEMENT: .has() en Set = O(1) vs .includes() en Array = O(n)
    if (RECENT_FAILURE_PATTERNS.has(patternKey)) {
        penalty += 30;
        reasons.push(`Patrón fallido reciente: ${patternKey} (+30)`);
    }

    const pass = penalty < 40;
    return {
        pass,
        reason: reasons.length > 0 ? reasons.join(' | ') : 'OK',
        adjustedScore: Math.max(0, 100 - penalty),
        penalty,
        riskScore,
        riskFactors
    };
}

function getAssetLearningContext(assetId) {
    const profile = ADAPTIVE_PROFILES[assetId];
    if (!profile || profile.confidence === 'LOW') {
        return 'Sin datos históricos suficientes para este activo.';
    }

    let ctx = `Datos históricos ${assetId} (${profile.totalTrades} trades, WR: ${profile.overallWR.toFixed(1)}%, Confianza: ${profile.confidence}):\n`;
    ctx += `- Filtros óptimos: CWEV<${profile.maxCWEV}, Alpha<${profile.maxAlpha}, |Edge|<${profile.maxAbsEdge}\n`;
    ctx += `- TFs rentables: ${profile.preferredTFs.join(', ')}\n`;

    if (profile.lossPatterns.length > 0) {
        ctx += `- Patrones de pérdida: ${profile.lossPatterns.join('; ')}\n`;
    }
    if (profile.lobBias > 3) {
        ctx += `- LOB alineado mejora WR en +${profile.lobBias.toFixed(1)}%\n`;
    }

    if (profile.currentLossStreak >= 2) {
        ctx += `- ⚠️ RACHA ACTIVA: ${profile.currentLossStreak} pérdidas consecutivas\n`;
    }
    if (profile.maxLossStreak >= 4) {
        ctx += `- Racha máxima histórica: ${profile.maxLossStreak} pérdidas\n`;
    }

    if (profile.losingCombos.length > 0) {
        const topLosing = profile.losingCombos.slice(0, 3);
        ctx += `- Combos tóxicos: ${topLosing.map(c => `${c.fingerprint}(${c.wr}% WR, ${c.n}t)`).join(', ')}\n`;
    }

    if (profile.winningCombos.length > 0) {
        const topWinning = profile.winningCombos.slice(0, 3);
        ctx += `- Combos rentables: ${topWinning.map(c => `${c.fingerprint}(${c.wr}% WR, ${c.n}t)`).join(', ')}\n`;
    }

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
    } catch(e) {
        // FIX: Error detallado en vez de mensaje genérico
        console.error('[TIME_SYNC]', { message: e.message, time: new Date().toISOString() });
    }
}
syncTimeWithBinance();
setInterval(syncTimeWithBinance, 60 * 60 * 1000);

function getSyncedTime() { return Date.now() + timeOffset; }

function getLocalTime() {
    return new Date(getSyncedTime()).toLocaleTimeString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour12: false });
}


// ═══════════════════════════════════════════════════════════════════
// MÓDULO 4: FUNCIONES DE TRADING (CFDs / QUANTFURY)
// ═══════════════════════════════════════════════════════════════════

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


// ═══════════════════════════════════════════════════════════════════
// MÓDULO 5: LÓGICA DINÁMICA (Aprendizaje de Zonas del CSV)
// ═══════════════════════════════════════════════════════════════════

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


// ═══════════════════════════════════════════════════════════════════
// MÓDULO 6: MOTOR CUANTITATIVO
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
    if (!TEST_MODE && entropy > 0.998) return null;
    const zScore = calculateZScore(recentCandles.map(c => c.c));
    if (!TEST_MODE && Math.abs(zScore) > 2.5) return null;

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

    // TEST_MODE: Relajar mínimo de matches (5 vs 15)
    const minMatches = TEST_MODE ? 5 : 15;
    if (matches.length < minMatches) return null;
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

    // TEST_MODE: Saltear filtro estricto de edge para pruebas mecánicas
    if (!TEST_MODE) {
        if (
            edge < 1.2 ||
            (regime === "COMPRESSION" && edge < 6) ||
            (regime === "TREND" && edge < 0.3)
        ) return null;
    }

    return {
        prob: finalProb,
        direction: signal,
        edge: (signal === 'BUY' ? edge : -edge),
        absEdge: edge,
        stability,
        n: topMatches.length,
        acs: (edge / 50) * stability * (topMatches.length / 100),
        cwev: edge * stability,
        currentPrice: candles[candles.length - 1].c,
        currentATR: currentATR
    };
}

function statisticalStrength(samples, alpha, prob) {
    if(samples < 15) return false;
    return ((prob*0.5) + (alpha*0.3) + (Math.log(samples)/10*0.2)) > 0.45;
}

function makeProgressBar(percent) {
    const f = Math.min(10, Math.max(0, Math.round(percent / 10)));
    return '█'.repeat(f) + '░'.repeat(10 - f);
}

/**
 * Envío seguro a Telegram: intenta Markdown, fallback sin formato.
 */
async function safeSend(text) {
    try {
        return await bot.sendMessage(chatId, text, { parse_mode: 'Markdown' });
    } catch (e) {
        console.error('[TG_MARKDOWN]', { message: e.message, time: new Date().toISOString() });
        return await bot.sendMessage(chatId, text.replace(/[*_`|]/g, ''));
    }
}


// ═══════════════════════════════════════════════════════════════════
// MÓDULO 7: IA AUTOMÁTICA (Gemini — se ejecuta al generar señal)
// ═══════════════════════════════════════════════════════════════════
async function executeIAAnalysis(s, modeString) {
    // ═══ CÓDIGO ORIGINAL DE IA (preservado para reactivar) ═══
    if (!s || !s.analysis) return null;


    const assetContext = getAssetLearningContext(s.assetId);
    const dualScore = s.dualScore || 0;

    const promptText = `Eres un Quant Trader Institucional. Decide el SCORE de ejecución y CLASIFICA el contexto.
MODO: ${modeString} (Dual Score: ${dualScore.toFixed(1)}).

SEÑAL:
Activo: ${s.assetId} | TF: ${s.tf} | Dir: ${s.analysis.direction} | Prob: ${s.analysis.prob.toFixed(1)}% | Edge: ${s.analysis.edge.toFixed(2)}%
Stability: ${(s.analysis.stability * 100).toFixed(0)}% | LOB: ${s.obi.toFixed(3)} | Momentum: ${s.momentumSlope.toFixed(4)} | Macro 4H: ${s.macro.h4}

HISTORIAL APRENDIDO:
${assetContext}

FILTRO ADAPTATIVO: Score ${s.adaptiveScore || 'N/A'}/100. ${s.adaptiveReason || ''}

REGLAS:
1. TRADE_CONTEXT:
   - CONTINUATION: Momentum y Macro a favor.
   - REVERSAL: Contra Macro pero Momentum fuerte a favor.
   - TRAP: Divergencia peligrosa.
2. AI_SCORE (0-100): >65 = EXECUTE. Penaliza si el historial muestra patrones de pérdida similares.

Responde SOLO:
AI_SCORE: (0-100)
TRADE_CONTEXT: (CONTINUATION, REVERSAL, o TRAP)
REASONING: (2 oraciones).`;

    try {
        const apiKey = process.env.GEMINI_API_KEY;
        const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key=${apiKey}`;
        const result = await axios.post(url, {
            contents: [{ parts: [{ text: promptText }] }],
            generationConfig: { temperature: 0.2 }
        });

        const candidates = result?.data?.candidates;
        if (!candidates || !candidates[0]?.content?.parts?.[0]?.text) {
            console.error('[IA_PARSE]', { message: 'Respuesta vacía de Gemini', time: new Date().toISOString() });
            return null;
        }
        const text = candidates[0].content.parts[0].text;

        const scoreMatch = text.match(/AI_SCORE:\s*(\d+)/i);
        const iaScore = scoreMatch ? Math.min(100, Math.max(0, parseInt(scoreMatch[1]))) : 0;

        const contextMatch = text.match(/TRADE_CONTEXT:\s*(CONTINUATION|REVERSAL|TRAP)/i);
        const iaContextText = contextMatch ? contextMatch[1].toUpperCase() : 'UNKNOWN';

        const reasonMatch = text.match(/REASONING:\s*([\s\S]*?)$/i);
        const iaReasoning = reasonMatch ? reasonMatch[1].trim().replace(/[*_[\]]/g, '').substring(0, 500) : 'Análisis completado.';

        if (!scoreMatch) {
            console.error('[IA_PARSE]', { message: 'No se pudo extraer AI_SCORE', rawText: text.substring(0, 200), time: new Date().toISOString() });
        }

        return {
            iaScore,
            iaContext: iaContextText,
            iaReasoning,
            iaVerdict: iaScore >= 65 ? 'EXECUTE' : 'PASS',
            isExecute: iaScore >= 65
        };
    } catch (e) {
        console.error('[IA_GEMINI]', { message: e.message, stack: e.stack, time: new Date().toISOString() });
        return null;
    }
}


// ═══════════════════════════════════════════════════════════════════
// MÓDULO 8: COMANDOS DE TELEGRAM
// ═══════════════════════════════════════════════════════════════════

bot.onText(/\/start/, (msg) => {
    if (msg.chat.id.toString() === chatId) {
        bot.sendMessage(chatId, '👋 *Quant Sniper V15 SUPABASE* operativo.\n🧠 Motor adaptativo + RL + IA on-demand.\n\n/scan — Escaneo manual\n/profile — Perfiles adaptativos\n/stats — Estadísticas (hoy + global)\n/analyze — Análisis adaptativo\n/broker — Estado del broker', { parse_mode: 'Markdown' });
    }
});

bot.onText(/\/scan/, async (msg) => {
    if (msg.chat.id.toString() === chatId) {
        await globalScan('manual');
    }
});

bot.onText(/\/profile/, async (msg) => {
    if (msg.chat.id.toString() === chatId) {
        // DB: Cargar trades resueltos desde Supabase
        const allRows = await db.getRecentTrades(1000);
        const resolvedRows = allRows.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA');
        const profiles = buildAdaptiveProfiles(resolvedRows);

        let text = '📊 *PERFILES ADAPTATIVOS*\n';
        for (const [assetId, p] of Object.entries(profiles)) {
            if (p.totalTrades < 5) continue;
            text += `\n*${assetId}* (${p.totalTrades}t, WR: ${p.overallWR.toFixed(1)}%)`;
            text += `\nCWEV<${p.maxCWEV} | Alpha<${p.maxAlpha} | |Edge|<${p.maxAbsEdge}`;
            text += `\nTFs: ${p.preferredTFs.join(', ')} | ${p.confidence}`;
            if (p.currentLossStreak >= 2) {
                text += `\n🔴 Racha activa: ${p.currentLossStreak} pérdidas`;
            }
            if (p.losingCombos.length > 0) {
                text += `\n⚠️ ${p.losingCombos.length} combos tóxicos`;
            }
            if (p.winningCombos.length > 0) {
                text += `\n✅ ${p.winningCombos.length} combos rentables`;
            }
            if (p.lossPatterns.length > 0) {
                text += `\n📉 ${p.lossPatterns[0]}`;
            }
            text += '\n';
        }

        await safeSend(text);
    }
});

bot.onText(/\/stats/, async (msg) => {
    if (msg.chat.id.toString() === chatId) {
        const allRows = await db.getRecentTrades(1000);
        const resolved = allRows.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA');

        // Trades de hoy (GMT-3 Argentina)
        const { todayResolved } = await db.getTodayTrades();
        const todayW = todayResolved.filter(r => r._win === 1).length;
        const todayL = todayResolved.length - todayW;
        const todayWR = todayResolved.length > 0 ? (todayW / todayResolved.length * 100).toFixed(1) : '0.0';

        if (resolved.length === 0 && todayResolved.length === 0) {
            await safeSend('Sin trades resueltos aun.');
            return;
        }

        const totalW = resolved.filter(r => r._win === 1).length;
        const totalL = resolved.length - totalW;
        const globalWR = resolved.length > 0 ? (totalW / resolved.length * 100).toFixed(1) : '0.0';

        let text = 'SESION HOY (GMT-3)\n';
        text += `Trades: ${todayResolved.length} | W: ${todayW} | L: ${todayL} | WR: ${todayWR}%\n\n`;
        text += 'HISTORIAL GLOBAL\n';
        text += `Trades: ${resolved.length} | W: ${totalW} | L: ${totalL} | WR: ${globalWR}%\n\n`;

        for (const market of CONFIG.MARKETS) {
            const ar = resolved.filter(r => r.Activo === market.id);
            if (ar.length === 0) continue;
            const w = ar.filter(r => r._win === 1).length;
            const wr = (w / ar.length * 100).toFixed(1);
            const bar = makeProgressBar(parseFloat(wr));
            text += `${market.id}: ${bar} ${wr}% (${ar.length})\n`;
        }

        await safeSend(text);
    }
});

console.log(`🤖 Quant Sniper V15 SUPABASE iniciando...`);

// HYDRATION: Cargar estado adaptativo ANTES de procesar cualquier señal
(async () => {
    try {
        const savedState = await adaptivePersistence.loadState();
        adaptive.hydrateState(savedState);
        console.log('[BOOT] Estado adaptativo hidratado desde Supabase');

        // Bootstrap inicial desde trades en DB para sincronizar ASSET_STATS
        const rows = await db.getRecentTrades(1000);
        const resolved = rows.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA');
        if (resolved.length > 0) {
            adaptive.learnFromCSV(resolved, 'incremental');
            console.log(`[BOOT] Stats sincronizados desde ${resolved.length} trades`);
        }
    } catch (e) {
        console.error('[BOOT]', { message: e.message, stack: e.stack, time: new Date().toISOString() });
    }

    bot.sendMessage(chatId, '🟢 *Quant Sniper V15 SUPABASE* encendido.\n🧠 RL persistente + Supabase + IA on-demand\n📊 /profile /stats /scan /analyze', { parse_mode: 'Markdown' });
})();


// ═══════════════════════════════════════════════════════════════════
// MÓDULO 9: ORQUESTADOR GLOBAL (SCAN)
// ═══════════════════════════════════════════════════════════════════

async function globalScan(scanType = 'auto') {
    // LOCK: Evita scans simultáneos
    if (isScanning) {
        console.log('[SCAN] Scan ya en progreso, ignorando.');
        return;
    }
    isScanning = true;

    try {
    // ── Inicio de lógica de scan (intacta) ──
    const currentServerTime = getSyncedTime();

    // DISABLED: Circuit Breaker desactivado durante fase de recolección de datos
    // if (currentServerTime < globalPauseUntil) {
    //     console.log(`[PAUSA] Bot en reposo hasta ${new Date(globalPauseUntil).toLocaleTimeString('es-AR')}`);
    //     return;
    // }

    const startTime = getLocalTime();
    console.log(`[${startTime}] 🚀 Scan ADAPTIVE...`);

    // SILENCED: No enviar mensaje de inicio de scan durante recolección
    // let statusMsg;
    // try {
    //     const title = scanType === 'auto' ? '🔄 *Scan Auto*' : '⏳ *Scan Manual*';
    //     statusMsg = await bot.sendMessage(chatId, `${title} ▶️ ${startTime}`, { parse_mode: 'Markdown' });
    // } catch(e) {
    //     console.error('[TG_SCAN_MSG]', { message: e.message, time: new Date().toISOString() });
    // }

    // --- Cargar datos desde Supabase con caché en RAM ---
    let parsedData = [];
    const assetPerformance = {};
    const candlesByAsset = {};

    try {
        // Caché: full fetch cada 1h, micro-update de últimos 30 entre medio
        const now = Date.now();
        if (GLOBAL_TRADES_CACHE.length === 0 || (now - LAST_TRADES_FETCH > 60 * 60 * 1000)) {
            GLOBAL_TRADES_CACHE = await db.getRecentTrades(1000);
            LAST_TRADES_FETCH = now;
            console.log(`[CACHE] Full fetch: ${GLOBAL_TRADES_CACHE.length} trades cargados`);
        } else {
            const recentTrades = await db.getRecentTrades(30);
            for (const rt of recentTrades) {
                if (!rt.signal_id) continue;
                const index = GLOBAL_TRADES_CACHE.findIndex(t => t.signal_id === rt.signal_id);
                if (index !== -1) { GLOBAL_TRADES_CACHE[index] = rt; }
                else { GLOBAL_TRADES_CACHE.push(rt); }
            }
            if (GLOBAL_TRADES_CACHE.length > 1000) {
                GLOBAL_TRADES_CACHE = GLOBAL_TRADES_CACHE.slice(-1000);
            }
        }
        parsedData = GLOBAL_TRADES_CACHE;

        if (parsedData.length > 0) {
            CONFIG.MARKETS.forEach(m => {
                const assetRows = parsedData.filter(r => r.Activo === m.id && (r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA')).slice(-20);
                if (assetRows.length > 0) {
                    const wins = assetRows.filter(r => r.Veredicto === 'GANADA').length;
                    assetPerformance[m.id] = { wr: (wins / assetRows.length * 100).toFixed(1), count: assetRows.length };
                }
            });

            // DISABLED: Circuit Breaker desactivado durante fase de recolección de datos
            // const totalFinishedTrades = parsedData.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA').length;
            // const globalLastTrades = parsedData.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA').slice(-4);
            //
            // if (globalLastTrades.length > 0 && globalLastTrades[globalLastTrades.length - 1].Veredicto === 'GANADA') {
            //     circuitBreakerLevel = 0;
            // }
            //
            // if (globalLastTrades.length === 4 && globalLastTrades.every(r => r.Veredicto === 'PERDIDA') && totalFinishedTrades > lastCircuitBreakerTradeCount) {
            //     lastCircuitBreakerTradeCount = totalFinishedTrades;
            //     const cooldownMinutes = 30 * Math.pow(2, Math.min(circuitBreakerLevel, 2));
            //     globalPauseUntil = currentServerTime + (cooldownMinutes * 60 * 1000);
            //     circuitBreakerLevel++;
            //
            //     bot.sendMessage(chatId, `🚨 *CIRCUIT BREAKER Nivel ${circuitBreakerLevel}*\n4 pérdidas consecutivas → Pausa ${cooldownMinutes}min`, { parse_mode: 'Markdown' });
            //     return;
            // }
        }
    } catch (e) {
        console.error('[SCAN_DB_LOAD]', { message: e.message, stack: e.stack, time: new Date().toISOString() });
    }

    // Construir zonas y perfiles adaptativos (datos desde Supabase)
    CURRENT_WINNING_ZONES = buildWinningZonesFromCSV(parsedData);
    // DB: resolvedRows ya vienen con _win, _cwev, etc. precalculados desde db.getRecentTrades
    const resolvedRows = parsedData.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA');
    ADAPTIVE_PROFILES = buildAdaptiveProfiles(resolvedRows);
    updateMemoryFromCSV(resolvedRows);
    // INTEGRATION 6.5: Aprendizaje adaptativo evolutivo
    adaptive.learnFromCSV(resolvedRows);

    // PRODUCCIÓN: Selección dinámica — solo Top 3 activos por WR
    let allowedAssets = CONFIG.MARKETS.map(m => m.id);
    const assetsWithData = Object.keys(assetPerformance);
    if (assetsWithData.length >= 3) {
        allowedAssets = assetsWithData
            .sort((a, b) => parseFloat(assetPerformance[b].wr) - parseFloat(assetPerformance[a].wr))
            .slice(0, 3);
        console.log(`[SCAN] Top 3 activos: ${allowedAssets.join(', ')}`);
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

        // DELAY: 1s entre activos para no saturar Binance API
        await new Promise(resolve => setTimeout(resolve, 1000));

        try {
            let historical = GLOBAL_CANDLE_CACHE.get(asset.id);

            // Helper: mapear klines de Binance a formato {o, h, l, c}
            const mapKlines = (data) => data.map(v => ({
                o: parseFloat(v[1]), h: parseFloat(v[2]), l: parseFloat(v[3]), c: parseFloat(v[4])
            }));

            /**
             * Carga paginada: Binance permite max 1000 por request.
             * Hacemos N páginas hacia atrás con endTime para obtener
             * suficiente historial para el pattern matching (necesita ~5000+).
             */
            async function fetchFullHistory(symbol, pages = 5) {
                let allCandles = [];
                let endTime = undefined;

                for (let p = 0; p < pages; p++) {
                    const params = `symbol=${symbol}&interval=5m&limit=1000${endTime ? `&endTime=${endTime - 1}` : ''}`;
                    const res = await axios.get(`${CONFIG.BINANCE_API}/klines?${params}`);
                    if (!res.data || res.data.length === 0) break;

                    const mapped = mapKlines(res.data);
                    allCandles = mapped.concat(allCandles); // Prepend (más viejas adelante)
                    endTime = res.data[0][0]; // openTime de la vela más vieja de este batch

                    if (res.data.length < 1000) break; // No hay más datos
                    await new Promise(r => setTimeout(r, 300)); // Rate limit entre páginas
                }
                return allCandles;
            }

            if (!historical) {
                // Sin cache: cargar historial paginado (5 páginas = ~5000 velas = ~17 días)
                historical = await fetchFullHistory(asset.symbolBinance, 5);
                if (!historical || historical.length < 100) continue;
                console.log(`[DATA] ${asset.id}: ${historical.length} velas cargadas desde Binance`);
                GLOBAL_CANDLE_CACHE.set(asset.id, historical);
            } else {
                // Con cache: pedir últimas 100 velas y mergear
                const recentRes = await axios.get(`${CONFIG.BINANCE_API}/klines?symbol=${asset.symbolBinance}&interval=5m&limit=100`);
                const recentCandles = mapKlines(recentRes.data);

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
                        // Sin overlap: rebuild completo paginado
                        historical = await fetchFullHistory(asset.symbolBinance, 5);
                    }

                    if (historical.length > 6000) historical = historical.slice(historical.length - 6000);
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
            } catch(e) {
                // FIX: Error silencioso en LOB → loggear
                console.error('[LOB_FETCH]', { asset: asset.id, message: e.message, time: new Date().toISOString() });
            }

            for (const item of tfs) {
                const res = runAnalysisElite(item.aggregate > 1 ? aggregateCandles(historical, item.aggregate) : historical);
                if (res) validSignals.push({ assetId: asset.id, symbolBinance: asset.symbolBinance, tf: item.tf, analysis: res, obi, macro: { h1: s1h, h4: s4h } });
            }
        } catch(e) {
            // FIX: Error detallado por activo
            console.error('[SCAN_ASSET]', { asset: asset.id, message: e.message, stack: e.stack, time: new Date().toISOString() });
        }
    }

    // --- Filtrado de consenso + Filtro Adaptativo ---
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

        // --- EVALUACIÓN ELITE ---
        let isElite = true;
        if (!passMacro) isElite = false;
        if (s.analysis.cwev < 1.2) isElite = false;
        if (recentLossesAsset >= 3) isElite = false;
        if (isB && momentumSlope < 0) isElite = false;
        if (!isB && momentumSlope > 0) isElite = false;
        if (isB && nearResistance) isElite = false;
        if (!isB && nearSupport) isElite = false;
        // TEST_MODE: threshold 55 para pruebas | PRODUCCIÓN: dinámico con piso 60
        const currentThreshold = TEST_MODE ? 55 : Math.max(60, adaptive.getDynamicThreshold());
        if (s.analysis.prob < currentThreshold) isElite = false;
        if (s.analysis.acs < 0.015) isElite = false;
        if (s.analysis.stability < 0.40) isElite = false;
        if (s.analysis.n < 15) isElite = false;
        if (!((isB && s.obi > 0) || (!isB && s.obi < 0))) isElite = false;
        if (!statisticalStrength(s.analysis.n, s.analysis.acs, s.analysis.prob)) isElite = false;

        // --- EVALUACIÓN AGRESIVA ---
        let isAggressive = true;
        if (!passMacro) isAggressive = false;
        if (s.analysis.prob < 54) isAggressive = false;
        if (s.analysis.stability < 0.35) isAggressive = false;
        if (s.analysis.n < 12) isAggressive = false;

        let passMomentumAgr = true;
        if (isB && momentumSlope < -0.0001) passMomentumAgr = false;
        if (!isB && momentumSlope > 0.0001) passMomentumAgr = false;
        if (!passMomentumAgr) isAggressive = false;

        // TEST_MODE: Forzar señal como AGRESIVA para probar ejecución mecánica
        if (TEST_MODE && !isElite && !isAggressive && s.analysis) {
            isAggressive = true;
            console.log(`[TEST_MODE] ${s.assetId} ${s.tf} forzada como AGRESIVA (prob: ${s.analysis.prob.toFixed(1)}%)`);
        }

        if (isElite || isAggressive) {
            // --- FILTRO ADAPTATIVO + HIDDEN RISK ---
            const adaptiveResult = applyAdaptiveFilter(s);
            s.adaptiveScore = adaptiveResult.adjustedScore;
            s.adaptiveReason = adaptiveResult.reason;
            s.adaptivePass = adaptiveResult.pass;
            s.riskScore = adaptiveResult.riskScore;
            s.riskFactors = adaptiveResult.riskFactors;

            // PRODUCCIÓN: Filtro adaptativo activo | TEST_MODE: solo loguea
            if (!adaptiveResult.pass) {
                const riskDetail = adaptiveResult.riskScore > 0 ? ` | RiskScore: ${adaptiveResult.riskScore}/100` : '';
                if (TEST_MODE) {
                    console.log(`[TEST_MODE] ${s.assetId} ${s.tf} filtro bypaseado: ${adaptiveResult.reason}${riskDetail}`);
                } else {
                    console.log(`[FILTRO] ${s.assetId} ${s.tf} BLOQUEADA: ${adaptiveResult.reason}${riskDetail}`);
                    continue;
                }
            }

            s.isElite = isElite;
            s.isAggressive = isAggressive;
            consensusSignals.push(s);
        }
    }

    consensusSignals.sort((a,b) => b.analysis.cwev - a.analysis.cwev);

    // --- Emitir señales con UI simplificada ---
    const endTime = getLocalTime();

    if (consensusSignals.length > 0) {
        // SILENCED: No editar mensaje de status durante recolección
        // if (statusMsg) {
        //     bot.editMessageText(`✅ *Scan completado* ${endTime} | ${consensusSignals.length} señales`, {
        //         chat_id: chatId, message_id: statusMsg.message_id, parse_mode: 'Markdown'
        //     }).catch((e) => { console.error('[TG_EDIT_SCAN]', { message: e.message, time: new Date().toISOString() }); });
        // }

        for (const s of consensusSignals) {
            const sigId = `sig_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
            const icon = s.analysis.direction === 'BUY' ? '🟢' : '🔴';
            const timeData = getRecommendedTimeData(s.tf, currentServerTime);

            // Modo Dual
            const eliteCheck = s.isElite ? '✅' : '❌';
            const agrCheck = s.isAggressive ? '✅' : '❌';
            const dualScore = (s.isElite ? 0.6 : 0) + (s.isAggressive ? 0.4 : 0);

            let modeString = "";
            let scoreVisual = "";
            if (dualScore === 1.0) {
                modeString = "ELITE+AGRESIVO";
                scoreVisual = "1.0 → PERFECTO";
            } else if (dualScore === 0.6) {
                modeString = "ELITE";
                scoreVisual = "0.6 → SOLO ELITE";
            } else if (dualScore === 0.4) {
                modeString = "AGRESIVO";
                scoreVisual = "0.4 → SOLO AGRESIVO";
            }

            // INTEGRATION 6.6: Ajuste adaptativo antes de emitir
            const adaptiveResult = adaptive.computeAdaptiveAdjustment(
                s.assetId, s.tf, s.analysis.direction, s.analysis.cwev,
                candlesByAsset[s.assetId]
            );

            // PRODUCCIÓN: Bloqueo adaptativo activo | TEST_MODE: solo loguea
            if (adaptiveResult.blocked) {
                if (TEST_MODE) {
                    console.log(`[TEST_MODE] ${s.assetId} bloqueo bypaseado: ${adaptiveResult.blockReason || 'ranking dinámico'}`);
                } else {
                    console.log(`[ADAPTIVE] ${s.assetId} BLOQUEADA: ${adaptiveResult.blockReason || 'ranking dinámico'} (hiddenRisk: ${adaptiveResult.hiddenRisk || 0})`);
                    continue;
                }
            }
            // ═══════════════════════════════════════════════════════
            // V17: FILTRO FRANCOTIRADOR (GOLDEN COMBOS PARA REAL)
            // ═══════════════════════════════════════════════════════
            const cwev = s.analysis.cwev;
            const lobAligned = (s.analysis.direction === 'BUY' && s.obi > 0) || (s.analysis.direction === 'SELL' && s.obi < 0);
            
            let isGolden = false;
            if (s.assetId === 'SOL/USD' && s.analysis.direction === 'BUY' && cwev >= 7) isGolden = true; // WR: 78.6%
            if (s.assetId === 'XRP/USD' && s.analysis.direction === 'BUY' && cwev < 0) isGolden = true;  // WR: 76.5%
            if (s.assetId === 'BTC/USD' && s.analysis.direction === 'BUY' && lobAligned) isGolden = true; // WR: 69.8%
            if (s.assetId === 'BTC/USD' && s.analysis.direction === 'SELL' && cwev < 0) isGolden = true; // WR: 66.7%
            if (s.assetId === 'ADA/USD' && s.analysis.direction === 'SELL' && cwev >= 7 && !lobAligned) isGolden = true; // WR: 66.7%

            if (!isGolden) {
                console.log(`[V17-FRANCOTIRADOR] ${s.assetId} ${s.tf} ${s.analysis.direction} ignorada. No es un Golden Combo.`);
                continue; // Salta a la siguiente señal sin gastar IA ni operar
            }

            // Preparar datos para IA antes de enviar mensaje
            s.modeString = modeString;
            s.dualScore = dualScore;

            // Preparar datos para IA antes de enviar mensaje
            s.modeString = modeString;
            s.dualScore = dualScore;
            s._adaptiveAdj = adaptiveResult;

            // IA AUTOMÁTICA: ejecutar Gemini para esta señal
            const iaResult = await executeIAAnalysis(s, modeString);

            // ═══════════════════════════════════════════════════════
            // V16: FILTROS ESTADÍSTICOS (basados en 637 trades reales)
            // ═══════════════════════════════════════════════════════

            // A. BLOQUEADOR TRAMPA IA: ia_score>70 + cwev<3 = 30% WR (23 trades)
            //    ia_score>75 + cwev<4 = 11% WR (9 trades) — casi pérdida garantizada
            if (iaResult && iaResult.iaScore > 70 && s.analysis.cwev < 3.0) {
                console.log(`[V16-TRAP] ${s.assetId} ${s.tf} BLOQUEADA: Trampa IA (score=${iaResult.iaScore}, cwev=${s.analysis.cwev.toFixed(1)}) — WR histórico <30%`);
                continue;
            }

            // B. INVERSOR CONTRARIAN: combos con WR < 40% sobre 20+ trades
            //    XRP/USD BUY: 38.5% → invertido 61.5% (52t)
            //    XRP/USD SELL: 38.1% → invertido 61.9% (42t)
            //    SOL/USD BUY: 36.4% → invertido 63.6% (22t)
            const KAMIKAZE_COMBOS = [
                { asset: 'XRP/USD', dir: 'BUY',  originalWR: 38.5, invertedWR: 61.5, n: 52 },
                { asset: 'XRP/USD', dir: 'SELL', originalWR: 38.1, invertedWR: 61.9, n: 42 },
                { asset: 'SOL/USD', dir: 'BUY',  originalWR: 36.4, invertedWR: 63.6, n: 22 }
            ];

            const kamikaze = KAMIKAZE_COMBOS.find(k => k.asset === s.assetId && k.dir === s.analysis.direction);
            if (kamikaze) {
                const oldDir = s.analysis.direction;
                s.analysis.direction = oldDir === 'BUY' ? 'SELL' : 'BUY';
                s.analysis.edge *= -1;
                s.isInverted = true;
                console.log(`[V16-INVERT] ${s.assetId} ${s.tf} invertida: ${oldDir}→${s.analysis.direction} (WR original: ${kamikaze.originalWR}%, invertido: ${kamikaze.invertedWR}%)`);
            }

            // ═══════════════════════════════════════════════════════

            // Trade levels: usar contexto IA si disponible, sino default
            const iaContext = (iaResult && iaResult.isExecute && iaResult.iaContext !== 'TRAP') ? iaResult.iaContext : 'CONTINUATION';
            const tradeData = calculateTradeLevels(
                s.analysis.currentPrice,
                s.analysis.direction,
                s.analysis.currentATR,
                s.analysis.absEdge,
                iaContext,
                circuitBreakerLevel
            );

            // --- MENSAJE COMPLETO (señal + IA + trading en un solo envío) ---
            const invertTag = s.isInverted ? '🔄 *INVERTIDA POR ESTADÍSTICA*\n\n' : '';
            const dirIcon = s.analysis.direction === 'BUY' ? '🟢' : '🔴';
            let msgText = `${invertTag}🎯 *${s.assetId} | ${s.tf}*\n\n`;
            msgText += `MODO: E:${eliteCheck} → A:${agrCheck}\n`;
            msgText += `🏆 ${scoreVisual}\n\n`;
            msgText += `${dirIcon} *${s.analysis.direction}*\n`;

            // Bloque IA
            if (iaResult) {
                const verdictIcon = iaResult.isExecute ? '🚀 EXECUTE' : '⛔ PASS';
                msgText += `\n🧠 *IA:* ${verdictIcon} (${iaResult.iaScore}/100)`;
            } else {
                msgText += `\n🧠 _IA no disponible_`;
            }

            // Bloque Trading
            if (tradeData) {
                msgText += `\n\n💰 *TRADING*\n`;
                msgText += `📍 Entry: \`${tradeData.entry}\`\n`;
                msgText += `🛑 SL: \`${tradeData.sl}\`\n`;
                msgText += `🎯 TP: \`${tradeData.tp}\``;
            }

            msgText += `\n\n⏱️ Ventana: ${timeData.text}`;

            s._createdAt = Date.now();

            // Enviar mensaje — fallback sin Markdown si falla el parseo
            let sentMsg;
            try {
                sentMsg = await bot.sendMessage(chatId, msgText, { parse_mode: 'Markdown' });
            } catch (tgErr) {
                // Fallback: enviar sin formato si Markdown falla
                console.error('[TG_MARKDOWN]', { message: tgErr.message, time: new Date().toISOString() });
                sentMsg = await bot.sendMessage(chatId, msgText.replace(/[*_`|]/g, ''));
            }

            s._messageText = msgText;
            s._messageId = sentMsg.message_id;
            SIGNAL_CACHE.set(sigId, { ...s, analysis: { ...s.analysis }, macro: { ...s.macro } });

            // ═══════════════════════════════════════════════════════
            // GATILLO: Disparar orden al bridge ExpertOption (Render)
            // ═══════════════════════════════════════════════════════
            try {
                const bridgeUrl = process.env.BRIDGE_URL || 'http://127.0.0.1:5000';
                const bridgeSecret = process.env.BRIDGE_SECRET || '';

                // Pre-warm: despertar Render si está dormido (free tier duerme tras 15min)
                try {
                    await axios.get(`${bridgeUrl}/health`, { timeout: 55000 });
                } catch (warmErr) {
                    console.log(`[BRIDGE_SEND] ⏳ Render despertando...`);
                }

                console.log(`[BRIDGE_SEND] =====> Enviando ${s.assetId} ${s.analysis.direction} ${s.tf} a ${bridgeUrl}/trade`);
                const bridgeRes = await axios.post(`${bridgeUrl}/trade`, {
                    asset: s.assetId,
                    direction: s.analysis.direction,
                    tf: s.tf
                }, {
                    timeout: 60000,
                    headers: bridgeSecret ? { 'Authorization': `Bearer ${bridgeSecret}` } : {}
                });
                console.log(`[BRIDGE_SEND] ✅ Respuesta: ${JSON.stringify(bridgeRes.data)}`);
            } catch (tradeErr) {
                console.error(`[BRIDGE_SEND] ❌ Error: ${tradeErr.message}`);
            }

            // DB: Insertar señal con IA incluida, veredicto siempre PENDIENTE
            await db.insertTrade({
                signalId: sigId,
                asset: s.assetId, tf: s.tf, dir: s.analysis.direction,
                prob: s.analysis.prob.toFixed(1), lob: s.obi.toFixed(3),
                edge: s.analysis.edge.toFixed(2), alpha: s.analysis.acs.toFixed(3),
                stab: (s.analysis.stability * 100).toFixed(0), cwev: s.analysis.cwev.toFixed(1),
                samples: s.analysis.n, veredicto: 'PENDIENTE',
                iaVerdict: iaResult ? iaResult.iaVerdict : '',
                iaScore: iaResult ? iaResult.iaScore : '',
                iaContext: iaResult ? iaResult.iaContext : '',
                mode: modeString
            });

            // Auditoría pendiente
            PENDING_AUDITS.push({
                sigId, assetId: s.assetId, symbolBinance: s.symbolBinance, tf: s.tf,
                direction: s.analysis.direction,
                startTs: timeData.startTs, endTs: timeData.endTs,
                messageId: sentMsg.message_id, retries: 0,
                logData: {
                    prob: s.analysis.prob.toFixed(1), lob: s.obi.toFixed(3),
                    edge: s.analysis.edge.toFixed(2), alpha: s.analysis.acs.toFixed(3),
                    stab: (s.analysis.stability * 100).toFixed(0), cwev: s.analysis.cwev.toFixed(1),
                    samples: s.analysis.n,
                    iaVerdict: iaResult ? iaResult.iaVerdict : '',
                    iaScore: iaResult ? iaResult.iaScore : '',
                    iaContext: iaResult ? iaResult.iaContext : '',
                    mode: modeString
                }
            });
        }
    }
    // SILENCED: No enviar mensaje de "sin señales" durante recolección
    // else if (statusMsg) {
    //     bot.editMessageText(`💤 *Scan finalizado* ${endTime} — Sin señales`, {
    //         chat_id: chatId, message_id: statusMsg.message_id, parse_mode: 'Markdown'
    //     }).catch((e) => { console.error('[TG_EDIT_NOSIG]', { message: e.message, time: new Date().toISOString() }); });
    // }

    } finally {
        // LOCK: Liberar scan
        isScanning = false;
    }
}


// ═══════════════════════════════════════════════════════════════════
// MÓDULO 10: CRONES
// ═══════════════════════════════════════════════════════════════════

let lastScanMinute = -1;
setInterval(() => {
    const now = new Date(getSyncedTime());
    const m = now.getMinutes();
    const s = now.getSeconds();
    // TIMING: Dispara 1 minuto antes del cierre de vela 5M (minutos 4,9,14,19,24,29,34,39,44,49,54,59)
    // Protección: lastScanMinute evita disparos dobles
    if ((m % 5 === 4) && s >= 0 && s <= 4 && lastScanMinute !== m) {
        lastScanMinute = m;
        globalScan('auto');
    }
}, 1000);

// Auditoría de trades pendientes
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
                const resultEmoji = isTie ? '🟡' : (isWin ? '✅' : '❌');

                const auditMsg = `${resultEmoji} *${audit.assetId}* ${audit.direction} → *${iconResult}*\nOpen: ${openPrice.toFixed(4)} | Close: ${closePrice.toFixed(4)}`;
                await bot.sendMessage(chatId, auditMsg, { parse_mode: 'Markdown', reply_to_message_id: audit.messageId });

                // DB: Actualizar la fila existente (no insertar duplicado)
                await db.updateTradeResult(audit.sigId, {
                    veredicto: iconResult,
                    open: openPrice,
                    close: closePrice
                });

                // ── Actualización en tiempo real de memoria dinámica ──
                const cwevRound = Math.round(parseFloat(audit.logData.cwev) || 0);
                if (iconResult === 'PERDIDA') {
                    LOSS_STREAKS[audit.assetId] = (LOSS_STREAKS[audit.assetId] || 0) + 1;
                    const failKey = `${audit.tf}_${audit.direction}_${cwevRound}`;
                    RECENT_FAILURE_PATTERNS.add(failKey);
                    if (RECENT_FAILURE_PATTERNS.size > 50) {
                        const arr = [...RECENT_FAILURE_PATTERNS];
                        RECENT_FAILURE_PATTERNS.clear();
                        for (const item of arr.slice(-50)) {
                            RECENT_FAILURE_PATTERNS.add(item);
                        }
                    }
                } else if (iconResult === 'GANADA') {
                    LOSS_STREAKS[audit.assetId] = 0;
                }

                // INTEGRATION 6.5b: Registro en tiempo real en motor adaptativo
                if (iconResult === 'GANADA' || iconResult === 'PERDIDA') {
                    adaptive.recordTradeResult(
                        audit.assetId, audit.tf, audit.direction,
                        cwevRound, iconResult === 'GANADA'
                    );
                }

                PENDING_AUDITS.splice(i, 1);
            } catch (error) {
                // FIX: Error silencioso en auditoría → loggear
                console.error('[AUDIT]', { asset: audit.assetId, message: error.message, retries: audit.retries, time: new Date().toISOString() });
                audit.retries++;
            }
        }
    }
}, 15000);

// Limpieza de cache de señales
setInterval(() => {
    const maxAge = 2 * 60 * 60 * 1000;
    const now = Date.now();
    for (const [key, val] of SIGNAL_CACHE.entries()) {
        if (val._createdAt && (now - val._createdAt) > maxAge) {
            SIGNAL_CACHE.delete(key);
        }
    }
}, 30 * 60 * 1000);

// DB: Rotación automática — mantener solo últimos 1000 trades (cada 6 horas)
setInterval(() => {
    db.cleanupOldTrades(2000);
}, 6 * 60 * 60 * 1000);

// Comando /analyze — Análisis adaptativo on-demand
bot.onText(/\/analyze/, async (msg) => {
    if (msg.chat.id.toString() === chatId) {
        await safeSend('Ejecutando analisis adaptativo...');

        try {
            const summary = await db.runAdaptiveAnalysis(adaptive);

            if (summary.trades === 0) {
                await safeSend('Sin trades resueltos en DB.');
                return;
            }

            // Datos de hoy
            const { todayResolved } = await db.getTodayTrades();
            const todayW = todayResolved.filter(r => r._win === 1).length;
            const todayWR = todayResolved.length > 0 ? (todayW / todayResolved.length * 100).toFixed(1) : '0.0';
            const globalWR = summary.trades > 0 ? ((summary.topAssets.reduce((a, x) => a + parseFloat(x.wr) * x.trades, 0) / summary.topAssets.reduce((a, x) => a + x.trades, 0)) || 0).toFixed(1) : '0.0';

            // Comparativa
            const todayNum = parseFloat(todayWR);
            const globalNum = parseFloat(globalWR);
            const trend = todayNum > globalNum ? 'Hoy MEJOR que el global' : (todayNum < globalNum ? 'Hoy PEOR que el global' : 'Hoy igual al global');

            let text = 'ANALISIS ADAPTATIVO\n\n';
            text += `Trades analizados: ${summary.trades}\n`;
            text += `Threshold: ${summary.threshold}\n\n`;
            text += `HOY: ${todayResolved.length}t | WR: ${todayWR}%\n`;
            text += `GLOBAL WR: ${globalWR}%\n`;
            text += `Tendencia: ${trend}\n\n`;

            if (summary.topAssets.length > 0) {
                text += 'Top Assets:\n';
                for (const a of summary.topAssets) {
                    text += `  ${a.asset}: ${a.wr}% WR (${a.trades}t)\n`;
                }
            }

            if (summary.topPatterns.length > 0) {
                text += '\nTop Patrones RL:\n';
                for (const p of summary.topPatterns) {
                    text += `  ${p.pattern}: score ${p.score} (${p.trades}t)\n`;
                }
            }

            await safeSend(text);
        } catch (e) {
            console.error('[ANALYZE]', { message: e.message, stack: e.stack, time: new Date().toISOString() });
            await safeSend('Error en analisis. Ver logs.');
        }
    }
});

// Comando /broker — Verificar estado del bridge ExpertOption
bot.onText(/\/broker/, async (msg) => {
    if (msg.chat.id.toString() === chatId) {
        try {
            const bridgeUrl = process.env.BRIDGE_URL || 'http://127.0.0.1:5000';
            const bridgeSecret = process.env.BRIDGE_SECRET || '';
            const res = await axios.get(`${bridgeUrl}/broker-status`, {
                timeout: 60000,
                headers: bridgeSecret ? { 'Authorization': `Bearer ${bridgeSecret}` } : {}
            });
            const d = res.data;
            if (d.connected) {
                const balanceText = d.balance !== null ? `$${d.balance}` : 'N/A (ver logs Render)';
                let text = `Broker: CONECTADO\nCuenta: ${d.mode}\nSaldo: ${balanceText}`;
                // Mostrar métodos disponibles si hay debug
                if (d.debug && d.debug.available_methods) {
                    const methods = d.debug.available_methods.substring(0, 200);
                    text += `\n\nMetodos API: ${methods}`;
                }
                await safeSend(text);
            } else {
                await safeSend(`Broker: DESCONECTADO\n${d.message || 'Sin conexion activa'}`);
            }
        } catch (e) {
            await safeSend(`Broker: ERROR\n${e.message}`);
        }
    }
});

// Comando /assets — Descubrir IDs reales de activos en ExpertOption
bot.onText(/\/assets/, async (msg) => {
    if (msg.chat.id.toString() === chatId) {
        await safeSend('Consultando assets del broker...');
        try {
            const bridgeUrl = process.env.BRIDGE_URL || 'http://127.0.0.1:5000';
            const bridgeSecret = process.env.BRIDGE_SECRET || '';
            const res = await axios.get(`${bridgeUrl}/debug-assets`, {
                timeout: 15000,
                headers: bridgeSecret ? { 'Authorization': `Bearer ${bridgeSecret}` } : {}
            });
            const d = res.data;

            let text = 'DEBUG ASSETS\n\n';

            if (d.msg_by_action_keys) {
                text += `Keys WS: ${JSON.stringify(d.msg_by_action_keys).substring(0, 300)}\n\n`;
            }
            if (d.assets_raw) {
                text += `Assets: ${d.assets_raw.substring(0, 500)}\n\n`;
            }
            if (d.crypto_matches && typeof d.crypto_matches === 'object') {
                text += `Crypto encontrados: ${JSON.stringify(d.crypto_matches).substring(0, 500)}\n`;
            }

            // Truncar si es muy largo
            if (text.length > 3500) text = text.substring(0, 3500) + '...\n\n(Ver logs Render para datos completos)';

            await safeSend(text);
        } catch (e) {
            await safeSend(`Error: ${e.message}`);
        }
    }
});
