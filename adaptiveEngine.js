// ═══════════════════════════════════════════════════════════════════
// adaptiveEngine.js — Sistema Adaptativo Evolutivo con Persistencia
// ═══════════════════════════════════════════════════════════════════

const persistence = require('./adaptivePersistence');

// ── 6.1 REINFORCEMENT LEARNING SIMPLE ──
const RL_MEMORY = {};

// ── 6.2 AUTO AJUSTE DE THRESHOLDS ──
let dynamicThreshold = 54;
let totalTradesForThreshold = 0;
let totalWinsForThreshold = 0;

// ── 6.3 RANKING DINÁMICO DE ACTIVOS ──
const ASSET_STATS = {};

// ── 6.4 CONTEXTO DE MERCADO ──
const MARKET_CONTEXT = {};

// FIX #7: Límite de patrones RL en memoria
const RL_WINDOW = 300;
const MAX_RL_PATTERNS = 2000;

// ═══════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════

function clamp(val, min, max) {
    return Math.max(min, Math.min(max, val));
}

function getRLPatternKey(asset, tf, direction, cwev) {
    return `${asset}_${tf}_${direction}_${Math.round(cwev)}`;
}

// ═══════════════════════════════════════════════════════════════════
// PERSISTENCIA: Hidratar y exportar estado
// ═══════════════════════════════════════════════════════════════════

/**
 * FIX #6: Hidrata el engine con estado previamente guardado en DB.
 * Se llama UNA VEZ al iniciar el bot, antes de cualquier procesamiento.
 */
function hydrateState(state) {
    if (!state) return;

    // Hidratar RL_MEMORY
    if (state.rl_memory && typeof state.rl_memory === 'object') {
        for (const key of Object.keys(RL_MEMORY)) delete RL_MEMORY[key];
        Object.assign(RL_MEMORY, state.rl_memory);
        console.log(`[ADAPTIVE_HYDRATE] RL: ${Object.keys(RL_MEMORY).length} patrones`);
    }

    // Hidratar ASSET_STATS
    if (state.asset_stats && typeof state.asset_stats === 'object') {
        for (const key of Object.keys(ASSET_STATS)) delete ASSET_STATS[key];
        Object.assign(ASSET_STATS, state.asset_stats);
        console.log(`[ADAPTIVE_HYDRATE] Assets: ${Object.keys(ASSET_STATS).length}`);
    }

    // Hidratar threshold
    if (typeof state.dynamic_threshold === 'number') {
        dynamicThreshold = clamp(state.dynamic_threshold, 50, 65);
        console.log(`[ADAPTIVE_HYDRATE] Threshold: ${dynamicThreshold}`);
    }
}

/**
 * Exporta el estado actual para guardarlo en DB.
 * FIX #7: Prune RL_MEMORY si supera MAX_RL_PATTERNS.
 */
function exportState() {
    // Pruning: si RL_MEMORY excede límite, eliminar los 500 con menos trades
    const rlKeys = Object.keys(RL_MEMORY);
    if (rlKeys.length > MAX_RL_PATTERNS) {
        const sorted = rlKeys
            .map(k => ({ key: k, trades: RL_MEMORY[k].trades }))
            .sort((a, b) => a.trades - b.trades);
        const toDelete = sorted.slice(0, 500);
        for (const item of toDelete) {
            delete RL_MEMORY[item.key];
        }
        console.log(`[ADAPTIVE] RL pruned: ${toDelete.length} patrones eliminados (${Object.keys(RL_MEMORY).length} restantes)`);
    }

    return {
        rl_memory: { ...RL_MEMORY },
        asset_stats: JSON.parse(JSON.stringify(ASSET_STATS)),
        dynamic_threshold: dynamicThreshold
    };
}

/**
 * Guarda estado con debounce (no bloquea, no spamea DB).
 */
function triggerSave() {
    persistence.debounceSave(exportState());
}

// ═══════════════════════════════════════════════════════════════════
// 6.1 — RL: Registrar resultado de trade
// ═══════════════════════════════════════════════════════════════════

function rlRecordResult(asset, tf, direction, cwev, isWin) {
    const key = getRLPatternKey(asset, tf, direction, cwev);
    if (!RL_MEMORY[key]) {
        RL_MEMORY[key] = { score: 0, trades: 0, wins: 0, losses: 0 };
    }
    const entry = RL_MEMORY[key];
    entry.trades++;
    if (isWin) {
        entry.wins++;
        entry.score += 1;
    } else {
        entry.losses++;
        entry.score -= 1;
    }
    entry.score = clamp(entry.score, -30, 30);
}

/**
 * Retorna boost entre -15 y +15.
 */
function rlGetBoost(asset, tf, direction, cwev) {
    const key = getRLPatternKey(asset, tf, direction, cwev);
    const entry = RL_MEMORY[key];
    if (!entry || entry.trades < 3) return 0;
    return clamp(entry.score * 0.5, -15, 15);
}

// ═══════════════════════════════════════════════════════════════════
// 6.2 — AUTO AJUSTE DE THRESHOLDS
// ═══════════════════════════════════════════════════════════════════

function thresholdRecordResult(isWin) {
    totalTradesForThreshold++;
    if (isWin) totalWinsForThreshold++;

    if (totalTradesForThreshold % 20 === 0 && totalTradesForThreshold > 0) {
        const wr = totalWinsForThreshold / totalTradesForThreshold;
        if (wr < 0.55) dynamicThreshold += 1;
        if (wr > 0.65) dynamicThreshold -= 1;
        dynamicThreshold = clamp(dynamicThreshold, 50, 65);
        console.log(`[ADAPTIVE] Threshold ajustado a ${dynamicThreshold} (WR: ${(wr * 100).toFixed(1)}% sobre ${totalTradesForThreshold} trades)`);
    }
}

// FIX #9: Preparación para per-asset threshold (función extraída)
function getThresholdForAsset(/* asset */) {
    // Futuro: retornar threshold específico por activo
    // Por ahora retorna el global
    return dynamicThreshold;
}

function getDynamicThreshold() {
    return dynamicThreshold;
}

// ═══════════════════════════════════════════════════════════════════
// 6.3 — RANKING DINÁMICO DE ACTIVOS
// ═══════════════════════════════════════════════════════════════════

function assetRecordResult(asset, isWin) {
    if (!ASSET_STATS[asset]) {
        ASSET_STATS[asset] = { wins: 0, losses: 0, trades: 0, recentResults: [] };
    }
    const stats = ASSET_STATS[asset];
    stats.trades++;
    if (isWin) stats.wins++;
    else stats.losses++;

    stats.recentResults.push(isWin ? 1 : 0);
    if (stats.recentResults.length > 20) stats.recentResults.shift();
}

function getAssetBoost(asset) {
    const stats = ASSET_STATS[asset];
    if (!stats || stats.trades < 5) return 0;

    const overallWR = stats.wins / stats.trades;
    const recentWR = stats.recentResults.length >= 5
        ? stats.recentResults.reduce((a, b) => a + b, 0) / stats.recentResults.length
        : overallWR;

    const assetScore = (overallWR * 0.6) + (recentWR * 0.4);

    if (assetScore >= 0.65) return 10;
    if (assetScore >= 0.55) return 5;
    if (assetScore < 0.35) return -10;
    if (assetScore < 0.45) return -5;
    return 0;
}

/**
 * FIX #8: Bloqueo menos agresivo.
 * Antes: recentWR < 0.30 && recentResults.length >= 10
 * Ahora: recentWR < 0.25 && trades >= 20 (más muestra requerida)
 */
function shouldFilterAsset(asset) {
    const stats = ASSET_STATS[asset];
    if (!stats || stats.trades < 20) return false;

    const recentWR = stats.recentResults.length >= 10
        ? stats.recentResults.reduce((a, b) => a + b, 0) / stats.recentResults.length
        : (stats.wins / stats.trades);

    return recentWR < 0.25 && stats.recentResults.length >= 10;
}

// ═══════════════════════════════════════════════════════════════════
// 6.4 — CONTEXTO DE MERCADO
// ═══════════════════════════════════════════════════════════════════

function updateMarketContext(asset, candles) {
    if (!candles || candles.length < 30) {
        MARKET_CONTEXT[asset] = { trend: 'neutral', volatility: 'medium' };
        return;
    }

    const recent = candles.slice(-30);
    const closes = recent.map(c => c.c);

    let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
    const n = closes.length;
    for (let i = 0; i < n; i++) {
        sumX += i; sumY += closes[i]; sumXY += i * closes[i]; sumX2 += i * i;
    }
    const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
    const avgPrice = sumY / n;
    const normalizedSlope = slope / (avgPrice || 1);

    let trend;
    if (normalizedSlope > 0.001) trend = 'bullish';
    else if (normalizedSlope < -0.001) trend = 'bearish';
    else trend = 'neutral';

    let totalRange = 0;
    for (const c of recent) {
        totalRange += (c.h - c.l);
    }
    const avgRange = totalRange / recent.length;
    const relativeVol = avgRange / (avgPrice || 1);

    let volatility;
    if (relativeVol > 0.025) volatility = 'high';
    else if (relativeVol < 0.008) volatility = 'low';
    else volatility = 'medium';

    MARKET_CONTEXT[asset] = { trend, volatility };
}

function getContextAdjustment(asset, direction) {
    const ctx = MARKET_CONTEXT[asset];
    if (!ctx) return 0;

    let adjustment = 0;

    if (direction === 'BUY' && ctx.trend === 'bullish') adjustment += 5;
    else if (direction === 'SELL' && ctx.trend === 'bearish') adjustment += 5;
    else if (direction === 'BUY' && ctx.trend === 'bearish') adjustment -= 5;
    else if (direction === 'SELL' && ctx.trend === 'bullish') adjustment -= 5;

    if (ctx.volatility === 'high') adjustment -= 5;
    if (ctx.volatility === 'low') adjustment -= 3;

    return clamp(adjustment, -10, 10);
}

// ═══════════════════════════════════════════════════════════════════
// 6.5 — APRENDIZAJE (batch)
// ═══════════════════════════════════════════════════════════════════

/**
 * FIX #2: Modo "bootstrap" vs "incremental".
 * - bootstrap: rebuild completo (cold start o análisis on-demand)
 * - incremental: NO borra RL_MEMORY, solo acumula (default para scans)
 *
 * FIX #7: Ventana RL configurable (RL_WINDOW = 300 en vez de 100)
 */
function learnFromCSV(resolvedRows, mode = 'incremental') {
    if (!resolvedRows || resolvedRows.length === 0) return;

    if (mode === 'bootstrap') {
        console.log(`[ADAPTIVE_BOOTSTRAP] Rebuild completo desde ${resolvedRows.length} trades`);

        // Reset threshold counters
        totalTradesForThreshold = 0;
        totalWinsForThreshold = 0;

        // Limpiar ASSET_STATS
        for (const key of Object.keys(ASSET_STATS)) {
            ASSET_STATS[key] = { wins: 0, losses: 0, trades: 0, recentResults: [] };
        }

        // Limpiar RL_MEMORY
        for (const key of Object.keys(RL_MEMORY)) {
            delete RL_MEMORY[key];
        }

        // FIX #7: Ventana RL configurable
        const rlWindow = Math.min(RL_WINDOW, resolvedRows.length);
        const recentForRL = resolvedRows.slice(-rlWindow);

        for (const r of recentForRL) {
            const isWin = r._win === 1;
            rlRecordResult(r.Activo, r.TF || '?', r.Dir || '?', r._cwev || 0, isWin);
        }

        // Reconstruir ASSET_STATS y threshold desde todo
        for (const r of resolvedRows) {
            const isWin = r._win === 1;
            if (r.Activo) assetRecordResult(r.Activo, isWin);
            thresholdRecordResult(isWin);
        }
    } else {
        // INCREMENTAL: No borrar nada — solo actualizar ASSET_STATS y threshold
        // RL_MEMORY se actualiza via recordTradeResult en tiempo real
        console.log(`[ADAPTIVE_INCREMENTAL] Actualizando stats desde ${resolvedRows.length} trades`);

        // Rebuild solo ASSET_STATS (siempre idempotente)
        for (const key of Object.keys(ASSET_STATS)) {
            ASSET_STATS[key] = { wins: 0, losses: 0, trades: 0, recentResults: [] };
        }
        totalTradesForThreshold = 0;
        totalWinsForThreshold = 0;

        for (const r of resolvedRows) {
            const isWin = r._win === 1;
            if (r.Activo) assetRecordResult(r.Activo, isWin);
            thresholdRecordResult(isWin);
        }
        // RL_MEMORY NO se toca — se preserva el aprendizaje incremental
    }

    // Persistir después de aprender
    triggerSave();
}

// ═══════════════════════════════════════════════════════════════════
// 6.6 — INTEGRACIÓN: Calcular ajuste final
// ═══════════════════════════════════════════════════════════════════

function computeAdaptiveAdjustment(asset, tf, direction, cwev, candles) {
    if (candles && candles.length > 0) {
        updateMarketContext(asset, candles);
    }

    const adaptiveBoost = rlGetBoost(asset, tf, direction, cwev);
    const assetBoost = getAssetBoost(asset);
    const contextAdj = getContextAdjustment(asset, direction);
    const blocked = shouldFilterAsset(asset);

    const totalAdjustment = adaptiveBoost + assetBoost + contextAdj;

    // AUDIT FIX #4: Hidden Risk — suma de factores negativos, clamped 0-50
    const negRL = adaptiveBoost < 0 ? Math.abs(adaptiveBoost) : 0;
    const negAsset = assetBoost < 0 ? Math.abs(assetBoost) : 0;
    const negContext = contextAdj < 0 ? Math.abs(contextAdj) : 0;
    const hiddenRisk = clamp(negRL + negAsset + negContext, 0, 50);

    // Bloqueo compuesto: shouldFilterAsset O hiddenRisk alto
    const finalBlocked = blocked || hiddenRisk > 30;

    return {
        totalAdjustment: clamp(totalAdjustment, -30, 30),
        breakdown: {
            rl: adaptiveBoost,
            asset: assetBoost,
            context: contextAdj,
            threshold: dynamicThreshold
        },
        blocked: finalBlocked,
        hiddenRisk,
        blockReason: finalBlocked
            ? (blocked ? 'WR reciente crítico' : `hiddenRisk ${hiddenRisk}/50`)
            : null
    };
}

// ═══════════════════════════════════════════════════════════════════
// 6.5b — REGISTRO EN TIEMPO REAL (trade cerrado)
// ═══════════════════════════════════════════════════════════════════

function recordTradeResult(asset, tf, direction, cwev, isWin) {
    rlRecordResult(asset, tf, direction, cwev, isWin);
    assetRecordResult(asset, isWin);
    thresholdRecordResult(isWin);
    // Persistir después de cada trade
    triggerSave();
}

// ═══════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════

module.exports = {
    // 6.1 RL
    rlGetBoost,
    rlRecordResult,
    RL_MEMORY,

    // 6.2 Threshold
    getDynamicThreshold,
    getThresholdForAsset,
    thresholdRecordResult,

    // 6.3 Asset ranking
    getAssetBoost,
    shouldFilterAsset,
    assetRecordResult,
    ASSET_STATS,

    // 6.4 Context
    updateMarketContext,
    getContextAdjustment,
    MARKET_CONTEXT,

    // 6.5 Learning
    learnFromCSV,

    // 6.5b Real-time
    recordTradeResult,

    // 6.6 Integration
    computeAdaptiveAdjustment,

    // Persistence
    hydrateState,
    exportState
};
