// ═══════════════════════════════════════════════════════════════════
// db.js — Capa de persistencia Supabase
// ═══════════════════════════════════════════════════════════════════

const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error('[DB] SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY son requeridos en .env');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const TABLE = 'trades';

/**
 * Inserta un trade nuevo en Supabase.
 * FIX: Incluye signal_id para identificación única y prevenir duplicados.
 */
async function insertTrade(trade) {
    try {
        const { error } = await supabase.from(TABLE).insert({
            signal_id: trade.signalId || null,
            asset: trade.asset || '',
            tf: trade.tf || '',
            direction: trade.dir || '',
            prob: parseFloat(trade.prob) || 0,
            lob: parseFloat(trade.lob) || 0,
            edge: parseFloat(trade.edge) || 0,
            alpha: parseFloat(trade.alpha) || 0,
            stability: parseFloat(trade.stab) || 0,
            cwev: parseFloat(trade.cwev) || 0,
            samples: parseInt(trade.samples) || 0,
            veredicto: trade.veredicto || '',
            open_price: parseFloat(trade.open) || null,
            close_price: parseFloat(trade.close) || null,
            ia_verdict: trade.iaVerdict || '',
            ia_score: parseFloat(trade.iaScore) || null,
            ia_context: trade.iaContext || '',
            mode: trade.mode || ''
        });
        if (error) {
            // AUDIT FIX #5: Ignorar duplicados silenciosamente (UNIQUE violation)
            if (error.code === '23505') {
                console.log(`[DB_INSERT] Duplicado ignorado: ${trade.signalId}`);
                return;
            }
            console.error('[DB_INSERT]', { message: error.message, code: error.code, time: new Date().toISOString() });
        }
    } catch (e) {
        console.error('[DB_INSERT]', { message: e.message, stack: e.stack, time: new Date().toISOString() });
    }
}

/**
 * FIX: Actualiza un trade existente por signal_id.
 * Usado por auditoría (GANADA/PERDIDA) e IA (IA_ANALIZADA) para
 * actualizar la misma fila en vez de insertar duplicados.
 */
async function updateTradeResult(signalId, data) {
    if (!signalId) {
        console.error('[DB_UPDATE] signalId es requerido');
        return;
    }
    try {
        const updatePayload = {};
        if (data.veredicto !== undefined) updatePayload.veredicto = data.veredicto;
        if (data.open !== undefined) updatePayload.open_price = parseFloat(data.open) || null;
        if (data.close !== undefined) updatePayload.close_price = parseFloat(data.close) || null;
        if (data.iaVerdict !== undefined) updatePayload.ia_verdict = data.iaVerdict;
        if (data.iaScore !== undefined) updatePayload.ia_score = parseFloat(data.iaScore) || null;
        if (data.iaContext !== undefined) updatePayload.ia_context = data.iaContext;

        const { error } = await supabase
            .from(TABLE)
            .update(updatePayload)
            .eq('signal_id', signalId);

        if (error) {
            console.error('[DB_UPDATE]', { signalId, message: error.message, time: new Date().toISOString() });
        }
    } catch (e) {
        console.error('[DB_UPDATE]', { signalId, message: e.message, stack: e.stack, time: new Date().toISOString() });
    }
}

/**
 * Obtiene los últimos N trades (DESC luego reverse para cronológico).
 * FIX #6: Solo mapea _win para trades resueltos.
 */
async function getRecentTrades(limit = 100) {
    try {
        const { data, error } = await supabase
            .from(TABLE)
            .select('*')
            .order('created_at', { ascending: false })
            .limit(limit);

        if (error) {
            console.error('[DB_SELECT]', { message: error.message, time: new Date().toISOString() });
            return [];
        }

        const rows = (data || []).reverse();

        return rows.map(r => ({
            Activo: r.asset,
            TF: r.tf,
            Dir: r.direction,
            Prob: String(r.prob),
            LOB: String(r.lob),
            Edge: String(r.edge),
            Alpha: String(r.alpha),
            Stability: String(r.stability),
            CWEV: String(r.cwev),
            Samples: String(r.samples),
            Veredicto: r.veredicto,
            Open: r.open_price ? String(r.open_price) : '',
            Close: r.close_price ? String(r.close_price) : '',
            IA_Verdict: r.ia_verdict,
            IA_Score: r.ia_score ? String(r.ia_score) : '',
            IA_Context: r.ia_context,
            Mode: r.mode,
            _prob: r.prob || 0,
            _lob: r.lob || 0,
            _edge: r.edge || 0,
            _stab: r.stability || 0,
            _cwev: r.cwev || 0,
            _alpha: r.alpha || 0,
            _samples: r.samples || 0,
            // FIX #6: _win solo tiene sentido para trades resueltos
            _win: r.veredicto === 'GANADA' ? 1 : 0
        }));
    } catch (e) {
        console.error('[DB_SELECT]', { message: e.message, stack: e.stack, time: new Date().toISOString() });
        return [];
    }
}

/**
 * Rotación: mantiene solo últimos N trades.
 */
async function cleanupOldTrades(limit = 1000) {
    try {
        const { data, error: selError } = await supabase
            .from(TABLE)
            .select('id, created_at')
            .order('created_at', { ascending: false })
            .limit(limit);

        if (selError || !data || data.length < limit) return;

        const cutoffDate = data[data.length - 1].created_at;

        const { error: delError, count } = await supabase
            .from(TABLE)
            .delete({ count: 'exact' })
            .lt('created_at', cutoffDate);

        if (delError) {
            console.error('[DB_CLEANUP]', { message: delError.message, time: new Date().toISOString() });
        } else if (count > 0) {
            console.log(`[DB_CLEANUP] ${count} trades antiguos eliminados`);
        }
    } catch (e) {
        console.error('[DB_CLEANUP]', { message: e.message, stack: e.stack, time: new Date().toISOString() });
    }
}

/**
 * Análisis adaptativo on-demand.
 */
async function runAdaptiveAnalysis(adaptive) {
    const rows = await getRecentTrades(1000);
    // FIX #6: Filtro estricto — solo resueltos
    const resolved = rows.filter(r => r.Veredicto === 'GANADA' || r.Veredicto === 'PERDIDA');

    if (resolved.length === 0) {
        return { trades: 0, message: 'Sin trades resueltos en DB' };
    }

    adaptive.learnFromCSV(resolved, 'bootstrap');

    const threshold = adaptive.getDynamicThreshold();
    const stats = adaptive.ASSET_STATS;
    const rl = adaptive.RL_MEMORY;

    const assetRanking = Object.entries(stats)
        .filter(([_, s]) => s.trades >= 5)
        .map(([asset, s]) => ({ asset, wr: ((s.wins / s.trades) * 100).toFixed(1), trades: s.trades }))
        .sort((a, b) => parseFloat(b.wr) - parseFloat(a.wr))
        .slice(0, 3);

    const rlRanking = Object.entries(rl)
        .filter(([_, s]) => s.trades >= 3)
        .map(([pattern, s]) => ({ pattern, score: s.score, trades: s.trades }))
        .sort((a, b) => b.score - a.score)
        .slice(0, 3);

    const summary = { trades: resolved.length, threshold, topAssets: assetRanking, topPatterns: rlRanking };
    console.log('[ADAPTIVE_ANALYSIS]', JSON.stringify(summary, null, 2));
    return summary;
}

module.exports = {
    insertTrade,
    updateTradeResult,
    getRecentTrades,
    cleanupOldTrades,
    runAdaptiveAnalysis
};
