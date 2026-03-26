// ═══════════════════════════════════════════════════════════════════
// adaptivePersistence.js — Persistencia de estado adaptativo en Supabase
// Carga y guarda RL_MEMORY, ASSET_STATS, dynamicThreshold
// ═══════════════════════════════════════════════════════════════════

const { createClient } = require('@supabase/supabase-js');

// FIX: Usar SERVICE_ROLE_KEY para bypasear RLS
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error('[ADAPTIVE_PERSISTENCE] SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY son requeridos');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const TABLE = 'adaptive_state';
const STATE_ID = 1; // Singleton row

// Debounce: evitar writes excesivos
let _saveTimer = null;
const DEBOUNCE_MS = 5000; // 5 segundos

/**
 * Carga el estado adaptativo desde Supabase.
 * Retorna { rl_memory, asset_stats, dynamic_threshold } o defaults si no existe.
 */
async function loadState() {
    try {
        const { data, error } = await supabase
            .from(TABLE)
            .select('*')
            .eq('id', STATE_ID)
            .single();

        if (error || !data) {
            console.log('[ADAPTIVE_LOAD] Sin estado previo en DB — usando defaults');
            return { rl_memory: {}, asset_stats: {}, dynamic_threshold: 54 };
        }

        // SAFETY: Defaults si JSON es null/undefined
        const state = {
            rl_memory: data.rl_memory || {},
            asset_stats: data.asset_stats || {},
            dynamic_threshold: typeof data.dynamic_threshold === 'number' ? data.dynamic_threshold : 54
        };

        const rlCount = Object.keys(state.rl_memory).length;
        const assetCount = Object.keys(state.asset_stats).length;
        console.log(`[ADAPTIVE_LOAD] Estado cargado: ${rlCount} patrones RL, ${assetCount} assets, threshold=${state.dynamic_threshold}`);

        return state;
    } catch (e) {
        console.error('[ADAPTIVE_LOAD]', { message: e.message, stack: e.stack, time: new Date().toISOString() });
        return { rl_memory: {}, asset_stats: {}, dynamic_threshold: 54 };
    }
}

/**
 * Guarda el estado adaptativo en Supabase (upsert en row id=1).
 */
async function saveState(state) {
    try {
        const payload = {
            id: STATE_ID,
            rl_memory: state.rl_memory || {},
            asset_stats: state.asset_stats || {},
            dynamic_threshold: typeof state.dynamic_threshold === 'number' ? state.dynamic_threshold : 54,
            updated_at: new Date().toISOString()
        };

        const { error } = await supabase
            .from(TABLE)
            .upsert(payload, { onConflict: 'id' });

        if (error) {
            console.error('[ADAPTIVE_SAVE]', { message: error.message, time: new Date().toISOString() });
        } else {
            console.log(`[ADAPTIVE_SAVE] Estado guardado (${Object.keys(payload.rl_memory).length} patrones RL)`);
        }
    } catch (e) {
        console.error('[ADAPTIVE_SAVE]', { message: e.message, stack: e.stack, time: new Date().toISOString() });
    }
}

/**
 * Guarda con debounce — evita saturar la DB en ráfagas de trades.
 * Se ejecuta máximo 1 vez cada DEBOUNCE_MS.
 */
function debounceSave(state) {
    if (_saveTimer) clearTimeout(_saveTimer);
    _saveTimer = setTimeout(() => {
        saveState(state);
        _saveTimer = null;
    }, DEBOUNCE_MS);
}

module.exports = {
    loadState,
    saveState,
    debounceSave
};
