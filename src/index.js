import { DurableObject } from 'cloudflare:workers';

/* ============ ثوابت ============ */
const LEVEL_STEP_MS = 8000;
const SEARCH_TIMEOUT_MS = 60000;
const HOST_SILENCE_MS = 15000;
const MATCH_MIN_MS = 150000;
const MATCH_MAX_MS = 20 * 60000;
const MAX_GOALS = 30;

const hashHex = async (text) => {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
};

const send = (ws, obj) => {
  if (!ws) return;
  try { ws.send(typeof obj === 'string' ? obj : JSON.stringify(obj)); } catch (_) {}
};

/* استدعاء Google Apps Script */
async function apiCall(env, action, token, data = {}) {
  const API_URL = env.API_URL;
  if (!API_URL) return { ok: false, error: 'no_api' };
  try {
    const r = await fetch(API_URL, {
      method: 'POST',
      redirect: 'follow',
      body: JSON.stringify({ action, token, ...data })
    });
    return await r.json();
  } catch (_) {
    return { ok: false, error: 'network' };
  }
}

/* ============ نقطة الدخول الرئيسية ============ */
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get('Origin') || '';

    // CORS للـ health
    if (url.pathname === '/health') {
      return new Response('ok', { status: 200 });
    }

    // نقطة اتصال اللعب عبر WebSocket
    if (url.pathname === '/game') {
      const upgrade = request.headers.get('Upgrade');
      if (!upgrade || upgrade.toLowerCase() !== 'websocket') {
        return new Response('Expected Upgrade: websocket', { status: 426 });
      }

      // التحقق من الـ Origin
      const allowed = (env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
      if (allowed.length && !allowed.includes(origin)) {
        return new Response('Forbidden origin', { status: 403 });
      }

      // توجيه الطلب إلى غرفة المطابقة العامة (Singleton)
      // (كل اللاعبين يدخلون نفس الغرفة، والغرفة تدير قائمة الانتظار والمباريات)
      const id = env.MATCH_ROOM.idFromName('global');
      const stub = env.MATCH_ROOM.get(id);
      return stub.fetch(request);
    }

    return new Response('FT5 Game Server', { status: 200 });
  }
};

/* ============================================================
   غرفة المطابقة المركزية: تدير قائمة الانتظار + المباريات + الترحيل
   ============================================================ */
export class MatchRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;

    // حالة الغرفة
    this.queue = new Map();      // ws -> { token, th, name, level, since }
    this.matches = new Map();    // matchId -> { h, g, score, t0, lastSnap, lastGoal, done, hWs, gWs }
    this.busy = new Set();       // hashes of tokens busy
    this.wsMeta = new Map();     // ws -> { token, th, name, level, state, matchId }

    // استعادة الجلسات بعد السبات
    this.ctx.getWebSockets().forEach((ws) => {
      const meta = ws.deserializeAttachment();
      if (meta) this.wsMeta.set(ws, meta);
    });

    // مؤقتات
    this.ensureAlarm();
  }

  /* جدولة المنبه القادم (Alarm) */
  ensureAlarm() {
    const now = Date.now();
    const candidates = [];

    // البحث عن مهلة بحث قريبة
    for (const info of this.queue.values()) {
      candidates.push(info.since + SEARCH_TIMEOUT_MS);
    }
    // فحص المباريات (انسحاب المضيف / سقف المدة)
    for (const mt of this.matches.values()) {
      candidates.push(mt.lastSnap + HOST_SILENCE_MS);
      candidates.push(mt.t0 + MATCH_MAX_MS);
    }

    if (candidates.length === 0) return;
    const next = Math.min(...candidates.filter(t => t > now));
    if (!isFinite(next)) return;

    // Alarm دقة 5 ثواني
    const target = Math.max(now + 5000, Math.min(next, now + 60000));
    this.ctx.storage.setAlarm(target);
  }

  async alarm() {
    const now = Date.now();

    // 1) مطابقة الباحثين
    this.tickMatchmaking(now);

    // 2) حراسة المباريات
    for (const [id, mt] of [...this.matches.entries()]) {
      if (mt.done) continue;
      if (now - mt.lastSnap > HOST_SILENCE_MS || now - mt.t0 > MATCH_MAX_MS) {
        send(mt.gWs, { t: 'opp_left' });
        await this.finalize(mt, mt.hWs);
      }
    }

    this.ensureAlarm();
  }

  /* ---------- المطابقة ---------- */
  tickMatchmaking(now) {
    const list = [...this.queue.entries()].sort((a, b) => a[1].since - b[1].since);
    const used = new Set();

    for (const [wsA, infoA] of list) {
      if (used.has(wsA)) continue;
      let bestWs = null, bestDl = Infinity;

      for (const [wsB, infoB] of list) {
        if (wsB === wsA || used.has(wsB)) continue;
        const dl = Math.abs(infoA.level - infoB.level);
        const allowedA = Math.floor((now - infoA.since) / LEVEL_STEP_MS);
        const allowedB = Math.floor((now - infoB.since) / LEVEL_STEP_MS);
        const allowed = Math.max(allowedA, allowedB);
        if (dl <= allowed && dl < bestDl) {
          bestDl = dl;
          bestWs = wsB;
        }
      }

      if (bestWs) {
        used.add(wsA);
        used.add(bestWs);
        this.startMatch(wsA, bestWs);
      } else if (now - infoA.since > SEARCH_TIMEOUT_MS) {
        used.add(wsA);
        send(wsA, { t: 'nomatch' });
        this.leaveQueue(wsA);
      }
    }
  }

  leaveQueue(ws) {
    const info = this.queue.get(ws);
    if (!info) return;
    this.queue.delete(ws);
    this.busy.delete(info.th);
    const meta = this.wsMeta.get(ws);
    if (meta) {
      meta.state = 'idle';
      this.wsMeta.set(ws, meta);
      this.safeSerialize(ws, meta);
    }
    this.env.MATCH_ROOM && apiCall(this.env, 'state', info.token, { state: 'active' }).catch(() => {});
  }

  startMatch(wsA, wsB) {
    const a = this.queue.get(wsA);
    const b = this.queue.get(wsB);
    this.queue.delete(wsA);
    this.queue.delete(wsB);

    const [hWs, gWs, hInfo, gInfo] = Math.random() < 0.5
      ? [wsA, wsB, a, b]
      : [wsB, wsA, b, a];

    const id = Date.now().toString(36) + '-' + crypto.randomUUID().slice(0, 8);
    const mt = {
      id, hWs, gWs,
      score: { red: 0, green: 0 },
      t0: Date.now(),
      lastSnap: Date.now(),
      lastGoal: 0,
      done: false
    };
    this.matches.set(id, mt);

    for (const [ws, info] of [[hWs, hInfo], [gWs, gInfo]]) {
      const meta = this.wsMeta.get(ws) || {};
      meta.state = 'match';
      meta.matchId = id;
      this.wsMeta.set(ws, meta);
      this.safeSerialize(ws, meta);
      apiCall(this.env, 'state', info.token, { state: 'busy' }).catch(() => {});
    }

    send(hWs, { t: 'start', role: 'host', matchId: id, opp: { name: gInfo.name, level: gInfo.level } });
    send(gWs, { t: 'start', role: 'guest', matchId: id, opp: { name: hInfo.name, level: hInfo.level } });
  }

  /* ---------- الإنهاء ---------- */
  async finalize(mt, forfeitWs = null) {
    if (mt.done) return;
    mt.done = true;
    this.matches.delete(mt.id);

    let { red, green } = mt.score;
    if (forfeitWs) {
      if (forfeitWs === mt.gWs) {
        if (red <= green) red = Math.min(MAX_GOALS, green + 1);
      } else {
        if (green <= red) green = Math.min(MAX_GOALS, red + 1);
      }
    }

    const report = async (ws, gf, ga) => {
      const meta = this.wsMeta.get(ws);
      if (!meta) return;
      const r = await apiCall(this.env, 'report', meta.token, {
        type: 'official', goalsFor: gf, goalsAgainst: ga, matchId: mt.id
      }).catch(() => ({ ok: false }));
      send(ws, {
        t: 'result',
        score: { red, green },
        profile: r.ok ? r.profile : null,
        leveledUp: !!r.leveledUp,
        fail: !r.ok,
        forfeit: !!forfeitWs,
        won: gf > ga
      });
      // تحرير اللاعب
      const m2 = this.wsMeta.get(ws);
      if (m2) {
        this.busy.delete(m2.th);
        m2.state = 'idle';
        m2.matchId = null;
        this.wsMeta.set(ws, m2);
        this.safeSerialize(ws, m2);
      }
    };

    await Promise.all([
      report(mt.hWs, red, green),
      report(mt.gWs, green, red)
    ]);
  }

  safeSerialize(ws, meta) {
    try { ws.serializeAttachment(meta); } catch (_) {}
  }

  /* ---------- استقبال WebSocket ---------- */
  async fetch(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.ctx.acceptWebSocket(server);

    const meta = { state: 'idle', token: null, th: null, name: null, level: 1, matchId: null };
    server.serializeAttachment(meta);
    this.wsMeta.set(server, meta);

    this.ensureAlarm();

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }
    if (!msg || typeof msg.t !== 'string') return;

    const meta = this.wsMeta.get(ws) || ws.deserializeAttachment() || { state: 'idle' };
    this.wsMeta.set(ws, meta);

    switch (msg.t) {
      /* ---- البحث ---- */
      case 'find': {
        if (meta.state !== 'idle') return;
        if (!/^[0-9a-f]{64}$/.test(String(msg.token || ''))) {
          return send(ws, { t: 'err', e: 'bad_token' });
        }
        meta.state = 'verifying';
        this.safeSerialize(ws, meta);

        const r = await apiCall(this.env, 'ping', msg.token);
        if (meta.state === 'gone') return;
        if (!r.ok) {
          meta.state = 'idle';
          this.safeSerialize(ws, meta);
          return send(ws, { t: 'err', e: r.error || 'server' });
        }

        const th = await hashHex(msg.token);
        if (this.busy.has(th)) {
          meta.state = 'idle';
          this.safeSerialize(ws, meta);
          return send(ws, { t: 'err', e: 'already' });
        }

        this.busy.add(th);
        meta.token = msg.token;
        meta.th = th;
        meta.name = String(r.profile.name || 'لاعب').slice(0, 20);
        meta.level = +r.profile.level || 1;
        meta.state = 'search';
        this.safeSerialize(ws, meta);

        this.queue.set(ws, {
          token: msg.token, th,
          name: meta.name, level: meta.level,
          since: Date.now()
        });

        apiCall(this.env, 'state', msg.token, { state: 'search' }).catch(() => {});
        send(ws, { t: 'queued' });
        this.ensureAlarm();
        return;
      }

      case 'cancel': {
        if (meta.state === 'search') this.leaveQueue(ws);
        return;
      }

      /* ---- أثناء المباراة ---- */
      case 'snap': {
        const mt = this.matches.get(meta.matchId);
        if (!mt || mt.done) return;
        if (ws !== mt.hWs) return;
        if (raw.length >= 8192) return;
        mt.lastSnap = Date.now();
        send(mt.gWs, raw);
        return;
      }

      case 'in':
      case 'act': {
        const mt = this.matches.get(meta.matchId);
        if (!mt || mt.done) return;
        if (ws !== mt.gWs) return;
        if (raw.length >= 512) return;
        send(mt.hWs, raw);
        return;
      }

      case 'goal': {
        const mt = this.matches.get(meta.matchId);
        if (!mt || mt.done) return;
        if (ws !== mt.hWs) return;
        const now = Date.now();
        if (now - mt.lastGoal < 3000) return;
        mt.lastGoal = now;
        if (msg.team === 'red' && mt.score.red < MAX_GOALS) mt.score.red++;
        else if (msg.team === 'green' && mt.score.green < MAX_GOALS) mt.score.green++;
        return;
      }

      case 'end': {
        const mt = this.matches.get(meta.matchId);
        if (!mt || mt.done) return;
        if (ws !== mt.hWs) return;
        if (Date.now() - mt.t0 < MATCH_MIN_MS) return;
        await this.finalize(mt);
        return;
      }
    }
  }

  async webSocketClose(ws, code, reason, wasClean) {
    const meta = this.wsMeta.get(ws);

    if (meta && meta.state === 'search') {
      this.leaveQueue(ws);
    } else if (meta && meta.state === 'match' && meta.matchId) {
      const mt = this.matches.get(meta.matchId);
      if (mt && !mt.done) {
        send(ws === mt.hWs ? mt.gWs : mt.hWs, { t: 'opp_left' });
        await this.finalize(mt, ws);
      }
    }

    if (meta) {
      meta.state = 'gone';
      this.wsMeta.set(ws, meta);
    }

    try { ws.close(code, 'closed'); } catch (_) {}

    this.ensureAlarm();
  }

  async webSocketError(ws, error) {
    // نفس سلوك الإغلاق
    await this.webSocketClose(ws, 1011, 'error', false);
  }
}
