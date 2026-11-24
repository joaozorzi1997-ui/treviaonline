/*
 * room-provider.js — provedor de salas do Trevia (Supabase Realtime: Broadcast + Presence)
 *
 * Este arquivo é a única parte do projeto que conhece o Supabase. O HTML do jogo só usa a
 * interface global `TreviaRoom` descrita abaixo, então o jogo pode ser atualizado sem tocar aqui.
 *
 * Configuração (somente valores PÚBLICOS, em config.js):
 *   window.TREVIA_CONFIG = {
 *     supabaseUrl:     "https://SEU-PROJETO.supabase.co",
 *     supabaseAnonKey: "chave anon/publishable do projeto",   // nunca a service_role!
 *     supabaseJsUrl:   "(opcional) URL do supabase-js, se quiser hospedar você mesmo"
 *   };
 *
 * Interface:
 *   TreviaRoom.isConfigured()            -> boolean
 *   TreviaRoom.generateCode()            -> string (código curto de sala)
 *   TreviaRoom.parseCode(codigoOuLink)   -> string | null   (aceita código puro ou link com #codigo)
 *   TreviaRoom.buildLink(codigo)         -> string          (link para compartilhar)
 *   TreviaRoom.createRoom(handlers)      -> Promise<Room>   (gera um código novo e entra)
 *   TreviaRoom.joinRoom(codigoOuLink, handlers) -> Promise<Room>
 *
 *   handlers (todos opcionais):
 *     onMessage({ data, peer, topic })   mensagem recebida de outro participante
 *     onPeerJoin(peerId)                 outro participante entrou
 *     onPeerLeave(peerId)                outro participante saiu ou perdeu a conexão
 *     onStatus(status)                   "connected" | "disconnected" | "reconnected"
 *
 *   Room:
 *     room.code, room.link, room.peerId
 *     room.send(topic, data)  -> Promise   envia para todos os outros participantes da sala
 *     room.peers()            -> string[]  ids dos outros participantes presentes agora
 *     room.leave()                          sai da sala (idempotente)
 *
 *   Erros (Error.code): "not-configured", "invalid-code", "load-failed", "connect-failed".
 */
(function (global) {
  "use strict";

  var CHANNEL_PREFIX = "trevia-";
  var EVENT = "msg";
  var ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789";
  var DEFAULT_SUPABASE_JS = "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.45.4/dist/umd/supabase.min.js";
  var CONNECT_TIMEOUT_MS = 12000;

  var clientPromise = null;

  function cfg() { return global.TREVIA_CONFIG || {}; }

  function fail(code, msg) { var e = new Error(msg || code); e.code = code; return e; }

  function isConfigured() {
    var c = cfg();
    return typeof c.supabaseUrl === "string" && /^https:\/\//.test(c.supabaseUrl) &&
           typeof c.supabaseAnonKey === "string" && c.supabaseAnonKey.length > 20;
  }

  function randomString(n, alphabet) {
    var out = "", i, buf;
    if (global.crypto && global.crypto.getRandomValues) {
      buf = new Uint32Array(n); global.crypto.getRandomValues(buf);
      for (i = 0; i < n; i++) out += alphabet[buf[i] % alphabet.length];
    } else {
      for (i = 0; i < n; i++) out += alphabet[Math.floor(Math.random() * alphabet.length)];
    }
    return out;
  }

  function generateCode() { return randomString(6, ALPHABET); }

  function parseCode(input) {
    if (input === null || input === undefined) return null;
    var s = String(input).trim();
    if (!s) return null;
    var h = s.indexOf("#");
    if (h >= 0) s = s.slice(h + 1);                       // link: usa o que vem depois do #
    else if (/^[a-z]+:\/\//i.test(s)) {                   // link sem #: tenta ?sala= / ?room= / último trecho do caminho
      try {
        var u = new URL(s);
        s = u.searchParams.get("sala") || u.searchParams.get("room") ||
            u.pathname.split("/").filter(Boolean).pop() || "";
      } catch (e) { return null; }
    }
    s = s.toLowerCase().replace(/[^a-z0-9]/g, "").slice(0, 12);
    return s.length >= 3 ? s : null;
  }

  function buildLink(code) {
    var base = String(global.location ? global.location.href : "").split("#")[0];
    return base + "#" + code;
  }

  function loadScript(url) {
    return new Promise(function (resolve, reject) {
      var s = document.createElement("script");
      s.src = url; s.async = true;
      s.onload = resolve;
      s.onerror = function () { reject(fail("load-failed", "Não foi possível carregar " + url)); };
      document.head.appendChild(s);
    });
  }

  function getClient() {
    if (clientPromise) return clientPromise;
    clientPromise = (async function () {
      if (!global.supabase || !global.supabase.createClient) await loadScript(cfg().supabaseJsUrl || DEFAULT_SUPABASE_JS);
      if (!global.supabase || !global.supabase.createClient) throw fail("load-failed", "supabase-js indisponível");
      return global.supabase.createClient(cfg().supabaseUrl, cfg().supabaseAnonKey, {
        auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
        realtime: { params: { eventsPerSecond: 20 } }
      });
    })();
    clientPromise.catch(function () { clientPromise = null; });   // permite tentar de novo depois de uma falha
    return clientPromise;
  }

  async function joinRoom(codeOrLink, handlers) {
    handlers = handlers || {};
    if (!isConfigured()) throw fail("not-configured", "Salas online não configuradas");
    var code = parseCode(codeOrLink);
    if (!code) throw fail("invalid-code", "Código inválido");

    var client = await getClient();
    var peerId = randomString(10, "abcdefghijklmnopqrstuvwxyz0123456789");
    var channel = client.channel(CHANNEL_PREFIX + code, {
      config: { broadcast: { self: false, ack: false }, presence: { key: peerId } }
    });

    var left = false, everConnected = false, wasDown = false, settled = false;
    function call(name, a) { try { if (typeof handlers[name] === "function") handlers[name](a); } catch (e) { console.error("[room-provider]", name, e); } }

    channel.on("broadcast", { event: EVENT }, function (m) {
      var p = m && m.payload;
      if (!p || p.p === peerId) return;
      call("onMessage", { data: p.d, peer: p.p, topic: p.t });
    });
    channel.on("presence", { event: "join" }, function (m) { if (m && m.key && m.key !== peerId) call("onPeerJoin", m.key); });
    channel.on("presence", { event: "leave" }, function (m) { if (m && m.key && m.key !== peerId && !left) call("onPeerLeave", m.key); });

    function peers() {
      var st = {}; try { st = channel.presenceState() || {}; } catch (e) {}
      return Object.keys(st).filter(function (k) { return k !== peerId; });
    }

    function down() { if (left || wasDown) return; wasDown = true; call("onStatus", "disconnected"); }
    function onOffline() { down(); }
    function onPageHide() { room.leave(); }

    var room = {
      code: code,
      link: buildLink(code),
      peerId: peerId,
      peers: peers,
      send: function (topic, data) {
        if (left) return Promise.reject(fail("connect-failed", "Fora da sala"));
        return Promise.resolve(channel.send({ type: "broadcast", event: EVENT, payload: { p: peerId, t: topic, d: data } }))
          .then(function (r) { if (r && r !== "ok") throw fail("connect-failed", String(r)); });
      },
      leave: function () {
        if (left) return; left = true;
        try { global.removeEventListener("offline", onOffline); global.removeEventListener("pagehide", onPageHide); } catch (e) {}
        try { channel.untrack(); } catch (e) {}
        try { client.removeChannel(channel); } catch (e) { try { channel.unsubscribe(); } catch (e2) {} }
      }
    };

    await new Promise(function (resolve, reject) {
      var timer = setTimeout(function () {
        if (settled) return; settled = true; room.leave();
        reject(fail("connect-failed", "Tempo esgotado ao conectar"));
      }, CONNECT_TIMEOUT_MS);

      channel.subscribe(function (status) {
        if (left) return;
        if (status === "SUBSCRIBED") {
          try { channel.track({ peer: peerId, at: Date.now() }); } catch (e) {}
          if (!settled) { settled = true; clearTimeout(timer); everConnected = true; resolve(); call("onStatus", "connected"); }
          else if (wasDown) { wasDown = false; call("onStatus", "reconnected"); }
        } else if (status === "CHANNEL_ERROR" || status === "TIMED_OUT" || status === "CLOSED") {
          if (!settled) { settled = true; clearTimeout(timer); room.leave(); reject(fail("connect-failed", String(status))); }
          else down();
        }
      });
    });

    global.addEventListener("offline", onOffline);
    global.addEventListener("pagehide", onPageHide);
    return room;
  }

  function createRoom(handlers) { return joinRoom(generateCode(), handlers); }

  global.TreviaRoom = {
    version: "1.0.0",
    isConfigured: isConfigured,
    generateCode: generateCode,
    parseCode: parseCode,
    buildLink: buildLink,
    createRoom: createRoom,
    joinRoom: joinRoom
  };
})(typeof window !== "undefined" ? window : this);
