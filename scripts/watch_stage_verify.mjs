#!/usr/bin/env node

import fs from "node:fs";

try {
  const args = parseArgs(process.argv.slice(2));
  const harPath = args.har || process.env.WATCH_STAGE_HAR;
  const steamIdArg = args.steamId || process.env.WATCH_STAGE_STEAM_ID;
  const maxWaitMs = Number(args.timeoutMs || process.env.WATCH_STAGE_TIMEOUT_MS || 75000);

  if (!harPath && !steamIdArg) {
    fail("Pass --har <file.har> and optionally --steam-id <id>, or set WATCH_STAGE_* env vars.");
  }

  const harContext = harPath ? readHarContext(harPath, steamIdArg) : null;
  const steamId = steamIdArg || harContext?.steamId;
  if (!steamId) fail("No steamId found.");

  const headers = buildHeaders(harContext?.headers || {});

  console.log(`Target steamId: ${steamId}`);

  const info = await getWebsocketInfo(steamId, headers);
  console.log(`getWebsocketInfo: code=${info.code} message=${info.message} ws=${Boolean(info.result?.websocketUrl)}`);
  if (!info.result?.websocketUrl) fail("No websocketUrl returned.");

const matchData = await waitForMatchData(info.result.websocketUrl, steamId, maxWaitMs);
const coverage = await verifyAllPlayerStats(matchData, headers);

console.log(
  `verified: players=${coverage.players} stats=${coverage.stats} missing=${coverage.missing} map=${coverage.map}`
);
console.log(`stat keys: ${coverage.keys.join(",")}`);
console.log(`score: CT ${matchData.ctScore} - T ${matchData.terroristScore}`);
for (const player of coverage.rows) {
  console.log(
    [
      player.side,
      player.steamId,
      player.nickname || "(anonymous)",
      `ratingPro=${fmt(player.ratingPro)}`,
      `kd=${fmt(player.kd)}`,
      `adr=${fmt(player.adr)}`,
      `we=${fmt(player.we)}`,
      `pvpScore=${fmt(player.pvpScore)}`,
    ].join(" | ")
  );
}
} catch (error) {
  fail(error?.message || String(error));
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--har") out.har = argv[++i];
    else if (arg === "--steam-id") out.steamId = argv[++i];
    else if (arg === "--timeout-ms") out.timeoutMs = argv[++i];
  }
  return out;
}

function readHarContext(file, preferredSteamId) {
  const har = JSON.parse(fs.readFileSync(file, "utf8"));
  const entries = har.log?.entries || [];
  const requests = entries.filter((entry) =>
    String(entry.request?.url || "").includes("/steamcn/match/watchStage/getWebsocketInfo")
  );
  const selected =
    requests.find((entry) => preferredSteamId && String(entry.request?.url || "").includes(preferredSteamId)) ||
    requests[requests.length - 1];

  if (!selected) fail("No getWebsocketInfo request found in HAR.");

  const url = new URL(selected.request.url);
  return {
    steamId: url.searchParams.get("steamId"),
    headers: Object.fromEntries((selected.request.headers || []).map((h) => [h.name.toLowerCase(), h.value])),
  };
}

function buildHeaders(source) {
  const headers = {
    Origin: "https://news.wmpvp.com",
    Referer: "https://news.wmpvp.com/",
    "X-Requested-With": "XMLHttpRequest",
    platform: source.platform || process.env.WATCH_STAGE_PLATFORM || "h5_ios",
    appversion: source.appversion || process.env.WATCH_STAGE_APP_VERSION || "4.1.0",
    appTheme: source.apptheme || process.env.WATCH_STAGE_APP_THEME || "0",
    "User-Agent":
      source["user-agent"] ||
      process.env.WATCH_STAGE_UA ||
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 EsportsApp Version=4.1.0",
    Accept: "application/json, text/plain, */*",
  };

  const device = source.device || process.env.WATCH_STAGE_DEVICE;
  const accessToken = source.accesstoken || process.env.WATCH_STAGE_ACCESS_TOKEN;
  if (device) headers.device = device;
  if (accessToken) headers.accessToken = accessToken;
  return headers;
}

async function getWebsocketInfo(steamId, headers) {
  const url = new URL("https://appactivity.wmpvp.com/steamcn/match/watchStage/getWebsocketInfo");
  url.searchParams.set("steamId", steamId);
  url.searchParams.set("platform", "2");
  const response = await fetch(url, { headers });
  if (!response.ok) fail(`getWebsocketInfo HTTP ${response.status}`);
  return response.json();
}

function waitForMatchData(websocketUrl, steamId, maxWait) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(websocketUrl);
    const timers = new Set();
    let done = false;

    const finish = (err, data) => {
      if (done) return;
      done = true;
      for (const timer of timers) clearTimeout(timer);
      timers.clear();
      try {
        ws.close();
      } catch {}
      if (err) reject(err);
      else resolve(data);
    };

    const addTimer = (fn, ms) => {
      const timer = setTimeout(() => {
        timers.delete(timer);
        fn();
      }, ms);
      timers.add(timer);
    };

    const subscribe = () => {
      ws.send(JSON.stringify({ messageType: 10001, messageData: { steam_id: steamId } }));
      console.log("subscribe sent");
    };

    addTimer(() => finish(new Error("Timed out waiting for messageType=10002.")), maxWait);

    ws.addEventListener("open", () => {
      console.log("ws open -> ping");
      ws.send("ping");
    });

    ws.addEventListener("message", (event) => {
      const text = typeof event.data === "string" ? event.data : Buffer.from(event.data).toString("utf8");
      if (text === "pong") {
        console.log("ws pong");
        subscribe();
        return;
      }

      let message;
      try {
        message = JSON.parse(text);
      } catch {
        return;
      }

      console.log(`ws messageType=${message.messageType}`);
      if (message.messageType === 10003) {
        addTimer(subscribe, 20000);
        return;
      }

      if (message.messageType === 10002 && message.messageData?.playerList?.length) {
        finish(null, message.messageData);
      }
    });

    ws.addEventListener("error", () => finish(new Error("WebSocket error.")));
  });
}

async function verifyAllPlayerStats(matchData, headers) {
  const ctTeamSteamIds = matchData.playerList.filter((player) => player.side === "CT").map((player) => String(player.steamId));
  const teTeamSteamIds = matchData.playerList
    .filter((player) => player.side === "TERRORIST")
    .map((player) => String(player.steamId));

  console.log(
    `matchData: players=${matchData.playerList.length} CT=${ctTeamSteamIds.length} T=${teTeamSteamIds.length} map=${matchData.map}`
  );

  const response = await fetch("https://appactivity.wmpvp.com/steamcn/match/watchStage/getPvPMatchTeamStatisticsData", {
    method: "POST",
    headers: { ...headers, "Content-Type": "application/json" },
    body: JSON.stringify({ ctTeamSteamIds, teTeamSteamIds, map: matchData.map }),
  });
  if (!response.ok) fail(`getPvPMatchTeamStatisticsData HTTP ${response.status}`);

  const data = await response.json();
  if (data.code !== 1 || !data.result) fail(`stats API failed: code=${data.code} message=${data.message}`);

  const stats = [...(data.result.ctPlayerStatsDTOList || []), ...(data.result.tplayerStatsDTOList || [])];
  const statsIds = new Set(stats.map((player) => String(player.steamId)));
  const missing = matchData.playerList.map((player) => String(player.steamId)).filter((id) => !statsIds.has(id));
  if (missing.length) fail(`Missing stats for ${missing.length} player(s).`);

  return {
    players: matchData.playerList.length,
    stats: statsIds.size,
    missing: missing.length,
    map: matchData.map,
    keys: Object.keys(stats[0] || {}).slice(0, 24),
    rows: matchData.playerList.map((player) => {
      const stat = stats.find((item) => String(item.steamId) === String(player.steamId)) || {};
      return {
        side: player.side,
        steamId: String(player.steamId),
        nickname: stat.nickname || player.nickname,
        ratingPro: stat.ratingPro,
        kd: stat.kd,
        adr: stat.adr,
        we: stat.we,
        pvpScore: stat.pvpScore,
      };
    }),
  };
}

function fail(message) {
  console.error(message);
  process.exit(1);
}

function fmt(value) {
  return value ?? "-";
}
