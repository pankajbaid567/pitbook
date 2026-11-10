// Pitbook store: a tiny authenticated object store in front of one R2 bucket.
//
// The Go server (internal/archive.HTTPStore) mirrors its WAL segments and
// snapshots here so the free-tier Render instance, which has no persistent
// disk, can restore them after a restart. It deliberately accepts only the key
// shapes the engine produces.
//
//   PUT    /o/<SYMBOL>/wal/wal-<20 digits>.log      body = file bytes
//   PUT    /o/<SYMBOL>/snap/snap-<20 digits>.snap
//   GET    /o/<key>        DELETE /o/<key>
//   GET    /l?prefix=<SYMBOL>/    newline-separated keys
//
// Every request needs `Authorization: Bearer <STORE_TOKEN>`.

const KEY = /^[A-Za-z0-9_-]{1,32}\/(wal\/wal-[0-9]{20}\.log|snap\/snap-[0-9]{20}\.snap)$/;
const PREFIX = /^[A-Za-z0-9_-]{1,32}\/$/;
const MAX_BYTES = 16 * 1024 * 1024;

async function sha256(s) {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s)));
}

// Compare digests, not the strings, so length and content do not leak via timing.
async function authorized(request, env) {
  if (!env.STORE_TOKEN) return false;
  const [a, b] = await Promise.all([sha256(request.headers.get("Authorization") || ""), sha256("Bearer " + env.STORE_TOKEN)]);
  return crypto.subtle.timingSafeEqual(a, b);
}

const text = (body, status = 200) => new Response(body, { status, headers: { "content-type": "text/plain; charset=utf-8", "cache-control": "no-store" } });

export default {
  async fetch(request, env) {
    if (!(await authorized(request, env))) return text("unauthorized", 401);
    const url = new URL(request.url);

    if (url.pathname.startsWith("/o/")) {
      const key = decodeURIComponent(url.pathname.slice(3));
      if (!KEY.test(key)) return text("bad key", 400);
      switch (request.method) {
        case "PUT": {
          const len = Number(request.headers.get("content-length") || "0");
          if (len > MAX_BYTES) return text("too large", 413);
          const body = await request.arrayBuffer();
          if (body.byteLength > MAX_BYTES) return text("too large", 413);
          await env.BUCKET.put(key, body);
          return text("ok");
        }
        case "GET": {
          const obj = await env.BUCKET.get(key);
          if (!obj) return text("not found", 404);
          return new Response(obj.body, { headers: { "content-type": "application/octet-stream", "cache-control": "no-store" } });
        }
        case "DELETE":
          await env.BUCKET.delete(key);
          return text("ok");
        default:
          return text("method not allowed", 405);
      }
    }

    if (url.pathname === "/l" && request.method === "GET") {
      const prefix = url.searchParams.get("prefix") || "";
      if (!PREFIX.test(prefix)) return text("bad prefix", 400);
      const keys = [];
      let cursor;
      do {
        const page = await env.BUCKET.list({ prefix, cursor, limit: 1000 });
        for (const o of page.objects) keys.push(o.key);
        cursor = page.truncated ? page.cursor : undefined;
      } while (cursor);
      return text(keys.join("\n"));
    }

    return text("not found", 404);
  },
};
