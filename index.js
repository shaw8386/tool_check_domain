// index.js
import axios from "axios";
import { HttpsProxyAgent } from "https-proxy-agent";
import pkg from "pg";
import cron from "node-cron";
import * as cheerio from "cheerio";

const { Pool } = pkg;

/**
 * ENV bắt buộc:
 * - GS_API_URL: URL Web App Apps Script (doGet/doPost)
 * - GS_TOKEN: token secret để auth
 * - DATABASE_URL: Postgres connection string (Railway provides this)
 *
 * ENV khuyến nghị:
 * - GS_INPUT_SPREADSHEET_ID: Spreadsheet ID input
 * - GS_OUTPUT_SPREADSHEET_ID: Spreadsheet ID output
 *
 * Tuỳ chọn:
 * - DEFAULT_MAX_SLOTS: fallback nếu sheet thiếu Max Slot try (default 3)
 * - DEFAULT_MAXTIME_TRY_SECONDS: fallback nếu sheet thiếu Maxtime try (default 10)
 * - REQUEST_TIMEOUT_MS: timeout mỗi request (default 20000)
 * - MAX_REDIRECT_FIX: giới hạn số lần tự "fix redirect" (default 3)
 * - CONCURRENCY_LIMIT: số row xử lý song song (default 100)
 * - CRON_SCHEDULE: cron chạy theo giờ VN (default: "0 0,3,6,9,12,15,18,21 * * *")
 */

const GS_API_URL = process.env.GS_API_URL;
const GS_TOKEN = process.env.GS_TOKEN;
const DATABASE_URL = process.env.DATABASE_URL;

const GS_INPUT_SPREADSHEET_ID = process.env.GS_INPUT_SPREADSHEET_ID;
const GS_OUTPUT_SPREADSHEET_ID = process.env.GS_OUTPUT_SPREADSHEET_ID;

const DEFAULT_MAX_SLOTS = parseInt(process.env.DEFAULT_MAX_SLOTS || "3", 10);
const DEFAULT_MAXTIME_TRY_SECONDS = parseInt(process.env.DEFAULT_MAXTIME_TRY_SECONDS || "10", 10);
const REQUEST_TIMEOUT_MS = parseInt(process.env.REQUEST_TIMEOUT_MS || "20000", 10);
const MAX_REDIRECT_FIX = parseInt(process.env.MAX_REDIRECT_FIX || "3", 10);
const CONCURRENCY_LIMIT = parseInt(process.env.CONCURRENCY_LIMIT || "200", 10);

// Cron cố định theo giờ VN (00:00,03:00,06:00,09:00,12:00,15:00,18:00,21:00)
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || "0 0,3,6,9,12,15,18,21 * * *";

if (!GS_API_URL || !GS_TOKEN) {
  console.error("Missing GS_API_URL or GS_TOKEN env");
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL env");
  process.exit(1);
}
if (!GS_INPUT_SPREADSHEET_ID) {
  console.warn("Warning: GS_INPUT_SPREADSHEET_ID not set. Will rely on Apps Script properties.");
}
if (!GS_OUTPUT_SPREADSHEET_ID) {
  console.warn("Warning: GS_OUTPUT_SPREADSHEET_ID not set. Will rely on Apps Script properties.");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false
});

const VIETNAM_TIMEZONE = "Asia/Ho_Chi_Minh";

/** =========================
 *  Vietnam time helpers
 *  ========================= */
function getVietnamDateParts(date = new Date()) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: VIETNAM_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });

  const parts = formatter.formatToParts(date).reduce((acc, part) => {
    acc[part.type] = part.value;
    return acc;
  }, {});
  return {
    year: parts.year,
    month: parts.month,
    day: parts.day,
    hour: parts.hour,
    minute: parts.minute,
    second: parts.second
  };
}

function getVietnamDate(date = new Date()) {
  const p = getVietnamDateParts(date);
  const vnDateString = `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:${p.second}`;
  return new Date(vnDateString + "+07:00");
}

function getVietnamISOString(date = new Date()) {
  const p = getVietnamDateParts(date);
  return `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:${p.second}+07:00`;
}

function getVietnamLogTime(date = new Date()) {
  const p = getVietnamDateParts(date);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second} (VN)`;
}

function vnSheetName(date = new Date()) {
  const p = new Intl.DateTimeFormat("en-US", {
    timeZone: VIETNAM_TIMEZONE,
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  })
    .formatToParts(date)
    .reduce((acc, x) => ((acc[x.type] = x.value), acc), {});
  return `${p.hour}:${p.minute}_${p.month}/${p.day}/${p.year}`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** =========================
 *  Sheet key helpers
 *  - Vì sheet của bạn đang dùng header có khoảng trắng:
 *    "Maxtime try", "Max Slot try"
 *  ========================= */
function pickFirst(obj, keys, fallback = "") {
  for (const k of keys) {
    if (obj && Object.prototype.hasOwnProperty.call(obj, k) && obj[k] !== "" && obj[k] != null) {
      return obj[k];
    }
  }
  return fallback;
}

/** =========================
 *  Proxy helpers
 *  ========================= */
function parseProxy(proxyRaw) {
  if (!proxyRaw) return null;
  const s = String(proxyRaw).trim();
  if (!s) return null;

  if (s.startsWith("http://") || s.startsWith("https://")) return s;

  // user:pass@ip:port
  if (s.includes("@") && s.includes(":")) return "http://" + s;

  // ip:port:user:pass
  const parts = s.split(":");
  if (parts.length === 4) {
    const [ip, port, user, pass] = parts;
    return `http://${encodeURIComponent(user)}:${encodeURIComponent(pass)}@${ip}:${port}`;
  }

  // ip:port
  if (parts.length === 2) {
    const [ip, port] = parts;
    return `http://${ip}:${port}`;
  }

  return "http://" + s;
}

function normalizeDomain(domainRaw) {
  const d = String(domainRaw || "").trim();
  if (!d) return "";
  try {
    if (d.startsWith("http://") || d.startsWith("https://")) {
      const u = new URL(d);
      return u.host;
    }
  } catch {}
  return d;
}

/**
 * URL candidates:
 * - Để proxy HTTP dễ sống, ưu tiên http trước rồi https.
 */
function buildCandidateUrls(domain) {
  const host = normalizeDomain(domain);
  if (!host) return [];

  const hasWww = host.startsWith("www.");
  const bare = hasWww ? host.slice(4) : host;
  const withWww = hasWww ? host : `www.${host}`;

  const hosts = [host];
  if (host !== bare) hosts.push(bare);
  if (host !== withWww) hosts.push(withWww);

  const urls = [];
  for (const h of hosts) {
    urls.push(`http://${h}`);
    urls.push(`https://${h}`);
  }
  
  // Ưu tiên https://www.xxx trước để giảm redirect
  const uniqueUrls = [...new Set(urls)];
  const httpsWww = uniqueUrls.find(u => u.startsWith("https://www."));
  const httpsNonWww = uniqueUrls.find(u => u.startsWith("https://") && !u.startsWith("https://www."));
  const httpWww = uniqueUrls.find(u => u.startsWith("http://www."));
  const httpNonWww = uniqueUrls.find(u => u.startsWith("http://") && !u.startsWith("http://www."));
  
  const prioritized = [];
  if (httpsWww) prioritized.push(httpsWww);
  if (httpsNonWww) prioritized.push(httpsNonWww);
  if (httpWww) prioritized.push(httpWww);
  if (httpNonWww) prioritized.push(httpNonWww);
  
  // Thêm các URLs còn lại (nếu có)
  uniqueUrls.forEach(u => {
    if (!prioritized.includes(u)) prioritized.push(u);
  });
  
  return prioritized;
}

/** =========================
 *  Fix plan theo status / lỗi network
 *  ========================= */
function getFixPlan({ status, errorCode, currentUrl, domain, redirectLocation }) {
  const plan = { extraDelayMs: 0, headers: null, nextUrls: [] };

  const browserHeaders = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
  };

  if (errorCode === "ETIMEDOUT" || errorCode === "ECONNABORTED") {
    plan.extraDelayMs = 3000;
    plan.headers = browserHeaders;
    plan.nextUrls = [currentUrl];
    return plan;
  }

  // Network errors: thử lại với browser headers và các URL candidates khác
  if (errorCode === "ECONNRESET" || errorCode === "ECONNREFUSED" || errorCode === "ENOTFOUND") {
    plan.extraDelayMs = 2000;
    plan.headers = browserHeaders;
    // Thử các URL candidates khác nếu có
    plan.nextUrls = buildCandidateUrls(domain);
    return plan;
  }

  if (status === 429) {
    plan.extraDelayMs = 10000;
    plan.headers = browserHeaders;
    plan.nextUrls = [currentUrl];
    return plan;
  }

  if (status === 403 || status === 406) {
    plan.extraDelayMs = 3000;
    plan.headers = browserHeaders;
    plan.nextUrls = [currentUrl];
    return plan;
  }

  if (status === 404) {
    plan.headers = browserHeaders;
    plan.nextUrls = buildCandidateUrls(domain);
    return plan;
  }

  // 3xx: FAIL nhưng xử lí = follow Location
  if (status >= 300 && status <= 399 && redirectLocation) {
    try {
      const resolved = new URL(redirectLocation, currentUrl).toString();
      plan.nextUrls = [resolved];
    } catch {
      plan.nextUrls = [currentUrl];
    }
    plan.headers = browserHeaders;
    return plan;
  }

  if (status >= 500 && status <= 599) {
    plan.extraDelayMs = 4000;
    plan.headers = browserHeaders;
    plan.nextUrls = [currentUrl];
    return plan;
  }

  plan.headers = browserHeaders;
  plan.nextUrls = [currentUrl];
  return plan;
}

/** =========================
 *  Extract text (chỉ dùng khi title rỗng)
 *  ========================= */
function extractTextFromHTML(html, maxWords = 120) {
  try {
    if (!html || typeof html !== "string") return "";
    const $ = cheerio.load(html);

    $("script, style, noscript, iframe, embed, object, code, pre").remove();

    let text = "";
    const mainContent = $("main, article, [role='main'], .content, .main, #content, #main").first();
    if (mainContent.length > 0) text = mainContent.text();
    else text = $("body").text() || $("html").text() || "";

    text = text
      .replace(/&nbsp;/g, " ")
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'");

    text = text.replace(/\s+/g, " ").trim();

    const words = text.split(/\s+/).filter((w) => w && w.length > 0);
    return words.slice(0, maxWords).join(" ");
  } catch (e) {
    console.error(`[${getVietnamLogTime()}] Error extracting text: ${e?.message || e}`);
    return "";
  }
}

/** =========================
 *  HTTP check once
 *  - Always returns status
 *  - Returns content_domain = title || (content if title empty)
 *  ========================= */
async function checkOnce({ url, proxyUrl, headers }) {
  const agent = proxyUrl ? new HttpsProxyAgent(proxyUrl) : undefined;

  const res = await axios.get(url, {
    timeout: REQUEST_TIMEOUT_MS,
    validateStatus: () => true,
    maxRedirects: 0,
    headers: headers || {
      "User-Agent": "Railway-Proxy-HTTP-Checker/1.0",
      "Accept": "*/*"
    },
    httpsAgent: agent,
    httpAgent: agent,
    responseType: "text"
  });

  const ct = res.headers?.["content-type"] || "";
  const loc = res.headers?.location || "";

  let title = "";
  let head = "";
  if (typeof res.data === "string") {
    head = res.data.slice(0, 250).replace(/\s+/g, " ");
    try {
      const $ = cheerio.load(res.data);
      title = ($("title").text() || "").trim();
    } catch {}
  }

  console.log(
    `[${getVietnamLogTime()}] [HTTP] url=${url} status=${res.status} ct=${ct} loc=${loc} title="${title}" head="${head}"`
  );

  let content_domain = title;

  // Chỉ lấy content nếu status=200 và title rỗng
  if (res.status === 200 && !content_domain && typeof res.data === "string" && res.data.length) {
    const content = extractTextFromHTML(res.data, 120);
    if (content && content.trim()) content_domain = content;
  }

  return {
    status: res.status,
    redirectLocation: loc || null,
    content_domain
  };
}

/**
 * Read input from Apps Script:
 * GET {GS_API_URL}?action=input&token=...&inputSpreadsheetId=...
 * Expect: { ok:true, rows:[{Domain,...}] }
 */
async function getInputRows() {
  let url = `${GS_API_URL}?action=input&token=${encodeURIComponent(GS_TOKEN)}`;
  
  // Add input spreadsheet ID if provided
  if (GS_INPUT_SPREADSHEET_ID) {
    url += `&inputSpreadsheetId=${encodeURIComponent(GS_INPUT_SPREADSHEET_ID)}`;
  }
  
  const res = await axios.get(url, { timeout: 20000 });
  if (!res.data?.ok) throw new Error(res.data?.error || "GS input error");
  return res.data.rows || [];
}

async function postOutput(sheetName, data) {
  const payload = {
    action: "output",
    token: GS_TOKEN,
    sheetName,
    headers: ["Domain", "ISP", "DNS", "Update", "StatusHTTP", "StatusFinal", "URL", "ContentDomain"],
    data: data.map((row) => ({
      Domain: row.Domain || "",
      ISP: row.ISP || "",
      DNS: row.DNS || "",
      Update: getVietnamISOString(),
      StatusHTTP: row.StatusHTTP != null ? String(row.StatusHTTP) : "",
      StatusFinal: row.StatusFinal || "FAIL",
      URL: row.URL || "",
      ContentDomain: row.ContentDomain || ""
    }))
  };

  if (GS_OUTPUT_SPREADSHEET_ID) payload.outputSpreadsheetId = GS_OUTPUT_SPREADSHEET_ID;

  const res = await axios.post(GS_API_URL, payload, { timeout: 60000 });
  if (!res.data?.ok) throw new Error(res.data?.error || "GS output error");
  return res.data;
}

/** =========================
 *  Database
 *  - Không lưu title/content/content_len
 *  ========================= */
async function initDatabase() {
  // ============================================
  // RESET DATABASE: Uncomment đoạn code dưới để xóa bảng cũ và tạo lại bảng mới
  // Sau khi deploy xong, nhớ comment lại để không bị xóa dữ liệu mỗi lần restart
  // ============================================
  /*
  try {
    console.log(`[${getVietnamLogTime()}] Dropping existing table domain_checks...`);
    await pool.query(`DROP TABLE IF EXISTS domain_checks CASCADE;`);
    console.log(`[${getVietnamLogTime()}] Table dropped successfully`);
  } catch (error) {
    console.error(`[${getVietnamLogTime()}] Error dropping table: ${error?.message || error}`);
  }
  */

  await pool.query(`
    CREATE TABLE IF NOT EXISTS domain_checks (
      id SERIAL PRIMARY KEY,
      domain VARCHAR(255) NOT NULL,
      isp VARCHAR(255),
      dns VARCHAR(255),
      update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      status_http VARCHAR(50),
      status_final VARCHAR(10) NOT NULL,
      content_domain TEXT,
      status VARCHAR(50),
      redirect_url TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // migrate an toàn (nếu table đã tồn tại)
  await pool.query(`ALTER TABLE domain_checks ADD COLUMN IF NOT EXISTS content_domain TEXT;`);
  await pool.query(`ALTER TABLE domain_checks ADD COLUMN IF NOT EXISTS status VARCHAR(50);`);
  await pool.query(`ALTER TABLE domain_checks ADD COLUMN IF NOT EXISTS redirect_url TEXT;`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_domain_checks_domain ON domain_checks(domain);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_domain_checks_update_time ON domain_checks(update_time);`);

  console.log(`[${getVietnamLogTime()}] Database schema initialized`);
}

async function saveToDatabase(result) {
  try {
    const vnDate = getVietnamDate();
    await pool.query(
      `INSERT INTO domain_checks (domain, isp, dns, update_time, status_http, status_final, content_domain, status, redirect_url)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        result.Domain || "",
        result.ISP || "",
        result.DNS || "",
        vnDate,
        result.StatusHTTP != null ? String(result.StatusHTTP) : "",
        result.StatusFinal || "FAIL",
        result.ContentDomain || "",
        result.StatusHTTP != null ? String(result.StatusHTTP) : "",
        result.URL || ""
      ]
    );
  } catch (error) {
    console.error(`[${getVietnamLogTime()}] Error saving to DB domain=${result.Domain}: ${error?.message || error}`);
  }
}

/** =========================
 *  Core per-row process
 *  ========================= */
async function processRow(row) {
  const domain = String(row.Domain || "").trim();
  const proxyRaw = row.Proxy_IP_PORT_USER_PASS;
  const proxyUrl = parseProxy(proxyRaw);

  const isp = row.ISP ?? "";
  const dns = row.DNS ?? "";

  // đọc cả 2 kiểu header: có dấu _ hoặc có khoảng trắng
  const maxSlotsRaw = pickFirst(row, ["Max_Slot_try", "Max Slot try"], DEFAULT_MAX_SLOTS);
  const maxTimeRaw = pickFirst(row, ["Maxtime_try", "Maxtime try"], DEFAULT_MAXTIME_TRY_SECONDS);

  const maxSlots = parseInt(maxSlotsRaw || DEFAULT_MAX_SLOTS, 10);
  const maxTimeTrySec = parseInt(maxTimeRaw || DEFAULT_MAXTIME_TRY_SECONDS, 10);

  const baseDelayMs = maxTimeTrySec * 1000 + 5000;

  const candidates = buildCandidateUrls(domain);
      if (!domain || candidates.length === 0) {
    return {
      Domain: domain,
      ISP: isp,
      DNS: dns,
      StatusHTTP: "",
      StatusFinal: "FAIL",
      ContentDomain: "",
      LastURL: "",
      URL: ""
    };
  }

  let tried = 0;
  let lastStatus = "";
  let lastUrl = candidates[0];
  let statusFinal = "FAIL";
  let contentDomain = "";
  let firstStatus = ""; // Lưu status của lần check đầu tiên
  let redirectUrl = ""; // Lưu URL đích khi 3xx -> 200 thành công

  let currentUrl = candidates[0];
  let currentHeaders = null;

  let redirectFixCount = 0;

  for (let slot = 1; slot <= maxSlots; slot++) {
    tried = slot;
    console.log(
      `[${getVietnamLogTime()}] Try slot ${slot}/${maxSlots} domain=${domain} url=${currentUrl} proxy=${proxyUrl ? "YES" : "NO"}`
    );

    try {
      const result = await checkOnce({
        url: currentUrl,
        proxyUrl,
        headers: currentHeaders
      });

      lastStatus = String(result.status);
      lastUrl = currentUrl;

      // Lưu status của lần check đầu tiên (slot 1)
      if (slot === 1) {
        firstStatus = String(result.status);
      }

      if (result.status === 200) {
        statusFinal = "SUCCESS";
        contentDomain = result.content_domain || "";
        
        // Nếu status đầu tiên là 3xx (redirect) và hiện tại thành công 200, lưu URL đích
        if (firstStatus && parseInt(firstStatus) >= 300 && parseInt(firstStatus) <= 399) {
          redirectUrl = currentUrl;
        }
        
        console.log(
          `[${getVietnamLogTime()}] [SUCCESS] domain=${domain} status=200 firstStatus=${firstStatus} content_domain="${contentDomain ? contentDomain.slice(0, 100) : ""}" redirectUrl="${redirectUrl}"`
        );
        break;
      }

      const fixPlan = getFixPlan({
        status: result.status,
        errorCode: null,
        currentUrl,
        domain,
        redirectLocation: result.redirectLocation
      });

      if (result.status >= 300 && result.status <= 399 && result.redirectLocation) {
        redirectFixCount += 1;

        // Đã có điều hướng (3xx) → luôn ghi lại URL đích (kể cả SUCCESS hay FAIL)
        if (fixPlan.nextUrls?.length) {
          const redirectTarget = fixPlan.nextUrls[0];
          // Lưu URL chuyển hướng cuối cùng (có thể sẽ được overwrite nếu có nhiều lần redirect)
          redirectUrl = redirectTarget;
        }

        if (redirectFixCount > MAX_REDIRECT_FIX) {
          redirectFixCount = 0;
        } else if (fixPlan.nextUrls?.length) {
          currentUrl = fixPlan.nextUrls[0];
        }
      } else {
        redirectFixCount = 0;
      }

      if (fixPlan.headers) currentHeaders = fixPlan.headers;

      if (fixPlan.nextUrls?.length) {
        const pick = fixPlan.nextUrls.find((u) => u !== currentUrl) || fixPlan.nextUrls[0];
        currentUrl = pick;
      }

      const waitMs = baseDelayMs + (fixPlan.extraDelayMs || 0);
      if (slot < maxSlots) {
        console.log(`[${getVietnamLogTime()}] Wait ${Math.round(waitMs / 1000)}s then retry domain=${domain}`);
        await sleep(waitMs);
      }
    } catch (err) {
      const msg = String(err?.message || err);
      const code = err?.code || null;

      // Network errors: không set lastStatus ngay, để retry với các strategies khác
      // Chỉ set lastStatus nếu đã retry nhiều lần mà vẫn fail
      // StatusHTTP chỉ nên chứa HTTP status codes (1xx-5xx), không phải error codes
      if (!lastStatus) {
        // Chỉ set error code tạm thời để track, nhưng không lưu vào firstStatus
        // Nếu retry thành công sau đó, sẽ có HTTP status code thực sự
        if (
          code === "CERT_HAS_EXPIRED" ||
          code === "UNABLE_TO_VERIFY_LEAF_SIGNATURE" ||
          code === "SELF_SIGNED_CERT_IN_CHAIN" ||
          msg.toLowerCase().includes("certificate")
        ) {
          lastStatus = ""; // Không set SSL_ERROR, để retry
        } else if (code === "ETIMEDOUT" || code === "ECONNABORTED" || msg.toLowerCase().includes("timeout")) {
          lastStatus = ""; // Không set TIMEOUT, để retry
        } else if (code === "ECONNREFUSED" || code === "ENOTFOUND" || code === "ECONNRESET") {
          lastStatus = ""; // Không set NETWORK_ERROR, để retry với các URL khác
        } else {
          lastStatus = ""; // Không set ERROR, để retry
        }
      }

      // Lưu status của lần check đầu tiên nếu là slot 1
      // CHỈ lưu nếu là HTTP status code (1xx-5xx), không lưu error codes
      if (slot === 1 && !firstStatus) {
        // Không lưu error codes vào firstStatus, để retry
        firstStatus = "";
      }

      lastUrl = currentUrl;

      const fixPlan = getFixPlan({
        status: null,
        errorCode: code,
        currentUrl,
        domain,
        redirectLocation: null
      });

      if (fixPlan.headers) currentHeaders = fixPlan.headers;
      if (fixPlan.nextUrls?.length) {
        // Nếu có nhiều URLs (ví dụ: network error thử các candidates), chọn URL khác với currentUrl
        const pick = fixPlan.nextUrls.find((u) => u !== currentUrl) || fixPlan.nextUrls[0];
        currentUrl = pick;
      }

      const waitMs = baseDelayMs + (fixPlan.extraDelayMs || 0);
      if (slot < maxSlots) {
        console.log(`[${getVietnamLogTime()}] Wait ${Math.round(waitMs / 1000)}s then retry domain=${domain}`);
        await sleep(waitMs);
      }

      if (slot === maxSlots) {
        statusFinal = "FAIL";
        // Nếu hết retry mà vẫn không có HTTP status code, giữ StatusHTTP rỗng
        // StatusHTTP chỉ nên chứa HTTP status codes (1xx-5xx)
        if (!firstStatus || firstStatus === "") {
          firstStatus = ""; // Giữ rỗng thay vì error code
        }
      }

      console.error(
        `[${getVietnamLogTime()}] [FAIL] domain=${domain} slot=${slot}/${maxSlots} url=${currentUrl} proxy=${proxyUrl ? "YES" : "NO"} err=${msg} errorCode=${code} firstStatus=${firstStatus || "none"}`
      );
    }
  }

  // Tính StatusHTTP cho output:
  // - Mặc định: status của lần check cuối cùng (lastStatus)
  // - Riêng trường hợp đặc biệt: nếu firstStatus là 3xx (redirect),
  //   thì StatusHTTP sẽ luôn là firstStatus (3xx) để biết lần đầu là redirect
  let statusHttpOutput = "";
  const firstCode = firstStatus ? parseInt(firstStatus, 10) : NaN;
  const lastCode = lastStatus ? parseInt(lastStatus, 10) : NaN;

  // Mặc định: lấy status lần cuối nếu là HTTP code hợp lệ
  if (!Number.isNaN(lastCode)) {
    statusHttpOutput = String(lastCode);
  }

  // Trường hợp đặc biệt: lần đầu là 3xx → luôn ghi nhận 3xx của lần đầu
  if (!Number.isNaN(firstCode) && firstCode >= 300 && firstCode <= 399) {
    statusHttpOutput = String(firstCode);
  }

  return {
    Domain: domain,
    ISP: isp,
    DNS: dns,
    // StatusHTTP:
    // - Bình thường: status của lần check cuối cùng
    // - Nếu lần đầu là 3xx: luôn giữ status 3xx của lần đầu
    StatusHTTP: statusHttpOutput,
    StatusFinal: statusFinal,
    ContentDomain: contentDomain,
    TriedCount: tried,
    LastURL: lastUrl,
    URL: redirectUrl // URL đích khi 3xx -> 200 thành công
  };
}

/** =========================
 *  Parallel processing with concurrency limit
 *  ========================= */
async function processRowsInParallel(rows) {
  const results = [];

  for (let i = 0; i < rows.length; i += CONCURRENCY_LIMIT) {
    const batch = rows.slice(i, i + CONCURRENCY_LIMIT);
    const batchNumber = Math.floor(i / CONCURRENCY_LIMIT) + 1;
    const totalBatches = Math.ceil(rows.length / CONCURRENCY_LIMIT);

    console.log(`[${getVietnamLogTime()}] Processing batch ${batchNumber}/${totalBatches} (${batch.length} rows)`);

    const batchPromises = batch.map(async (row, batchIndex) => {
      const globalIndex = i + batchIndex + 1;
      console.log(`[${getVietnamLogTime()}] Processing row ${globalIndex}/${rows.length}: ${row.Domain || "N/A"}`);

      try {
        const result = await processRow(row);
        await saveToDatabase(result);
        return result;
      } catch (error) {
        console.error(
          `[${getVietnamLogTime()}] Error processing row ${globalIndex} (${row.Domain || "N/A"}): ${error?.message || error}`
        );

        const failed = {
          Domain: row.Domain || "",
          ISP: row.ISP || "",
          DNS: row.DNS || "",
          StatusHTTP: "ERROR",
          StatusFinal: "FAIL",
          ContentDomain: "",
          TriedCount: 0,
          LastURL: "",
          URL: ""
        };

        await saveToDatabase(failed);
        return failed;
      }
    });

    const batchResults = await Promise.all(batchPromises);
    results.push(...batchResults);

    console.log(`[${getVietnamLogTime()}] Completed batch ${batchNumber}/${totalBatches}`);
  }

  return results;
}

/** =========================
 *  Main
 *  ========================= */
async function main() {
  console.log(`[${getVietnamLogTime()}] Starting domain check process...`);
  console.log(`[${getVietnamLogTime()}] Concurrency limit: ${CONCURRENCY_LIMIT} rows at a time`);
  
  try {
    const rows = await getInputRows();
    console.log(`[${getVietnamLogTime()}] Retrieved ${rows.length} rows from Google Sheets`);
    
    if (rows.length > 0) {
      // Log sample 1 row (ẩn proxy password)
      const sampleRow = { ...rows[0] };
      if (sampleRow.Proxy_IP_PORT_USER_PASS) {
        sampleRow.Proxy_IP_PORT_USER_PASS = "***MASKED***";
      }
    
      console.log(
        `[${getVietnamLogTime()}] Sample input row[0]:`,
        sampleRow
      );
    }
    
    // Process rows in parallel with concurrency limit
    const output = await processRowsInParallel(rows);

    const sheetName = vnSheetName(getVietnamDate());
    await postOutput(sheetName, output);

    console.log(`[${getVietnamLogTime()}] DONE outputSheet=${sheetName} rows=${output.length}`);
  } catch (error) {
    console.error(`[${getVietnamLogTime()}] Error in main process:`, error);
    throw error;
  }
}

/** =========================
 *  Start + Schedule
 *  ========================= */
async function start() {
  try {
    await initDatabase();
    console.log(`[${getVietnamLogTime()}] Database initialized successfully`);

    // chạy ngay khi start để test
    await main().catch((err) => console.error(`[${getVietnamLogTime()}] Startup run failed: ${err?.message || err}`));

    console.log(`[${getVietnamLogTime()}] Scheduling job with cron: ${CRON_SCHEDULE} (Vietnam timezone)`);
    console.log(
      `[${getVietnamLogTime()}] Will run at: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 (Vietnam time)`
    );

    cron.schedule(
      CRON_SCHEDULE,
      async () => {
        console.log(`[${getVietnamLogTime()}] Scheduled job triggered`);
        try {
          await main();
        } catch (error) {
          console.error(`[${getVietnamLogTime()}] Scheduled job error: ${error?.message || error}`);
        }
      },
      { timezone: VIETNAM_TIMEZONE }
    );

    console.log(`[${getVietnamLogTime()}] Scheduler started. Waiting for next scheduled run...`);

    process.on("SIGTERM", async () => {
      console.log(`[${getVietnamLogTime()}] SIGTERM received, closing database pool...`);
      await pool.end();
      process.exit(0);
    });

    process.on("SIGINT", async () => {
      console.log(`[${getVietnamLogTime()}] SIGINT received, closing database pool...`);
      await pool.end();
      process.exit(0);
    });
  } catch (error) {
    console.error(`[${getVietnamLogTime()}] Error starting application: ${error?.message || error}`);
    process.exit(1);
  }
}

start();
