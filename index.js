// import axios from "axios";
// import { HttpsProxyAgent } from "https-proxy-agent";
// import pkg from "pg";
// import cron from "node-cron";
// import * as cheerio from "cheerio";
// const { Pool } = pkg;

// /**
//  * ENV cần có:
//  * - GS_API_URL: URL Web App Apps Script (doGet/doPost)
//  * - GS_TOKEN: token secret để auth
//  * - DATABASE_URL: Postgres connection string (Railway provides this)
//  *
//  * Tuỳ chọn:
//  * - DEFAULT_MAX_SLOTS: fallback nếu sheet thiếu Max_Slot_try
//  * - DEFAULT_MAXTIME_TRY_SECONDS: fallback nếu sheet thiếu Maxtime_try
//  * - REQUEST_TIMEOUT_MS: timeout mỗi request (mặc định 15000ms)
//  * - MAX_REDIRECT_FIX: giới hạn số lần tự "fix redirect"
//  * - CRON_SCHEDULE: cron schedule in server timezone (default: "0 *\/3 * * *" = every 3 hours)
//  *   Note: All logs and output use Vietnam time (Asia/Ho_Chi_Minh). Adjust CRON_SCHEDULE based on server timezone.
//  */


// const GS_API_URL = process.env.GS_API_URL;
// const GS_TOKEN = process.env.GS_TOKEN;
// const DATABASE_URL = process.env.DATABASE_URL;
// const GS_INPUT_SPREADSHEET_ID = process.env.GS_INPUT_SPREADSHEET_ID;
// const GS_OUTPUT_SPREADSHEET_ID = process.env.GS_OUTPUT_SPREADSHEET_ID;
// const DEFAULT_MAX_SLOTS = parseInt(process.env.DEFAULT_MAX_SLOTS || "3", 10);
// const DEFAULT_MAXTIME_TRY_SECONDS = parseInt(process.env.DEFAULT_MAXTIME_TRY_SECONDS || "10", 10);
// const REQUEST_TIMEOUT_MS = parseInt(process.env.REQUEST_TIMEOUT_MS || "15000", 10);
// const MAX_REDIRECT_FIX = parseInt(process.env.MAX_REDIRECT_FIX || "3", 10);
// // Cron schedule for Vietnam time: 0h, 3h, 6h, 9h, 12h, 15h, 18h, 21h (every 3 hours)
// const CRON_SCHEDULE = process.env.CRON_SCHEDULE || "0 0,3,6,9,12,15,18,21 * * *";

// if (!GS_API_URL || !GS_TOKEN) {
//   console.error("Missing GS_API_URL or GS_TOKEN env");
//   process.exit(1);
// }

// if (!DATABASE_URL) {
//   console.error("Missing DATABASE_URL env");
//   process.exit(1);
// }

// // Spreadsheet IDs are optional if set in Apps Script properties, but recommended to set here
// // Note: These warnings appear before timezone functions are defined, so they use server time
// if (!GS_INPUT_SPREADSHEET_ID) {
//   console.warn("Warning: GS_INPUT_SPREADSHEET_ID not set. Will rely on Apps Script properties.");
// }

// if (!GS_OUTPUT_SPREADSHEET_ID) {
//   console.warn("Warning: GS_OUTPUT_SPREADSHEET_ID not set. Will rely on Apps Script properties.");
// }

// // Initialize Postgres connection pool
// const pool = new Pool({
//   connectionString: DATABASE_URL,
//   ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false
// });

// const VIETNAM_TIMEZONE = "Asia/Ho_Chi_Minh";

// /**
//  * Get date/time parts in Vietnam timezone
//  */
// function getVietnamDateParts(date = new Date()) {
//   const formatter = new Intl.DateTimeFormat("en-US", {
//     timeZone: VIETNAM_TIMEZONE,
//     year: "numeric",
//     month: "2-digit",
//     day: "2-digit",
//     hour: "2-digit",
//     minute: "2-digit",
//     second: "2-digit",
//     hour12: false
//   });
  
//   const parts = formatter.formatToParts(date).reduce((acc, part) => {
//     acc[part.type] = part.value;
//     return acc;
//   }, {});
  
//   return {
//     year: parts.year,
//     month: parts.month,
//     day: parts.day,
//     hour: parts.hour,
//     minute: parts.minute,
//     second: parts.second
//   };
// }

// /**
//  * Get current date/time as Date object representing Vietnam time
//  * Note: This creates a Date object that represents the same moment in Vietnam time
//  */
// function getVietnamDate(date = new Date()) {
//   const parts = getVietnamDateParts(date);
//   // Create a date string in ISO format and parse it
//   // Vietnam is UTC+7, so we create a UTC date that represents the Vietnam time
//   const vnDateString = `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}`;
//   // Parse as if it's UTC+7, then convert to actual Date
//   const utcDate = new Date(vnDateString + "+07:00");
//   return utcDate;
// }

// /**
//  * Format date to ISO string in Vietnam timezone
//  * Returns: YYYY-MM-DDTHH:mm:ss+07:00 format
//  */
// function getVietnamISOString(date = new Date()) {
//   const parts = getVietnamDateParts(date);
//   return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}+07:00`;
// }

// /**
//  * Format date for logging in Vietnam timezone
//  * Returns: YYYY-MM-DD HH:mm:ss (VN)
//  */
// function getVietnamLogTime(date = new Date()) {
//   const parts = getVietnamDateParts(date);
//   return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} (VN)`;
// }

// function sleep(ms) {
//   return new Promise((r) => setTimeout(r, ms));
// }

// /**
//  * Parse proxy string from sheet column `Proxy_IP_PORT_USER_PASS`.
//  *
//  * Hỗ trợ các format phổ biến:
//  * 1) ip:port:user:pass
//  * 2) user:pass@ip:port
//  * 3) http://user:pass@ip:port
//  * 4) https://user:pass@ip:port
//  *
//  * Return: proxyUrl (string) hoặc null
//  */
// function parseProxy(proxyRaw) {
//   if (!proxyRaw) return null;
//   const s = String(proxyRaw).trim();
//   if (!s) return null;

//   // already a url
//   if (s.startsWith("http://") || s.startsWith("https://")) return s;

//   // user:pass@ip:port
//   if (s.includes("@") && s.includes(":")) {
//     return "http://" + s; // default http proxy scheme
//   }

//   // ip:port:user:pass  (4 parts)
//   const parts = s.split(":");
//   if (parts.length === 4) {
//     const [ip, port, user, pass] = parts;
//     return `http://${encodeURIComponent(user)}:${encodeURIComponent(pass)}@${ip}:${port}`;
//   }

//   // ip:port (no auth)
//   if (parts.length === 2) {
//     const [ip, port] = parts;
//     return `http://${ip}:${port}`;
//   }

//   // fallback: treat as host:port or raw
//   return "http://" + s;
// }

// function normalizeDomain(domainRaw) {
//   const d = String(domainRaw || "").trim();
//   if (!d) return "";
//   // nếu người dùng nhập full url, tách host
//   try {
//     if (d.startsWith("http://") || d.startsWith("https://")) {
//       const u = new URL(d);
//       return u.host;
//     }
//   } catch {}
//   return d;
// }

// /**
//  * Generate target URLs to try.
//  * Ưu tiên HTTPS trước, rồi HTTP; thử thêm/bớt www nếu cần.
//  */
// function buildCandidateUrls(domain) {
//   const host = normalizeDomain(domain);
//   if (!host) return [];

//   const hasWww = host.startsWith("www.");
//   const bare = hasWww ? host.slice(4) : host;
//   const withWww = hasWww ? host : `www.${host}`;

//   // ưu tiên dạng host gốc trước, sau đó biến thể
//   const hosts = [host];
//   if (host !== bare) hosts.push(bare);
//   if (host !== withWww) hosts.push(withWww);

//   const urls = [];
//   for (const h of hosts) {
//     urls.push(`https://${h}`);
//     urls.push(`http://${h}`);
//   }

//   // loại trùng
//   return [...new Set(urls)];
// }

// /**
//  * “Xử lí lỗi theo status”:
//  * - Tuỳ theo status/failure type, điều chỉnh headers, url list, delay...
//  */
// function getFixPlan({ status, errorCode, currentUrl, domain, redirectLocation }) {
//   const plan = {
//     extraDelayMs: 0,
//     headers: null,
//     nextUrls: []
//   };

//   // Base “browser-like” headers (đặc biệt hữu ích với 403/406)
//   const browserHeaders = {
//     "User-Agent":
//       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
//     "Accept":
//       "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
//     "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
//     "Cache-Control": "no-cache",
//     "Pragma": "no-cache"
//   };

//   // Nếu lỗi timeout / network
//   if (errorCode === "ETIMEDOUT" || errorCode === "ECONNABORTED") {
//     plan.extraDelayMs = 2000;
//     plan.headers = browserHeaders;
//     plan.nextUrls = [currentUrl]; // try same
//     return plan;
//   }

//   // 429: rate limit -> chờ lâu hơn
//   if (status === 429) {
//     plan.extraDelayMs = 10000;
//     plan.headers = browserHeaders;
//     plan.nextUrls = [currentUrl];
//     return plan;
//   }

//   // 403/406: thử “browser-like” headers
//   if (status === 403 || status === 406) {
//     plan.extraDelayMs = 3000;
//     plan.headers = browserHeaders;
//     plan.nextUrls = [currentUrl];
//     return plan;
//   }

//   // 404: thử biến thể www và http/https
//   if (status === 404) {
//     plan.headers = browserHeaders;
//     plan.nextUrls = buildCandidateUrls(domain);
//     return plan;
//   }

//   // 3xx: vì bạn muốn 3xx là FAIL, nhưng “xử lí” bằng cách thử Location
//   if (status >= 300 && status <= 399 && redirectLocation) {
//     // if Location là relative, resolve
//     try {
//       const resolved = new URL(redirectLocation, currentUrl).toString();
//       plan.nextUrls = [resolved];
//     } catch {
//       plan.nextUrls = [currentUrl];
//     }
//     plan.headers = browserHeaders;
//     return plan;
//   }

//   // 5xx: lỗi server -> backoff nhẹ, thử lại
//   if (status >= 500 && status <= 599) {
//     plan.extraDelayMs = 4000;
//     plan.headers = browserHeaders;
//     plan.nextUrls = [currentUrl];
//     return plan;
//   }

//   // Default: retry same with browser headers
//   plan.headers = browserHeaders;
//   plan.nextUrls = [currentUrl];
//   return plan;
// }

// /**
//  * Extract text content from HTML and return first N words
//  */
// function extractTextFromHTML(html, maxWords = 100) {
//   try {
//     if (!html || typeof html !== "string") return "";
    
//     const $ = cheerio.load(html);
    
//     // Remove script, style, and other non-content elements
//     $("script, style").remove();
    
//     // Get text content
//     const text = $("body").text() || $("html").text() || "";
    
//     // Clean up: remove extra whitespace, newlines, tabs
//     const cleaned = text
//       .replace(/\s+/g, " ")
//       .replace(/\n+/g, " ")
//       .trim();
    
//     // Split into words and take first maxWords
//     const words = cleaned.split(/\s+/).filter(word => word.length > 0);
//     const firstWords = words.slice(0, maxWords);
    
//     return firstWords.join(" ");
//   } catch (error) {
//     console.error(`[${getVietnamLogTime()}] Error extracting text from HTML:`, error.message);
//     return "";
//   }
// }

// /**
//  * Single HTTP check attempt.
//  * SUCCESS only if status === 200
//  * For redirect handling: we keep maxRedirects=0 để bắt 3xx và xử lí theo Location.
//  * Returns content when status is 200.
//  */
// async function checkOnce({ url, proxyUrl, headers }) {
//   const agent = proxyUrl ? new HttpsProxyAgent(proxyUrl) : undefined;

//   const res = await axios.get(url, {
//     timeout: REQUEST_TIMEOUT_MS,
//     validateStatus: () => true,
//     maxRedirects: 0, // quan trọng: bắt 3xx, tự xử lí "fix"
//     headers: headers || {
//       "User-Agent": "Railway-Proxy-HTTP-Checker/1.0",
//       "Accept": "*/*"
//     },
//     httpsAgent: agent,
//     httpAgent: agent,
//     responseType: "text" // Get response as text for HTML parsing
//   });
//   const ct = res.headers?.["content-type"] || "";
//   const loc = res.headers?.location || "";
//   let title = "";
//   let head = "";
  
//   if (typeof res.data === "string") {
//     head = res.data.slice(0, 400).replace(/\s+/g, " ");
//     try {
//       const $ = cheerio.load(res.data);
//       title = ($("title").text() || "").trim();
//     } catch {}
//   }
  
//   console.log(`[${getVietnamLogTime()}] [HTTP] url=${url} status=${res.status} ct=${ct} loc=${loc} title="${title}" head="${head}"`);

//   let content = "";
  
//   // Extract content if status is 200
//   if (res.status === 200 && res.data) {
//     try {
//       content = extractTextFromHTML(res.data, 100);
//     } catch (error) {
//       console.error(`[${getVietnamLogTime()}] Error processing content for ${url}:`, error.message);
//     }
//   }

//   return {
//     status: res.status,
//     redirectLocation: res.headers?.location || null,
//     content: content
//   };
// }

// /**
//  * Read input from Apps Script:
//  * GET {GS_API_URL}?action=input&token=...&inputSpreadsheetId=...
//  * Expect: { ok:true, rows:[{Domain,...}] }
//  */
// async function getInputRows() {
//   let url = `${GS_API_URL}?action=input&token=${encodeURIComponent(GS_TOKEN)}`;
  
//   // Add input spreadsheet ID if provided
//   if (GS_INPUT_SPREADSHEET_ID) {
//     url += `&inputSpreadsheetId=${encodeURIComponent(GS_INPUT_SPREADSHEET_ID)}`;
//   }
  
//   const res = await axios.get(url, { timeout: 20000 });
//   if (!res.data?.ok) throw new Error(res.data?.error || "GS input error");
//   return res.data.rows || [];
// }

// /**
//  * Initialize database schema
//  */
// async function initDatabase() {
//   try {
//     await pool.query(`
//       CREATE TABLE IF NOT EXISTS domain_checks (
//         id SERIAL PRIMARY KEY,
//         domain VARCHAR(255) NOT NULL,
//         isp VARCHAR(255),
//         dns VARCHAR(255),
//         update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//         status_http VARCHAR(10),
//         status_final VARCHAR(10) NOT NULL,
//         content TEXT,
//         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//       );
      
//       CREATE INDEX IF NOT EXISTS idx_domain_checks_domain ON domain_checks(domain);
//       CREATE INDEX IF NOT EXISTS idx_domain_checks_update_time ON domain_checks(update_time);
//     `);
//     console.log(`[${getVietnamLogTime()}] Database schema initialized`);
//   } catch (error) {
//     console.error(`[${getVietnamLogTime()}] Error initializing database:`, error);
//     throw error;
//   }
// }

// /**
//  * Save result to Postgres database
//  */
// async function saveToDatabase(result) {
//   try {
//     // Use Vietnam time for database timestamp
//     const vnDate = getVietnamDate();
//     await pool.query(
//       `INSERT INTO domain_checks (domain, isp, dns, update_time, status_http, status_final, content)
//        VALUES ($1, $2, $3, $4, $5, $6, $7)`,
//       [
//         result.Domain || "",
//         result.ISP || "",
//         result.DNS || "",
//         vnDate,
//         result.StatusHTTP ? String(result.StatusHTTP) : "",
//         result.StatusFinal || "FAIL",
//         result.Content || ""
//       ]
//     );
//   } catch (error) {
//     console.error(`[${getVietnamLogTime()}] Error saving to database for domain ${result.Domain}:`, error);
//     // Don't throw - continue processing other rows
//   }
// }

// /**
//  * Write output to Apps Script:
//  * POST {action:"output", token, outputSpreadsheetId, sheetName, headers, data}
//  */
// async function postOutput(sheetName, data) {
//   const payload = {
//     action: "output",
//     token: GS_TOKEN,
//     sheetName,
//     headers: [
//       "Domain",
//       "ISP",
//       "DNS",
//       "Update",
//       "StatusHTTP",
//       "StatusFinal",
//       "Content"
//     ],
//     data: data.map(row => ({
//       Domain: row.Domain || "",
//       ISP: row.ISP || "",
//       DNS: row.DNS || "",
//       Update: getVietnamISOString(),
//       StatusHTTP: row.StatusHTTP ? String(row.StatusHTTP) : "",
//       StatusFinal: row.StatusFinal || "FAIL",
//       Content: row.Content || ""
//     }))
//   };

//   // Add output spreadsheet ID if provided
//   if (GS_OUTPUT_SPREADSHEET_ID) {
//     payload.outputSpreadsheetId = GS_OUTPUT_SPREADSHEET_ID;
//   }

//   const res = await axios.post(GS_API_URL, payload, { timeout: 30000 });
//   if (!res.data?.ok) throw new Error(res.data?.error || "GS output error");
//   return res.data;
// }

// /**
//  * Name output sheet: HH:mm_MM/DD/YYYY theo Asia/Ho_Chi_Minh
//  */
// function vnSheetName(date = new Date()) {
//   const parts = new Intl.DateTimeFormat("en-US", {
//     timeZone: VIETNAM_TIMEZONE,
//     hour12: false,
//     year: "numeric",
//     month: "2-digit",
//     day: "2-digit",
//     hour: "2-digit",
//     minute: "2-digit"
//   })
//     .formatToParts(date)
//     .reduce((acc, p) => ((acc[p.type] = p.value), acc), {});
//   return `${parts.hour}:${parts.minute}_${parts.month}/${parts.day}/${parts.year}`;
// }

// /**
//  * Main per-row process:
//  * - Try candidates
//  * - SUCCESS only if status 200
//  * - Retry Max_Slot_try times
//  * - Delay between retries: Maxtime_try + 5s (+ extra delay from fixPlan)
//  * - Each retry must apply “fix plan” based on last fail status
//  */
// async function processRow(row) {
//   const domain = String(row.Domain || "").trim();
//   const proxyRaw = row.Proxy_IP_PORT_USER_PASS;
//   const proxyUrl = parseProxy(proxyRaw);

//   const isp = row.ISP ?? "";
//   const dns = row.DNS ?? "";

//   const maxSlots = parseInt(row.Max_Slot_try || DEFAULT_MAX_SLOTS, 10);
//   const maxTimeTrySec = parseInt(row.Maxtime_try || DEFAULT_MAXTIME_TRY_SECONDS, 10);
//   // Nếu Maxtime_try đang là milliseconds, đổi dòng dưới thành: const baseDelayMs = maxTimeTrySec + 5000;
//   const baseDelayMs = maxTimeTrySec * 1000 + 5000;

//   const candidates = buildCandidateUrls(domain);
//   if (!domain || candidates.length === 0) {
//     return {
//       ...row,
//       StatusHTTP: "",
//       StatusFinal: "FAIL",
//       TriedCount: 0,
//       LastURL: ""
//     };
//   }

//   let tried = 0;
//   let lastStatus = "";
//   let lastUrl = candidates[0];
//   let statusFinal = "FAIL";
//   let content = "";

//   // vòng retry slots
//   let currentUrl = candidates[0];
//   let currentHeaders = null;

//   // để “xử lí redirect” nhiều bước nhưng vẫn nằm trong 1 slot:
//   let redirectFixCount = 0;

//   for (let slot = 1; slot <= maxSlots; slot++) {
//     tried = slot;
//     console.log(
//       `[${getVietnamLogTime()}] Try slot ${slot}/${maxSlots} domain=${domain} url=${currentUrl} proxy=${proxyUrl ? "YES" : "NO"}`
//     );

//     try {
//       const result = await checkOnce({
//         url: currentUrl,
//         proxyUrl,
//         headers: currentHeaders
//       });

//       lastStatus = result.status;
//       lastUrl = currentUrl;

//       // SUCCESS only when 200
//       if (result.status === 200) {
//         statusFinal = "SUCCESS";
//         // Capture content when status is 200
//         content = result.content || "";
//         break;
//       }

//       // FAIL: xử lí theo status
//       const fixPlan = getFixPlan({
//         status: result.status,
//         errorCode: null,
//         currentUrl,
//         domain,
//         redirectLocation: result.redirectLocation
//       });

//       // special: redirect fix limited
//       if (result.status >= 300 && result.status <= 399 && result.redirectLocation) {
//         redirectFixCount += 1;
//         if (redirectFixCount > MAX_REDIRECT_FIX) {
//           // quá nhiều redirect “fix” -> cứ coi là fail bình thường và đi retry slot tiếp
//           redirectFixCount = 0;
//         } else if (fixPlan.nextUrls?.length) {
//           // thử URL location trong lần slot kế tiếp
//           currentUrl = fixPlan.nextUrls[0];
//         }
//       } else {
//         redirectFixCount = 0;
//       }

//       if (fixPlan.headers) currentHeaders = fixPlan.headers;

//       // chọn next url nếu fixPlan đề xuất (vd 404)
//       if (fixPlan.nextUrls?.length) {
//         // ưu tiên URL khác current nếu có
//         const pick = fixPlan.nextUrls.find((u) => u !== currentUrl) || fixPlan.nextUrls[0];
//         currentUrl = pick;
//       }

//       // delay trước khi retry slot tiếp theo
//       const waitMs = baseDelayMs + (fixPlan.extraDelayMs || 0);
//       if (slot < maxSlots) await sleep(waitMs);
//     } catch (err) {
//       const msg = String(err?.message || err);
//       const code = err?.code || null;

//       lastStatus = ""; // không có HTTP status
//       lastUrl = currentUrl;

//       // xử lí theo error
//       const fixPlan = getFixPlan({
//         status: null,
//         errorCode: code,
//         currentUrl,
//         domain,
//         redirectLocation: null
//       });

//       if (fixPlan.headers) currentHeaders = fixPlan.headers;
//       if (fixPlan.nextUrls?.length) currentUrl = fixPlan.nextUrls[0];

//       const waitMs = baseDelayMs + (fixPlan.extraDelayMs || 0);
//       if (slot < maxSlots) await sleep(waitMs);

//       // vẫn tiếp tục retry cho tới slot cuối
//       if (slot === maxSlots) {
//         // final FAIL
//         statusFinal = "FAIL";
//       }

//       // ghi log ra console để Railway log
//       console.error(`[${getVietnamLogTime()}] [FAIL] domain=${domain} slot=${slot}/${maxSlots} url=${currentUrl} proxy=${proxyUrl ? "YES" : "NO"} err=${msg}`);
//     }
//   }

//   return {
//     Domain: domain,
//     Proxy_IP_PORT_USER_PASS: proxyRaw ?? "",
//     ISP: isp,
//     DNS: dns,
//     Maxtime_try: row.Maxtime_try ?? "",
//     Max_Slot_try: row.Max_Slot_try ?? "",
//     StatusHTTP: lastStatus,
//     StatusFinal: statusFinal,
//     Content: content,
//     TriedCount: tried,
//     LastURL: lastUrl
//   };
// }

// async function main() {
//   console.log(`[${getVietnamLogTime()}] Starting domain check process...`);
  
//   try {
//     const rows = await getInputRows();
//     if (rows.length > 0) {
//       // Log sample 1 row (ẩn proxy password)
//       const sampleRow = { ...rows[0] };
//       if (sampleRow.Proxy_IP_PORT_USER_PASS) {
//         sampleRow.Proxy_IP_PORT_USER_PASS = "***MASKED***";
//       }
    
//       console.log(
//         `[${getVietnamLogTime()}] Sample input row[0]:`,
//         sampleRow
//       );
//     }
//     const output = [];
//     for (let i = 0; i < rows.length; i++) {
//       const row = rows[i];
//       console.log(`[${getVietnamLogTime()}] Processing row ${i + 1}/${rows.length}: ${row.Domain || "N/A"}`);
      
//       const r = await processRow(row);
//       output.push(r);
      
//       // Save to database
//       await saveToDatabase(r);
//     }

//     const sheetName = vnSheetName(getVietnamDate());
//     await postOutput(sheetName, output);

//     console.log(`[${getVietnamLogTime()}] DONE outputSheet=${sheetName} rows=${output.length}`);
//   } catch (error) {
//     console.error(`[${getVietnamLogTime()}] Error in main process:`, error);
//     throw error;
//   }
// }

// // Initialize database and start scheduled job
// async function start() {
//   try {
//     await initDatabase();
//     console.log(`[${getVietnamLogTime()}] Database initialized successfully`);
    
//     // Run immediately on startup (optional - remove if you only want scheduled runs)
//     await main().catch(err => console.error(`[${getVietnamLogTime()}] Startup run failed:`, err));
    
//     // Schedule job to run at specific Vietnam times: 0h, 3h, 6h, 9h, 12h, 15h, 18h, 21h (every 3 hours)
//     // Using node-cron's built-in timezone support
//     console.log(`[${getVietnamLogTime()}] Scheduling job with cron: ${CRON_SCHEDULE} (Vietnam timezone)`);
//     console.log(`[${getVietnamLogTime()}] Will run at: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 (Vietnam time)`);
//     console.log(`[${getVietnamLogTime()}] Current Vietnam time: ${getVietnamLogTime()}`);
    
//     cron.schedule(CRON_SCHEDULE, async () => {
//       console.log(`[${getVietnamLogTime()}] Scheduled job triggered`);
//       try {
//         await main();
//       } catch (error) {
//         console.error(`[${getVietnamLogTime()}] Scheduled job error:`, error);
//         // Don't exit - let it retry on next schedule
//       }
//     }, {
//       timezone: VIETNAM_TIMEZONE
//     });
    
//     console.log(`[${getVietnamLogTime()}] Scheduler started. Waiting for next scheduled run...`);
    
//     // Keep process alive
//     process.on("SIGTERM", async () => {
//       console.log(`[${getVietnamLogTime()}] SIGTERM received, closing database pool...`);
//       await pool.end();
//       process.exit(0);
//     });
    
//     process.on("SIGINT", async () => {
//       console.log(`[${getVietnamLogTime()}] SIGINT received, closing database pool...`);
//       await pool.end();
//       process.exit(0);
//     });
//   } catch (error) {
//     console.error(`[${getVietnamLogTime()}] Error starting application:`, error);
//     process.exit(1);
//   }
// }

// start();


import axios from "axios";
import { HttpsProxyAgent } from "https-proxy-agent";
import pkg from "pg";
import cron from "node-cron";
import * as cheerio from "cheerio";

const { Pool } = pkg;

/**
 * ENV bắt buộc:
 * - GS_API_URL
 * - GS_TOKEN
 * - DATABASE_URL
 *
 * ENV khuyến nghị:
 * - GS_INPUT_SPREADSHEET_ID
 * - GS_OUTPUT_SPREADSHEET_ID
 *
 * Tuỳ chọn:
 * - DEFAULT_MAX_SLOTS (default 3)
 * - DEFAULT_MAXTIME_TRY_SECONDS (default 10)
 * - REQUEST_TIMEOUT_MS (default 15000)
 * - MAX_REDIRECT_FIX (default 3)
 * - CONCURRENCY_LIMIT (default 5)
 * - CRON_SCHEDULE (default: "0 0,3,6,9,12,15,18,21 * * *" theo giờ VN)
 */

const GS_API_URL = process.env.GS_API_URL;
const GS_TOKEN = process.env.GS_TOKEN;
const DATABASE_URL = process.env.DATABASE_URL;

const GS_INPUT_SPREADSHEET_ID = process.env.GS_INPUT_SPREADSHEET_ID;
const GS_OUTPUT_SPREADSHEET_ID = process.env.GS_OUTPUT_SPREADSHEET_ID;

const DEFAULT_MAX_SLOTS = parseInt(process.env.DEFAULT_MAX_SLOTS || "3", 10);
const DEFAULT_MAXTIME_TRY_SECONDS = parseInt(process.env.DEFAULT_MAXTIME_TRY_SECONDS || "10", 10);
const REQUEST_TIMEOUT_MS = parseInt(process.env.REQUEST_TIMEOUT_MS || "15000", 10);
const MAX_REDIRECT_FIX = parseInt(process.env.MAX_REDIRECT_FIX || "3", 10);
const CONCURRENCY_LIMIT = parseInt(process.env.CONCURRENCY_LIMIT || "5", 10);

// Cron chạy 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 theo giờ VN
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || "0 0,3,6,9,12,15,18,21 * * *";

if (!GS_API_URL || !GS_TOKEN) {
  console.error("Missing GS_API_URL or GS_TOKEN env");
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL env");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false
});

const VIETNAM_TIMEZONE = "Asia/Ho_Chi_Minh";

/* -------------------- TIME (VN) -------------------- */
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
  return new Date(`${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:${p.second}+07:00`);
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
  const p = getVietnamDateParts(date);
  return `${p.hour}:${p.minute}_${p.month}/${p.day}/${p.year}`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/* -------------------- INPUT NORMALIZE -------------------- */
/**
 * Sheet input của bạn đang có key có dấu cách:
 * - "Maxtime try"
 * - "Max Slot try"
 * Nên phải đọc linh hoạt.
 */
function getRowValue(row, keys, fallback = "") {
  for (const k of keys) {
    if (row && Object.prototype.hasOwnProperty.call(row, k)) {
      const v = row[k];
      if (v !== null && v !== undefined && String(v).trim() !== "") return v;
    }
  }
  return fallback;
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
 * Ưu tiên HTTP trước (proxy HTTP free thường fail HTTPS CONNECT)
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
  return [...new Set(urls)];
}

/* -------------------- PROXY -------------------- */
function parseProxy(proxyRaw) {
  if (!proxyRaw) return null;
  const s = String(proxyRaw).trim();
  if (!s) return null;

  if (s.startsWith("http://") || s.startsWith("https://")) return s;

  // user:pass@ip:port
  if (s.includes("@") && s.includes(":")) {
    return "http://" + s;
  }

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

/* -------------------- FIX PLAN -------------------- */
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

  // network/timeouts
  if (errorCode === "ETIMEDOUT" || errorCode === "ECONNABORTED") {
    plan.extraDelayMs = 2000;
    plan.headers = browserHeaders;
    plan.nextUrls = [currentUrl];
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

/* -------------------- CONTENT EXTRACT (LESS AGGRESSIVE) -------------------- */
/**
 * Bản extractor trước bạn lọc quá gắt => dễ ra rỗng.
 * Bản này đơn giản, ổn định:
 * - remove script/style/noscript/iframe
 * - lấy text body
 * - gom whitespace
 * - cắt maxWords
 */
function extractTextFromHTML(html, maxWords = 300) {
  try {
    if (!html || typeof html !== "string") return "";
    const $ = cheerio.load(html);

    $("script, style, noscript, iframe, embed, object").remove();

    const bodyText = ($("body").text() || $("html").text() || "").replace(/\s+/g, " ").trim();
    if (!bodyText) return "";

    const words = bodyText.split(/\s+/).filter(Boolean);
    return words.slice(0, maxWords).join(" ");
  } catch {
    return "";
  }
}

/* -------------------- HTTP CHECK -------------------- */
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
    head = res.data.slice(0, 300).replace(/\s+/g, " ");
    try {
      const $ = cheerio.load(res.data);
      title = ($("title").text() || "").trim();
    } catch {}
  }

  console.log(
    `[${getVietnamLogTime()}] [HTTP] url=${url} status=${res.status} ct=${ct} loc=${loc} title="${title}" head="${head}"`
  );

  let content = "";
  let contentLen = 0;

  if (res.status === 200 && typeof res.data === "string") {
    contentLen = res.data.length;
    content = extractTextFromHTML(res.data, 300);
  }

  return {
    status: res.status,
    redirectLocation: loc || null,
    content,
    title,
    contentLen
  };
}

/* -------------------- GOOGLE SHEETS API -------------------- */
async function getInputRows() {
  let url = `${GS_API_URL}?action=input&token=${encodeURIComponent(GS_TOKEN)}`;
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
    headers: ["Domain", "ISP", "DNS", "Update", "StatusHTTP", "StatusFinal", "Title", "ContentLen", "Content"],
    data: data.map((row) => ({
      Domain: row.Domain || "",
      ISP: row.ISP || "",
      DNS: row.DNS || "",
      Update: getVietnamISOString(),
      StatusHTTP: row.StatusHTTP != null ? String(row.StatusHTTP) : "",
      StatusFinal: row.StatusFinal || "FAIL",
      Title: row.Title || "",
      ContentLen: row.ContentLen != null ? String(row.ContentLen) : "0",
      Content: row.Content || ""
    }))
  };

  if (GS_OUTPUT_SPREADSHEET_ID) payload.outputSpreadsheetId = GS_OUTPUT_SPREADSHEET_ID;

  const res = await axios.post(GS_API_URL, payload, { timeout: 60000 });
  if (!res.data?.ok) throw new Error(res.data?.error || "GS output error");
  return res.data;
}

/* -------------------- POSTGRES -------------------- */
async function initDatabase() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS domain_checks (
      id SERIAL PRIMARY KEY,
      domain VARCHAR(255) NOT NULL,
      isp VARCHAR(255),
      dns VARCHAR(255),
      update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      status_http VARCHAR(50),
      status_final VARCHAR(10) NOT NULL,
      title TEXT,
      content_len INTEGER DEFAULT 0,
      content TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_domain_checks_domain ON domain_checks(domain);
    CREATE INDEX IF NOT EXISTS idx_domain_checks_update_time ON domain_checks(update_time);
  `);

  console.log(`[${getVietnamLogTime()}] Database schema initialized`);
}

async function saveToDatabase(result) {
  try {
    const vnDate = getVietnamDate();
    await pool.query(
      `INSERT INTO domain_checks (domain, isp, dns, update_time, status_http, status_final, title, content_len, content)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        result.Domain || "",
        result.ISP || "",
        result.DNS || "",
        vnDate,
        result.StatusHTTP != null ? String(result.StatusHTTP) : "",
        result.StatusFinal || "FAIL",
        result.Title || "",
        parseInt(result.ContentLen || 0, 10) || 0,
        result.Content || ""
      ]
    );
  } catch (error) {
    console.error(`[${getVietnamLogTime()}] Error saving to DB domain=${result.Domain}: ${error?.message || error}`);
  }
}

/* -------------------- CORE PROCESS -------------------- */
function mapInputRow(row) {
  // hỗ trợ cả tên cột có dấu cách và dạng underscore
  const domain = getRowValue(row, ["Domain", "domain"], "");
  const proxy = getRowValue(row, ["Proxy_IP_PORT_USER_PASS", "Proxy", "proxy"], "");
  const isp = getRowValue(row, ["ISP", "isp"], "");
  const dns = getRowValue(row, ["DNS", "dns"], "");

  const maxtimeTry = getRowValue(row, ["Maxtime try", "Maxtime_try", "Maxtime", "maxtime_try"], "");
  const maxSlotTry = getRowValue(row, ["Max Slot try", "Max_Slot_try", "MaxSlot", "max_slot_try"], "");

  return {
    Domain: domain,
    Proxy_IP_PORT_USER_PASS: proxy,
    ISP: isp,
    DNS: dns,
    _MaxtimeTryRaw: maxtimeTry,
    _MaxSlotTryRaw: maxSlotTry
  };
}

function detectNetworkStatus(err) {
  const msg = String(err?.message || err);
  const code = err?.code || "";

  if (
    code === "CERT_HAS_EXPIRED" ||
    code === "UNABLE_TO_VERIFY_LEAF_SIGNATURE" ||
    code === "SELF_SIGNED_CERT_IN_CHAIN" ||
    msg.toLowerCase().includes("certificate")
  ) return "SSL_ERROR";

  if (code === "ETIMEDOUT" || code === "ECONNABORTED" || msg.toLowerCase().includes("timeout")) return "TIMEOUT";
  if (code === "ECONNREFUSED" || code === "ENOTFOUND") return "CONNECTION_ERROR";
  if (code === "ECONNRESET") return "NETWORK_ERROR";
  return "ERROR";
}

async function processRow(rawRow) {
  const row = mapInputRow(rawRow);

  const domain = String(row.Domain || "").trim();
  const proxyUrl = parseProxy(row.Proxy_IP_PORT_USER_PASS);

  const isp = row.ISP || "";
  const dns = row.DNS || "";

  const maxSlots = parseInt(row._MaxSlotTryRaw || DEFAULT_MAX_SLOTS, 10);
  const maxTimeTrySec = parseInt(row._MaxtimeTryRaw || DEFAULT_MAXTIME_TRY_SECONDS, 10);
  const baseDelayMs = maxTimeTrySec * 1000 + 5000;

  const candidates = buildCandidateUrls(domain);
  if (!domain || candidates.length === 0) {
    return {
      Domain: domain,
      ISP: isp,
      DNS: dns,
      StatusHTTP: "INVALID_DOMAIN",
      StatusFinal: "FAIL",
      Title: "",
      ContentLen: 0,
      Content: ""
    };
  }

  let currentUrl = candidates[0];
  let currentHeaders = null;

  let lastStatus = "";
  let lastUrl = currentUrl;
  let statusFinal = "FAIL";

  let content = "";
  let title = "";
  let contentLen = 0;

  let redirectFixCount = 0;

  for (let slot = 1; slot <= maxSlots; slot++) {
    console.log(
      `[${getVietnamLogTime()}] Try slot ${slot}/${maxSlots} domain=${domain} url=${currentUrl} proxy=${proxyUrl ? "YES" : "NO"}`
    );

    try {
      const result = await checkOnce({ url: currentUrl, proxyUrl, headers: currentHeaders });

      lastStatus = String(result.status);
      lastUrl = currentUrl;

      if (result.status === 200) {
        statusFinal = "SUCCESS";
        content = result.content || "";
        title = result.title || "";
        contentLen = result.contentLen || 0;

        console.log(
          `[${getVietnamLogTime()}] [SUCCESS] domain=${domain} status=200 title="${title}" contentLen=${contentLen} extractedWords=${content ? content.split(/\s+/).length : 0}`
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
      if (slot < maxSlots) await sleep(waitMs);
    } catch (err) {
      const code = err?.code || "";
      const msg = String(err?.message || err);

      // luôn có status để output không bị trống
      if (!lastStatus) lastStatus = detectNetworkStatus(err);
      lastUrl = currentUrl;

      const fixPlan = getFixPlan({
        status: null,
        errorCode: code,
        currentUrl,
        domain,
        redirectLocation: null
      });

      if (fixPlan.headers) currentHeaders = fixPlan.headers;
      if (fixPlan.nextUrls?.length) currentUrl = fixPlan.nextUrls[0];

      const waitMs = baseDelayMs + (fixPlan.extraDelayMs || 0);
      if (slot < maxSlots) await sleep(waitMs);

      if (slot === maxSlots) statusFinal = "FAIL";

      console.error(
        `[${getVietnamLogTime()}] [FAIL] domain=${domain} slot=${slot}/${maxSlots} url=${currentUrl} proxy=${proxyUrl ? "YES" : "NO"} err=${msg} lastStatus=${lastStatus}`
      );
    }
  }

  return {
    Domain: domain,
    ISP: isp,
    DNS: dns,
    StatusHTTP: lastStatus || "",
    StatusFinal: statusFinal,
    Title: title || "",
    ContentLen: contentLen || 0,
    Content: content || "",
    LastURL: lastUrl
  };
}

async function processRowsInParallel(rows) {
  const results = [];

  for (let i = 0; i < rows.length; i += CONCURRENCY_LIMIT) {
    const batch = rows.slice(i, i + CONCURRENCY_LIMIT);
    const batchNumber = Math.floor(i / CONCURRENCY_LIMIT) + 1;
    const totalBatches = Math.ceil(rows.length / CONCURRENCY_LIMIT);

    console.log(`[${getVietnamLogTime()}] Processing batch ${batchNumber}/${totalBatches} (${batch.length} rows)`);

    const batchPromises = batch.map(async (row, batchIndex) => {
      const globalIndex = i + batchIndex + 1;
      const d = row?.Domain || row?.domain || "N/A";
      console.log(`[${getVietnamLogTime()}] Processing row ${globalIndex}/${rows.length}: ${d}`);

      try {
        const r = await processRow(row);
        await saveToDatabase(r);
        return r;
      } catch (e) {
        console.error(`[${getVietnamLogTime()}] Error processing row ${globalIndex} (${d}): ${e?.message || e}`);
        const fail = {
          Domain: String(d || ""),
          ISP: row?.ISP || "",
          DNS: row?.DNS || "",
          StatusHTTP: "ERROR",
          StatusFinal: "FAIL",
          Title: "",
          ContentLen: 0,
          Content: ""
        };
        await saveToDatabase(fail);
        return fail;
      }
    });

    const batchResults = await Promise.all(batchPromises);
    results.push(...batchResults);

    console.log(`[${getVietnamLogTime()}] Completed batch ${batchNumber}/${totalBatches}`);
  }

  return results;
}

async function main() {
  console.log(`[${getVietnamLogTime()}] Starting domain check process...`);
  console.log(`[${getVietnamLogTime()}] Concurrency limit: ${CONCURRENCY_LIMIT} rows at a time`);

  const rows = await getInputRows();
  console.log(`[${getVietnamLogTime()}] Retrieved ${rows.length} rows from Google Sheets`);

  if (rows.length > 0) {
    const sample = { ...rows[0] };
    if (sample.Proxy_IP_PORT_USER_PASS) sample.Proxy_IP_PORT_USER_PASS = "***MASKED***";
    console.log(`[${getVietnamLogTime()}] Sample input row[0]:`, sample);
  }

  const output = await processRowsInParallel(rows);

  const sheetName = vnSheetName(getVietnamDate());
  await postOutput(sheetName, output);

  console.log(`[${getVietnamLogTime()}] DONE outputSheet=${sheetName} rows=${output.length}`);
}

/* -------------------- START -------------------- */
async function start() {
  try {
    await initDatabase();
    console.log(`[${getVietnamLogTime()}] Database initialized successfully`);

    // chạy ngay 1 lần khi start (để test)
    await main().catch((err) =>
      console.error(`[${getVietnamLogTime()}] Startup run failed: ${err?.message || err}`)
    );

    console.log(`[${getVietnamLogTime()}] Scheduling job with cron: ${CRON_SCHEDULE} (Vietnam timezone)`);
    console.log(
      `[${getVietnamLogTime()}] Will run at: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 (VN)`
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

