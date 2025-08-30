const fs = require("fs");
const path = require("path");

// --------- small utils shared by endpoints & runtime ----------
function nowISOString() {
  return new Date().toISOString();
}

function deepClone(obj) {
  return JSON.parse(JSON.stringify(obj || {}));
}

function withDefaults(schema = {}) {
  // Ensure a complete schema shape with defaults
  return {
    inputPath: schema.inputPath ?? "data",
    inputPathType: schema.inputPathType ?? "msg",

    includeSheetRegex: schema.includeSheetRegex ?? "",
    excludeSheetRegex: schema.excludeSheetRegex ?? "",

    filterLogic: schema.filterLogic ?? "AND",
    rules: Array.isArray(schema.rules) ? schema.rules : [],

    selectMode: schema.selectMode ?? "none",
    selectList: Array.isArray(schema.selectList) ? schema.selectList : [],

    renameList: Array.isArray(schema.renameList) ? schema.renameList : [],

    conditionalRename: {
      enabled: schema.conditionalRename?.enabled ?? false,
      whenLhsType: schema.conditionalRename?.whenLhsType ?? "msg",
      whenLhs: schema.conditionalRename?.whenLhs ?? "",
      op: schema.conditionalRename?.op ?? "==",
      rhsType: schema.conditionalRename?.rhsType ?? "str",
      rhs: schema.conditionalRename?.rhs ?? "",
      list: Array.isArray(schema.conditionalRename?.list) ? schema.conditionalRename.list : []
    },

    deriveList: Array.isArray(schema.deriveList) ? schema.deriveList : [],

    output: {
      targetType: schema.output?.targetType ?? "msg",
      targetPath: schema.output?.targetPath ?? "filtered",
      structure: schema.output?.structure ?? "hierarchical",
      includeSummary: schema.output?.includeSummary ?? true
    }
  };
}

function makeTemplateJSON() {
  return {
    version: 1,
    updatedAt: nowISOString(),
    schema: withDefaults({})
  };
}

module.exports = function(RED) {

  // -------------------- Admin HTTP endpoints --------------------
  // Path handling: relative paths resolved under userDir; no traversal outside; .json only
  function resolveSafePath(rawPath) {
    if (!rawPath || typeof rawPath !== "string") {
      const err = new Error("Invalid config file path.");
      err.status = 400; throw err;
    }
    const userDir = RED.settings.userDir || process.cwd();
    const normalized = path.normalize(rawPath);

    const abs = path.isAbsolute(normalized)
      ? normalized
      : path.join(userDir, normalized);

    const rel = path.relative(userDir, abs);
    if (rel.startsWith("..") || path.isAbsolute(rel)) {
      const err = new Error("Path is outside userDir.");
      err.status = 400; throw err;
    }
    if (path.extname(abs).toLowerCase() !== ".json") {
      const err = new Error("Config file must have .json extension.");
      err.status = 400; throw err;
    }
    return abs;
  }

  // GET: read config JSON
  RED.httpAdmin.get("/xlsx-filter/config", async function(req, res) {
    try {
      const p = resolveSafePath(req.query.path);
      if (!fs.existsSync(p)) {
        return res.status(404).json({ missing: true, message: "Config file not found." });
      }
      const txt = fs.readFileSync(p, "utf8");
      const json = JSON.parse(txt);
      return res.json(json);
    } catch (e) {
      const status = e.status || 500;
      return res.status(status).json({ error: String(e.message || e) });
    }
  });

  // POST: write config JSON (pretty)
  RED.httpAdmin.post("/xlsx-filter/config", async function(req, res) {
    try {
      const body = req.body || {};
      const p = resolveSafePath(body.path);
      const data = body.data;
      if (!data || typeof data !== "object") {
        const err = new Error("Missing or invalid data.");
        err.status = 400; throw err;
      }
      const toWrite = deepClone(data);
      // stamp updatedAt if not present
      if (!toWrite.updatedAt) toWrite.updatedAt = nowISOString();
      fs.mkdirSync(path.dirname(p), { recursive: true });
      fs.writeFileSync(p, JSON.stringify(toWrite, null, 2), "utf8");
      return res.json({ ok: true, path: p });
    } catch (e) {
      const status = e.status || 500;
      return res.status(status).json({ error: String(e.message || e) });
    }
  });

  // POST: create template if missing
  RED.httpAdmin.post("/xlsx-filter/config/template", async function(req, res) {
    try {
      const body = req.body || {};
      const p = resolveSafePath(body.path);
      if (fs.existsSync(p)) {
        return res.status(409).json({ exists: true, message: "File already exists." });
      }
      fs.mkdirSync(path.dirname(p), { recursive: true });
      const tmpl = makeTemplateJSON();
      fs.writeFileSync(p, JSON.stringify(tmpl, null, 2), "utf8");
      return res.json({ ok: true, path: p, created: true, data: tmpl });
    } catch (e) {
      const status = e.status || 500;
      return res.status(status).json({ error: String(e.message || e) });
    }
  });

  // -------------------- Node implementation --------------------
  function XlsxFilterNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // New config-file options
    node.useConfigFile   = !!config.useConfigFile;
    node.configFilePath  = config.configFilePath || "";
    node.lockToFile      = !!config.lockToFile;
    node.watchConfigFile = !!config.watchConfigFile;

    node._watcher = null;
    node.rt = null; // runtime schema (withDefaults), used by processing

    // Build runtime schema from embedded editor config (fallback/default)
    function embeddedToSchemaObj() {
      return withDefaults({
        inputPath: config.inputPath || "data",
        inputPathType: config.inputPathType || "msg",

        includeSheetRegex: config.includeSheetRegex || "",
        excludeSheetRegex: config.excludeSheetRegex || "",

        filterLogic: config.filterLogic || "AND",
        rules: Array.isArray(config.rules) ? config.rules : [],

        selectMode: config.selectMode || "none",
        selectList: Array.isArray(config.selectList) ? config.selectList : [],

        renameList: Array.isArray(config.renameList) ? config.renameList : [],

        conditionalRename: {
          enabled: !!config.conditionalRenameEnabled,
          whenLhsType: config.conditionalRenameWhenLhsType || "msg",
          whenLhs: config.conditionalRenameWhenLhs || "",
          op: config.conditionalRenameOp || "==",
          rhsType: config.conditionalRenameRhsType || "str",
          rhs: config.conditionalRenameRhs || "",
          list: Array.isArray(config.conditionalRenameList) ? config.conditionalRenameList : []
        },

        deriveList: Array.isArray(config.deriveList) ? config.deriveList : [],

        output: {
          targetType: config.outputTargetType || "msg",
          targetPath: config.outputTargetPath || "filtered",
          structure: config.structure || "hierarchical",
          includeSummary: config.hasOwnProperty("includeSummary") ? !!config.includeSummary : true
        }
      });
    }

    function setRuntimeSchemaFromEmbedded() {
      node.rt = embeddedToSchemaObj();
    }

    function setRuntimeSchemaFromFile(fileObj) {
      // fileObj is the outer {version, updatedAt, schema}
      const safe = withDefaults(fileObj?.schema || {});
      node.rt = safe;
    }

    function loadFileToRuntime(showStatusOnError = true) {
      try {
        const p = resolveSafePath(node.configFilePath);
        if (!fs.existsSync(p)) {
          throw new Error("Config file not found.");
        }
        const txt = fs.readFileSync(p, "utf8");
        const parsed = JSON.parse(txt);
        setRuntimeSchemaFromFile(parsed);
        node.status({ fill: "blue", shape: "dot", text: "config loaded" });
        return true;
      } catch (e) {
        if (showStatusOnError) {
          node.status({ fill: "red", shape: "ring", text: `config load failed: ${String(e.message || e)}` });
        }
        return false;
      }
    }

    function startWatcher() {
      if (!node.watchConfigFile || !node.configFilePath) return;
      try {
        const p = resolveSafePath(node.configFilePath);
        if (!fs.existsSync(p)) return;
        // clear previous
        if (node._watcher) {
          fs.unwatchFile(p, node._watcher);
          node._watcher = null;
        }
        let timer = null;
        const onChange = () => {
          clearTimeout(timer);
          timer = setTimeout(() => {
            if (loadFileToRuntime(false)) {
              node.status({ fill: "blue", shape: "dot", text: "config reloaded" });
            }
          }, 250);
        };
        fs.watchFile(p, { interval: 750 }, onChange);
        node._watcher = onChange;
      } catch (e) {
        // ignore watcher errors
      }
    }

    // Initialize runtime schema
    if (node.useConfigFile && node.lockToFile && node.configFilePath) {
      if (!loadFileToRuntime(true)) {
        // fallback to embedded config
        setRuntimeSchemaFromEmbedded();
      }
      startWatcher();
    } else {
      setRuntimeSchemaFromEmbedded();
    }

    // --------------- message processing ---------------
    node.on("input", async function(msg, send, done) {
      const rt = node.rt || embeddedToSchemaObj();
      try {
        node.status({ fill: "blue", shape: "dot", text: "processing..." });

        // Resolve input object at inputPath
        const inputRoot = getRootContainer(node, msg, rt.inputPathType);
        const inputData = inputRoot ? deepGet(inputRoot, rt.inputPath) : undefined;

        // Expect { data: { file: { sheet: rows[] } }, ... }
        if (!inputData || typeof inputData !== "object" || !inputData.data) {
          throw new Error("Input missing or invalid. Expect an object with a 'data' map under the configured path.");
        }

        // Build regexes (sheet only)
        const incSheet = safeRegex(rt.includeSheetRegex);
        const excSheet = safeRegex(rt.excludeSheetRegex);

        const resultMap = {};
        let fileCount = 0, sheetCount = 0, rowIn = 0, rowOut = 0;

        for (const [file, sheets] of Object.entries(inputData.data)) {
          // Skip Office temp lock files (~$...)
          const base = path.basename(file || "");
          if (base.startsWith("~$")) continue;

          fileCount++;

          for (const [sheetName, rows] of Object.entries(sheets || {})) {
            if (!passNameFilters(sheetName, incSheet, excSheet)) continue;
            sheetCount++;

            if (!Array.isArray(rows)) continue;
            rowIn += rows.length;

            // 1) Row filters (async per row) — supports multiple columns in a single rule
            const filtered = [];
            for (const row of rows) {
              if (await rowPasses(RED, rt, node, row, msg, sheetName)) filtered.push(row);
            }

            // 2) Transforms
            //    IMPORTANT: Select (keep/drop) FIRST — before any renames — so dynamic column names
            //    match the original headers present in the sheet.
            let transformed = filtered.map(r => ({ ...r }));

            // 2a) Select keep/drop (sheet-scoped, dynamic column names; array-aware)
            if (rt.selectMode !== "none" && Array.isArray(rt.selectList) && rt.selectList.length) {
              const colSet = await buildScopedColumnSet(RED, rt, node, msg, sheetName, rt.selectList);
              if (rt.selectMode === "keep" && colSet.size) {
                transformed = transformed.map(r => pickSet(r, colSet));
              } else if (rt.selectMode === "drop" && colSet.size) {
                transformed = transformed.map(r => omitSet(r, colSet));
              }
            }

            // 2b) Static rename (sheet-scoped; arrays supported)
            if (Array.isArray(rt.renameList) && rt.renameList.length) {
              const renamed = [];
              for (const r of transformed) {
                renamed.push(await renameWithList(RED, rt, node, r, msg, sheetName, rt.renameList));
              }
              transformed = renamed;
            }

            // 2c) Conditional rename (sheet-scoped, message-level condition)
            if (rt.conditionalRename.enabled && await conditionTrue(RED, rt, node, msg)) {
              const cRenamed = [];
              for (const r of transformed) {
                cRenamed.push(await renameWithList(RED, rt, node, r, msg, sheetName, rt.conditionalRename.list));
              }
              transformed = cRenamed;
            }

            // 2d) Derive columns (JSONata)
            if (Array.isArray(rt.deriveList) && rt.deriveList.length) {
              const derived = [];
              for (const r of transformed) {
                const out = { ...r };
                for (const d of rt.deriveList) {
                  if (!d || !d.col) continue;
                  if (d.exprType === "jsonata") {
                    try {
                      const val = await evalJSONata(RED, node, sanitizeExpr(d.expr || ""), { ...msg, row: r, sheet: sheetName });
                      out[d.col] = val;
                    } catch (e) {
                      // ignore on error
                    }
                  }
                }
                derived.push(out);
              }
              transformed = derived;
            }

            rowOut += transformed.length;

            if (rt.output.structure === "hierarchical") {
              resultMap[file] = resultMap[file] || {};
              resultMap[file][sheetName] = transformed;
            } else {
              resultMap.__flat = resultMap.__flat || [];
              for (const rr of transformed) {
                resultMap.__flat.push({ _file: file, _sheet: sheetName, ...rr });
              }
            }
          }
        }

        const outObj = (rt.output.structure === "hierarchical")
          ? { data: resultMap }
          : { data: resultMap.__flat || [] };

        if (rt.output.includeSummary) {
          outObj.summary = { fileCount, sheetCount, rowIn, rowOut, filteredRatio: rowIn ? (rowOut/rowIn) : null };
          outObj.rules   = { logic: rt.filterLogic, count: Array.isArray(rt.rules) ? rt.rules.length : 0 };
        }

        setOutput(RED, node, msg, outObj, rt.output.targetType, rt.output.targetPath);

        node.status({ fill: "green", shape: "dot", text: `${rowOut}/${rowIn} rows` });
        send(msg);
        if (done) done();
      } catch (err) {
        node.status({ fill: "red", shape: "ring", text: String(err.message || err) });
        node.error(err, msg);
        if (done) done(err);
      }
    });

    node.on("close", function() {
      try {
        if (node._watcher && node.configFilePath) {
          const p = resolveSafePath(node.configFilePath);
          fs.unwatchFile(p, node._watcher);
        }
      } catch (e) {}
      node._watcher = null;
    });
  }

  RED.nodes.registerType("xlsx-filter", XlsxFilterNode);

  // ---------------- helpers used by runtime processing ----------------

  function sanitizeExpr(src) {
    // remove zero-width chars + the common right-arrow from copy/paste
    return String(src || "").replace(/[\u200B-\u200D\uFEFF\u2192]/g, "").trim();
  }

  function safeRegex(rx) { try { return rx ? new RegExp(rx) : null; } catch { return null; } }
  function passNameFilters(name, inc, exc) { if (inc && !inc.test(name)) return false; if (exc && exc.test(name)) return false; return true; }

  function getRootContainer(node, msg, scope) {
    if (scope === "msg") return msg;
    if (scope === "flow") return node.context().flow;
    if (scope === "global") return node.context().global;
    return msg; // default
  }

  function deepGet(root, pathStr) {
    if (!root || !pathStr) return undefined;
    if (root.get && root.set) return root.get(pathStr); // flow/global context
    const parts = String(pathStr).split(".").filter(Boolean);
    let cur = root;
    for (const p of parts) {
      if (cur == null) return undefined;
      cur = cur[p];
    }
    return cur;
  }

  function setOutput(RED, node, msg, value, scope, pathStr) {
    if (scope === "msg") {
      RED.util.setMessageProperty(msg, pathStr, value, true);
      return;
    }
    const ctx = node.context()[scope]; // flow/global
    if (!ctx || typeof ctx.get !== "function" || typeof ctx.set !== "function") {
      RED.util.setMessageProperty(msg, pathStr, value, true);
      return;
    }
    if (!pathStr || !pathStr.trim()) {
      ctx.set("xlsxFilter", value);
      return;
    }
    const parts = pathStr.split(".").filter(Boolean);
    const rootKey = parts.shift();
    if (!rootKey) { ctx.set("xlsxFilter", value); return; }
    if (parts.length === 0) { ctx.set(rootKey, value); return; }
    let rootObj = ctx.get(rootKey);
    if (typeof rootObj !== "object" || rootObj === null) rootObj = {};
    let cur = rootObj;
    for (let i=0;i<parts.length-1;i++) {
      const k = parts[i];
      if (typeof cur[k] !== "object" || cur[k] === null) cur[k] = {};
      cur = cur[k];
    }
    cur[parts[parts.length-1]] = value;
    ctx.set(rootKey, rootObj);
  }

  // Promise wrapper for Node-RED's callback-style JSONata evaluation
  async function evalJSONata(RED, node, src, contextObj) {
    const expr = RED.util.prepareJSONataExpression(src || "", node);
    return await new Promise((resolve, reject) => {
      try {
        RED.util.evaluateJSONataExpression(expr, contextObj, (err, val) => {
          if (err) return reject(err);
          resolve(val);
        });
      } catch (e) {
        reject(e);
      }
    });
  }

  // Ensure array (undefined/null -> [], scalar -> [scalar], array -> same)
  function ensureArray(v) {
    if (Array.isArray(v)) return v;
    if (v === null || v === undefined) return [];
    return [v];
  }

  // Dynamic resolver (async when jsonata)
  async function resolveDynamic(RED, node, msg, val, typ, rowCtx, extras = {}) {
    switch (typ || "str") {
      case "str":    return val;
      case "msg":    return RED.util.getMessageProperty(msg, val);
      case "flow":   return node.context().flow.get(val);
      case "global": return node.context().global.get(val);
      case "env":    return process.env[val];
      case "jsonata":
        try {
          const out = await evalJSONata(RED, node, sanitizeExpr(val), { ...msg, row: rowCtx, ...extras });
          return out;
        } catch (e) {
          node.status({ fill: "red", shape: "ring", text: `JSONata ERR: ${String(e.message || e)}` });
          return undefined;
        }
      default:       return val;
    }
  }

  function coerceVal(v) {
    if (typeof v === "string") {
      const t = v.trim();
      if (t === "true") return true;
      if (t === "false") return false;
      if (t !== "" && !isNaN(t)) return Number(t);
    }
    return v;
  }
  function isEmpty(v) { return v == null || (typeof v === "string" && v.trim() === ""); }

  // Rules sheet-scope check (can contain jsonata sheet scope)
  async function ruleAppliesTo(RED, rt, node, msg, r, sheet, rowCtx) {
    if (r.sheetScope) {
      const t = r.sheetScopeType || "str";
      if (t === "str") {
        if (sheet !== r.sheetScope) return false;
      } else if (t === "regex") {
        try { if (!new RegExp(String(r.sheetScope)).test(sheet)) return false; } catch { return false; }
      } else if (t === "jsonata") {
        try {
          const ok = await evalJSONata(RED, node, sanitizeExpr(r.sheetScope), { ...msg, sheet, row: rowCtx });
          if (!ok) return false;
        } catch { return false; }
      }
    }
    return true;
  }

  // Resolve column name(s) and RHS (async where needed)
  async function resolveColumnName(RED, node, msg, r, rowCtx, sheet) {
    return await resolveDynamic(RED, node, msg, r.col, r.colType, rowCtx, { sheet });
  }
  async function resolveRHS(RED, node, msg, r, rowCtx, sheet) {
    return await resolveDynamic(RED, node, msg, r.rhs, r.rhsType, rowCtx, { sheet });
  }

  // Row filter evaluation (async) — supports multiple columns per rule
  async function rowPasses(RED, rt, node, row, msg, sheet) {
    if (!Array.isArray(rt.rules) || rt.rules.length === 0) return true;

    const evalRule = async (r) => {
      if (!(await ruleAppliesTo(RED, rt, node, msg, r, sheet, row))) return true;

      // JSONata rule (per-row expression)
      if (r.op === "jsonata") {
        try {
          const ok = await evalJSONata(RED, node, sanitizeExpr(r.rhs || ""), { ...msg, row, sheet });
          return !!ok;
        } catch {
          return false;
        }
      }

      // Resolve possibly multiple columns for this rule
      const colResolved = await resolveColumnName(RED, node, msg, r, row, sheet);
      const colNames = ensureArray(colResolved).filter(c => c !== undefined && c !== null).map(String);

      // No columns resolved -> treat as non-match (rule false)
      if (colNames.length === 0) return false;

      // Resolve RHS once
      const rval = await resolveRHS(RED, node, msg, r, row, sheet);
      const Rraw = r.coerce === false ? rval : coerceVal(rval);
      const caseSensitive = !!r.caseSensitive;

      // Helper to compare one L against R according to op
      const compare = (L) => {
        const Lc  = r.coerce === false ? L : coerceVal(L);
        const Ls  = (typeof Lc === "string" && !caseSensitive) ? Lc.toLowerCase() : Lc;
        const Rs  = (typeof Rraw === "string" && !caseSensitive) ? Rraw.toLowerCase() : Rraw;

        switch (r.op) {
          case "==": return Ls == Rs;
          case "!=": return Ls != Rs;
          case "<":  return Number(Lc) <  Number(Rraw);
          case "<=": return Number(Lc) <= Number(Rraw);
          case ">":  return Number(Lc) >  Number(Rraw);
          case ">=": return Number(Lc) >= Number(Rraw);
          case "contains":
            if (typeof Ls !== "string" || typeof Rs !== "string") return false;
            return Ls.includes(Rs);
          case "!contains":
            if (typeof Ls !== "string" || typeof Rs !== "string") return true;
            return !Ls.includes(Rs);
          case "regex":
            try { return new RegExp(String(Rraw)).test(String(Lc)); } catch { return false; }
          case "isEmpty":  return isEmpty(Lc);
          case "!isEmpty": return !isEmpty(Lc);
          default: return false;
        }
      };

      // Rule passes if ANY of the listed columns satisfies the comparator
      for (const cn of colNames) {
        if (!Object.prototype.hasOwnProperty.call(row, cn)) continue;
        if (compare(row[cn])) return true;
      }
      return false;
    };

    if (rt.filterLogic === "OR") {
      for (const r of rt.rules) {
        if (await evalRule(r)) return true;
      }
      return false;
    } else {
      for (const r of rt.rules) {
        if (!(await evalRule(r))) return false;
      }
      return true;
    }
  }

  // Build set of columns for current sheet based on selectList (async for jsonata, supports arrays)
  async function buildScopedColumnSet(RED, rt, node, msg, sheet, selectList) {
    const set = new Set();
    for (const it of (selectList || [])) {
      if (!(await ruleAppliesTo(RED, rt, node, msg, it, sheet, null))) continue;
      const resolved = await resolveDynamic(RED, node, msg, it.col, it.colType, null, { sheet });
      const cols = ensureArray(resolved).map(c => String(c));
      for (const col of cols) if (col) set.add(col);
    }
    return set;
  }

  // Tolerant keep/drop (exact, else trim+case-insensitive)
  function pickSet(row, set) {
    const out = {};
    const keys = Object.keys(row);
    for (const wantRaw of set) {
      const want = String(wantRaw);
      let k = keys.find(x => x === want);
      if (!k) {
        const wantN = want.trim().toLowerCase();
        k = keys.find(x => x.trim().toLowerCase() === wantN);
      }
      if (k) out[k] = row[k];
    }
    return out;
  }

  function omitSet(row, set) {
    const out = {};
    const keys = Object.keys(row);
    const dropExact = new Set(Array.from(set, s => String(s)));
    const dropNorm  = new Set(Array.from(set, s => String(s).trim().toLowerCase()));
    for (const k of keys) {
      if (dropExact.has(k)) continue;
      if (dropNorm.has(k.trim().toLowerCase())) continue;
      out[k] = row[k];
    }
    return out;
  }

  // RENAME using list entries (sheet-scoped + dynamic from/to) — supports arrays
  async function renameWithList(RED, rt, node, row, msg, sheet, list) {
    let out = { ...row };
    for (const it of (list || [])) {
      if (!(await ruleAppliesTo(RED, rt, node, msg, it, sheet, row))) continue;

      const fromRes = await resolveDynamic(RED, node, msg, it.from, it.fromType, row, { sheet });
      const toRes   = await resolveDynamic(RED, node, msg, it.to,   it.toType,   row, { sheet });

      const fromArr = ensureArray(fromRes).map(s => String(s));
      const toArr   = ensureArray(toRes).map(s => String(s));

      // Pairwise mapping
      if (fromArr.length && toArr.length) {
        const n = Math.max(fromArr.length, toArr.length);
        for (let i = 0; i < n; i++) {
          const from = fromArr[Math.min(i, fromArr.length - 1)];
          const to   = toArr[Math.min(i, toArr.length - 1)];
          if (!from || !to || from === to) continue;
          if (Object.prototype.hasOwnProperty.call(out, from)) {
            out[to] = out[from];
            delete out[from];
          }
        }
      }
    }
    return out;
  }

  // Conditional (message-level) — async
  async function conditionTrue(RED, rt, node, msg) {
    try {
      const lhsVal = await resolveDynamic(RED, node, msg, rt.conditionalRename.whenLhs, rt.conditionalRename.whenLhsType);
      const rhsVal = await resolveDynamic(RED, node, msg, rt.conditionalRename.rhs, rt.conditionalRename.rhsType);
      const L = coerceVal(lhsVal);
      const R = coerceVal(rhsVal);
      switch (rt.conditionalRename.op) {
        case "==": return L == R;
        case "!=": return L != R;
        case "contains":   return (typeof L === "string" && typeof R === "string") ? L.includes(R) : false;
        case "!contains":  return (typeof L === "string" && typeof R === "string") ? !L.includes(R) : true;
        case "regex":      try { return new RegExp(String(R)).test(String(L)); } catch { return false; }
        case "isEmpty":    return isEmpty(L);
        case "!isEmpty":   return !isEmpty(L);
        default:           return false;
      }
    } catch { return false; }
  }
};
