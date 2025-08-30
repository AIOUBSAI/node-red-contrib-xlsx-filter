const path = require("path");

module.exports = function(RED) {
  function XlsxFilterNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // INPUT
    node.inputPath = config.inputPath || "data";
    node.inputPathType = config.inputPathType || "msg"; // msg|flow|global
    node.includeSheetRegex = config.includeSheetRegex || "";
    node.excludeSheetRegex = config.excludeSheetRegex || "";

    // FILTERS (sheet-scoped)
    node.filterLogic = config.filterLogic || "AND";
    node.rules = Array.isArray(config.rules) ? config.rules : []; // [{sheetScope,sheetScopeType,col,colType,op,rhsType,rhs,caseSensitive,coerce}]

    // SELECT (keep/drop) — sheet-scoped, dynamic column names (string or array)
    node.selectMode  = config.selectMode || "none"; // none|keep|drop
    node.selectList  = Array.isArray(config.selectList) ? config.selectList : []; // [{sheetScope,sheetScopeType,col,colType}]

    // RENAME (sheet-scoped) — supports scalar or arrays
    node.renameList  = Array.isArray(config.renameList) ? config.renameList : []; // [{sheetScope,sheetScopeType,from,fromType,to,toType}]

    // CONDITIONAL RENAME (message-level condition + sheet-scoped list) — supports arrays
    node.conditionalRenameEnabled     = !!config.conditionalRenameEnabled;
    node.conditionalRenameWhenLhsType = config.conditionalRenameWhenLhsType || "msg";
    node.conditionalRenameWhenLhs     = config.conditionalRenameWhenLhs || "";
    node.conditionalRenameOp          = config.conditionalRenameOp || "==";
    node.conditionalRenameRhsType     = config.conditionalRenameRhsType || "str";
    node.conditionalRenameRhs         = config.conditionalRenameRhs || "";
    node.conditionalRenameList        = Array.isArray(config.conditionalRenameList) ? config.conditionalRenameList : [];

    // DERIVE
    node.deriveList = Array.isArray(config.deriveList) ? config.deriveList : [];

    // OUTPUT
    node.outputTargetType = config.outputTargetType || "msg";
    node.outputTargetPath = config.outputTargetPath || "filtered";
    node.structure        = config.structure || "hierarchical"; // hierarchical|flat
    node.includeSummary   = config.hasOwnProperty("includeSummary") ? !!config.includeSummary : true;

    node.on("input", async function(msg, send, done) {
      try {
        node.status({ fill: "blue", shape: "dot", text: "processing..." });

        // Resolve input object at inputPath
        const inputRoot = getRootContainer(node, msg, node.inputPathType);
        const inputData = inputRoot ? deepGet(inputRoot, node.inputPath) : undefined;

        // Expect { data: { file: { sheet: rows[] } }, ... }
        if (!inputData || typeof inputData !== "object" || !inputData.data) {
          throw new Error("Input missing or invalid. Expect an object with a 'data' map under the configured path.");
        }

        // Build regexes (sheet only)
        const incSheet = safeRegex(node.includeSheetRegex);
        const excSheet = safeRegex(node.excludeSheetRegex);

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

            // 1) Row filters (async per row)
            const filtered = [];
            for (const row of rows) {
              if (await rowPasses(RED, node, row, msg, sheetName)) filtered.push(row);
            }

            // 2) Transforms
            //    IMPORTANT: Select (keep/drop) FIRST — before any renames — so dynamic column names
            //    match the original headers present in the sheet.
            let transformed = filtered.map(r => ({ ...r }));

            // 2a) Select keep/drop (sheet-scoped, dynamic column names)
            if (node.selectMode !== "none" && Array.isArray(node.selectList) && node.selectList.length) {
              const colSet = await buildScopedColumnSet(RED, node, msg, sheetName, node.selectList);
              if (node.selectMode === "keep" && colSet.size) {
                transformed = transformed.map(r => pickSet(r, colSet));
              } else if (node.selectMode === "drop" && colSet.size) {
                transformed = transformed.map(r => omitSet(r, colSet));
              }
            }

            // 2b) Static rename (sheet-scoped)
            if (Array.isArray(node.renameList) && node.renameList.length) {
              const renamed = [];
              for (const r of transformed) {
                renamed.push(await renameWithList(RED, node, r, msg, sheetName, node.renameList));
              }
              transformed = renamed;
            }

            // 2c) Conditional rename (sheet-scoped, message-level condition)
            if (node.conditionalRenameEnabled && await conditionTrue(RED, node, msg)) {
              const cRenamed = [];
              for (const r of transformed) {
                cRenamed.push(await renameWithList(RED, node, r, msg, sheetName, node.conditionalRenameList));
              }
              transformed = cRenamed;
            }

            // 2d) Derive columns (JSONata)
            if (Array.isArray(node.deriveList) && node.deriveList.length) {
              const derived = [];
              for (const r of transformed) {
                const out = { ...r };
                for (const d of node.deriveList) {
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

            if (node.structure === "hierarchical") {
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

        const outObj = (node.structure === "hierarchical")
          ? { data: resultMap }
          : { data: resultMap.__flat || [] };

        if (node.includeSummary) {
          outObj.summary = { fileCount, sheetCount, rowIn, rowOut, filteredRatio: rowIn ? (rowOut/rowIn) : null };
          outObj.rules   = { logic: node.filterLogic, count: Array.isArray(node.rules) ? node.rules.length : 0 };
        }

        setOutput(RED, node, msg, outObj, node.outputTargetType, node.outputTargetPath);

        node.status({ fill: "green", shape: "dot", text: `${rowOut}/${rowIn} rows` });
        send(msg);
        if (done) done();
      } catch (err) {
        node.status({ fill: "red", shape: "ring", text: String(err.message || err) });
        node.error(err, msg);
        if (done) done(err);
      }
    });
  }

  RED.nodes.registerType("xlsx-filter", XlsxFilterNode);

  // ---------- helpers (async-capable where needed) ----------

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
    for (let i=0; i<parts.length-1; i++) {
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

  // Ensure value is an array (undefined/null -> [], scalar -> [scalar])
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
  async function ruleAppliesTo(RED, node, msg, r, sheet, rowCtx) {
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

  // Resolve column name and RHS (async where needed)
  async function resolveColumnName(RED, node, msg, r, rowCtx, sheet) {
    return await resolveDynamic(RED, node, msg, r.col, r.colType, rowCtx, { sheet });
  }
  async function resolveRHS(RED, node, msg, r, rowCtx, sheet) {
    return await resolveDynamic(RED, node, msg, r.rhs, r.rhsType, rowCtx, { sheet });
  }

  // Row filter evaluation (async) — NOW SUPPORTS MULTIPLE COLUMNS PER RULE
  async function rowPasses(RED, node, row, msg, sheet) {
    if (!Array.isArray(node.rules) || node.rules.length === 0) return true;

    const evalRule = async (r) => {
      if (!(await ruleAppliesTo(RED, node, msg, r, sheet, row))) return true;

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

    if (node.filterLogic === "OR") {
      for (const r of node.rules) {
        if (await evalRule(r)) return true;
      }
      return false;
    } else {
      for (const r of node.rules) {
        if (!(await evalRule(r))) return false;
      }
      return true;
    }
  }

  // Build set of columns for current sheet based on selectList (async for jsonata, supports arrays)
  async function buildScopedColumnSet(RED, node, msg, sheet, selectList) {
    const set = new Set();
    for (const it of (selectList || [])) {
      if (!(await ruleAppliesTo(RED, node, msg, it, sheet, null))) continue;
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
  async function renameWithList(RED, node, row, msg, sheet, list) {
    let out = { ...row };
    for (const it of (list || [])) {
      if (!(await ruleAppliesTo(RED, node, msg, it, sheet, row))) continue;

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
      } else if (fromArr.length && !toArr.length) {
        // Only from[] given → no-op
        continue;
      } else if (!fromArr.length && toArr.length) {
        // Only to[] given → no-op
        continue;
      }
    }
    return out;
  }

  // Conditional (message-level) — async
  async function conditionTrue(RED, node, msg) {
    try {
      const lhsVal = await resolveDynamic(RED, node, msg, node.conditionalRenameWhenLhs, node.conditionalRenameWhenLhsType);
      const rhsVal = await resolveDynamic(RED, node, msg, node.conditionalRenameRhs, node.conditionalRenameRhsType);
      const L = coerceVal(lhsVal);
      const R = coerceVal(rhsVal);
      switch (node.conditionalRenameOp) {
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
