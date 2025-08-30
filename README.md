# node-red-contrib-xlsx-filter

Filter and transform tabular data produced by an XLSX reader (e.g. `node-red-contrib-xlsx-reader`).  
Supports powerful **row filters**, **select/keep/drop columns**, **static & conditional renames**, **derived columns (JSONata)**, and an optional **config file mode** (load/save your whole schema to JSON on disk, lock & watch).

## Features

- **Row filters** per sheet  
  - Column can be a **string** or a **JSONata expression** that returns a **single name** or an **array of names**.  
  - Operators: `==`, `!=`, `<`, `<=`, `>`, `>=`, `contains`, `!contains`, `regex`, `isEmpty`, `!isEmpty`, or `JSONata` (boolean expr).  
  - A rule passes if **any** of its listed columns satisfies the comparator.
- **Select columns** per sheet (keep or drop)  
  - Column cell accepts **string** or **JSONata** (may return a **list** of columns).
- **Rename columns** (static & conditional)  
  - `from`/`to` can be **scalars or arrays** (pairwise mapping).  
  - Conditional rename activates when a message-level condition is true (typed inputs for LHS/RHS, incl. JSONata).
- **Derived columns** via JSONata  
  - Expression context includes `msg`, the current `row`, and `sheet`.
- **Output**  
  - Write to `msg`/`flow`/`global` at a path you choose.  
  - Choose **hierarchical** `{file -> sheet -> rows[]}` or **flat** rows (adds `_file`, `_sheet`).
- **Config file mode** (optional)  
  - Load/Save the entire node schema to a `.json` file under your Node-RED `userDir`.  
  - **Lock to file** (runtime always uses the file).  
  - **Watch file** (hot-reload on changes).  
  - Safe paths: relative to `userDir`, `.json` only.

---

## Install

From your Node-RED userDir (or via Palette Manager):

```bash
npm i node-red-contrib-xlsx-filter
````

Restart Node-RED if needed, then find **“xlsx-filter”** in the Function section.

> This node expects the input structure produced by your XLSX reader:
> `msg.<path>.data = { "<file>": { "<sheet>": [ {row}, ... ] }, ... }`

---

## Quick start

1. Place **xlsx-reader** (or your reader) → **xlsx-filter** → **debug**.
2. In **xlsx-filter**:

   * **Input**: set source (e.g. `msg.data`).
   * **Sheet filters**: include/exclude by regex if needed.
   * **Row filters**: add rules (e.g. keep rows where `Status == "OK"`).
   * **Select columns**: choose `keep` and list columns you want.
   * **Output**: set destination (e.g. `msg.filtered`) and structure.

Example: Keep columns whose names contain the current project diagram
(using JSONata to return a list of names):

```
Select mode: Keep
Select rows:
  Sheet:   Tarch
  Column:  ($pd := $string(msg.data.const.projectDiagram);
            $headers := $keys(msg.data.data.*.$lookup($, sheet)[0]);
            $headers[$contains($, $pd)])
  Column type: jsonata
```

---

## JSONata context & tips

* Wherever you see a **JSONata**-typed field, the expression is evaluated with:

  * `msg`: the whole message
  * `row`: (for row-specific contexts)
  * `sheet`: the current sheet name (string)

Common patterns:

```jsonata
/* Dynamic column name: TL1_TLC1_<diagram> */
"TL1_TLC1_" & $string(msg.data.const.projectDiagram)

/* Build a column from several variables */
$string(msg.path.a) & "_" & $string($flow("b")) & "_" & $string($global("c"))

/* Pick headers that contain a token */
(
  $t := "LIL";
  $keys(row)[ $contains($, $t) ]
)
```

> In **row filters**, the **Column** field may return a **string or an array of strings**.
> The rule passes if **any** of those columns satisfies the comparator.

---

## Row filters (details)

* **Sheet**: scope the rule to a sheet (exact / `regex` / `jsonata`).
* **Column(s)**: string or JSONata (may return a list of column names).
* **Op**: comparator. If `JSONata`, we ignore Column and evaluate RHS as a boolean with `{msg,row,sheet}`.
* **RHS**: typed input (`str/num/bool/msg/flow/global/env/jsonata`).
* **Case**: case-insensitive string compare when unchecked.
* **Coerce**: attempts to coerce string numbers/booleans before comparison.

**Logic**: Choose `AND` or `OR` across the list of rules.

---

## Select columns

* `Mode`: `none`, `keep`, `drop`
* Lines of **(Sheet, Column)** where Column is a string or JSONata (may return an array).
* Executed **before renames** so you can select by original headers.

---

## Rename

* Static rename list of **(Sheet, From, To)**.
* `From`/`To` accept string or JSONata (each may return array). When both return arrays, mapping is pairwise.

**Conditional rename**:

* Enable with a checkbox.
* Condition: `(LHS [typed]) (op) (RHS [typed])`.
* If true, apply the **Conditional rename list** (same structure as static).

---

## Derive columns

Add lines of `(New column name, JSONata expression)`.
Evaluated per row with `{msg, row, sheet}`.

Example:

```jsonata
/* Copy of a field */
row["Some Header"]

/* Build a code */
$uppercase($string(row.Family)) & "_" & $string(row.CategoryID)
```

---

## Output

* Target: `msg` / `flow` / `global` + path (deep path allowed).
* Structure:

  * **Hierarchical**: `{ "file.xlsx": { "Sheet1": [ ...rows ] } }`
  * **Flat**: `[ { _file, _sheet, ...row }, ... ]`
* Include summary (file/sheet/row counts & rules meta).

---

## Config file mode (optional)

In the editor’s **Config file** section:

* **Use config file**: Enable file mode for this node.
* **Config path**: e.g. `configs/xlsx-filter.app.json` (relative to your Node-RED `userDir`) or an absolute path **under** `userDir`. Only `.json` is allowed.
* **Load from file**: Reads JSON → populates the form.
* **Save to file**: Writes the current form schema to JSON (pretty).
* **Create template**: Writes a default template file if missing.
* **Lock to file**: On deploy, runtime loads the schema from file and uses it (form is grayed out so the file is the source of truth).
* **Watch file**: If the file changes, runtime hot-reloads the schema (debounced).

Security / path rules:

* Paths are normalized and must stay **inside `userDir`**.
* Only `.json` files are accepted.
* If load fails at runtime while locked, the node falls back to the embedded config and sets a **red status**.

---

## Skips temporary Excel lock files

When iterating files, the node **ignores** filenames starting with `~$` (Office temp locks).

---

## Example flow (mini)

```json
[{"id":"reader","type":"xlsx-reader","name":"read dir","path":"/data/xlsx","pathType":"str","mode":"directory","wires":[["filter"]]},
 {"id":"filter","type":"xlsx-filter","name":"keep LIL headers","inputPath":"data","inputPathType":"msg",
  "selectMode":"keep",
  "selectList":[{"sheetScope":"Tarch","sheetScopeType":"str","col":"(\n  $pd := $string(msg.data.const.projectDiagram);\n  $headers := $keys(msg.data.data.*.$lookup($, sheet)[0]);\n  $headers[$contains($, $pd)]\n)","colType":"jsonata"}],
  "outputTargetType":"msg","outputTargetPath":"filtered","structure":"hierarchical","wires":[["debug"]]},
 {"id":"debug","type":"debug","complete":"true"}]
```

---

## Development notes

* Requires Node-RED 2.x+ (JSONata evaluation with callback is used under the hood).
* No extra npm deps; uses Node-RED admin HTTP for load/save.
* Code handles async JSONata, array normalization, and robust string matching.

---

## License

MIT © AIOUBSAI
