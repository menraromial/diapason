//! Request templates, variables, data files and scenario selection.
//!
//! A template is a string with `{{name}}` references, compiled once into
//! literal and reference parts; rendering concatenates them, with no
//! interpreter in the hot path. A reference names `seq`, a variable from
//! `[vars]`, or a column of a data file as `file.column`.
//!
//! Every random choice (variable values, random data rows, the scenario) is a
//! function of the master seed and the request's `seq` only, so a request has
//! the same content whichever worker, connection or moment sends it.

use crate::config::{ConfigError, DataFormat, DataMode, RunConfig, Scalar, VarConfig};
use crate::trace::mix;
use rand::{Rng, RngCore};
use rand_distr::{Distribution, Normal, WeightedIndex, Zipf};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A tiny counter-based generator: splitmix64 seeded from (seed, seq, stream).
pub struct ReqRng(u64);

impl ReqRng {
    pub fn new(seed: u64, seq: u64, stream: u64) -> ReqRng {
        ReqRng(mix(seed ^ mix(seq ^ mix(stream.wrapping_add(0x51A7_E5E5)))))
    }
}

impl RngCore for ReqRng {
    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for chunk in dest.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.fill_bytes(dest);
        Ok(())
    }
}

/// Picks the scenario of a request from (seed, seq) and the weights.
#[derive(Clone, Debug)]
pub struct ScenarioPicker {
    seed: u64,
    cum: Vec<f64>,
}

impl ScenarioPicker {
    pub fn new(cfg: &RunConfig, seed: u64) -> ScenarioPicker {
        let mut cum = Vec::new();
        let mut t = 0.0;
        for s in &cfg.scenarios {
            t += s.weight;
            cum.push(t);
        }
        ScenarioPicker { seed, cum }
    }

    #[inline]
    pub fn pick(&self, seq: u64) -> u16 {
        if self.cum.len() <= 1 {
            return 0;
        }
        let total = *self.cum.last().unwrap();
        let u =
            (mix(self.seed ^ 0x5CE7_A210 ^ mix(seq)) >> 11) as f64 / (1u64 << 53) as f64 * total;
        self.cum
            .iter()
            .position(|&c| u < c)
            .unwrap_or(self.cum.len() - 1) as u16
    }

    /// Scenario names, in index order ("default" when none is configured).
    pub fn names(cfg: &RunConfig) -> Vec<String> {
        if cfg.scenarios.is_empty() {
            vec!["default".into()]
        } else {
            cfg.scenarios.iter().map(|s| s.name.clone()).collect()
        }
    }
}

enum Var {
    Uniform(i64, i64),
    Zipf(Zipf<f64>),
    Normal(Normal<f64>, bool),
    Choice(Vec<String>, Option<WeightedIndex<f64>>),
    Sequence(i64, i64),
}

struct DataSet {
    name: String,
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    mode: DataMode,
    wrapped: AtomicU64,
}

#[derive(Clone, Debug, PartialEq)]
enum Part {
    Lit(Vec<u8>),
    Seq,
    Var(usize),
    Data(usize, usize),
}

/// A compiled template.
#[derive(Clone, Debug, PartialEq)]
pub struct Template {
    parts: Vec<Part>,
}

impl Template {
    /// The text, when the template has no reference.
    pub fn as_static(&self) -> Option<&[u8]> {
        match self.parts.as_slice() {
            [] => Some(b""),
            [Part::Lit(b)] => Some(b),
            _ => None,
        }
    }
}

/// Variables and data sets, shared by all workers.
pub struct TemplateEnv {
    seed: AtomicU64,
    vars: Vec<(String, Var)>,
    data: Vec<DataSet>,
}

/// Per-request scratch: each variable and data row is drawn once per request.
#[derive(Default)]
pub struct Renderer {
    seq: u64,
    vars: Vec<Option<String>>,
    rows: Vec<Option<usize>>,
}

impl Renderer {
    pub fn start(&mut self, env: &TemplateEnv, seq: u64) {
        self.seq = seq;
        self.vars.clear();
        self.vars.resize(env.vars.len(), None);
        self.rows.clear();
        self.rows.resize(env.data.len(), None);
    }
}

fn parse_csv_line(line: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = line.chars().peekable();
    let mut quoted = false;
    let mut field_start = true;
    while let Some(c) = chars.next() {
        if quoted {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    cur.push('"');
                } else {
                    quoted = false;
                }
            } else {
                cur.push(c);
            }
        } else if c == '"' && field_start {
            quoted = true;
            field_start = false;
        } else if c == ',' {
            out.push(std::mem::take(&mut cur));
            field_start = true;
        } else {
            cur.push(c);
            field_start = false;
        }
    }
    if quoted {
        return Err("unterminated quote".into());
    }
    out.push(cur);
    Ok(out)
}

fn load_data(name: &str, cfg: &crate::config::DataConfig) -> Result<DataSet, ConfigError> {
    let e = |m: String| ConfigError(format!("data.{name} ({}): {m}", cfg.file.display()));
    let text = std::fs::read_to_string(&cfg.file).map_err(|x| e(x.to_string()))?;
    let format =
        cfg.format
            .unwrap_or_else(|| match cfg.file.extension().and_then(|x| x.to_str()) {
                Some("jsonl") | Some("ndjson") => DataFormat::Jsonl,
                _ => DataFormat::Csv,
            });
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    match format {
        DataFormat::Csv => {
            let mut lines = text.lines().filter(|l| !l.trim().is_empty());
            let header = lines.next().ok_or_else(|| e("empty file".into()))?;
            columns = parse_csv_line(header).map_err(|m| e(format!("header: {m}")))?;
            for (i, l) in lines.enumerate() {
                let r = parse_csv_line(l).map_err(|m| e(format!("row {}: {m}", i + 1)))?;
                if r.len() != columns.len() {
                    return Err(e(format!(
                        "row {} has {} fields, the header {}",
                        i + 1,
                        r.len(),
                        columns.len()
                    )));
                }
                rows.push(r);
            }
        }
        DataFormat::Jsonl => {
            let mut index: HashMap<String, usize> = HashMap::new();
            let mut objs = Vec::new();
            for (i, l) in text
                .lines()
                .enumerate()
                .filter(|(_, l)| !l.trim().is_empty())
            {
                let v: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(l).map_err(|x| e(format!("line {}: {x}", i + 1)))?;
                for k in v.keys() {
                    if !index.contains_key(k) {
                        index.insert(k.clone(), columns.len());
                        columns.push(k.clone());
                    }
                }
                objs.push(v);
            }
            for v in objs {
                let mut r = vec![String::new(); columns.len()];
                for (k, val) in v {
                    r[index[&k]] = match val {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                }
                rows.push(r);
            }
        }
    }
    if rows.is_empty() {
        return Err(e("no data rows".into()));
    }
    Ok(DataSet {
        name: name.to_string(),
        columns,
        rows,
        mode: cfg.mode,
        wrapped: AtomicU64::new(0),
    })
}

impl TemplateEnv {
    pub fn load(cfg: &RunConfig, seed: u64) -> Result<Arc<TemplateEnv>, ConfigError> {
        let mut vars = Vec::new();
        for (name, v) in &cfg.vars {
            let e = |m: &str| ConfigError(format!("vars.{name}: {m}"));
            let var = match v {
                VarConfig::Uniform { min, max } => {
                    if min > max {
                        return Err(e("min > max"));
                    }
                    Var::Uniform(*min, *max)
                }
                VarConfig::Zipf { n, s } => {
                    Var::Zipf(Zipf::new(*n, *s).map_err(|x| e(&x.to_string()))?)
                }
                VarConfig::Normal { mean, stdev, round } => Var::Normal(
                    Normal::new(*mean, *stdev).map_err(|x| e(&x.to_string()))?,
                    *round,
                ),
                VarConfig::Choice { values, weights } => {
                    if values.is_empty() {
                        return Err(e("values is empty"));
                    }
                    let w = match weights {
                        Some(w) if w.len() != values.len() => {
                            return Err(e("weights and values differ in length"))
                        }
                        Some(w) => Some(WeightedIndex::new(w).map_err(|x| e(&x.to_string()))?),
                        None => None,
                    };
                    Var::Choice(values.iter().map(Scalar::to_string).collect(), w)
                }
                VarConfig::Sequence { start, step } => Var::Sequence(*start, *step),
            };
            vars.push((name.clone(), var));
        }
        let mut data = Vec::new();
        for (name, d) in &cfg.data {
            if cfg.vars.contains_key(name) {
                return Err(ConfigError(format!(
                    "'{name}' is both a variable and a data file"
                )));
            }
            data.push(load_data(name, d)?);
        }
        Ok(Arc::new(TemplateEnv {
            seed: AtomicU64::new(seed),
            vars,
            data,
        }))
    }

    /// Sets the master seed, once it is known and before the run starts.
    pub fn set_seed(&self, seed: u64) {
        self.seed.store(seed, Ordering::Relaxed);
    }

    fn seed(&self) -> u64 {
        self.seed.load(Ordering::Relaxed)
    }

    pub fn compile(&self, text: &str) -> Result<Template, ConfigError> {
        let mut parts = Vec::new();
        let mut rest = text;
        while let Some(start) = rest.find("{{") {
            if start > 0 {
                parts.push(Part::Lit(rest.as_bytes()[..start].to_vec()));
            }
            let after = &rest[start + 2..];
            let end = after
                .find("}}")
                .ok_or_else(|| ConfigError(format!("template '{text}': '{{{{' without '}}}}'")))?;
            let name = after[..end].trim();
            let part = if name == "seq" {
                Part::Seq
            } else if let Some(i) = self.vars.iter().position(|(n, _)| n == name) {
                Part::Var(i)
            } else if let Some((file, col)) = name.split_once('.') {
                let d = self
                    .data
                    .iter()
                    .position(|d| d.name == file)
                    .ok_or_else(|| {
                        ConfigError(format!("template '{text}': no data file '{file}'"))
                    })?;
                let c = self.data[d]
                    .columns
                    .iter()
                    .position(|c| c == col)
                    .ok_or_else(|| {
                        ConfigError(format!(
                            "template '{text}': data file '{file}' has no column '{col}'"
                        ))
                    })?;
                Part::Data(d, c)
            } else {
                return Err(ConfigError(format!(
                    "template '{text}': unknown name '{name}'"
                )));
            };
            parts.push(part);
            rest = &after[end + 2..];
        }
        if !rest.is_empty() {
            parts.push(Part::Lit(rest.as_bytes().to_vec()));
        }
        // Merge adjacent literals.
        let mut merged: Vec<Part> = Vec::new();
        for p in parts {
            match (merged.last_mut(), p) {
                (Some(Part::Lit(a)), Part::Lit(b)) => a.extend_from_slice(&b),
                (_, p) => merged.push(p),
            }
        }
        Ok(Template { parts: merged })
    }

    fn var_value(&self, i: usize, seq: u64) -> String {
        let mut rng = ReqRng::new(self.seed(), seq, i as u64 + 1);
        match &self.vars[i].1 {
            Var::Uniform(a, b) => rng.gen_range(*a..=*b).to_string(),
            Var::Zipf(z) => (z.sample(&mut rng) as u64).to_string(),
            Var::Normal(n, round) => {
                let x = n.sample(&mut rng);
                if *round {
                    (x.round() as i64).to_string()
                } else {
                    x.to_string()
                }
            }
            Var::Choice(values, w) => {
                let k = match w {
                    Some(w) => w.sample(&mut rng),
                    None => rng.gen_range(0..values.len()),
                };
                values[k].clone()
            }
            Var::Sequence(start, step) => start
                .wrapping_add(step.wrapping_mul(seq as i64))
                .to_string(),
        }
    }

    fn row(&self, d: usize, seq: u64) -> usize {
        let ds = &self.data[d];
        let n = ds.rows.len() as u64;
        (match ds.mode {
            DataMode::Sequential => seq % n,
            DataMode::Random => {
                ReqRng::new(self.seed(), seq, 0xDA7A_0000 + d as u64).next_u64() % n
            }
            DataMode::Unique => {
                if seq >= n {
                    ds.wrapped.fetch_add(1, Ordering::Relaxed);
                }
                seq % n
            }
        }) as usize
    }

    /// Appends the rendering of `t` to `out`. `r` must have been started for
    /// the current request.
    pub fn render(&self, t: &Template, r: &mut Renderer, out: &mut Vec<u8>) {
        for p in &t.parts {
            match p {
                Part::Lit(b) => out.extend_from_slice(b),
                Part::Seq => out.extend_from_slice(r.seq.to_string().as_bytes()),
                Part::Var(i) => {
                    if r.vars[*i].is_none() {
                        r.vars[*i] = Some(self.var_value(*i, r.seq));
                    }
                    out.extend_from_slice(r.vars[*i].as_ref().unwrap().as_bytes());
                }
                Part::Data(d, c) => {
                    if r.rows[*d].is_none() {
                        r.rows[*d] = Some(self.row(*d, r.seq));
                    }
                    let row = r.rows[*d].unwrap();
                    out.extend_from_slice(self.data[*d].rows[row][*c].as_bytes());
                }
            }
        }
    }

    /// Data files read past their last row in `unique` mode: name → count.
    pub fn wrapped(&self) -> Vec<(String, u64)> {
        self.data
            .iter()
            .filter(|d| d.mode == DataMode::Unique)
            .map(|d| (d.name.clone(), d.wrapped.load(Ordering::Relaxed)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(extra: &str) -> (RunConfig, Arc<TemplateEnv>) {
        let cfg = RunConfig::from_toml(&format!(
            "[target]\nurl = \"http://127.0.0.1:1/\"\n[load]\n[[load.phases]]\nname = \"m\"\nduration = \"1s\"\nrate = 1\n{extra}"
        ))
        .unwrap();
        let env = TemplateEnv::load(&cfg, 42).unwrap();
        (cfg, env)
    }

    fn render(env: &TemplateEnv, t: &str, seq: u64) -> String {
        let t = env.compile(t).unwrap();
        let mut r = Renderer::default();
        r.start(env, seq);
        let mut out = Vec::new();
        env.render(&t, &mut r, &mut out);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn variables_are_a_function_of_seq() {
        let (_, e) = env(
            "[vars]\nu = { dist = \"uniform\", min = 1, max = 6 }\nz = { dist = \"zipf\", n = 100, s = 1.2 }\n\
             n = { dist = \"normal\", mean = 50.0, stdev = 5.0, round = true }\n\
             c = { dist = \"choice\", values = [\"a\", 2, 3.5], weights = [1.0, 1.0, 2.0] }\n\
             q = { dist = \"sequence\", start = 100, step = 10 }\n",
        );
        let t = "/x/{{u}}/{{z}}/{{n}}/{{c}}/{{q}}/{{seq}}/{{u}}";
        for seq in 0..200 {
            let a = render(&e, t, seq);
            assert_eq!(a, render(&e, t, seq));
            let f: Vec<&str> = a.split('/').collect();
            let u: i64 = f[2].parse().unwrap();
            assert!((1..=6).contains(&u));
            assert_eq!(f[2], f[8], "a variable is drawn once per request");
            let z: u64 = f[3].parse().unwrap();
            assert!((1..=100).contains(&z));
            assert!(["a", "2", "3.5"].contains(&f[5]));
            assert_eq!(f[6], (100 + 10 * seq).to_string());
            assert_eq!(f[7], seq.to_string());
        }
        let distinct: std::collections::HashSet<String> =
            (0..200).map(|s| render(&e, "{{u}}", s)).collect();
        assert_eq!(distinct.len(), 6);
    }

    #[test]
    fn data_files() {
        let dir = std::env::temp_dir().join(format!("diapason-tpl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("u.csv"),
            "id,name\n1,\"Smith, J\"\n2,\"say \"\"hi\"\"\"\n3,c\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("p.jsonl"),
            "{\"sku\": \"A\", \"qty\": 3}\n{\"sku\": \"B\"}\n",
        )
        .unwrap();
        let (_, e) = env(&format!(
            "[data.u]\nfile = \"{}\"\nmode = \"unique\"\n[data.p]\nfile = \"{}\"\n",
            dir.join("u.csv").display(),
            dir.join("p.jsonl").display()
        ));
        assert_eq!(render(&e, "{{u.id}}:{{u.name}}", 0), "1:Smith, J");
        assert_eq!(render(&e, "{{u.name}}", 1), "say \"hi\"");
        assert_eq!(render(&e, "{{p.sku}}{{p.qty}}", 0), "A3");
        assert_eq!(render(&e, "{{p.sku}}{{p.qty}}", 1), "B");
        assert_eq!(e.wrapped(), vec![("u".to_string(), 0)]);
        render(&e, "{{u.id}}", 5);
        assert_eq!(e.wrapped(), vec![("u".to_string(), 1)]);
        assert!(e.compile("{{u.nope}}").is_err());
        assert!(e.compile("{{nope}}").is_err());
        assert!(e.compile("{{u.id").is_err());
        assert_eq!(e.compile("plain").unwrap().as_static(), Some(&b"plain"[..]));
    }

    #[test]
    fn scenario_mix() {
        let (cfg, _) = env(
            "[[scenarios]]\nname = \"a\"\nweight = 3\n[[scenarios]]\nname = \"b\"\nweight = 1\n[[scenarios]]\nname = \"z\"\nweight = 0\n",
        );
        let p = ScenarioPicker::new(&cfg, 7);
        let mut n = [0u32; 3];
        for seq in 0..100_000 {
            n[p.pick(seq) as usize] += 1;
        }
        assert_eq!(n[2], 0);
        assert!((n[0] as f64 / 75_000.0 - 1.0).abs() < 0.02, "{n:?}");
    }
}
