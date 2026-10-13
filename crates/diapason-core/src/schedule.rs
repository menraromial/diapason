//! Rate profiles and arrival processes.
//!
//! The profile gives the rate λ(t) over the whole run as a list of linear
//! segments. Arrivals are generated in Λ-space, where Λ(t) = ∫₀ᵗ λ, and mapped
//! back to time with Λ⁻¹. Worker w of W takes the constant arrivals of global
//! rank k ≡ w (mod W), or an independent Poisson process of intensity λ/W.

use crate::config::{Arrival, ConfigError, Mode, RunConfig};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::Exp1;
use std::io::BufRead;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct Segment {
    t0: f64,
    t1: f64,
    r0: f64,
    r1: f64,
    /// Λ(t0).
    c0: f64,
    /// Λ(t1).
    c1: f64,
}

#[derive(Clone, Debug)]
pub struct PhaseSpan {
    pub name: String,
    pub start_ns: u64,
    pub end_ns: u64,
    pub nominal_rate: f64,
}

impl PhaseSpan {
    pub fn duration_s(&self) -> f64 {
        (self.end_ns - self.start_ns) as f64 / 1e9
    }
}

#[derive(Clone, Debug)]
pub struct RateProfile {
    segments: Vec<Segment>,
    pub phases: Vec<PhaseSpan>,
    pub end_ns: u64,
    /// Λ at the end of the run.
    pub total: f64,
}

fn check_rate(phase: &str, r: f64) -> Result<f64, ConfigError> {
    if r.is_finite() && r >= 0.0 {
        Ok(r)
    } else {
        Err(ConfigError(format!(
            "phase '{phase}': rate {r} is not a finite non-negative number"
        )))
    }
}

impl RateProfile {
    pub fn from_config(cfg: &RunConfig) -> Result<RateProfile, ConfigError> {
        let rates_used = cfg.mode == Mode::Open && cfg.load.arrival != Arrival::Replay;
        let mut segs: Vec<(f64, f64, f64)> = Vec::new(); // (duration, r0, r1)
        let mut phase_lens = Vec::new();
        for p in &cfg.load.phases {
            let n = &p.name;
            let given = p.rate.is_some() as u8 + p.steps.is_some() as u8 + p.points.is_some() as u8;
            if given > 1 {
                return Err(ConfigError(format!(
                    "phase '{n}': give only one of rate, steps or points"
                )));
            }
            if p.rate_end.is_some() && p.rate.is_none() {
                return Err(ConfigError(format!("phase '{n}': rate_end needs rate")));
            }
            let start = segs.len();
            let len;
            if let Some(steps) = &p.steps {
                if steps.is_empty() {
                    return Err(ConfigError(format!("phase '{n}': steps is empty")));
                }
                let sum: f64 = steps.iter().map(|s| s.duration.as_secs_f64()).sum();
                if let Some(d) = p.duration {
                    if (d.as_secs_f64() - sum).abs() > 1e-9 {
                        return Err(ConfigError(format!(
                            "phase '{n}': duration {d} differs from the sum of its steps ({sum}s)"
                        )));
                    }
                }
                for s in steps {
                    let r = check_rate(n, s.rate)?;
                    segs.push((s.duration.as_secs_f64(), r, r));
                }
                len = sum;
            } else {
                let d = p
                    .duration
                    .ok_or_else(|| ConfigError(format!("phase '{n}': duration is required")))?
                    .as_secs_f64();
                len = d;
                if let Some(points) = &p.points {
                    if points.is_empty() || !points[0].at.0.is_zero() {
                        return Err(ConfigError(format!(
                            "phase '{n}': points must start with at = \"0s\""
                        )));
                    }
                    for w in points.windows(2) {
                        if w[1].at <= w[0].at {
                            return Err(ConfigError(format!(
                                "phase '{n}': points must have increasing 'at'"
                            )));
                        }
                    }
                    if points.last().unwrap().at.as_secs_f64() > d {
                        return Err(ConfigError(format!(
                            "phase '{n}': a point lies after the end of the phase"
                        )));
                    }
                    for w in points.windows(2) {
                        segs.push((
                            w[1].at.as_secs_f64() - w[0].at.as_secs_f64(),
                            check_rate(n, w[0].rate)?,
                            check_rate(n, w[1].rate)?,
                        ));
                    }
                    let last = points.last().unwrap();
                    let rest = d - last.at.as_secs_f64();
                    if rest > 0.0 {
                        let r = check_rate(n, last.rate)?;
                        segs.push((rest, r, r));
                    }
                } else if let Some(r) = p.rate {
                    let r0 = check_rate(n, r)?;
                    let r1 = check_rate(n, p.rate_end.unwrap_or(r))?;
                    segs.push((d, r0, r1));
                } else if rates_used {
                    return Err(ConfigError(format!(
                        "phase '{n}': give rate, steps or points"
                    )));
                } else {
                    segs.push((d, 0.0, 0.0));
                }
            }
            if len.is_nan() || len <= 0.0 {
                return Err(ConfigError(format!(
                    "phase '{n}': duration must be positive"
                )));
            }
            if !rates_used {
                // Rates are ignored in closed-loop and replay modes.
                for s in &mut segs[start..] {
                    s.1 = 0.0;
                    s.2 = 0.0;
                }
            }
            phase_lens.push((n.clone(), len));
        }

        let mut segments = Vec::with_capacity(segs.len());
        let (mut t, mut c) = (0.0f64, 0.0f64);
        for (d, r0, r1) in segs {
            let c1 = c + d * (r0 + r1) / 2.0;
            segments.push(Segment {
                t0: t,
                t1: t + d,
                r0,
                r1,
                c0: c,
                c1,
            });
            t += d;
            c = c1;
        }
        let mut phases = Vec::new();
        let mut pt = 0.0f64;
        for (name, len) in phase_lens {
            let start_ns = (pt * 1e9).round() as u64;
            pt += len;
            let end_ns = (pt * 1e9).round() as u64;
            phases.push(PhaseSpan {
                name,
                start_ns,
                end_ns,
                nominal_rate: 0.0,
            });
        }
        let mut profile = RateProfile {
            segments,
            end_ns: phases.last().map(|p| p.end_ns).unwrap_or(0),
            phases,
            total: c,
        };
        for i in 0..profile.phases.len() {
            let p = &profile.phases[i];
            let (a, b) = (p.start_ns as f64 / 1e9, p.end_ns as f64 / 1e9);
            profile.phases[i].nominal_rate =
                (profile.cumulative(b) - profile.cumulative(a)) / (b - a);
        }
        Ok(profile)
    }

    /// Λ(t), t in seconds.
    pub fn cumulative(&self, t: f64) -> f64 {
        for s in &self.segments {
            if t < s.t1 {
                if t <= s.t0 {
                    return s.c0;
                }
                let tau = t - s.t0;
                let d = s.t1 - s.t0;
                return s.c0 + s.r0 * tau + (s.r1 - s.r0) * tau * tau / (2.0 * d);
            }
        }
        self.total
    }

    /// Λ⁻¹(x) in seconds, or None when x is beyond the end of the run. `hint`
    /// is a segment index that only moves forward, for increasing queries.
    fn inverse(&self, x: f64, hint: &mut usize) -> Option<f64> {
        while *hint < self.segments.len() && x >= self.segments[*hint].c1 {
            *hint += 1;
        }
        let s = self.segments.get(*hint)?;
        let d = s.t1 - s.t0;
        let xr = (x - s.c0).max(0.0);
        let a = (s.r1 - s.r0) / (2.0 * d);
        // Solve a·τ² + r0·τ = xr in the form that is stable for any sign of a.
        let disc = (s.r0 * s.r0 + 4.0 * a * xr).max(0.0);
        let denom = s.r0 + disc.sqrt();
        let tau = if denom > 0.0 { 2.0 * xr / denom } else { 0.0 };
        Some(s.t0 + tau.clamp(0.0, d))
    }

    /// Index of the phase that contains time `t_ns`.
    pub fn phase_of(&self, t_ns: u64) -> usize {
        match self.phases.iter().position(|p| t_ns < p.end_ns) {
            Some(i) => i,
            None => self.phases.len().saturating_sub(1),
        }
    }
}

/// One scheduled request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scheduled {
    pub seq: u64,
    pub intended_ns: u64,
    pub phase: u16,
}

enum Source {
    Constant,
    Poisson {
        pos: f64,
        rng: Box<ChaCha8Rng>,
    },
    Replay {
        lines: std::io::Lines<std::io::BufReader<std::fs::File>>,
        line_no: u64,
    },
}

/// The schedule of one worker. Produces intended times in increasing order.
pub struct Arrivals {
    profile: Arc<RateProfile>,
    source: Source,
    worker: u64,
    workers: u64,
    j: u64,
    hint: usize,
    phase: usize,
    last_ns: u64,
}

pub fn worker_rng(seed: u64, worker: usize) -> ChaCha8Rng {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    rng.set_stream(worker as u64);
    rng
}

#[derive(Debug, thiserror::Error)]
#[error("replay file line {line}: {msg}")]
pub struct ReplayError {
    pub line: u64,
    pub msg: String,
}

impl Arrivals {
    pub fn new(
        cfg: &RunConfig,
        profile: Arc<RateProfile>,
        seed: u64,
        worker: usize,
        workers: usize,
    ) -> std::io::Result<Arrivals> {
        let source = match cfg.load.arrival {
            Arrival::Constant => Source::Constant,
            Arrival::Poisson => Source::Poisson {
                pos: 0.0,
                rng: Box::new(worker_rng(seed, worker)),
            },
            Arrival::Replay => {
                let path = cfg.load.replay_file.as_ref().expect("validated");
                let f = std::fs::File::open(path)?;
                Source::Replay {
                    lines: std::io::BufReader::new(f).lines(),
                    line_no: 0,
                }
            }
        };
        Ok(Arrivals {
            profile,
            source,
            worker: worker as u64,
            workers: workers as u64,
            j: 0,
            hint: 0,
            phase: 0,
            last_ns: 0,
        })
    }

    fn phase_of(&mut self, t_ns: u64) -> u16 {
        while self.phase + 1 < self.profile.phases.len()
            && t_ns >= self.profile.phases[self.phase].end_ns
        {
            self.phase += 1;
        }
        self.phase as u16
    }

    pub fn next_arrival(&mut self) -> Result<Option<Scheduled>, ReplayError> {
        let w = self.workers;
        let t_ns = match &mut self.source {
            Source::Constant => {
                let x = (self.j * w + self.worker) as f64 + 0.5;
                match self.profile.inverse(x, &mut self.hint) {
                    Some(t) => (t * 1e9).round() as u64,
                    None => return Ok(None),
                }
            }
            Source::Poisson { pos, rng } => {
                let gap: f64 = rng.sample(Exp1);
                *pos += gap * w as f64;
                let x = *pos;
                match self.profile.inverse(x, &mut self.hint) {
                    Some(t) => (t * 1e9).round() as u64,
                    None => return Ok(None),
                }
            }
            Source::Replay { lines, line_no } => loop {
                let line = match lines.next() {
                    None => return Ok(None),
                    Some(Err(e)) => {
                        return Err(ReplayError {
                            line: *line_no + 1,
                            msg: e.to_string(),
                        })
                    }
                    Some(Ok(l)) => l,
                };
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    *line_no += 1;
                    continue;
                }
                let n = *line_no;
                *line_no += 1;
                // Data lines are numbered from 0 for the modulo split.
                let data_index = n;
                if data_index % w != self.worker {
                    continue;
                }
                let secs: f64 = trimmed.parse().map_err(|_| ReplayError {
                    line: n + 1,
                    msg: format!("'{trimmed}' is not a number of seconds"),
                })?;
                if !(secs.is_finite() && secs >= 0.0) {
                    return Err(ReplayError {
                        line: n + 1,
                        msg: format!("{secs} is not a valid time"),
                    });
                }
                break (secs * 1e9).round() as u64;
            },
        };
        if t_ns >= self.profile.end_ns {
            return Ok(None);
        }
        if t_ns < self.last_ns {
            return Err(ReplayError {
                line: 0,
                msg: "intended times must not decrease".into(),
            });
        }
        self.last_ns = t_ns;
        let seq = self.j * w + self.worker;
        self.j += 1;
        let phase = self.phase_of(t_ns);
        Ok(Some(Scheduled {
            seq,
            intended_ns: t_ns,
            phase,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(text: &str) -> RunConfig {
        RunConfig::from_toml(&format!("[target]\nurl = \"http://127.0.0.1:1/\"\n{text}")).unwrap()
    }

    fn all(cfg: &RunConfig, workers: usize) -> Vec<Scheduled> {
        let p = Arc::new(RateProfile::from_config(cfg).unwrap());
        let mut v = Vec::new();
        for w in 0..workers {
            let mut a = Arrivals::new(cfg, p.clone(), 7, w, workers).unwrap();
            while let Some(s) = a.next_arrival().unwrap() {
                v.push(s);
            }
        }
        v.sort_by_key(|s| s.intended_ns);
        v
    }

    #[test]
    fn constant_is_evenly_spaced_across_workers() {
        let c = cfg("[load]\narrival = \"constant\"\n[[load.phases]]\nname = \"measure\"\nduration = \"1s\"\nrate = 1000\n");
        for workers in [1, 3, 7, 64] {
            let v = all(&c, workers);
            assert_eq!(v.len(), 1000);
            for (k, s) in v.iter().enumerate() {
                assert_eq!(s.seq, k as u64);
                let want = (k as u64) * 1_000_000 + 500_000;
                assert!(
                    s.intended_ns.abs_diff(want) <= 1,
                    "{} vs {want}",
                    s.intended_ns
                );
            }
        }
    }

    #[test]
    fn ramp_count_matches_integral() {
        let c = cfg("[load]\narrival = \"constant\"\n[[load.phases]]\nname = \"a\"\nduration = \"10s\"\nrate = 100\nrate_end = 300\n[[load.phases]]\nname = \"b\"\nsteps = [{duration = \"1s\", rate = 50}, {duration = \"2s\", rate = 0}, {duration = \"1s\", rate = 10}]\n");
        let v = all(&c, 4);
        assert_eq!(v.len(), 2000 + 50 + 10);
        let p = RateProfile::from_config(&c).unwrap();
        assert_eq!(p.phases[1].start_ns, 10_000_000_000);
        assert!(v.iter().filter(|s| s.phase == 1).all(|s| {
            let t = s.intended_ns;
            !(11_000_000_000..13_000_000_000).contains(&t)
        }));
        // Rate at the start of the ramp is 100/s, at the end 300/s.
        let first_gap = v[1].intended_ns - v[0].intended_ns;
        assert!((first_gap as f64 - 1e7).abs() < 1e5, "{first_gap}");
    }

    #[test]
    fn poisson_mean_and_reproducibility() {
        let c = cfg("[load]\narrival = \"poisson\"\n[[load.phases]]\nname = \"m\"\nduration = \"20s\"\nrate = 5000\n");
        let a = all(&c, 5);
        let b = all(&c, 5);
        assert_eq!(a, b);
        let n = a.len() as f64;
        assert!((n - 100_000.0).abs() < 5.0 * 100_000f64.sqrt(), "{n}");
    }

    #[test]
    fn piecewise_points() {
        let c = cfg("[load]\narrival = \"constant\"\n[[load.phases]]\nname = \"m\"\nduration = \"4s\"\npoints = [{at = \"0s\", rate = 0}, {at = \"2s\", rate = 100}]\n");
        // Λ = ∫ ramp 0..100 over 2 s (100) + 100/s for 2 s (200).
        assert_eq!(all(&c, 2).len(), 300);
    }
}
