use dashmap::DashMap;

use crate::routing::Route;

#[derive(Debug, Clone, Copy, Default)]
pub struct CmdStats {
    pub total: u64,
    pub replica_fallback_to_master: u64,
}

/// Process-wide statistics (shared across all client connections).
///
/// The intent is operational visibility: "which commands actually go where".
#[derive(Debug, Default)]
pub struct Stats {
    // Keyed by (route, command_upper).
    by_route_cmd: DashMap<(Route, String), CmdStats>,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, route: Route, cmd_upper: &str) {
        let key = (route, cmd_upper.to_string());
        let mut entry = self.by_route_cmd.entry(key).or_default();
        entry.total = entry.total.saturating_add(1);
    }

    pub fn record_replica_fallback(&self, cmd_upper: &str) {
        let key = (Route::Replica, cmd_upper.to_string());
        let mut entry = self.by_route_cmd.entry(key).or_default();
        entry.replica_fallback_to_master = entry.replica_fallback_to_master.saturating_add(1);
    }

    /// Render summary lines similar to:
    ///
    /// ```text
    /// BOTH    CLIENT 125 times
    /// REPLICA GET    8056 times
    /// ...
    /// ```
    pub fn render_summary_lines(&self) -> Vec<String> {
        let mut rows: Vec<(Route, String, CmdStats)> = self
            .by_route_cmd
            .iter()
            .map(|e| {
                let ((route, cmd), stats) = (e.key(), *e.value());
                (*route, cmd.clone(), stats)
            })
            .collect();

        rows.sort_by(|a, b| {
            // Prefer BOTH/REPLICA visibility first (typical interest for this proxy).
            let ra = route_rank(a.0);
            let rb = route_rank(b.0);
            ra.cmp(&rb)
                .then_with(|| b.2.total.cmp(&a.2.total))
                .then_with(|| a.1.cmp(&b.1))
        });

        let mut out = Vec::with_capacity(rows.len());
        for (route, cmd, stats) in rows {
            let route_s = match route {
                Route::Both => "BOTH",
                Route::Replica => "REPLICA",
                Route::Master => "MASTER",
            };

            // Keep formatting close to the example while staying readable.
            let mut line = format!("{:<7} {:<16} {} times", route_s, cmd, stats.total);

            if route == Route::Replica && stats.replica_fallback_to_master > 0 {
                line.push_str(&format!(
                    " (fallback {}times)",
                    stats.replica_fallback_to_master
                ));
            }

            out.push(line);
        }

        out
    }
}

fn route_rank(r: Route) -> u8 {
    match r {
        Route::Both => 0,
        Route::Replica => 1,
        Route::Master => 2,
    }
}
