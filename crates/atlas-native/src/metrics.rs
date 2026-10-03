// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! Minimal Prometheus text-exposition writer for `RaftServer::render_metrics` and
//! `NativeEngine::render_metrics`. Callers serve the string on whatever `/metrics` endpoint hosts
//! the node.

use std::fmt::{Display, Write};

#[derive(Debug, Default)]
pub struct PromText {
    out: String,
}

impl PromText {
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts a metric family. `kind` is `gauge` or `counter`.
    pub fn family(&mut self, name: &str, kind: &str, help: &str) -> &mut Self {
        let _ = writeln!(self.out, "# HELP {name} {help}");
        let _ = writeln!(self.out, "# TYPE {name} {kind}");
        self
    }

    pub fn sample(
        &mut self,
        name: &str,
        labels: &[(&str, &str)],
        value: impl Display,
    ) -> &mut Self {
        self.out.push_str(name);
        if !labels.is_empty() {
            self.out.push('{');
            for (i, (k, v)) in labels.iter().enumerate() {
                if i > 0 {
                    self.out.push(',');
                }
                let _ = write!(self.out, "{k}=\"{}\"", escape(v));
            }
            self.out.push('}');
        }
        let _ = writeln!(self.out, " {value}");
        self
    }

    /// A family with a single unlabelled sample.
    pub fn single(&mut self, name: &str, kind: &str, help: &str, value: impl Display) -> &mut Self {
        self.family(name, kind, help).sample(name, &[], value)
    }

    pub fn finish(self) -> String {
        self.out
    }
}

fn escape(v: &str) -> String {
    let mut s = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => s.push_str("\\\\"),
            '"' => s.push_str("\\\""),
            '\n' => s.push_str("\\n"),
            c => s.push(c),
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_families_and_escapes_labels() {
        let mut p = PromText::new();
        p.single("atlas_x", "gauge", "An x.", 3);
        p.family("atlas_y_total", "counter", "Ys.").sample(
            "atlas_y_total",
            &[("peer", "a\"b\\c\nd")],
            7,
        );
        let text = p.finish();
        assert!(text.contains("# TYPE atlas_x gauge\natlas_x 3\n"));
        assert!(text.contains(r#"atlas_y_total{peer="a\"b\\c\nd"} 7"#));
    }
}
