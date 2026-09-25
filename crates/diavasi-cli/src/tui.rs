//! Ratatui dashboard. It is an HTTP client of the control plane.

use std::io;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use ratatui::Frame;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Paragraph, Row, Table, Wrap};
use serde::Deserialize;

const REFRESH: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Probe {
    Unknown,
    Ok,
    Down,
}

struct GroupLine {
    id: String,
    running: bool,
    lifecycle: String,
}

struct Detail {
    running: bool,
    lifecycle: String,
    committed: String,
    fetched: String,
    buffer_records: u64,
    buffer_bytes: u64,
    inflight_records: u64,
    consumers: String,
    records_fetched: u64,
    records_delivered: u64,
    records_acked: u64,
    records_replayed: u64,
    bytes: u64,
    checkpoint_lag: u64,
    restarts: u64,
    consumer_disconnects: u64,
    adapter_errors: u64,
    last_stop_reason: String,
    recovered: bool,
}

struct Dashboard {
    url: String,
    health: Probe,
    ready: Probe,
    version: String,
    running_groups: usize,
    groups: Vec<GroupLine>,
    selected: usize,
    detail: Option<Detail>,
    error: Option<String>,
}

enum Effect {
    None,
    Quit,
    Refresh,
    Post(String),
}

pub async fn run(base: &str, token: &str) -> Result<(), ExitCode> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|err| {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        })?;
    let mut dash = Dashboard::new(base);
    let mut terminal = ratatui::try_init().map_err(|err| {
        eprintln!("error: {err}");
        ExitCode::FAILURE
    })?;
    let result = drive(&mut terminal, &http, base, token, &mut dash).await;
    ratatui::restore();
    result
}

async fn drive(
    terminal: &mut ratatui::DefaultTerminal,
    http: &reqwest::Client,
    base: &str,
    token: &str,
    dash: &mut Dashboard,
) -> Result<(), ExitCode> {
    let mut next_refresh = Instant::now();
    loop {
        if Instant::now() >= next_refresh {
            dash.apply(load(http, base, token, dash.selected_id()).await);
            next_refresh = Instant::now() + REFRESH;
        }
        terminal.draw(|frame| render(frame, dash)).map_err(|err| {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        })?;
        let wait = next_refresh.saturating_duration_since(Instant::now());
        if !event::poll(wait).map_err(io_exit)? {
            continue;
        }
        let Event::Key(key) = event::read().map_err(io_exit)? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return Ok(());
        }
        match on_key(dash, key.code) {
            Effect::Quit => return Ok(()),
            Effect::None => {}
            Effect::Refresh => next_refresh = Instant::now(),
            Effect::Post(path) => {
                if let Err(err) = post(http, base, token, &path).await {
                    dash.error = Some(err);
                }
                next_refresh = Instant::now();
            }
        }
    }
}

fn io_exit(err: io::Error) -> ExitCode {
    eprintln!("error: {err}");
    ExitCode::FAILURE
}

fn on_key(dash: &mut Dashboard, code: KeyCode) -> Effect {
    match code {
        KeyCode::Char('q') | KeyCode::Esc => Effect::Quit,
        KeyCode::Char('r') => Effect::Refresh,
        KeyCode::Char('j') | KeyCode::Down => {
            if !dash.groups.is_empty() {
                dash.selected = (dash.selected + 1).min(dash.groups.len() - 1);
            }
            Effect::Refresh
        }
        KeyCode::Char('k') | KeyCode::Up => {
            dash.selected = dash.selected.saturating_sub(1);
            Effect::Refresh
        }
        KeyCode::Char('s') => dash
            .selected_id()
            .map(|id| Effect::Post(format!("/v1/groups/{id}/start")))
            .unwrap_or(Effect::None),
        KeyCode::Char('p') => dash
            .selected_id()
            .map(|id| Effect::Post(format!("/v1/groups/{id}/pause")))
            .unwrap_or(Effect::None),
        KeyCode::Char('d') => dash
            .selected_id()
            .map(|id| Effect::Post(format!("/v1/groups/{id}/drain")))
            .unwrap_or(Effect::None),
        _ => Effect::None,
    }
}

fn render(frame: &mut Frame, dash: &Dashboard) {
    let [header_area, body_area, footer_area] = Layout::vertical([
        Constraint::Length(4),
        Constraint::Fill(1),
        Constraint::Length(3),
    ])
    .areas(frame.area());

    let header = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("diavasi", Style::new().add_modifier(Modifier::BOLD)),
            Span::raw(format!("  {}", dash.url)),
        ]),
        Line::from(vec![
            Span::raw("health "),
            probe_span(dash.health),
            Span::raw("  ready "),
            probe_span(dash.ready),
            Span::raw(format!(
                "  version {}  running {}",
                dash.version, dash.running_groups
            )),
        ]),
    ])
    .block(Block::bordered().title("server"));
    frame.render_widget(header, header_area);

    let (list_area, detail_area) = if body_area.width < 80 {
        let [top, bottom] =
            Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)])
                .areas(body_area);
        (top, bottom)
    } else {
        let [left, right] =
            Layout::horizontal([Constraint::Percentage(42), Constraint::Percentage(58)])
                .areas(body_area);
        (left, right)
    };

    let rows: Vec<Row> = dash
        .groups
        .iter()
        .enumerate()
        .map(|(index, group)| {
            let style = if index == dash.selected {
                Style::new().add_modifier(Modifier::REVERSED)
            } else {
                Style::new()
            };
            Row::new([
                Cell::from(group.id.as_str()),
                Cell::from(if group.running { "yes" } else { "no" }),
                Cell::from(group.lifecycle.as_str()),
            ])
            .style(style)
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Fill(1),
            Constraint::Length(8),
            Constraint::Length(12),
        ],
    )
    .header(
        Row::new(["group", "running", "lifecycle"])
            .style(Style::new().add_modifier(Modifier::BOLD)),
    )
    .block(Block::bordered().title("groups"));
    frame.render_widget(table, list_area);

    let detail_text = match &dash.detail {
        None => "No group selected.".to_string(),
        Some(detail) => format!(
            "running: {}    lifecycle: {}    recovered: {}\n\
             committed: {}\n\
             fetched: {}\n\
             buffer: {} records, {} bytes    inflight: {}    lag: {}\n\
             fetched/delivered/acked/replayed: {}/{}/{}/{}\n\
             bytes: {}    restarts: {}    disconnects: {}    adapter errors: {}\n\
             consumers: {}\n\
             last stop: {}",
            detail.running,
            detail.lifecycle,
            detail.recovered,
            detail.committed,
            detail.fetched,
            detail.buffer_records,
            detail.buffer_bytes,
            detail.inflight_records,
            detail.checkpoint_lag,
            detail.records_fetched,
            detail.records_delivered,
            detail.records_acked,
            detail.records_replayed,
            detail.bytes,
            detail.restarts,
            detail.consumer_disconnects,
            detail.adapter_errors,
            if detail.consumers.is_empty() {
                "-"
            } else {
                detail.consumers.as_str()
            },
            detail.last_stop_reason,
        ),
    };
    let mut body = ratatui::text::Text::default();
    for line in detail_text.lines() {
        body.push_line(Line::from(line.to_string()));
    }
    if let Some(err) = &dash.error {
        body.push_line(Line::styled(err.clone(), Style::new().fg(Color::Red)));
    }
    let detail = Paragraph::new(body)
        .wrap(Wrap { trim: false })
        .block(Block::bordered().title("diagnostics"));
    frame.render_widget(detail, detail_area);

    let footer = Paragraph::new("j/k move    s start    p pause    d drain    r refresh    q quit")
        .block(Block::bordered().title("keys"));
    frame.render_widget(footer, footer_area);
}

fn probe_span(probe: Probe) -> Span<'static> {
    match probe {
        Probe::Ok => Span::styled("ok", Style::new().fg(Color::Green)),
        Probe::Down => Span::styled("down", Style::new().fg(Color::Red)),
        Probe::Unknown => Span::styled("...", Style::new().fg(Color::Yellow)),
    }
}

impl Dashboard {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            health: Probe::Unknown,
            ready: Probe::Unknown,
            version: "-".into(),
            running_groups: 0,
            groups: Vec::new(),
            selected: 0,
            detail: None,
            error: None,
        }
    }

    fn selected_id(&self) -> Option<&str> {
        self.groups
            .get(self.selected)
            .map(|group| group.id.as_str())
    }

    fn apply(&mut self, refresh: Refresh) {
        let keep = self.selected_id().map(str::to_string);
        self.health = refresh.health;
        self.ready = refresh.ready;
        self.version = refresh.version;
        self.running_groups = refresh.running_groups;
        self.groups = refresh.groups;
        self.detail = refresh.detail;
        self.error = refresh.error;
        if self.groups.is_empty() {
            self.selected = 0;
            return;
        }
        if let Some(id) = keep {
            if let Some(index) = self.groups.iter().position(|group| group.id == id) {
                self.selected = index;
                return;
            }
        }
        self.selected = self.selected.min(self.groups.len() - 1);
    }
}

struct Refresh {
    health: Probe,
    ready: Probe,
    version: String,
    running_groups: usize,
    groups: Vec<GroupLine>,
    detail: Option<Detail>,
    error: Option<String>,
}

async fn load(http: &reqwest::Client, base: &str, token: &str, selected: Option<&str>) -> Refresh {
    let health_url = format!("{base}/health");
    let ready_url = format!("{base}/ready");
    let (health, ready, status, groups) = tokio::join!(
        probe(http, &health_url),
        probe(http, &ready_url),
        get_json(http, base, token, "/v1/status"),
        get_json(http, base, token, "/v1/groups"),
    );
    let mut error = None;
    let (version, running_groups) = match status {
        Ok(value) => (
            value["version"].as_str().unwrap_or("-").to_string(),
            value["running_groups"]
                .as_array()
                .map(|groups| groups.len())
                .unwrap_or(0),
        ),
        Err(err) => {
            error = Some(err);
            ("-".into(), 0)
        }
    };
    let groups = match groups {
        Ok(value) => match serde_json::from_value::<Vec<GroupBody>>(value) {
            Ok(groups) => groups
                .into_iter()
                .map(|group| GroupLine {
                    id: group.group_id,
                    running: group.running,
                    lifecycle: group.lifecycle,
                })
                .collect(),
            Err(err) => {
                error = Some(format!("groups: {err}"));
                Vec::new()
            }
        },
        Err(err) => {
            error = Some(err);
            Vec::new()
        }
    };
    let selected = selected
        .map(str::to_string)
        .or_else(|| groups.first().map(|group| group.id.clone()));
    let detail = if let Some(id) = selected {
        match get_json(http, base, token, &format!("/v1/groups/{id}/diagnostics")).await {
            Ok(value) => match serde_json::from_value::<DiagnosticsBody>(value) {
                Ok(body) => Some(body.into_detail()),
                Err(err) => {
                    error = Some(format!("diagnostics: {err}"));
                    None
                }
            },
            Err(err) => {
                error = Some(err);
                None
            }
        }
    } else {
        None
    };
    Refresh {
        health,
        ready,
        version,
        running_groups,
        groups,
        detail,
        error,
    }
}

async fn post(http: &reqwest::Client, base: &str, token: &str, path: &str) -> Result<(), String> {
    let response = http
        .post(format!("{base}{path}"))
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .map_err(|err| err.to_string())?;
    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = response.text().await.unwrap_or_default();
        Err(format!("HTTP {status}: {body}"))
    }
}

async fn probe(http: &reqwest::Client, url: &str) -> Probe {
    match http.get(url).send().await {
        Ok(response) if response.status().is_success() => Probe::Ok,
        _ => Probe::Down,
    }
}

async fn get_json(
    http: &reqwest::Client,
    base: &str,
    token: &str,
    path: &str,
) -> Result<serde_json::Value, String> {
    let response = http
        .get(format!("{base}{path}"))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|err| err.to_string())?;
    let status = response.status();
    let text = response.text().await.map_err(|err| err.to_string())?;
    if !status.is_success() {
        return Err(format!("HTTP {status}: {text}"));
    }
    serde_json::from_str(&text).map_err(|err| err.to_string())
}

#[derive(Deserialize)]
struct GroupBody {
    group_id: String,
    running: bool,
    lifecycle: String,
}

#[derive(Deserialize)]
struct DiagnosticsBody {
    running: bool,
    lifecycle: String,
    committed_cursor: serde_json::Value,
    fetched_cursor: serde_json::Value,
    buffer_records: u64,
    buffer_bytes: u64,
    inflight_records: u64,
    consumers: Vec<String>,
    records_fetched: u64,
    records_delivered: u64,
    records_acked: u64,
    records_replayed: u64,
    bytes: u64,
    checkpoint_lag: u64,
    restarts: u64,
    consumer_disconnects: u64,
    adapter_errors: u64,
    last_stop_reason: Option<String>,
    recovered: bool,
}

impl DiagnosticsBody {
    fn into_detail(self) -> Detail {
        Detail {
            running: self.running,
            lifecycle: self.lifecycle,
            committed: cursor_text(&self.committed_cursor),
            fetched: cursor_text(&self.fetched_cursor),
            buffer_records: self.buffer_records,
            buffer_bytes: self.buffer_bytes,
            inflight_records: self.inflight_records,
            consumers: self.consumers.join(", "),
            records_fetched: self.records_fetched,
            records_delivered: self.records_delivered,
            records_acked: self.records_acked,
            records_replayed: self.records_replayed,
            bytes: self.bytes,
            checkpoint_lag: self.checkpoint_lag,
            restarts: self.restarts,
            consumer_disconnects: self.consumer_disconnects,
            adapter_errors: self.adapter_errors,
            last_stop_reason: self.last_stop_reason.unwrap_or_else(|| "-".into()),
            recovered: self.recovered,
        }
    }
}

fn cursor_text(value: &serde_json::Value) -> String {
    if value.is_null() {
        "-".into()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn sample() -> Dashboard {
        let mut dash = Dashboard::new("http://127.0.0.1:7700");
        dash.health = Probe::Ok;
        dash.ready = Probe::Ok;
        dash.version = "0.1.0".into();
        dash.running_groups = 1;
        dash.groups = vec![
            GroupLine {
                id: "alpha".into(),
                running: true,
                lifecycle: "Running".into(),
            },
            GroupLine {
                id: "beta".into(),
                running: false,
                lifecycle: "Stopped".into(),
            },
        ];
        dash.detail = Some(Detail {
            running: true,
            lifecycle: "Running".into(),
            committed: "[{\"U64\":4}]".into(),
            fetched: "[{\"U64\":8}]".into(),
            buffer_records: 4,
            buffer_bytes: 32,
            inflight_records: 2,
            consumers: "c1".into(),
            records_fetched: 8,
            records_delivered: 6,
            records_acked: 4,
            records_replayed: 2,
            bytes: 64,
            checkpoint_lag: 6,
            restarts: 1,
            consumer_disconnects: 0,
            adapter_errors: 0,
            last_stop_reason: "-".into(),
            recovered: false,
        });
        dash
    }

    fn view(dash: &Dashboard) -> String {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| render(frame, dash)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let mut text = String::new();
        for y in 0..buffer.area.height {
            for x in 0..buffer.area.width {
                text.push_str(buffer[(x, y)].symbol());
            }
            text.push('\n');
        }
        text
    }

    #[test]
    fn dashboard_shows_health_group_and_lag() {
        let text = view(&sample());
        assert!(text.contains("health"), "{text}");
        assert!(text.contains("alpha"), "{text}");
        assert!(text.contains("lag: 6"), "{text}");
        assert!(text.contains("8/6/4/2"), "{text}");
    }

    #[test]
    fn keys_move_selection_and_build_lifecycle_posts() {
        let mut dash = sample();
        assert!(matches!(
            on_key(&mut dash, KeyCode::Char('j')),
            Effect::Refresh
        ));
        assert_eq!(dash.selected_id(), Some("beta"));
        assert!(matches!(
            on_key(&mut dash, KeyCode::Char('p')),
            Effect::Post(path) if path == "/v1/groups/beta/pause"
        ));
        assert!(matches!(
            on_key(&mut dash, KeyCode::Char('k')),
            Effect::Refresh
        ));
        assert!(matches!(
            on_key(&mut dash, KeyCode::Char('s')),
            Effect::Post(path) if path == "/v1/groups/alpha/start"
        ));
        assert!(matches!(
            on_key(&mut dash, KeyCode::Char('d')),
            Effect::Post(path) if path == "/v1/groups/alpha/drain"
        ));
        assert!(matches!(
            on_key(&mut dash, KeyCode::Char('q')),
            Effect::Quit
        ));
    }

    #[test]
    fn refresh_keeps_the_selected_group() {
        let mut dash = sample();
        dash.selected = 1;
        dash.apply(Refresh {
            health: Probe::Ok,
            ready: Probe::Down,
            version: "0.1.0".into(),
            running_groups: 1,
            groups: vec![
                GroupLine {
                    id: "beta".into(),
                    running: false,
                    lifecycle: "Stopped".into(),
                },
                GroupLine {
                    id: "alpha".into(),
                    running: true,
                    lifecycle: "Running".into(),
                },
            ],
            detail: None,
            error: None,
        });
        assert_eq!(dash.selected_id(), Some("beta"));
        assert_eq!(dash.ready, Probe::Down);
    }
}
