//! A minimal arrow-key interactive selector, with a numbered-prompt
//! fallback when stdin/stdout aren't attached to a TTY.

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::{cursor, style, terminal, ExecutableCommand, QueueableCommand};
use std::io::{self, IsTerminal, Write};

/// One selectable row: the primary label plus a short trailing hint.
pub struct MenuItem {
    pub label: String,
    pub hint: String,
}

/// Prompt the user to choose one of `items`. Returns `None` if the user
/// cancelled (Esc/q/Ctrl-C in the interactive path, or blank/invalid input
/// in the non-interactive fallback).
pub fn select(title: &str, items: &[MenuItem]) -> Option<usize> {
    if items.is_empty() {
        return None;
    }
    if io::stdout().is_terminal() && io::stdin().is_terminal() {
        interactive_select(title, items)
    } else {
        plain_select(title, items)
    }
}

fn plain_select(title: &str, items: &[MenuItem]) -> Option<usize> {
    println!("{title}");
    for (i, item) in items.iter().enumerate() {
        println!("  {:>3}) {}  {}", i + 1, item.label, item.hint);
    }
    print!("> ");
    let _ = io::stdout().flush();

    let mut line = String::new();
    if io::stdin().read_line(&mut line).is_err() {
        return None;
    }
    let n: usize = line.trim().parse().ok()?;
    if n >= 1 && n <= items.len() {
        Some(n - 1)
    } else {
        None
    }
}

fn interactive_select(title: &str, items: &[MenuItem]) -> Option<usize> {
    let mut stdout = io::stdout();

    // Printed in cooked mode, before raw mode changes newline handling.
    println!("{title}");
    println!("(\u{2191}/\u{2193} or j/k to move, enter to choose, q/esc to cancel)");
    let _ = stdout.flush();

    if terminal::enable_raw_mode().is_err() {
        // Can't get a raw terminal — fall back rather than hang forever
        // waiting on an event stream that won't behave as expected.
        return plain_select(title, items);
    }
    let _ = stdout.execute(cursor::Hide);

    for (i, item) in items.iter().enumerate() {
        let _ = stdout.queue(style::Print(render_item(item, i == 0)));
        let _ = stdout.queue(cursor::MoveToNextLine(1));
    }
    let _ = stdout.flush();

    let mut selected = 0usize;
    let mut result = None;

    loop {
        match event::read() {
            Ok(Event::Key(k)) if k.kind != KeyEventKind::Release => {
                let prev = selected;
                match k.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        selected = if selected == 0 {
                            items.len() - 1
                        } else {
                            selected - 1
                        };
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        selected = (selected + 1) % items.len();
                    }
                    KeyCode::Enter => {
                        result = Some(selected);
                        break;
                    }
                    KeyCode::Esc | KeyCode::Char('q') => break,
                    KeyCode::Char('c') if k.modifiers.contains(KeyModifiers::CONTROL) => break,
                    _ => continue,
                }
                if selected != prev {
                    redraw(&mut stdout, items, selected);
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }

    let _ = terminal::disable_raw_mode();
    let _ = stdout.execute(cursor::Show);
    // Raw mode ate the newline handling — restore a clean prompt line.
    let _ = stdout.execute(style::Print("\r\n"));
    result
}

fn redraw(stdout: &mut io::Stdout, items: &[MenuItem], selected: usize) {
    let _ = stdout.queue(cursor::MoveToPreviousLine(items.len() as u16));
    for (i, item) in items.iter().enumerate() {
        let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
        let _ = stdout.queue(style::Print(render_item(item, i == selected)));
        let _ = stdout.queue(cursor::MoveToNextLine(1));
    }
    let _ = stdout.flush();
}

fn render_item(item: &MenuItem, active: bool) -> String {
    if active {
        format!("\x1b[7m\u{25b8} {}  {}\x1b[0m", item.label, item.hint)
    } else {
        format!("  {}  {}", item.label, item.hint)
    }
}
