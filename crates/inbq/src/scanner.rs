use anyhow::anyhow;

use crate::ast::{Token, TokenType};

pub struct Scanner {
    source_chars: Vec<char>,
    tokens: Vec<Token>,
    start: usize,
    current: usize,
    line: u32,
    col: u32,
    open_type_brackets: Option<u32>,
}

impl Scanner {
    pub fn new(source: &str) -> Self {
        Self {
            source_chars: source.chars().collect(),
            tokens: vec![],
            start: 0,
            current: 0,
            line: 1,
            col: 0,
            open_type_brackets: None,
        }
    }

    pub fn tokens(&self) -> &Vec<Token> {
        &self.tokens
    }

    fn advance(&mut self) -> char {
        let c = self.source_chars[self.current];
        self.current += 1;
        self.col += 1;
        c
    }

    fn n_advance(&mut self, n: usize) -> char {
        debug_assert!(n > 0);
        let mut c = self.advance();
        for _ in 1..n {
            c = self.advance();
        }
        c
    }

    fn is_at_end(&self) -> bool {
        self.current >= self.source_chars.len()
    }

    fn peek(&self) -> char {
        if self.is_at_end() {
            '\0'
        } else {
            self.source_chars[self.current]
        }
    }

    #[allow(dead_code)]
    fn peek_prev(&self) -> Option<char> {
        self.peek_prev_i(1)
    }

    fn peek_prev_i(&self, i: usize) -> Option<char> {
        let idx = self.current.checked_sub(i)?;
        Some(self.source_chars[idx])
    }

    fn peek_next_i(&mut self, i: usize) -> char {
        if self.current + i >= self.source_chars.len() {
            '\0'
        } else {
            self.source_chars[self.current + i]
        }
    }

    fn n_peek(&mut self, n: usize) -> Option<&[char]> {
        self.source_chars.get(self.current..self.current + n)
    }

    fn match_char(&mut self, expected: char) -> bool {
        if self.peek() != expected {
            return false;
        };

        self.current += 1;
        true
    }

    fn add_token(&mut self, token_type: TokenType) {
        self.tokens.push(Token {
            kind: token_type,
            lexeme: self.source_chars[self.start..self.current].iter().collect(),
            line: self.line,
            col: self.col,
        });
    }

    fn current_source_str(&self) -> String {
        self.source_chars[self.start..self.current].iter().collect()
    }

    fn reset(&mut self) {
        self.tokens.clear();
        self.start = 0;
        self.current = 0;
        self.col = 1;
        self.line = 1;
    }

    fn new_line(&mut self) {
        self.line += 1;
        self.col = 1;
    }

    pub fn scan(&mut self) -> anyhow::Result<()> {
        self.reset();
        while self.current < self.source_chars.len() {
            self.start = self.current;
            self.scan_token()?;
        }
        self.tokens.push(Token {
            kind: TokenType::Eof,
            lexeme: String::from("eof"),
            line: self.line,
            col: self.col,
        });

        Ok(())
    }

    fn is_raw_string(&mut self, c: char) -> bool {
        let next_c = self.peek();
        (c == 'r' || c == 'R') && (next_c == '\'' || next_c == '"')
    }

    fn is_bytes(&mut self, c: char) -> bool {
        let next_c = self.peek();
        (c == 'b' || c == 'B') && (next_c == '\'' || next_c == '"')
    }

    fn is_raw_bytes(&mut self, c: char) -> bool {
        let next_c = self.peek();
        let next_next_c = self.peek_next_i(1);
        (c == 'b' || c == 'B' || c == 'r' || c == 'R')
            && ((c == 'b' && next_c == 'r')
                || (c == 'B' && next_c == 'R')
                || (c == 'r' && next_c == 'b')
                || (c == 'R' && next_c == 'B'))
            && (next_next_c == '\'' || next_next_c == '"')
    }

    fn is_escaped(&self) -> bool {
        let mut escaped = false;
        let mut i = 1;
        while self.peek_prev_i(i).is_some_and(|p| p == '\\') {
            escaped = !escaped;
            i += 1;
        }
        escaped
    }

    fn scan_string(&mut self, delimiter: char) -> anyhow::Result<()> {
        loop {
            let peek_char = self.peek();
            if peek_char == '\0' {
                return Err(anyhow!(self.error_str("Found unterminated string")));
            }
            let escaped = self.is_escaped();

            if !escaped && self.match_char(delimiter) {
                break;
            }
            self.advance();
        }
        Ok(())
    }

    fn scan_triple_quoted_string(&mut self, delimiter: char) -> anyhow::Result<()> {
        loop {
            let peek_char = self.peek();
            if peek_char == '\0' {
                return Err(anyhow!(self.error_str("Found unterminated string")));
            }
            let escaped = self.is_escaped();
            if !escaped && self.match_char(delimiter) {
                let curr = self.current - 1;
                if self.match_char(delimiter) && self.match_char(delimiter) {
                    break;
                } else {
                    self.current = curr;
                }
            }
            if peek_char == '\n' {
                self.new_line();
            }
            self.advance();
        }
        Ok(())
    }

    fn string_slice(&mut self, start_offset: usize, end_offset: usize) -> String {
        self.source_chars[self.start + 1 + start_offset..self.current - 1 - end_offset]
            .iter()
            .collect::<String>()
    }

    fn match_number(&mut self) -> anyhow::Result<()> {
        let mut found_dot = false;
        let mut found_e = false;
        loop {
            let peek_char = self.peek();

            if peek_char == '\0' {
                self.add_token(TokenType::Number(
                    self.source_chars[self.start..self.current]
                        .iter()
                        .collect::<String>(),
                ));
                break;
            }

            if peek_char == '.' {
                if found_dot || found_e {
                    return Err(anyhow!(self.error_str("Found invalid number")));
                }
                found_dot = true;
                self.advance();
            } else if peek_char == 'e' || peek_char == 'E' {
                if found_e {
                    return Err(anyhow!(self.error_str("Found invalid number")));
                }
                found_e = true;
                let peek_next_char = self.peek_next_i(1);
                if peek_next_char == '+' || peek_next_char == '-' {
                    self.advance();
                    if !(self.peek_next_i(1).is_ascii_digit()) {
                        return Err(anyhow!(self.error_str("Found invalid number")));
                    }
                    self.advance();
                } else if peek_next_char.is_ascii_digit() {
                    self.advance();
                } else {
                    return Err(anyhow!(self.error_str("Found invalid number")));
                }
            } else if peek_char.is_ascii_digit() {
                self.advance();
            } else {
                self.add_token(TokenType::Number(
                    self.source_chars[self.start..self.current]
                        .iter()
                        .collect::<String>(),
                ));
                break;
            }
        }

        Ok(())
    }

    fn match_string(&mut self, delimiter: char) -> anyhow::Result<()> {
        self.scan_string(delimiter)?;
        let str_slice = self.string_slice(0, 0);
        self.add_token(TokenType::String(str_slice));
        Ok(())
    }

    fn match_triple_quoted_string(&mut self, delimiter: char) -> anyhow::Result<()> {
        self.scan_triple_quoted_string(delimiter)?;
        let str_slice = self.string_slice(2, 2);
        self.add_token(TokenType::String(str_slice));
        Ok(())
    }

    fn match_bytes(&mut self, delimiter: char) -> anyhow::Result<()> {
        self.scan_string(delimiter)?;
        let str_slice = self.string_slice(1, 0);
        self.add_token(TokenType::Bytes(str_slice));
        Ok(())
    }

    fn match_triple_quoted_bytes(&mut self, delimiter: char) -> anyhow::Result<()> {
        self.scan_triple_quoted_string(delimiter)?;
        let str_slice = self.string_slice(3, 2);
        self.add_token(TokenType::Bytes(str_slice));
        Ok(())
    }

    fn match_raw_bytes(&mut self, delimiter: char) -> anyhow::Result<()> {
        self.scan_string(delimiter)?;
        let str_slice = self.string_slice(2, 0);
        self.add_token(TokenType::RawBytes(str_slice));
        Ok(())
    }

    fn match_triple_quoted_raw_bytes(&mut self, delimiter: char) -> anyhow::Result<()> {
        self.scan_triple_quoted_string(delimiter)?;
        let str_slice = self.string_slice(4, 2);
        self.add_token(TokenType::RawBytes(str_slice));
        Ok(())
    }

    fn match_raw_string(&mut self, delimiter: char) -> anyhow::Result<()> {
        self.scan_string(delimiter)?;
        let str_slice = self.string_slice(1, 0);
        self.add_token(TokenType::RawString(str_slice));
        Ok(())
    }

    fn match_triple_quoted_raw_string(&mut self, delimiter: char) -> anyhow::Result<()> {
        self.scan_triple_quoted_string(delimiter)?;
        let str_slice = self.string_slice(3, 2);
        self.add_token(TokenType::RawString(str_slice));
        Ok(())
    }

    fn match_reserved_keyword(&mut self, token_type: TokenType) {
        if let Some(Token {
            kind: TokenType::Dot,
            ..
        }) = self.tokens.last()
        {
            self.add_token(TokenType::Identifier(self.current_source_str()));
            return;
        }
        self.add_token(token_type);
    }

    fn match_keyword_or_identifier(&mut self) {
        loop {
            let peek_char = self.peek();
            if !(peek_char.is_alphanumeric() || peek_char == '_') {
                break;
            }
            self.advance();
        }
        let chars = &self.source_chars[self.start..self.current];

        if let Some(token_type) = Self::match_token_keyword(chars) {
            match token_type {
                TokenType::Array => {
                    self.match_reserved_keyword(TokenType::Array);
                    if self.peek() == '<' && self.open_type_brackets.is_none() {
                        self.open_type_brackets = Some(0);
                    }
                }
                TokenType::Struct => {
                    self.match_reserved_keyword(TokenType::Struct);
                    if self.peek() == '<' && self.open_type_brackets.is_none() {
                        self.open_type_brackets = Some(0);
                    }
                }
                _ => self.match_reserved_keyword(token_type),
            }
        } else {
            self.add_token(TokenType::Identifier(self.current_source_str()));
        }
    }

    fn match_token_keyword(chars: &[char]) -> Option<TokenType> {
        fn chars_match_keyword(chars: &[char], keyword: &[u8]) -> bool {
            chars
                .iter()
                .zip(keyword)
                .all(|(&c, &b)| c.to_ascii_lowercase() as u8 == b)
        }
        macro_rules! match_token_kw {
            ($chars:expr, $kw:expr, $tok:expr) => {
                if chars_match_keyword($chars, $kw) {
                    return Some($tok);
                }
            };
        }
        match chars.len() {
            2 => {
                match_token_kw!(chars, b"as", TokenType::As);
                match_token_kw!(chars, b"at", TokenType::At);
                match_token_kw!(chars, b"by", TokenType::By);
                match_token_kw!(chars, b"if", TokenType::If);
                match_token_kw!(chars, b"in", TokenType::In);
                match_token_kw!(chars, b"is", TokenType::Is);
                match_token_kw!(chars, b"no", TokenType::No);
                match_token_kw!(chars, b"of", TokenType::Of);
                match_token_kw!(chars, b"on", TokenType::On);
                match_token_kw!(chars, b"or", TokenType::Or);
                match_token_kw!(chars, b"to", TokenType::To);
                None
            }
            3 => {
                match_token_kw!(chars, b"all", TokenType::All);
                match_token_kw!(chars, b"and", TokenType::And);
                match_token_kw!(chars, b"any", TokenType::Any);
                match_token_kw!(chars, b"asc", TokenType::Asc);
                match_token_kw!(chars, b"end", TokenType::End);
                match_token_kw!(chars, b"for", TokenType::For);
                match_token_kw!(chars, b"new", TokenType::New);
                match_token_kw!(chars, b"not", TokenType::Not);
                match_token_kw!(chars, b"set", TokenType::Set);
                None
            }
            4 => {
                match_token_kw!(chars, b"case", TokenType::Case);
                match_token_kw!(chars, b"cast", TokenType::Cast);
                match_token_kw!(chars, b"cube", TokenType::Cube);
                match_token_kw!(chars, b"desc", TokenType::Desc);
                match_token_kw!(chars, b"else", TokenType::Else);
                match_token_kw!(chars, b"enum", TokenType::Enum);
                match_token_kw!(chars, b"from", TokenType::From);
                match_token_kw!(chars, b"full", TokenType::Full);
                match_token_kw!(chars, b"hash", TokenType::Hash);
                match_token_kw!(chars, b"into", TokenType::Into);
                match_token_kw!(chars, b"join", TokenType::Join);
                match_token_kw!(chars, b"left", TokenType::Left);
                match_token_kw!(chars, b"like", TokenType::Like);
                match_token_kw!(chars, b"null", TokenType::Null);
                match_token_kw!(chars, b"over", TokenType::Over);
                match_token_kw!(chars, b"rows", TokenType::Rows);
                match_token_kw!(chars, b"some", TokenType::Some);
                match_token_kw!(chars, b"then", TokenType::Then);
                match_token_kw!(chars, b"true", TokenType::True);
                match_token_kw!(chars, b"when", TokenType::When);
                match_token_kw!(chars, b"with", TokenType::With);
                None
            }
            5 => {
                match_token_kw!(chars, b"array", TokenType::Array);
                match_token_kw!(chars, b"cross", TokenType::Cross);
                match_token_kw!(chars, b"false", TokenType::False);
                match_token_kw!(chars, b"fetch", TokenType::Fetch);
                match_token_kw!(chars, b"group", TokenType::Group);
                match_token_kw!(chars, b"inner", TokenType::Inner);
                match_token_kw!(chars, b"limit", TokenType::Limit);
                match_token_kw!(chars, b"merge", TokenType::Merge);
                match_token_kw!(chars, b"nulls", TokenType::Nulls);
                match_token_kw!(chars, b"order", TokenType::Order);
                match_token_kw!(chars, b"outer", TokenType::Outer);
                match_token_kw!(chars, b"proto", TokenType::Proto);
                match_token_kw!(chars, b"range", TokenType::Range);
                match_token_kw!(chars, b"right", TokenType::Right);
                match_token_kw!(chars, b"treat", TokenType::Treat);
                match_token_kw!(chars, b"union", TokenType::Union);
                match_token_kw!(chars, b"using", TokenType::Using);
                match_token_kw!(chars, b"where", TokenType::Where);
                None
            }
            6 => {
                match_token_kw!(chars, b"create", TokenType::Create);
                match_token_kw!(chars, b"define", TokenType::Define);
                match_token_kw!(chars, b"escape", TokenType::Escape);
                match_token_kw!(chars, b"except", TokenType::Except);
                match_token_kw!(chars, b"exists", TokenType::Exists);
                match_token_kw!(chars, b"groups", TokenType::Groups);
                match_token_kw!(chars, b"having", TokenType::Having);
                match_token_kw!(chars, b"ignore", TokenType::Ignore);
                match_token_kw!(chars, b"lookup", TokenType::Lookup);
                match_token_kw!(chars, b"rollup", TokenType::Rollup);
                match_token_kw!(chars, b"select", TokenType::Select);
                match_token_kw!(chars, b"struct", TokenType::Struct);
                match_token_kw!(chars, b"unnest", TokenType::Unnest);
                match_token_kw!(chars, b"window", TokenType::Window);
                match_token_kw!(chars, b"within", TokenType::Within);
                None
            }
            7 => {
                match_token_kw!(chars, b"between", TokenType::Between);
                match_token_kw!(chars, b"collate", TokenType::Collate);
                match_token_kw!(chars, b"current", TokenType::Current);
                match_token_kw!(chars, b"default", TokenType::Default);
                match_token_kw!(chars, b"exclude", TokenType::Exclude);
                match_token_kw!(chars, b"extract", TokenType::Extract);
                match_token_kw!(chars, b"lateral", TokenType::Lateral);
                match_token_kw!(chars, b"natural", TokenType::Natural);
                match_token_kw!(chars, b"qualify", TokenType::Qualify);
                match_token_kw!(chars, b"respect", TokenType::Respect);
                None
            }
            8 => {
                match_token_kw!(chars, b"contains", TokenType::Contains);
                match_token_kw!(chars, b"distinct", TokenType::Distinct);
                match_token_kw!(chars, b"grouping", TokenType::Grouping);
                match_token_kw!(chars, b"interval", TokenType::Interval);
                None
            }
            9 => {
                match_token_kw!(chars, b"following", TokenType::Following);
                match_token_kw!(chars, b"intersect", TokenType::Intersect);
                match_token_kw!(chars, b"partition", TokenType::Partition);
                match_token_kw!(chars, b"preceding", TokenType::Preceding);
                match_token_kw!(chars, b"recursive", TokenType::Recursive);
                None
            }
            11 => {
                match_token_kw!(chars, b"tablesample", TokenType::Tablesample);
                None
            }
            20 => {
                match_token_kw!(
                    chars,
                    b"assert_rows_modified",
                    TokenType::AssertRowsModified
                );
                None
            }
            _ => None,
        }
    }

    fn scan_token(&mut self) -> anyhow::Result<()> {
        let curr_char = self.advance();
        match curr_char {
            '(' => self.add_token(TokenType::LeftParen),
            ')' => self.add_token(TokenType::RightParen),
            '[' => self.add_token(TokenType::LeftSquare),
            ']' => self.add_token(TokenType::RightSquare),
            '*' => self.add_token(TokenType::Star),
            ',' => self.add_token(TokenType::Comma),
            ':' => self.add_token(TokenType::Colon),
            ';' => self.add_token(TokenType::Semicolon),
            '.' => {
                let peek_char = self.peek();
                if peek_char.is_ascii_digit() {
                    self.match_number()?;
                } else {
                    self.add_token(TokenType::Dot);
                }
            }
            '+' => self.add_token(TokenType::Plus),
            '=' => {
                if self.match_char('>') {
                    self.add_token(TokenType::RightArrow);
                } else {
                    self.add_token(TokenType::Equal)
                }
            }
            '/' => {
                if self.match_char('*') {
                    loop {
                        if self.peek() == '\0' {
                            return Err(anyhow!(self.error_str("Found unterminated comment")));
                        }
                        if self.peek() == '\n' {
                            self.new_line();
                        }
                        let peek_chars = self.n_peek(2);
                        if peek_chars.is_some()
                            && peek_chars
                                .unwrap()
                                .iter()
                                .zip("*/".chars())
                                .all(|(&c1, c2)| c1 == c2)
                        {
                            self.n_advance(2);
                            break;
                        }
                        self.advance();
                    }
                } else {
                    self.add_token(TokenType::Slash)
                }
            }
            '#' => loop {
                let peek_char = self.peek();
                if peek_char == '\n' || peek_char == '\0' {
                    break;
                }
                self.advance();
            },
            '-' => {
                if self.match_char('-') {
                    loop {
                        let peek_char = self.peek();
                        if peek_char == '\n' || peek_char == '\0' {
                            break;
                        }
                        self.advance();
                    }
                } else {
                    self.add_token(TokenType::Minus)
                }
            }
            '<' => {
                if self.match_char('>') {
                    self.add_token(TokenType::NotEqual);
                } else if self.match_char('=') {
                    self.add_token(TokenType::LessEqual);
                } else if self.match_char('<') {
                    self.add_token(TokenType::BitwiseLeftShift);
                } else {
                    if self.open_type_brackets.is_some() {
                        self.open_type_brackets = self.open_type_brackets.map(|n| n + 1);
                    }
                    self.add_token(TokenType::Less);
                }
            }
            '!' => {
                if self.match_char('=') {
                    self.add_token(TokenType::BangEqual);
                } else {
                    self.add_token(TokenType::Bang);
                }
            }
            '>' => {
                if self.match_char('=') {
                    self.add_token(TokenType::GreaterEqual);
                } else if self.peek() == '>' {
                    if self.open_type_brackets.is_some() {
                        self.open_type_brackets = self.open_type_brackets.map(|n| n - 1);
                        self.add_token(TokenType::Greater);
                    } else {
                        self.match_char('>');
                        self.add_token(TokenType::BitwiseRightShift);
                    }
                } else {
                    if self.open_type_brackets.is_some() {
                        self.open_type_brackets = self.open_type_brackets.and_then(|n| {
                            let new_n = n - 1;
                            if new_n == 0 { None } else { Some(new_n) }
                        });
                    }
                    self.add_token(TokenType::Greater);
                }
            }
            '~' => {
                self.add_token(TokenType::BitwiseNot);
            }
            '&' => {
                self.add_token(TokenType::BitwiseAnd);
            }
            '|' => {
                if self.match_char('|') {
                    self.add_token(TokenType::ConcatOperator);
                } else {
                    self.add_token(TokenType::BitwiseOr);
                }
            }
            '^' => {
                self.add_token(TokenType::BitwiseXor);
            }
            '\n' => {
                self.new_line();
            }
            '\r' | ' ' | '\t' => {}

            // strings
            c if c == '\'' || c == '"' => {
                let peek = self.peek();
                if peek == c && peek == self.peek_next_i(1) {
                    self.advance();
                    self.advance();
                    self.match_triple_quoted_string(c)?;
                } else {
                    self.match_string(c)?;
                }
            }

            // raw strings
            c if self.is_raw_string(c) => {
                let peek_next = self.peek_next_i(1);
                if self.peek() == peek_next && peek_next == self.peek_next_i(2) {
                    self.advance();
                    self.advance();
                    let delimiter = self.advance();
                    self.match_triple_quoted_raw_string(delimiter)?;
                } else {
                    let delimiter = self.advance();
                    self.match_raw_string(delimiter)?;
                }
            }

            // bytes
            c if self.is_bytes(c) => {
                let peek_next = self.peek_next_i(1);
                if self.peek() == peek_next && peek_next == self.peek_next_i(2) {
                    self.advance();
                    let delimiter = self.advance();
                    self.match_triple_quoted_bytes(delimiter)?;
                } else {
                    let delimiter = self.advance();
                    self.match_bytes(delimiter)?;
                }
            }

            // raw bytes
            c if self.is_raw_bytes(c) => {
                let peek_next_next = self.peek_next_i(2);
                if self.peek_next_i(1) == peek_next_next && peek_next_next == self.peek_next_i(3) {
                    self.advance();
                    self.advance();
                    self.advance();
                    let delimiter = self.advance();
                    self.match_triple_quoted_raw_bytes(delimiter)?;
                } else {
                    self.advance();
                    let delimiter = self.advance();
                    self.match_raw_bytes(delimiter)?;
                }
            }

            // numeric
            c if c.is_ascii_digit() => {
                self.match_number()?;
            }

            // Keywords and identifiers
            c if c.is_alphabetic() || c == '_' => {
                self.match_keyword_or_identifier();
            }

            // Query named parameter or System variable
            '@' => {
                let is_system_variable = self.match_char('@');
                loop {
                    let peek_char = self.peek();
                    if !(peek_char.is_alphanumeric() || peek_char == '_') {
                        break;
                    }
                    self.advance();
                }
                if is_system_variable {
                    self.add_token(TokenType::SystemVariable(
                        self.source_chars[self.start + 2..self.current]
                            .iter()
                            .collect(),
                    ));
                } else {
                    self.add_token(TokenType::QueryNamedParameter(
                        self.source_chars[self.start + 1..self.current]
                            .iter()
                            .collect(),
                    ));
                }
            }

            // Query positional parameter
            '?' => {
                self.add_token(TokenType::QueryPositionalParameter);
            }

            '`' => {
                let quoted_ident_start_idx = self.current - 1;
                loop {
                    let curr_char = self.advance();
                    if curr_char == '`' {
                        let quoted_ident_end_idx = self.current - 1;
                        if quoted_ident_end_idx == quoted_ident_start_idx + 1 {
                            return Err(anyhow!(self.error_str("Found empty quoted identifier.")));
                        }
                        self.add_token(TokenType::QuotedIdentifier(
                            self.source_chars[(quoted_ident_start_idx + 1)..quoted_ident_end_idx]
                                .iter()
                                .collect::<String>(),
                        ));
                        break;
                    }
                    if self.peek() == '\0' {
                        return Err(anyhow!(
                            self.error_str("Found unterminated quoted identifier")
                        ));
                    }
                }
            }

            _ => {
                return Err(anyhow!(self.error_str(&format!(
                    "Found unexpected character while scanning: {}",
                    curr_char
                ))));
            }
        }
        Ok(())
    }

    fn error_str(&mut self, error: &str) -> String {
        format!(
            "[line: {}, col: {}] Scanner error: {}",
            self.line, self.col, error
        )
    }
}
