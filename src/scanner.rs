use anyhow::anyhow;
use strum_macros::EnumDiscriminants;
use strum_macros::{AsRefStr, IntoStaticStr};

#[derive(PartialEq, Clone, Debug, EnumDiscriminants)]
#[strum_discriminants(name(TokenTypeVariant))]
pub enum TokenType {
    LeftParen,
    RightParen,
    LeftSquare,
    RightSquare,
    Comma,
    Dot,
    Minus,
    Plus,
    BitwiseNot,
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
    BitwiseRightShift,
    BitwiseLeftShift,
    Colon,
    Semicolon,
    Slash,
    Star,
    Tick,
    ConcatOperator,
    Bang,
    BangEqual,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    QuotedIdentifier(String),
    Identifier(String),
    String(String),
    Number(f64),
    Eof,

    // Reserved kewords
    Asc,
    Desc,
    Create,
    Recursive,
    And,
    Or,
    Not,
    True,
    False,
    Union,
    All,
    Exists,
    If,
    Distinct,
    Intersect,
    Except,
    Null,
    Nulls,
    Is,
    In,
    Into,
    Between,
    Like,
    With,
    Select,
    From,
    Where,
    When,
    As,
    Array,
    Struct,
    Group,
    Order,
    By,
    Having,
    Qualify,
    Inner,
    Join,
    Left,
    Right,
    Outer,
    Full,
    Then,
    Cross,
    Using,
    On,
    Set,
    Limit,
}

impl TokenTypeVariant {
    pub(crate) fn variant_str(&self) -> &str {
        match self {
            TokenTypeVariant::LeftParen => "(",
            TokenTypeVariant::RightParen => ")",
            TokenTypeVariant::LeftSquare => "[",
            TokenTypeVariant::RightSquare => "]",
            TokenTypeVariant::Comma => ",",
            TokenTypeVariant::Dot => ".",
            TokenTypeVariant::Minus => "-",
            TokenTypeVariant::Plus => "+",
            TokenTypeVariant::BitwiseNot => "~",
            TokenTypeVariant::BitwiseOr => "|",
            TokenTypeVariant::BitwiseAnd => "&",
            TokenTypeVariant::BitwiseXor => "^",
            TokenTypeVariant::BitwiseRightShift => ">>",
            TokenTypeVariant::BitwiseLeftShift => "<<",
            TokenTypeVariant::Colon => ":",
            TokenTypeVariant::Semicolon => ";",
            TokenTypeVariant::Slash => "/",
            TokenTypeVariant::Star => "*",
            TokenTypeVariant::Tick => "`",
            TokenTypeVariant::ConcatOperator => "||",
            TokenTypeVariant::Bang => "!",
            TokenTypeVariant::BangEqual => "!=",
            TokenTypeVariant::Equal => "=",
            TokenTypeVariant::NotEqual => "<>",
            TokenTypeVariant::Greater => ">",
            TokenTypeVariant::GreaterEqual => ">=",
            TokenTypeVariant::Less => "<",
            TokenTypeVariant::LessEqual => "<=",
            TokenTypeVariant::QuotedIdentifier => "QuotedIdentifier",
            TokenTypeVariant::Identifier => "Identifier",
            TokenTypeVariant::String => "String",
            TokenTypeVariant::Number => "Number",
            TokenTypeVariant::Eof => "EOF",
            TokenTypeVariant::Asc => "ASC",
            TokenTypeVariant::Desc => "DESC",
            TokenTypeVariant::Create => "CREATE",
            TokenTypeVariant::Recursive => "RECURSIVE",
            TokenTypeVariant::And => "AND",
            TokenTypeVariant::Or => "OR",
            TokenTypeVariant::Not => "NOT",
            TokenTypeVariant::True => "TRUE",
            TokenTypeVariant::False => "FALSE",
            TokenTypeVariant::Union => "UNION",
            TokenTypeVariant::All => "ALL",
            TokenTypeVariant::Exists => "EXISTS",
            TokenTypeVariant::If => "IF",
            TokenTypeVariant::Distinct => "DISTINCT",
            TokenTypeVariant::Intersect => "INTERSECT",
            TokenTypeVariant::Except => "EXCEPT",
            TokenTypeVariant::Null => "NULL",
            TokenTypeVariant::Nulls => "NULLS",
            TokenTypeVariant::Is => "IS",
            TokenTypeVariant::In => "IN",
            TokenTypeVariant::Into => "INTO",
            TokenTypeVariant::Between => "BETWEEN",
            TokenTypeVariant::Like => "LIKE",
            TokenTypeVariant::With => "WITH",
            TokenTypeVariant::Select => "SELECT",
            TokenTypeVariant::From => "FROM",
            TokenTypeVariant::Where => "WHERE",
            TokenTypeVariant::When => "WHEN",
            TokenTypeVariant::As => "AS",
            TokenTypeVariant::Array => "ARRAY",
            TokenTypeVariant::Struct => "STRUCT",
            TokenTypeVariant::Group => "GROUP",
            TokenTypeVariant::Order => "ORDER",
            TokenTypeVariant::By => "BY",
            TokenTypeVariant::Having => "HAVING",
            TokenTypeVariant::Qualify => "QUALIFY",
            TokenTypeVariant::Inner => "INNER",
            TokenTypeVariant::Join => "JOIN",
            TokenTypeVariant::Left => "LEFT",
            TokenTypeVariant::Right => "RIGHT",
            TokenTypeVariant::Outer => "OUTER",
            TokenTypeVariant::Full => "FULL",
            TokenTypeVariant::Then => "THEN",
            TokenTypeVariant::Cross => "CROSS",
            TokenTypeVariant::Using => "USING",
            TokenTypeVariant::On => "ON",
            TokenTypeVariant::Set => "SET",
            TokenTypeVariant::Limit => "LIMIT",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenType,
    pub lexeme: String,
    pub line: i32,
    pub col: i32,
}

pub struct Scanner {
    source_chars: Vec<char>,
    tokens: Vec<Token>,
    start: i32,
    current: i32,
    line: i32,
    col: i32,
    pub had_error: bool,
    open_type_brackets: Option<i32>,
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
            had_error: false,
            open_type_brackets: None,
        }
    }

    fn advance(&mut self) -> char {
        let c = self.source_chars[self.current as usize];
        self.current += 1;
        self.col += 1;
        c
    }

    fn n_advance(&mut self, n: i32) -> char {
        assert!(n > 0);
        let mut c = self.advance();
        for _ in 1..n {
            c = self.advance();
        }
        c
    }

    fn is_at_end(&self) -> bool {
        self.current >= self.source_chars.len() as i32
    }

    fn peek(&self) -> char {
        if self.is_at_end() {
            '\0'
        } else {
            self.source_chars[self.current as usize]
        }
    }

    fn peek_next_i(&mut self, i: i32) -> char {
        if self.current + i >= self.source_chars.len() as i32 {
            '\0'
        } else {
            self.source_chars[self.current as usize + (i as usize)]
        }
    }

    fn n_peek(&mut self, n: i32) -> Option<&[char]> {
        self.source_chars
            .get(self.current as usize..(self.current + n) as usize)
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
            lexeme: self.source_chars[self.start as usize..self.current as usize]
                .iter()
                .collect(),
            line: self.line,
            col: self.col,
        });
    }

    fn current_source_str(&self) -> String {
        self.source_chars[self.start as usize..self.current as usize]
            .iter()
            .collect()
    }

    fn reset(&mut self) {
        self.tokens.clear();
        self.start = 0;
        self.current = 0;
        self.col = 1;
        self.line = 1;
        self.had_error = false;
    }

    fn new_line(&mut self) {
        self.line += 1;
        self.col = 1;
    }

    pub fn scan(&mut self) -> Vec<Token> {
        self.reset();
        while self.current < self.source_chars.len() as i32 {
            self.start = self.current;
            self.scan_token();
        }
        self.tokens.push(Token {
            kind: TokenType::Eof,
            lexeme: String::from("eof"),
            line: self.line,
            col: self.col,
        });
        self.tokens.clone()
    }

    fn match_number(&mut self) {
        let mut found_dot = false;
        let mut found_e = false;
        loop {
            let peek_char = self.peek();

            if peek_char == '\0' {
                self.add_token(TokenType::Number(
                    self.source_chars[self.start as usize..(self.current as usize)]
                        .iter()
                        .collect::<String>()
                        .parse()
                        .unwrap(),
                ));
                break;
            }

            if peek_char == '.' {
                if found_dot || found_e {
                    self.error("Found invalid number");
                    break;
                }
                found_dot = true;
                self.advance();
            } else if peek_char == 'e' || peek_char == 'E' {
                if found_e {
                    self.error("Found invalid number");
                    break;
                }
                found_e = true;
                let peek_next_char = self.peek_next_i(1);
                if peek_next_char == '+' || peek_next_char == '-' {
                    self.advance();
                    if !(self.peek_next_i(1).is_ascii_digit()) {
                        self.error("Found invalid number");
                        break;
                    }
                    self.advance();
                } else if peek_next_char.is_ascii_digit() {
                    self.advance();
                } else {
                    self.error("Found invalid number");
                    break;
                }
            } else if peek_char.is_ascii_digit() {
                self.advance();
            } else {
                self.add_token(TokenType::Number(
                    self.source_chars[self.start as usize..(self.current as usize)]
                        .iter()
                        .collect::<String>()
                        .parse()
                        .unwrap(),
                ));
                break;
            }
        }
    }

    fn match_string(&mut self, delimiter: char) {
        loop {
            let peek_char = self.peek();
            if peek_char == '\0' {
                self.error("Found unterminated string");
                break;
            }
            if self.match_char(delimiter) {
                self.add_token(TokenType::String(
                    self.source_chars[self.start as usize + 1..self.current as usize - 1]
                        .iter()
                        .collect::<String>(),
                ));
                break;
            }
            if peek_char == '\n' {
                self.new_line();
            }
            self.advance();
        }
    }

    fn match_keyword_or_identifier(&mut self) {
        loop {
            let peek_char = self.peek();
            if !(peek_char.is_alphanumeric() || peek_char == '_') {
                break;
            }
            self.advance();
        }
        let identifer: String = self.source_chars[self.start as usize..self.current as usize]
            .iter()
            .collect();

        match identifer.to_lowercase().as_str() {
            "array" => {
                self.add_token(TokenType::Array);
                if self.peek() == '<' && self.open_type_brackets.is_none() {
                    self.open_type_brackets = Some(0);
                }
            }
            "struct" => {
                self.add_token(TokenType::Struct);
                if self.peek() == '<' && self.open_type_brackets.is_none() {
                    self.open_type_brackets = Some(0)
                }
            }
            "limit" => self.add_token(TokenType::Limit),
            "asc" => self.add_token(TokenType::Asc),
            "desc" => self.add_token(TokenType::Desc),
            "with" => self.add_token(TokenType::With),
            "create" => self.add_token(TokenType::Create),
            "exists" => self.add_token(TokenType::Exists),
            "if" => self.add_token(TokenType::If),
            "recursive" => self.add_token(TokenType::Recursive),
            "select" => self.add_token(TokenType::Select),
            "as" => self.add_token(TokenType::As),
            "from" => self.add_token(TokenType::From),
            "where" => self.add_token(TokenType::Where),
            "when" => self.add_token(TokenType::When),
            "order" => self.add_token(TokenType::Order),
            "group" => self.add_token(TokenType::Group),
            "by" => self.add_token(TokenType::By),
            "having" => self.add_token(TokenType::Having),
            "qualify" => self.add_token(TokenType::Qualify),
            "left" => self.add_token(TokenType::Left),
            "right" => self.add_token(TokenType::Right),
            "join" => self.add_token(TokenType::Join),
            "inner" => self.add_token(TokenType::Inner),
            "outer" => self.add_token(TokenType::Outer),
            "full" => self.add_token(TokenType::Full),
            "cross" => self.add_token(TokenType::Cross),
            "using" => self.add_token(TokenType::Using),
            "on" => self.add_token(TokenType::On),
            "and" => self.add_token(TokenType::And),
            "or" => self.add_token(TokenType::Or),
            "not" => self.add_token(TokenType::Not),
            "true" => self.add_token(TokenType::True),
            "false" => self.add_token(TokenType::False),
            "null" => self.add_token(TokenType::Null),
            "nulls" => self.add_token(TokenType::Nulls),
            "is" => self.add_token(TokenType::Is),
            "in" => self.add_token(TokenType::In),
            "into" => self.add_token(TokenType::Into),
            "between" => self.add_token(TokenType::Between),
            "like" => self.add_token(TokenType::Like),
            "then" => self.add_token(TokenType::Then),
            "union" => self.add_token(TokenType::Union),
            "all" => self.add_token(TokenType::All),
            "distinct" => self.add_token(TokenType::Distinct),
            "set" => self.add_token(TokenType::Set),
            "intersect" => self.add_token(TokenType::Intersect),
            "except" => self.add_token(TokenType::Except),
            _ => self.add_token(TokenType::Identifier(self.current_source_str())),
        }
    }

    fn scan_token(&mut self) {
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
                if peek_char.is_ascii_digit() || peek_char == '.' {
                    self.match_number();
                } else {
                    self.add_token(TokenType::Dot);
                }
            }
            '+' => self.add_token(TokenType::Plus),
            '=' => self.add_token(TokenType::Equal),
            '/' => {
                if self.match_char('*') {
                    loop {
                        if self.peek() == '\n' {
                            self.new_line();
                        }
                        let peek_chars = self.n_peek(2);
                        if peek_chars.is_none()
                            || peek_chars
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
                            if new_n == 0 {
                                None
                            } else {
                                Some(new_n)
                            }
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
            // TODO: we should also handle triple quoted strings
            c if c == '\'' || c == '"' => {
                // TODO: biquery supports also escaped sequences. We should handle them.
                self.match_string(c);
            }

            // numeric
            c if c.is_ascii_digit() => {
                self.match_number();
            }

            // Keywords and identifiers
            c if c.is_alphabetic() || c == '_' => {
                self.match_keyword_or_identifier();
            }

            '`' => {
                let quoted_ident_start_idx = self.current - 1;
                loop {
                    let curr_char = self.advance();
                    if curr_char == '`' {
                        let quoted_ident_end_idx = self.current - 1;
                        if quoted_ident_end_idx == quoted_ident_start_idx + 1 {
                            self.error("Found empty quoted identifier.");
                        }
                        self.add_token(TokenType::QuotedIdentifier(
                            self.source_chars[(quoted_ident_start_idx + 1) as usize
                                ..quoted_ident_end_idx as usize]
                                .iter()
                                .collect::<String>(),
                        ));
                        break;
                    }
                    if self.peek() == '\0' {
                        self.error("Found unterminated quoted identifier");
                        break;
                    }
                }
            }

            _ => {
                self.error(&format!(
                    "Found unexpected character while scanning: {}",
                    curr_char
                ));
            }
        }
    }

    fn error(&mut self, error: &str) {
        self.had_error = true;
        log::debug!("[line: {}, col: {}] {}", self.line, self.col, error)
    }
}
