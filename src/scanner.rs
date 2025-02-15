#[derive(PartialEq, Clone, Copy, Debug)]
pub enum TokenType {
    Asc,
    Desc,
    First,
    Last,

    LeftParen,
    RightParen,
    LeftSquare,
    RightSquare,
    Comma,
    Dot,
    Minus,
    Plus,
    Colon,
    Semicolon,
    Slash,
    Star,
    Tick,

    Bang,
    BangEqual,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,

    QuotedIdentifier,
    Identifier,
    String,
    Number,

    Recursive,

    Concat,

    BitwiseNot,
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
    BitwiseRightShift,
    BitwiseLeftShift,

    And,
    Or,
    Not,
    True,
    False,

    Union,
    All,
    Distinct,
    Intersect,
    Except,

    Null,

    Is,
    In,
    Between,
    Like,

    With,
    Select,
    From,
    Where,
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
    Cross,
    Using,
    On,

    Limit,
    Offset,

    Eof,
}

#[derive(Debug, Clone)]
pub enum TokenLiteral {
    String(String),
    Number(f64),
}


#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenType,
    pub lexeme: String,
    pub literal: Option<TokenLiteral>,
    pub line: i32,
}

pub struct Scanner {
    source_chars: Vec<char>,
    tokens: Vec<Token>,
    start: i32,
    current: i32,
    line: i32,
    pub had_error: bool,
}

impl Scanner {
    pub fn new(source: &str) -> Self {
        Self {
            source_chars: source.chars().collect(),
            tokens: vec![],
            start: 0,
            current: 0,
            line: 1,
            had_error: false,
        }
    }

    fn advance(&mut self) -> char {
        let c = self.source_chars[self.current as usize];
        self.current += 1;
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
        self.source_chars.get(self.current as usize..(self.current+n) as usize)
    }

    fn match_char(&mut self, expected: char) -> bool {
        if self.peek() != expected {
            return false;
        };

        self.current += 1;
        true
    }

    fn _add_token(&mut self, token_type: TokenType, literal: Option<TokenLiteral>) {
        self.tokens.push(Token {
            kind: token_type,
            lexeme: self.source_chars[self.start as usize..self.current as usize]
                .iter()
                .collect(),
            literal,
            line: self.line,
        });
    }

    fn add_token(&mut self, token_type: TokenType) {
        self._add_token(token_type, None);
    }

    fn add_token_w_literal(&mut self, token_type: TokenType, literal: TokenLiteral) {
        self._add_token(token_type, Some(literal));
    }

    fn current_source_str(&self) -> String {
        self.source_chars[self.start as usize..self.current as usize].iter().collect()
    }

    fn reset(&mut self) {
        self.tokens.clear();
        self.start = 0;
        self.current= 0;
        self.line = 1;
        self.had_error = false;
    }

    pub fn scan(&mut self) -> Vec<Token> {
        self.reset();
        while self.current < self.source_chars.len() as i32 {
            self.start = self.current;
            self.scan_token();
        }
        self.tokens.push(Token { kind: TokenType::Eof, lexeme: String::from("eof"), literal: None, line: self.line });
        self.tokens.clone()
    }

    fn match_number(&mut self) {
        let mut found_dot = false;
        let mut found_e = false;
        loop {
            let peek_char = self.peek();

            if peek_char == '\0' {
                self.add_token_w_literal(TokenType::Number, TokenLiteral::Number(self.source_chars[self.start as usize..(self.current as usize)].iter().collect::<String>().parse().unwrap()));
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
                self.add_token_w_literal(TokenType::Number, TokenLiteral::Number(self.source_chars[self.start as usize..(self.current as usize)].iter().collect::<String>().parse().unwrap()));
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
                self.add_token_w_literal(TokenType::String, TokenLiteral::String(self.source_chars[self.start as usize + 1..self.current as usize - 1].iter().collect::<String>()));
                break;
            }
            if peek_char == '\n' {
                self.line += 1;
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
        let identifer: String = self.source_chars[self.start as usize..self.current as usize].iter().collect();
        match identifer.to_lowercase().as_str() {
            "limit" => self.add_token(TokenType::Limit),
            "offset" => self.add_token(TokenType::Offset),
            "first" => self.add_token(TokenType::First),
            "last" => self.add_token(TokenType::Last),
            "asc" => self.add_token(TokenType::Asc),
            "desc" => self.add_token(TokenType::Desc),
            "with" => self.add_token(TokenType::With),
            "recursive" => self.add_token(TokenType::Recursive),
            "select" => self.add_token(TokenType::Select),
            "as" => self.add_token(TokenType::As),
            "from" => self.add_token(TokenType::From),
            "where" => self.add_token(TokenType::Where),
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
            "is" => self.add_token(TokenType::Is),
            "in" => self.add_token(TokenType::In),
            "between" => self.add_token(TokenType::Between),
            "like" => self.add_token(TokenType::Like),
            "union" => self.add_token(TokenType::Union),
            "all" => self.add_token(TokenType::All),
            "distinct" => self.add_token(TokenType::Distinct),
            "intersect" => self.add_token(TokenType::Intersect),
            "except" => self.add_token(TokenType::Except),
            _ => self.add_token_w_literal(TokenType::Identifier, TokenLiteral::String(self.current_source_str())),
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
            },
            '+' => {
                self.add_token(TokenType::Plus)
            }
            '=' => self.add_token(TokenType::Equal),
            '/' => {
                if self.match_char('*') {
                    loop {
                        if self.peek() == '\n' {
                            self.line += 1;
                        }
                        let peek_chars = self.n_peek(2);
                        if peek_chars.is_none() || peek_chars.unwrap().iter().zip("*/".chars()).all(|(&c1, c2)| c1==c2) {
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
                } else if self.match_char('<'){
                    self.add_token(TokenType::BitwiseLeftShift);
                } else {
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
                } else if self.match_char('>') {
                    self.add_token(TokenType::BitwiseRightShift);
                } else {
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
                    self.add_token(TokenType::Concat);
                } else {
                    self.add_token(TokenType::BitwiseOr);
                }
            }
            '^' => {
                self.add_token(TokenType::BitwiseXor);
            }
            '\n' => {
                self.line += 1;
            }
            '\r' | ' ' | '\t' => { }

            // strings
            // TODO: we should also handle triple quoted strings
            c if c == '\'' || c == '"'  => {
                // TODO: biquery supports also escaped sequences. We should handle them.
                self.match_string(c);
            },

            // numeric
            c if c.is_ascii_digit() => {
                self.match_number();
            },

            // Keywords and identifiers
            c if c.is_alphabetic() || c == '_' => {
                self.match_keyword_or_identifier();
            }

            '`' => {
                let quoted_ident_start_idx = self.current-1;
                loop {
                    let curr_char = self.advance();
                    if curr_char == '`' {
                        let quoted_ident_end_idx = self.current-1;
                        if quoted_ident_end_idx == quoted_ident_start_idx+1 {
                            self.error("Found empty quoted identifier.");
                        }
                        self.add_token_w_literal(TokenType::QuotedIdentifier, TokenLiteral::String(self.source_chars[(quoted_ident_start_idx+1) as usize..quoted_ident_end_idx as usize].iter().collect::<String>()));
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
        println!("[line: {}] {}", self.line, error)
    }
}
