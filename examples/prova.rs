use std::{borrow::BorrowMut, cell::RefCell, error::Error, fmt::{format, Display}, rc::Rc};
use anyhow::{anyhow, Context};

// struct Base{
//     x: i32,
//     y: i32,
// }

// struct Derived {
//     base: Base,
//     z: i32
// }
// 
// 
// 
// 
// 
use self::B::Base;

enum B {
    Base()
} 


fn main() {
    Base();
    ()
}
