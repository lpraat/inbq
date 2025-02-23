use std::ops::{Index, IndexMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArenaIndex {
    pub index: usize,
}

#[derive(Debug)]
pub struct Arena<T> {
    nodes: Vec<T>,
}

impl<T> Default for Arena<T> {
    fn default() -> Self {
        Self {
            nodes: Vec::default(),
        }
    }
}

impl<T> Arena<T> {
    pub fn allocate(&mut self, node: T) -> ArenaIndex {
        self.nodes.push(node);
        ArenaIndex {
            index: self.nodes.len() - 1,
        }
    }
    
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}

impl<T> Index<ArenaIndex> for Arena<T> {
    type Output = T;

    fn index(&self, index: ArenaIndex) -> &Self::Output {
        &self.nodes[index.index]
    }
}

impl<T> IndexMut<ArenaIndex> for Arena<T> {
    fn index_mut(&mut self, index: ArenaIndex) -> &mut Self::Output {
        &mut self.nodes[index.index]
    }
}
