use std::ops::{Index, IndexMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ArenaIndex {
    pub(crate) index: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Arena<T> {
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
    pub(crate) fn allocate(&mut self, node: T) -> ArenaIndex {
        self.nodes.push(node);
        ArenaIndex {
            index: self.nodes.len() - 1,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }

    #[allow(dead_code)]
    pub(crate) fn truncate(&mut self, n: usize) {
        self.nodes.truncate(n);
    }
}

pub(crate) struct ArenaIter<'a, T> {
    arena: &'a Arena<T>,
    i: usize,
}

impl<'a, T> Iterator for ArenaIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.arena.nodes.len() {
            None
        } else {
            let next = Some(&self.arena.nodes[self.i]);
            self.i += 1;
            next
        }
    }
}

impl<'a, T> IntoIterator for &'a Arena<T> {
    type Item = &'a T;

    type IntoIter = ArenaIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        ArenaIter { arena: self, i: 0 }
    }
}

impl<T> IntoIterator for Arena<T> {
    type Item = T;

    type IntoIter = <Vec<T> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes.into_iter()
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
