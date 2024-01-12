use arrow2::array::Array as ArrowArray;

pub struct AlignedArray {
    iter: Box<dyn Iterator<Item = Box<dyn ArrowArray>>>,
    current_chunk: Option<Box<dyn ArrowArray>>,
    offset: usize,
}

impl AlignedArray {
    pub fn new(mut iter: Box<dyn Iterator<Item = Box<dyn ArrowArray>>>) -> Self {
        let current_chunk = iter.next();
        Self {
            iter,
            current_chunk,
            offset: 0,
        }
    }

    pub fn length(&self) -> usize {
        self.current_chunk.as_ref().unwrap().len() - self.offset
    }
}

pub struct AlignedArrowArrayIterator {
    items: Vec<AlignedArray>,
}

impl AlignedArrowArrayIterator {
    pub fn new(iterators: Vec<Box<dyn Iterator<Item = Box<dyn ArrowArray>>>>) -> Self {
        let items = iterators.into_iter().map(AlignedArray::new).collect();
        Self { items }
    }
}

impl Iterator for AlignedArrowArrayIterator {
    type Item = Vec<Box<dyn ArrowArray>>;

    fn next(&mut self) -> Option<Self::Item> {
        let missing_chunks: usize = self
            .items
            .iter_mut()
            .map(|v| {
                if v.length() == 0 {
                    v.current_chunk = v.iter.next();
                    v.offset = 0;
                    if v.current_chunk.is_none() {
                        1
                    } else {
                        0
                    }
                } else {
                    0
                }
            })
            .sum();

        if missing_chunks == self.items.len() {
            return None;
        } else if missing_chunks > 0 {
            panic!(
                "Misaligned arrays, {} arrays didn't return a next chunk",
                missing_chunks
            );
        }

        let smallest_chunk = self.items.iter().map(|v| v.length()).min().unwrap();

        Some(
            self.items
                .iter_mut()
                .map(|v| {
                    let len = v.length();
                    let offset = v.offset;
                    v.offset += smallest_chunk;

                    if len == smallest_chunk {
                        v.current_chunk.clone().unwrap()
                    } else {
                        v.current_chunk
                            .as_ref()
                            .unwrap()
                            .sliced(offset, smallest_chunk)
                    }
                })
                .collect::<Vec<_>>(),
        )
    }
}
