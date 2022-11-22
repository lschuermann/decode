pub struct TransposedSlices<'a, T, I: AsRef<[T]> + 'a, O: AsRef<[I]> + 'a> {
    slices: O,
    _pd: std::marker::PhantomData<&'a (T, I)>,
}

impl<'a, T, I: AsRef<[T]> + 'a, O: AsRef<[I]> + 'a> TransposedSlices<'a, T, I, O> {
    pub fn new(slices: O) -> Self {
        TransposedSlices {
            slices,
            _pd: std::marker::PhantomData,
        }
    }

    pub fn iter<'s>(&'s self) -> Iter<'a, 's, T, I, O> {
        Iter {
            inner: self,
            column_offset: 0,
            row_offset: 0,
            _pd: std::marker::PhantomData,
        }
    }
}

impl<'a, T, I: AsRef<[T]> + AsMut<[T]> + 'a, O: AsRef<[I]> + AsMut<[I]> + 'a>
    TransposedSlices<'a, T, I, O>
{
    // Unfortunately, providing an [`IterMut`] for this is rather
    // hard. Dereferencing the passed outer / inner slices captures the lifetime
    // of `&mut self`, which cannot be reflected in the Iterator's `Item`
    // associated type. We can presumably hack something together by taking a
    // `&mut [&mut T]` directly and using `std::mem::take` and friends, however,
    // given that it's not (cheaply) possible to turn a `&mut Vec<Vec<T>>` or
    // even `&mut [Vec<T>]` into a 2D mutable slice, that won't be very
    // practical. Hence we provide a few methods which work around these issues,
    // such as collecting an `Iterator<Item = T>` into this structure:
    pub fn collect_iter(&mut self, iter: &mut impl Iterator<Item = T>) -> (usize, bool) {
        let rows = self.slices.as_ref().len();

        let mut row_offset = 0;
        let mut column_offset = 0;
        let mut items = 0;
        let mut iter_exhausted = false;

        while let Some(slot) = self
            .slices
            .as_mut()
            .get_mut(row_offset)
            .map(AsMut::as_mut)
            .and_then(|slice| slice.get_mut(column_offset))
        {
            if let Some(item) = iter.next() {
                *slot = item;

                // Increment the row_offset, wrapping around when reaching rows:
                if row_offset + 1 >= rows {
                    row_offset = 0;
                    column_offset += 1;
                } else {
                    row_offset += 1;
                }

                items += 1;
            } else {
                // The iterator did not yield any more items.
                iter_exhausted = true;
                break;
            }
        }

        (items, iter_exhausted)
    }
}

pub struct Iter<'a, 'inner, T, I: AsRef<[T]> + 'a, O: AsRef<[I]> + 'a> {
    inner: &'inner TransposedSlices<'a, T, I, O>,
    column_offset: usize,
    row_offset: usize,
    _pd: std::marker::PhantomData<(T, I)>,
}

impl<'a, 'inner: 'a, T, I: AsRef<[T]> + 'a, O: AsRef<[I]> + 'a> Iterator
    for Iter<'a, 'inner, T, I, O>
{
    type Item = &'inner T;

    fn next(&mut self) -> Option<Self::Item> {
        let rows = self.inner.slices.as_ref().len();
        self.inner
            .slices
            .as_ref()
            .get(self.row_offset)
            .map(AsRef::as_ref)
            .and_then(|slice| slice.get(self.column_offset))
            .map(|element| {
                // Increment the row_offset, wrapping around when reaching rows:
                if self.row_offset + 1 >= rows {
                    self.row_offset = 0;
                    self.column_offset += 1;
                } else {
                    self.row_offset += 1;
                }

                // Return the element reference:
                element
            })
    }
}
