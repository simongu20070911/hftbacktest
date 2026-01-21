use std::{
    alloc,
    borrow::{Borrow, BorrowMut},
    fmt,
    fmt::{Debug, Formatter, Pointer},
    mem::{align_of, forget, needs_drop, size_of},
    ops::{Deref, DerefMut, Index, IndexMut},
    ptr::{NonNull, slice_from_raw_parts_mut},
    slice::SliceIndex,
};

pub const CACHE_LINE_SIZE: usize = 64;

pub struct AlignedArray<T, const ALIGNMENT: usize> {
    ptr: NonNull<[T]>,
    len: usize,
}

impl<T, const ALIGNMENT: usize> Drop for AlignedArray<T, ALIGNMENT> {
    #[inline]
    fn drop(&mut self) {
        if self.len == 0 {
            return;
        }
        unsafe {
            if needs_drop::<T>() {
                std::ptr::drop_in_place(self.ptr.as_ptr());
            }
            if size_of::<T>() == 0 {
                return;
            }
            let Some(byte_len) = size_of::<T>().checked_mul(self.len) else {
                return;
            };
            let Ok(layout) = alloc::Layout::from_size_align(byte_len, ALIGNMENT) else {
                return;
            };
            alloc::dealloc(self.ptr.as_ptr() as *mut T as _, layout);
        }
    }
}

impl<T, const ALIGNMENT: usize> AlignedArray<T, ALIGNMENT> {
    /// Dictated by the requirements of
    /// [`alloc::Layout`](https://doc.rust-lang.org/alloc/alloc/struct.Layout.html).
    /// "`size`, when rounded up to the nearest multiple of `align`, must not overflow `isize`
    /// (i.e. the rounded value must be less than or equal to `isize::MAX`)".
    pub const MAX_CAPACITY: usize = isize::MAX as usize - (ALIGNMENT - 1);

    #[inline]
    pub fn new(len: usize) -> Self
    where
        T: Default,
    {
        assert!(
            ALIGNMENT.is_power_of_two(),
            "`ALIGNMENT` must be a power of two"
        );
        assert!(
            ALIGNMENT >= align_of::<T>(),
            "`ALIGNMENT` must be >= align_of::<T>()"
        );

        let byte_len = size_of::<T>()
            .checked_mul(len)
            .expect("len * size_of::<T>() overflow");
        assert!(
            byte_len <= Self::MAX_CAPACITY,
            "`len * size_of::<T>()` cannot exceed isize::MAX - (ALIGNMENT - 1)"
        );

        let layout = alloc::Layout::from_size_align(byte_len, ALIGNMENT)
            .expect("invalid layout for allocation");
        if layout.size() == 0 {
            let ptr = NonNull::<T>::dangling().as_ptr();
            let ptr = unsafe { NonNull::new_unchecked(slice_from_raw_parts_mut(ptr, len)) };
            return Self { ptr, len };
        }

        let ptr = unsafe {
            struct InitGuard<T> {
                ptr: *mut T,
                layout: alloc::Layout,
                init: usize,
            }

            impl<T> Drop for InitGuard<T> {
                fn drop(&mut self) {
                    unsafe {
                        if needs_drop::<T>() {
                            let initialized =
                                slice_from_raw_parts_mut(self.ptr as *mut T, self.init);
                            std::ptr::drop_in_place(initialized);
                        }
                        alloc::dealloc(self.ptr as _, self.layout);
                    }
                }
            }

            let ptr = alloc::alloc(layout);
            if ptr.is_null() {
                alloc::handle_alloc_error(layout);
            }

            let ptr = ptr as *mut T;
            let mut guard = InitGuard {
                ptr,
                layout,
                init: 0,
            };
            for i in 0..len {
                ptr.add(i).write(T::default());
                guard.init += 1;
            }
            forget(guard);

            NonNull::new_unchecked(slice_from_raw_parts_mut(ptr, len))
        };

        Self { ptr, len }
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut [T] {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { self.ptr.as_mut() }
    }

    #[inline]
    pub fn as_ptr(&self) -> *const [T] {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        unsafe { self.ptr.as_ref() }
    }

    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Consumes the ``AlignedArray``, returning a wrapped fat pointer, similar to ``Box::into_raw``.
    pub fn into_raw(mut self) -> *mut [T] {
        let ptr = self.as_mut_ptr();
        forget(self);
        ptr
    }

    /// Constructs an AlignedArray from a fat pointer, similar to ``Box::from_raw``, but with
    /// the specified alignment.
    ///
    /// # Safety
    /// This function is unsafe because improper use may lead to memory problems. For example, a
    /// double-free may occur if the function is called twice on the same fat pointer.
    ///
    /// The fat pointer must originate from [`AlignedArray::into_raw`].
    pub unsafe fn from_raw(ptr: *mut [T]) -> Self {
        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            len: ptr.len(),
        }
    }
}

impl<T, const ALIGNMENT: usize> AsMut<[T]> for AlignedArray<T, ALIGNMENT> {
    #[inline]
    fn as_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<T, const ALIGNMENT: usize> AsRef<[T]> for AlignedArray<T, ALIGNMENT> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T, const ALIGNMENT: usize> Borrow<[T]> for AlignedArray<T, ALIGNMENT> {
    #[inline]
    fn borrow(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T, const ALIGNMENT: usize> BorrowMut<[T]> for AlignedArray<T, ALIGNMENT> {
    #[inline]
    fn borrow_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<T, const ALIGNMENT: usize> Debug for AlignedArray<T, ALIGNMENT> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}

impl<T, const ALIGNMENT: usize> Deref for AlignedArray<T, ALIGNMENT> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T, const ALIGNMENT: usize> DerefMut for AlignedArray<T, ALIGNMENT> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T, const ALIGNMENT: usize, Idx: SliceIndex<[T]>> Index<Idx> for AlignedArray<T, ALIGNMENT> {
    type Output = <Idx as SliceIndex<[T]>>::Output;

    #[inline]
    fn index(&self, index: Idx) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl<T, const ALIGNMENT: usize, Idx: SliceIndex<[T]>> IndexMut<Idx> for AlignedArray<T, ALIGNMENT> {
    #[inline]
    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        &mut self.as_mut_slice()[index]
    }
}

#[cfg(test)]
mod tests {
    use super::{AlignedArray, CACHE_LINE_SIZE};

    #[test]
    fn alignedarray_len_zero_is_allowed() {
        let arr = AlignedArray::<u8, CACHE_LINE_SIZE>::new(0);
        assert_eq!(arr.len(), 0);
        assert!(arr.as_slice().is_empty());
    }

    #[test]
    #[should_panic(expected = "len * size_of::<T>() overflow")]
    fn alignedarray_new_panics_on_len_times_size_overflow() {
        let _ = AlignedArray::<u16, CACHE_LINE_SIZE>::new(usize::MAX);
    }
}
