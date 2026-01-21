mod npy;
mod reader;

use std::{
    marker::PhantomData,
    mem::size_of,
    ops::{Index, IndexMut},
    rc::Rc,
    slice::SliceIndex,
};

pub use npy::{
    Field,
    NpyDTyped,
    NpyHeader,
    NpyReadOptions,
    read_npy_file,
    read_npy_file_with_options,
    read_npz_file,
    read_npz_file_with_options,
    write_npy,
};
pub use reader::{Cache, DataPreprocess, DataSource, FeedLatencyAdjustment, Reader, ReaderBuilder};

use crate::utils::{AlignedArray, CACHE_LINE_SIZE};

/// Marker trait for C representation plain old data.
///
/// # Safety
/// This marker trait should be implemented only if the struct has a C representation and contains
/// only plain old data.
pub unsafe trait POD: Sized {}

/// Provides access to an array of structs from the buffer.
#[derive(Clone, Debug)]
pub struct Data<D>
where
    D: POD + Clone,
{
    ptr: Rc<DataPtr>,
    offset: usize,
    _d_marker: PhantomData<D>,
}

impl<D> Data<D>
where
    D: POD + Clone,
{
    #[inline(always)]
    fn elem_size() -> usize {
        size_of::<D>()
    }

    /// Returns the length of the array.
    #[inline(always)]
    pub fn len(&self) -> usize {
        let size = Self::elem_size();
        if size == 0 {
            return 0;
        }

        self.ptr.len().saturating_sub(self.offset) / size
    }

    /// Returns `true` if the `Data` is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Constructs an empty `Data`.
    pub fn empty() -> Self {
        Self {
            ptr: Default::default(),
            offset: 0,
            _d_marker: PhantomData,
        }
    }

    pub fn from_data(data: &[D]) -> Self {
        if data.is_empty() {
            return Self::empty();
        }

        let byte_len = size_of_val(data);
        let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, byte_len) };

        let dest_data_ptr = DataPtr::new(byte_len);

        unsafe {
            let dest_data = dest_data_ptr.ptr.as_mut().unwrap();

            dest_data.copy_from_slice(bytes);
            Self::from_data_ptr(dest_data_ptr, 0)
        }
    }

    /// Constructs `Data` from [`DataPtr`] with the specified offset.
    ///
    /// # Safety
    /// The underlying memory layout must match the layout of type `D` and be aligned from the
    /// offset.
    pub unsafe fn from_data_ptr(ptr: DataPtr, offset: usize) -> Self {
        Self {
            ptr: Rc::new(ptr),
            offset,
            _d_marker: PhantomData,
        }
    }

    /// Returns a reference to an element, without doing bounds checking.
    ///
    /// # Safety
    /// Calling this method with an out-of-bounds index is undefined behavior even if the resulting
    /// reference is not used.
    #[inline(always)]
    pub unsafe fn get_unchecked(&self, index: usize) -> &D {
        let size = size_of::<D>();
        let i = self.offset + index * size;
        unsafe { &*(self.ptr.at(i) as *const D) }
    }

    /// Returns `true` if the two `Data` point to the same data.
    pub fn data_eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.ptr, &other.ptr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic;

    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    struct Row {
        a: i64,
    }

    unsafe impl POD for Row {}

    #[test]
    fn data_from_empty_slice_does_not_panic() {
        let data = Data::<Row>::from_data(&[]);
        assert!(data.is_empty());
        assert_eq!(data.len(), 0);
    }

    #[test]
    fn data_is_empty_uses_logical_len() {
        let ptr = DataPtr::new(64);
        let data = unsafe { Data::<Row>::from_data_ptr(ptr, 64) };
        assert!(data.is_empty());
        assert_eq!(data.len(), 0);
    }

    #[test]
    fn dataptr_default_is_non_null() {
        let ptr = DataPtr::default();
        let thin = ptr.ptr as *const u8;
        assert!(!thin.is_null());
        assert_eq!(ptr.len(), 0);
    }

    #[test]
    fn data_empty_index_panics() {
        let data = Data::<Row>::empty();
        let err = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let _ = data[0];
        }));
        assert!(err.is_err());
    }

    #[test]
    fn data_index_large_panics_with_bounds_message() {
        let data = Data::<Row>::from_data(&[Row { a: 1 }]);
        let err = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let _ = data[usize::MAX];
        }))
        .expect_err("expected panic");

        let msg = if let Some(s) = err.downcast_ref::<&str>() {
            *s
        } else if let Some(s) = err.downcast_ref::<String>() {
            s.as_str()
        } else {
            "<non-string panic>"
        };
        assert!(
            msg.contains("Out of the size."),
            "unexpected panic message: {msg}"
        );
    }

    #[test]
    fn data_index_mut_is_copy_on_write() {
        let base = Data::<Row>::from_data(&[Row { a: 1 }, Row { a: 2 }]);
        let mut left = base.clone();
        let right = base.clone();

        left[0].a = 10;
        assert_eq!(right[0].a, 1);
    }
}

impl<D> Index<usize> for Data<D>
where
    D: POD + Clone,
{
    type Output = D;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        let len = self.len();
        if index >= len {
            panic!("Out of the size.");
        }

        let size = Self::elem_size();
        let i = self.offset + index * size;
        unsafe { &*(self.ptr.at(i) as *const D) }
    }
}

impl<D> IndexMut<usize> for Data<D>
where
    D: POD + Clone,
{
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let len = self.len();
        if index >= len {
            panic!("Out of the size.");
        }

        if Rc::get_mut(&mut self.ptr).is_none() {
            self.ptr = Rc::new(self.ptr.to_managed_copy());
        }

        let size = Self::elem_size();
        let i = self.offset + index * size;
        unsafe { &mut *(self.ptr.at(i) as *mut D) }
    }
}

#[derive(Debug)]
pub struct DataPtr {
    ptr: *mut [u8],
    managed: bool,
}

impl DataPtr {
    pub fn new(size: usize) -> Self {
        let arr = AlignedArray::<u8, CACHE_LINE_SIZE>::new(size);
        Self {
            ptr: arr.into_raw(),
            managed: true,
        }
    }

    /// Constructs a `DataPtr` from a fat pointer.
    ///
    /// Unlike other methods that construct an instance from a raw pointer, the raw pointer is not
    /// owned by the resulting `DataPtr`. Memory should still be managed by the caller.
    ///
    /// # Safety
    /// The fat pointer must remain valid for the lifetime of the resulting `DataPtr`.
    pub unsafe fn from_ptr(ptr: *mut [u8]) -> Self {
        Self {
            ptr,
            managed: false,
        }
    }

    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.ptr.len()
    }

    /// Returns a pointer offset by the given index.
    ///
    /// # Safety
    /// The `index` must be within the bounds of the array referenced by this pointer.
    /// Accessing an out-of-bounds offset is undefined behavior.
    #[inline]
    pub unsafe fn at(&self, index: usize) -> *const u8 {
        let ptr = self.ptr as *const u8;
        unsafe { ptr.add(index) }
    }

    #[inline]
    fn to_managed_copy(&self) -> Self {
        let copied = DataPtr::new(self.len());

        unsafe {
            let src = self.ptr.as_ref().expect("source pointer must be non-null");
            let dest = copied.ptr.as_mut().expect("destination pointer must be non-null");
            dest.copy_from_slice(src);
        }

        copied
    }
}

impl Default for DataPtr {
    fn default() -> Self {
        Self {
            ptr: std::ptr::slice_from_raw_parts_mut(std::ptr::NonNull::<u8>::dangling().as_ptr(), 0),
            managed: false,
        }
    }
}

impl<Idx> Index<Idx> for DataPtr
where
    Idx: SliceIndex<[u8]>,
{
    type Output = Idx::Output;

    #[inline]
    fn index(&self, index: Idx) -> &Self::Output {
        let arr = unsafe { &*self.ptr };
        &arr[index]
    }
}

impl<Idx> IndexMut<Idx> for DataPtr
where
    Idx: SliceIndex<[u8]>,
{
    #[inline]
    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        let arr = unsafe { &mut *self.ptr };
        &mut arr[index]
    }
}

impl Drop for DataPtr {
    fn drop(&mut self) {
        if self.managed {
            let _ = unsafe { AlignedArray::<u8, CACHE_LINE_SIZE>::from_raw(self.ptr) };
        }
    }
}
