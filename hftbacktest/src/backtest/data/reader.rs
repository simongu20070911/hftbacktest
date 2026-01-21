use std::{
    any::Any,
    cell::RefCell,
    collections::HashMap,
    io::{Error as IoError, ErrorKind},
    rc::Rc,
    sync::{
        Arc,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
};

use uuid::Uuid;

use crate::{
    backtest::{
        BacktestError,
        data::{
            Data,
            DataPtr,
            POD,
            npy::{NpyDTyped, read_npy_file, read_npz_file},
        },
    },
    types::Event,
};

/// Data source for the [`Reader`].
#[derive(Clone, Debug)]
pub enum DataSource<D>
where
    D: POD + Clone,
{
    /// Data needs to be loaded from the specified file. This should be a `numpy` file.
    ///
    /// It will be loaded when needed and released
    /// when no [Processor](`crate::backtest::proc::Processor`) is reading the data.
    File(String),
    /// Data is loaded and set by the user.
    Data(Data<D>),
}

#[derive(Debug)]
struct CachedData<D>
where
    D: POD + Clone,
{
    count: usize,
    ready: bool,
    data: Data<D>,
}

impl<D> CachedData<D>
where
    D: POD + Clone,
{
    pub fn new(data: Data<D>) -> Self {
        Self {
            count: 0,
            ready: true,
            data,
        }
    }

    pub fn empty() -> Self {
        Self {
            count: 0,
            ready: false,
            data: Data::empty(),
        }
    }

    pub fn set(&mut self, data: Data<D>) {
        self.data = data;
    }

    pub fn checkout(&mut self) -> Data<D> {
        self.count += 1;
        self.data.clone()
    }

    pub fn turn_in(&mut self) -> bool {
        self.count -= 1;
        self.count == 0
    }
}

/// Provides a data cache that allows both the local processor and exchange processor to access the
/// same or different data based on their timestamps without the need for reloading.
#[derive(Clone, Debug)]
pub struct Cache<D>(Rc<RefCell<HashMap<String, CachedData<D>>>>)
where
    D: POD + Clone;

impl<D> Cache<D>
where
    D: POD + Clone,
{
    /// Constructs an instance of `Cache`.
    pub fn new() -> Self {
        Self(Default::default())
    }

    /// Inserts a key-value pair into the `Cache`.
    pub fn insert(&mut self, key: String, data: Data<D>) {
        self.0.borrow_mut().insert(key, CachedData::new(data));
    }

    /// Prepares cached data by inserting a key-value pair with empty data into the `Cache`.
    /// This placeholder will be replaced when the actual data is ready.
    pub fn prepare(&mut self, key: String) {
        self.0.borrow_mut().insert(key, CachedData::empty());
    }

    /// Removes the [`Data`] if all retrieved [`Data`] are released.
    pub fn remove(&mut self, data: Data<D>) {
        let mut remove = None;
        for (key, cached_data) in self.0.borrow_mut().iter_mut() {
            if data.data_eq(&cached_data.data) {
                if cached_data.turn_in() {
                    remove = Some(key.clone());
                }
                break;
            }
        }
        if let Some(key) = remove {
            self.0.borrow_mut().remove(&key).unwrap();
        }
    }

    /// Returns `true` if the `Cache` contains the [`Data`] for the specified key.
    pub fn contains(&self, key: &str) -> bool {
        self.0.borrow().contains_key(key)
    }

    /// Returns the [`Data`] corresponding to the key.
    pub fn get(&mut self, key: &str) -> Data<D> {
        let mut borrowed = self.0.borrow_mut();
        let cached_data = borrowed.get_mut(key).unwrap();
        cached_data.checkout()
    }

    /// Sets the [`Data`] for the specified key and marks it as ready.
    pub fn set(&mut self, key: &str, data: Data<D>) {
        let mut borrowed = self.0.borrow_mut();
        let cached_data = borrowed.get_mut(key).unwrap();
        cached_data.set(data);
        cached_data.ready = true;
    }

    /// Returns `true` if the [`Data`] for the specified key is ready.
    pub fn is_ready(&self, key: &str) -> bool {
        self.0.borrow().get(key).unwrap().ready
    }
}

impl<D> Default for Cache<D>
where
    D: POD + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: `DataPtr` is an owning pointer to an allocated byte slice. Transferring unique ownership
// between threads is safe as long as it is not aliased. `DataPtr` remains !Sync, preventing shared
// references from being used across threads without additional synchronization.
unsafe impl Send for DataPtr {}

/// Directly implementing `Send` for `Data` may lead to unsafe sharing between threads. To mitigate
/// this risk, `DataSend` transfers owned bytes ([`DataPtr`]) and reconstructs `Data` on the
/// receiver thread.
struct DataSend {
    ptr: DataPtr,
    offset: usize,
}

impl DataSend {
    fn from_data<D>(data: Data<D>) -> Self
    where
        D: POD + Clone,
    {
        let Data {
            ptr,
            offset,
            _d_marker: _,
        } = data;

        // `Data<D>` uses `Rc`, which is not `Send`. Convert to owned bytes for safe cross-thread
        // transfer. If we have unique ownership, avoid a copy.
        let ptr = match Rc::try_unwrap(ptr) {
            Ok(ptr) => ptr,
            Err(ptr) => {
                let mut owned = DataPtr::new(ptr.len());
                owned[..].copy_from_slice(&ptr[..]);
                owned
            }
        };

        Self { ptr, offset }
    }

    fn into_data<D>(self) -> Data<D>
    where
        D: POD + Clone,
    {
        // SAFETY: `DataSend` is created only from a valid `Data<D>` and preserves the same offset,
        // so the memory layout and alignment match `D`.
        unsafe { Data::from_data_ptr(self.ptr, self.offset) }
    }
}

struct LoadDataResult {
    key: String,
    result: Result<DataSend, IoError>,
}

impl LoadDataResult {
    pub fn ok(key: String, data: DataSend) -> Self {
        Self { key, result: Ok(data) }
    }

    pub fn err(key: String, error: IoError) -> Self {
        Self { key, result: Err(error) }
    }
}

/// A builder for constructing [`Reader`].
pub struct ReaderBuilder<D>
where
    D: NpyDTyped + POD + Clone,
{
    data_key_list: Vec<String>,
    cache: Cache<D>,
    temporary_data: HashMap<String, Data<D>>,
    parallel_load: bool,
    preprocessor: Option<Arc<Box<dyn DataPreprocess<D> + Sync + Send + 'static>>>,
}

impl<D> Default for ReaderBuilder<D>
where
    D: NpyDTyped + POD + Clone,
{
    fn default() -> Self {
        Self {
            data_key_list: Default::default(),
            cache: Default::default(),
            temporary_data: Default::default(),
            parallel_load: false,
            preprocessor: None,
        }
    }
}

impl<D> ReaderBuilder<D>
where
    D: NpyDTyped + POD + Clone,
{
    /// Constructs a `ReaderBuilder`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether to load the next data in parallel. This allows [`Reader`] to not only load the
    /// next data but also preload subsequent data, ensuring it is ready in advance.
    ///
    /// Loading is performed by spawning a separate thread.
    ///
    /// The default value is `true`.
    pub fn parallel_load(self, parallel_load: bool) -> Self {
        Self {
            parallel_load,
            ..self
        }
    }

    /// Sets a [`DataPreprocess`].
    pub fn preprocessor<Preprocessor>(self, preprocessor: Preprocessor) -> Self
    where
        Preprocessor: DataPreprocess<D> + Sync + Send + 'static,
    {
        Self {
            preprocessor: Some(Arc::new(Box::new(preprocessor))),
            ..self
        }
    }

    /// Sets the data to be read by [`Reader`]. The items in the `data` vector should be arranged in
    /// the chronological order.
    pub fn data(self, data: Vec<DataSource<D>>) -> Self {
        let mut data_key_list = self.data_key_list;
        let mut temporary_data = self.temporary_data;
        for item in data {
            match item {
                DataSource::File(filepath) => {
                    data_key_list.push(filepath);
                }
                DataSource::Data(data) => {
                    let key = Uuid::new_v4().to_string();
                    data_key_list.push(key.clone());
                    temporary_data.insert(key, data);
                }
            }
        }
        Self {
            data_key_list,
            temporary_data,
            ..self
        }
    }

    /// Builds a [`Reader`].
    pub fn build(self) -> Result<Reader<D>, IoError> {
        let mut cache = self.cache.clone();
        for (key, mut data) in self.temporary_data {
            if let Some(p) = &self.preprocessor {
                p.preprocess(&mut data)?;
            }
            cache.insert(key, data)
        }

        let (tx, rx) = channel();
        Ok(Reader {
            data_key_list: self.data_key_list.clone(),
            cache,
            data_num: 0,
            tx,
            rx: Rc::new(rx),
            parallel_load: self.parallel_load,
            preprocessor: self.preprocessor.clone(),
        })
    }
}

/// Provides `Data` reading based on the given sequence of data through `Cache`.
#[derive(Clone)]
pub struct Reader<D>
where
    D: NpyDTyped + Clone,
{
    data_key_list: Vec<String>,
    cache: Cache<D>,
    data_num: usize,
    tx: Sender<LoadDataResult>,
    rx: Rc<Receiver<LoadDataResult>>,
    parallel_load: bool,
    preprocessor: Option<Arc<Box<dyn DataPreprocess<D> + Sync + Send + 'static>>>,
}

impl<D> Reader<D>
where
    D: NpyDTyped + Clone + 'static,
{
    /// Returns a [`ReaderBuilder`].
    pub fn builder() -> ReaderBuilder<D> {
        ReaderBuilder::default()
    }

    /// Releases this [`Data`] from the `Cache`. The `Cache` will delete the [`Data`] if there are
    /// no readers accessing it.
    pub fn release(&mut self, data: Data<D>) {
        self.cache.remove(data);
    }

    /// Retrieves the next [`Data`] based on the order of your additions.
    pub fn next_data(&mut self) -> Result<Data<D>, BacktestError> {
        if self.data_num < self.data_key_list.len() {
            let key = self.data_key_list.get(self.data_num).cloned().unwrap();
            self.load_data(&key)?;

            if self.parallel_load {
                let next_key = self.data_key_list.get(self.data_num + 1).cloned();
                if let Some(next_key) = next_key {
                    self.load_data(&next_key)?;
                }
            }

            while !self.cache.is_ready(&key) {
                let msg = self.rx.recv().map_err(|err| {
                    BacktestError::DataError(IoError::new(
                        ErrorKind::Other,
                        format!(
                            "Loader channel closed unexpectedly while waiting for '{key}': {err}"
                        ),
                    ))
                })?;

                match msg {
                    LoadDataResult {
                        key: loaded_key,
                        result: Ok(data_send),
                    } => {
                        self.cache.set(&loaded_key, data_send.into_data::<D>());
                    }
                    LoadDataResult {
                        key: loaded_key,
                        result: Err(err),
                    } => {
                        // Ensure a subsequent call doesn't wait forever on a non-ready placeholder
                        // if the caller chooses to retry after an error.
                        self.cache.0.borrow_mut().remove(&loaded_key);
                        return Err(BacktestError::DataError(std::io::Error::new(
                            err.kind(),
                            format!("Failed to read file '{loaded_key}': {err}"),
                        )));
                    }
                }
            }

            let data = self.cache.get(&key);
            self.data_num += 1;
            Ok(data)
        } else {
            Err(BacktestError::EndOfData)
        }
    }

    fn load_data(&mut self, key: &str) -> Result<(), BacktestError> {
        if !self.cache.contains(key) {
            self.cache.prepare(key.to_string());

            if key.ends_with(".npy") {
                let tx = self.tx.clone();
                let filepath = key.to_string();
                let preprocessor = self.preprocessor.clone();

                let _ = thread::spawn(move || {
                    let load_data = |filepath: &str| {
                        let mut data = read_npy_file::<D>(filepath)?;
                        if let Some(preprocessor) = &preprocessor {
                            preprocessor.preprocess(&mut data)?;
                        }
                        Ok(data)
                    };
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        load_data(&filepath)
                    }));
                    // SendError occurs only if Reader is already destroyed. Since no data is needed
                    // once the Reader is destroyed, SendError is safely suppressed.
                    let result: Result<DataSend, IoError> = match result {
                        Ok(Ok(data)) => Ok(DataSend::from_data(data)),
                        Ok(Err(err)) => Err(err),
                        Err(payload) => Err(IoError::new(
                            ErrorKind::Other,
                            format!("loader thread panicked: {}", panic_payload_to_string(&payload)),
                        )),
                    };

                    let _ = tx.send(match result {
                        Ok(data) => LoadDataResult::ok(filepath, data),
                        Err(err) => LoadDataResult::err(filepath, err),
                    });
                });
            } else if key.ends_with(".npz") {
                let tx = self.tx.clone();
                let filepath = key.to_string();
                let preprocessor = self.preprocessor.clone();

                let _ = thread::spawn(move || {
                    let load_data = |filepath: &str| {
                        let mut data = read_npz_file::<D>(filepath, "data")?;
                        if let Some(preprocessor) = &preprocessor {
                            preprocessor.preprocess(&mut data)?;
                        }
                        Ok(data)
                    };
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        load_data(&filepath)
                    }));
                    // SendError occurs only if Reader is already destroyed. Since no data is needed
                    // once the Reader is destroyed, SendError is safely suppressed.
                    let result: Result<DataSend, IoError> = match result {
                        Ok(Ok(data)) => Ok(DataSend::from_data(data)),
                        Ok(Err(err)) => Err(err),
                        Err(payload) => Err(IoError::new(
                            ErrorKind::Other,
                            format!("loader thread panicked: {}", panic_payload_to_string(&payload)),
                        )),
                    };

                    let _ = tx.send(match result {
                        Ok(data) => LoadDataResult::ok(filepath, data),
                        Err(err) => LoadDataResult::err(filepath, err),
                    });
                });
            } else {
                return Err(BacktestError::DataError(IoError::new(
                    ErrorKind::InvalidData,
                    "unsupported data type",
                )));
            }
        }
        Ok(())
    }
}

fn panic_payload_to_string(payload: &Box<dyn Any + Send + 'static>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// `DataPreprocess` offers a function to preprocess data before it is fed into the backtesting.
/// This feature is primarily introduced to adjust timestamps, making it particularly useful when
/// backtesting the market from a location different from where your order latency was originally
/// collected.
///
/// For example, if you're backtesting an arbitrage strategy between Binance Futures and ByBit,
/// and your order latency data was collected in a colocated AWS region, you may need to adjust
/// for the geographical difference. If your strategy is running with a base in Tokyo
/// (where Binance Futures is located), you would need to account for the latency between
/// Singapore (where ByBit is located) and Tokyo by applying an appropriate offset.
pub trait DataPreprocess<D>
where
    D: POD + Clone,
{
    fn preprocess(&self, data: &mut Data<D>) -> Result<(), IoError>;
}

/// Pre-processes the feed data to adjust for latency. `local_ts` is offset by the specified latency
/// offset.
#[derive(Clone)]
pub struct FeedLatencyAdjustment {
    latency_offset: i64,
}

impl FeedLatencyAdjustment {
    /// Constructs a `FeedLatencyAdjustment`.
    pub fn new(latency_offset: i64) -> Self {
        Self { latency_offset }
    }
}

impl DataPreprocess<Event> for FeedLatencyAdjustment {
    fn preprocess(&self, data: &mut Data<Event>) -> Result<(), IoError> {
        for i in 0..data.len() {
            let row = &mut data[i];
            row.local_ts = row.local_ts.checked_add(self.latency_offset).ok_or_else(|| {
                IoError::new(
                    ErrorKind::InvalidData,
                    "`local_ts` overflowed while applying the latency offset",
                )
            })?;
            if row.local_ts <= row.exch_ts {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    "`local_ts` became less than or \
                    equal to `exch_ts` after applying the latency offset",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        sync::mpsc::channel,
        time::Duration,
    };

    use std::io::ErrorKind;

    use super::*;

    #[test]
    fn feed_latency_adjustment_detects_timestamp_overflow() {
        let adjustment = FeedLatencyAdjustment::new(1);
        let mut data = Data::from_data(&[Event {
            ev: 0,
            exch_ts: 0,
            local_ts: i64::MAX,
            px: 0.0,
            qty: 0.0,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        }]);

        let err = adjustment.preprocess(&mut data).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn next_data_returns_error_if_loader_panics_instead_of_hanging() {
        struct PanicPreprocessor;

        impl DataPreprocess<Event> for PanicPreprocessor {
            fn preprocess(&self, _data: &mut Data<Event>) -> Result<(), IoError> {
                panic!("intentional panic in loader thread");
            }
        }

        let filepath = std::env::temp_dir().join(format!(
            "hftbacktest_reader_panic_{}.npy",
            Uuid::new_v4()
        ));
        let mut file = File::create(&filepath).unwrap();

        crate::backtest::data::write_npy(
            &mut file,
            &[Event {
                ev: 0,
                exch_ts: 1,
                local_ts: 2,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            }],
        )
        .unwrap();

        let filepath_str = filepath.to_string_lossy().to_string();
        let (tx, rx) = channel();
        let handle = thread::spawn(move || {
            let mut reader = Reader::<Event>::builder()
                .parallel_load(false)
                .preprocessor(PanicPreprocessor)
                .data(vec![DataSource::File(filepath_str)])
                .build()
                .unwrap();

            let res = match reader.next_data() {
                Ok(_) => Ok(()),
                Err(BacktestError::DataError(err)) => Err(err.kind()),
                Err(_) => Err(ErrorKind::Other),
            };
            let _ = tx.send(res);
        });

        let res = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("Reader::next_data() hung instead of returning an error");

        let _ = handle.join();
        let _ = std::fs::remove_file(filepath);

        assert_eq!(res, Err(ErrorKind::Other));
    }
}
