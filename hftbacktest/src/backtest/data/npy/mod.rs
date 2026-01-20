#[cfg(feature = "s3")]
use std::io::Cursor;
use std::{
    fs::File,
    io::{Error, ErrorKind, Read, Write},
};

use crate::{
    backtest::data::{Data, DataPtr, POD, npy::parser::Value},
    utils::CACHE_LINE_SIZE,
};

mod parser;

/// Trait
pub trait NpyDTyped: POD {
    fn descr() -> DType;
}

pub type DType = Vec<Field>;

/// Representation of a Numpy file header.
#[derive(PartialEq, Eq, Debug)]
pub struct NpyHeader {
    pub descr: DType,
    pub fortran_order: bool,
    pub shape: Vec<usize>,
}

/// Representation of a field in a Numpy structured array.
#[derive(PartialEq, Eq, Debug)]
pub struct Field {
    pub name: String,
    pub ty: String,
}

impl NpyHeader {
    pub fn descr(&self) -> String {
        self.descr
            .iter()
            .map(|Field { name, ty }| format!("('{name}', '{ty}'), "))
            .fold("[".to_string(), |o, n| o + &n)
            + "]"
    }

    pub fn fortran_order(&self) -> String {
        if self.fortran_order {
            "True".to_string()
        } else {
            "False".to_string()
        }
    }

    pub fn shape(&self) -> String {
        self.shape
            .iter()
            .map(|len| format!("{len}, "))
            .fold("(".to_string(), |o, n| o + &n)
            + ")"
    }

    pub fn from_header(header: &str) -> std::io::Result<Self> {
        let (_, header) = parser::parse::<(&str, nom::error::ErrorKind)>(header)
            .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
        let dict = header.get_dict()?;
        let mut descr: Option<Vec<Field>> = None;
        let mut fortran_order: Option<bool> = None;
        let mut shape: Option<Vec<usize>> = None;
        for (key, value) in dict {
            match key.as_str() {
                "descr" => {
                    if descr.is_some() {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "duplicate header key: descr".to_string(),
                        ));
                    }
                    let mut fields = Vec::new();
                    let list = value.get_list()?;
                    for item in list {
                        let tuple = item.get_list()?;
                        match tuple.len() {
                            2 => {
                                match (&tuple[0], &tuple[1]) {
                                    (Value::String(name), Value::String(dtype)) => {
                                        fields.push(Field {
                                            name: name.clone(),
                                            ty: dtype.clone(),
                                        });
                                    }
                                    _ => return Err(Error::new(
                                        ErrorKind::InvalidData,
                                        "list entry must contain a string for id and a valid dtype"
                                            .to_string(),
                                    )),
                                }
                            }
                            _ => {
                                return Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "list entry must contain 2 items".to_string(),
                                ));
                            }
                        }
                    }
                    if fields.is_empty() {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "descr must not be empty".to_string(),
                        ));
                    }
                    descr = Some(fields);
                }
                "fortran_order" => {
                    if fortran_order.is_some() {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "duplicate header key: fortran_order".to_string(),
                        ));
                    }
                    fortran_order = Some(value.get_bool()?);
                }
                "shape" => {
                    if shape.is_some() {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "duplicate header key: shape".to_string(),
                        ));
                    }
                    let mut dims = Vec::new();
                    for num in value.get_list()? {
                        dims.push(num.get_integer()?);
                    }
                    if dims.is_empty() {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "shape must not be empty".to_string(),
                        ));
                    }
                    shape = Some(dims);
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected header key: {key}"),
                    ));
                }
            }
        }
        let descr = descr.ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "missing required header key: descr".to_string(),
            )
        })?;
        let fortran_order = fortran_order.ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "missing required header key: fortran_order".to_string(),
            )
        })?;
        let shape = shape.ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "missing required header key: shape".to_string(),
            )
        })?;
        Ok(NpyHeader {
            descr,
            fortran_order,
            shape,
        })
    }

    fn to_string_padding(&self) -> String {
        let descr = self.descr();
        let fortran_order = self.fortran_order();
        let shape = self.shape();
        let mut header =
            format!("{{'descr': {descr}, 'fortran_order': {fortran_order}, 'shape': {shape}}}");
        let header_len = 10 + header.len() + 1;
        if header_len % 64 != 0 {
            let padding = (header_len / 64 + 1) * 64 - header_len;
            for _ in 0..padding {
                header += " ";
            }
        }
        header += "\n";
        header
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct FieldCheckResult {
    expected: String,
    found: String,
}

fn check_field_consistency(
    expected_types: &DType,
    found_types: &DType,
) -> Result<Vec<FieldCheckResult>, String> {
    let mut discrepancies = vec![];
    if expected_types.len() != found_types.len() {
        return Err(format!(
            "Field count mismatch: expected {}, but found {}",
            expected_types.len(),
            found_types.len()
        ));
    }
    for (expected, found) in expected_types.iter().zip(found_types.iter()) {
        if expected.ty != found.ty {
            return Err(format!(
                "Field type mismatch: expected '{}: {}', but found '{}: {}'",
                expected.name, expected.ty, found.name, found.ty
            ));
        }
        if expected.name != found.name {
            discrepancies.push(FieldCheckResult {
                expected: expected.name.to_string(),
                found: found.name.to_string(),
            });
        }
    }
    Ok(discrepancies)
}

// S3-related code is only compiled when the "s3" feature is enabled
#[cfg(feature = "s3")]
mod s3_support {
    use super::*;

    pub async fn read_s3_object_async(s3_path: &str) -> std::io::Result<Vec<u8>> {
        // Parse S3 path: s3://bucket/key
        let path_without_prefix = s3_path
            .strip_prefix("s3://")
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Invalid S3 path format"))?;

        let parts: Vec<&str> = path_without_prefix.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Invalid S3 path format",
            ));
        }

        let bucket = parts[0];
        let key = parts[1];

        // Get AWS profile from environment
        let profile_name = std::env::var("AWS_PROFILE").map_err(|_| {
            Error::new(
                ErrorKind::NotFound,
                "AWS_PROFILE environment variable not found",
            )
        })?;

        // Create session with profile
        let session = aws_config::from_env()
            .profile_name(&profile_name)
            .load()
            .await;

        let s3_client = aws_sdk_s3::Client::new(&session);

        // Get object from S3
        let response = s3_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Error::other(format!("S3 request failed: {e}")))?;

        let bytes = response
            .body
            .collect()
            .await
            .map_err(|e| Error::other(format!("Failed to read response body: {e}")))?;

        Ok(bytes.into_bytes().to_vec())
    }

    pub fn read_s3_object(s3_path: &str) -> std::io::Result<Vec<u8>> {
        // Create runtime
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::other(format!("Failed to create runtime: {e}")))?;

        rt.block_on(read_s3_object_async(s3_path))
    }
}

pub fn read_npy<R: Read, D: NpyDTyped + Clone>(
    reader: &mut R,
    size: usize,
) -> std::io::Result<Data<D>> {
    if size == 0 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "empty input",
        ));
    }

    // Minimum size for v1.0 header: magic(6) + ver(2) + header_len(2).
    if size < 10 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "file too small to be a valid .npy",
        ));
    }

    let mut buf = DataPtr::new(size);
    reader.read_exact(&mut buf[..])?;

    let bytes = &buf[..];
    if bytes.get(0..6) != Some(b"\x93NUMPY") {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "must start with \\x93NUMPY",
        ));
    }
    if bytes.get(6..8) != Some(b"\x01\x00") {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "support only version 1.0",
        ));
    }

    let header_len = {
        let header_len_bytes = bytes.get(8..10).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "missing .npy header length",
            )
        })?;
        let mut arr = [0u8; 2];
        arr.copy_from_slice(header_len_bytes);
        u16::from_le_bytes(arr) as usize
    };

    let data_offset = 10usize
        .checked_add(header_len)
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "header length overflow"))?;
    if data_offset > size {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "truncated .npy header",
        ));
    }
    let header_bytes = bytes
        .get(10..data_offset)
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "truncated .npy header"))?;
    if !header_bytes.ends_with(b"\n") {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "header must end with newline",
        ));
    }

    let header_str = std::str::from_utf8(header_bytes)
        .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
    let header = NpyHeader::from_header(header_str)?;

    if header.fortran_order {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "fortran order is unsupported",
        ));
    }

    let expected_descr = D::descr();
    if expected_descr != header.descr {
        match check_field_consistency(&expected_descr, &header.descr) {
            Ok(diff) => {
                println!("Warning: Field name mismatch - {diff:?}");
            }
            Err(err) => {
                return Err(Error::new(ErrorKind::InvalidData, err));
            }
        }
    }

    if header.shape.len() != 1 {
        return Err(Error::new(ErrorKind::InvalidData, "only 1-d is supported"));
    }

    if !data_offset.is_multiple_of(CACHE_LINE_SIZE) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("Not aligned with cache line size ({CACHE_LINE_SIZE} bytes)."),
        ));
    }

    let d_align = std::mem::align_of::<D>();
    if d_align > CACHE_LINE_SIZE {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "alignment of record type ({d_align}) exceeds cache line size ({CACHE_LINE_SIZE})"
            ),
        ));
    }
    if data_offset % d_align != 0 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "data offset is not aligned for record type",
        ));
    }

    let payload_len = size - data_offset;
    let record_size = std::mem::size_of::<D>();
    if record_size == 0 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "zero-sized record types are unsupported",
        ));
    }
    if payload_len % record_size != 0 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "data payload is truncated (not a whole number of records)",
        ));
    }
    let record_count = payload_len / record_size;
    if header.shape[0] != record_count {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "shape mismatch: header has {}, but payload has {record_count} records",
                header.shape[0]
            ),
        ));
    }

    let data = unsafe { Data::from_data_ptr(buf, data_offset) };
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    struct Row1 {
        a: i64,
    }
    unsafe impl POD for Row1 {}
    impl NpyDTyped for Row1 {
        fn descr() -> DType {
            vec![Field {
                name: "a".to_string(),
                ty: "<i8".to_string(),
            }]
        }
    }

    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    struct Row2 {
        a: i64,
        b: i64,
    }
    unsafe impl POD for Row2 {}
    impl NpyDTyped for Row2 {
        fn descr() -> DType {
            vec![
                Field {
                    name: "a".to_string(),
                    ty: "<i8".to_string(),
                },
                Field {
                    name: "b".to_string(),
                    ty: "<i8".to_string(),
                },
            ]
        }
    }

    #[test]
    fn read_npy_rejects_empty_input() {
        let mut cursor = Cursor::new(Vec::new());
        let err = read_npy::<_, Row1>(&mut cursor, 0).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn read_npy_does_not_hang_on_premature_eof() {
        let mut buf = Vec::new();
        write_npy(&mut buf, &[Row1 { a: 1 }]).unwrap();

        // Claim the file is longer than it actually is; the old loop would spin forever once
        // `read()` started returning Ok(0).
        let mut cursor = Cursor::new(buf);
        let size = cursor.get_ref().len();
        let err = read_npy::<_, Row1>(&mut cursor, size + 1).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn read_npy_rejects_truncated_header_by_len() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"\x93NUMPY\x01\x00");
        bytes.extend_from_slice(&1u16.to_le_bytes()); // header_len = 1, but no header bytes follow.

        let mut cursor = Cursor::new(bytes);
        let size = cursor.get_ref().len();
        let err = read_npy::<_, Row1>(&mut cursor, size).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn read_npy_rejects_field_count_mismatch() {
        let mut buf = Vec::new();
        write_npy(&mut buf, &[Row2 { a: 1, b: 2 }]).unwrap();

        let mut cursor = Cursor::new(buf);
        let size = cursor.get_ref().len();
        let err = read_npy::<_, Row1>(&mut cursor, size).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn from_header_rejects_missing_required_keys() {
        let err = NpyHeader::from_header("{'fortran_order': False, 'shape': (1,)}").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);

        let err = NpyHeader::from_header("{'descr': [('a','<i8')], 'shape': (1,)}").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);

        let err =
            NpyHeader::from_header("{'descr': [('a','<i8')], 'fortran_order': False}").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn read_npy_rejects_truncated_payload_partial_record() {
        let mut buf = Vec::new();
        write_npy(&mut buf, &[Row1 { a: 1 }]).unwrap();
        buf.pop(); // truncate by 1 byte

        let mut cursor = Cursor::new(buf);
        let size = cursor.get_ref().len();
        let err = read_npy::<_, Row1>(&mut cursor, size).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }
}

/// Reads a structured array `numpy` file. Currently, it doesn't check if the data structure is the
/// same as what the file contains. Users should be cautious about this.
///
/// # S3 Support
/// Supports S3 paths in format: `s3://bucket-name/path/to/file.npy` when the "s3" feature is enabled.
/// Enable the feature in Cargo.toml: `features = ["s3"]`
pub fn read_npy_file<D: NpyDTyped + Clone>(filepath: &str) -> std::io::Result<Data<D>> {
    if filepath.starts_with("s3://") {
        #[cfg(feature = "s3")]
        {
            let data = s3_support::read_s3_object(filepath)?;
            let size = data.len();
            let mut cursor = Cursor::new(data);
            read_npy(&mut cursor, size)
        }

        #[cfg(not(feature = "s3"))]
        {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "S3 support is not enabled. Enable the 's3' feature in Cargo.toml to use S3 paths: features = [\"s3\"]",
            ));
        }
    } else {
        let mut file = File::open(filepath)?;
        file.sync_all()?;
        let size = file.metadata()?.len() as usize;
        read_npy(&mut file, size)
    }
}

/// Reads a structured array `numpy` zip archived file. Currently, it doesn't check if the data
/// structure is the same as what the file contains. Users should be cautious about this.
///
/// # S3 Support
/// Supports S3 paths in format: `s3://bucket-name/path/to/file.npz` when the "s3" feature is enabled.
/// Enable the feature in Cargo.toml: `features = ["s3"]`
pub fn read_npz_file<D: NpyDTyped + Clone>(filepath: &str, name: &str) -> std::io::Result<Data<D>> {
    if filepath.starts_with("s3://") {
        #[cfg(feature = "s3")]
        {
            let data = s3_support::read_s3_object(filepath)?;
            let cursor = Cursor::new(data);
            let mut archive = zip::ZipArchive::new(cursor)?;
            let mut file = archive.by_name(&format!("{name}.npy"))?;
            let size = file.size() as usize;
            read_npy(&mut file, size)
        }

        #[cfg(not(feature = "s3"))]
        {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "S3 support is not enabled. Enable the 's3' feature in Cargo.toml to use S3 paths: features = [\"s3\"]",
            ));
        }
    } else {
        let mut archive = zip::ZipArchive::new(File::open(filepath)?)?;
        let mut file = archive.by_name(&format!("{name}.npy"))?;
        let size = file.size() as usize;
        read_npy(&mut file, size)
    }
}

pub fn write_npy<W: Write, T: NpyDTyped>(write: &mut W, data: &[T]) -> std::io::Result<()> {
    let descr = T::descr();
    let header = NpyHeader {
        descr,
        fortran_order: false,
        shape: vec![data.len()],
    };

    write.write_all(b"\x93NUMPY\x01\x00")?;
    let header_str = header.to_string_padding();
    let len = header_str.len() as u16;
    write.write_all(&len.to_le_bytes())?;
    write.write_all(header_str.as_bytes())?;
    write.write_all(vec_as_bytes(data))?;
    Ok(())
}

fn vec_as_bytes<T>(vec: &[T]) -> &[u8] {
    let len = std::mem::size_of_val(vec);
    let ptr = vec.as_ptr() as *const u8;
    unsafe { std::slice::from_raw_parts(ptr, len) }
}
