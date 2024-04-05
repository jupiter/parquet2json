use std::sync::Arc;

use arrow_array::{types::GenericBinaryType, Array, ArrayRef, GenericByteArray, OffsetSizeTrait};
use arrow_cast::base64::{b64_encode, BASE64_STANDARD};
use arrow_schema::ArrowError;

pub fn cast_binary_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<GenericByteArray<GenericBinaryType<O>>>()
        .unwrap();

    Ok(Arc::new(b64_encode(&BASE64_STANDARD, array)))
}
