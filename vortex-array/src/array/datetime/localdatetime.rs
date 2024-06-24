use lazy_static::lazy_static;
use vortex_dtype::{DType, ExtDType, ExtID};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::array::datetime::TimeUnit;
use crate::array::extension::ExtensionArray;
use crate::{Array, ArrayDType, ArrayData, IntoArrayData};

lazy_static! {
    pub static ref ID: ExtID = ExtID::from(LocalDateTimeArray::ID);
}

pub struct LocalDateTimeArray {
    ext: ExtensionArray,
    time_unit: TimeUnit,
}

impl LocalDateTimeArray {
    pub const ID: &'static str = "vortex.localdatetime";

    pub fn try_new(time_unit: TimeUnit, timestamps: Array) -> VortexResult<Self> {
        if !timestamps.dtype().is_int() {
            vortex_bail!("Timestamps must be an integer array")
        }
        Ok(Self {
            ext: ExtensionArray::new(Self::ext_dtype(time_unit), timestamps),
            time_unit,
        })
    }

    pub fn ext_dtype(time_unit: TimeUnit) -> ExtDType {
        ExtDType::new(ID.clone(), Some(time_unit.metadata().clone()))
    }

    pub fn dtype(&self) -> &DType {
        self.ext.dtype()
    }

    pub fn time_unit(&self) -> TimeUnit {
        self.time_unit
    }

    pub fn timestamps(&self) -> Array {
        self.ext.storage()
    }
}

impl TryFrom<LocalDateTimeArray> for ExtensionArray {
    type Error = VortexError;

    fn try_from(value: LocalDateTimeArray) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&LocalDateTimeArray> for ExtensionArray {
    type Error = VortexError;

    fn try_from(value: &LocalDateTimeArray) -> Result<Self, Self::Error> {
        let DType::Extension(ext_dtype, _) = value.dtype().clone() else {
            vortex_bail!(ComputeError: "expected dtype to be Extension variant");
        };

        Ok(Self::new(ext_dtype, value.ext.storage()))
    }
}

impl TryFrom<&ExtensionArray> for LocalDateTimeArray {
    type Error = VortexError;

    fn try_from(value: &ExtensionArray) -> Result<Self, Self::Error> {
        Self::try_new(try_parse_time_unit(value.ext_dtype())?, value.storage())
    }
}

impl TryFrom<&Array> for LocalDateTimeArray {
    type Error = VortexError;

    fn try_from(value: &Array) -> Result<Self, Self::Error> {
        let ext = ExtensionArray::try_from(value)?;
        Self::try_new(try_parse_time_unit(ext.ext_dtype())?, ext.storage())
    }
}

impl IntoArrayData for LocalDateTimeArray {
    fn into_array_data(self) -> ArrayData {
        self.ext.into_array_data()
    }
}

pub fn try_parse_time_unit(ext_dtype: &ExtDType) -> VortexResult<TimeUnit> {
    let byte: [u8; 1] = ext_dtype
        .metadata()
        .ok_or_else(|| vortex_err!("Missing metadata"))?
        .as_ref()
        .try_into()?;
    TimeUnit::try_from(byte[0]).map_err(|_| vortex_err!("Invalid time unit in metadata"))
}
