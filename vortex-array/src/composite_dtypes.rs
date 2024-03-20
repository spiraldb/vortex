// const LOCALTIME_DTYPE: &str = "localtime";
//
// pub fn localtime(unit: TimeUnit, width: IntWidth, nullability: Nullability) -> DType {
//     DType::Composite(
//         Arc::new(LOCALTIME_DTYPE.to_string()),
//         Box::new(DType::Int(width, Signedness::Signed, nullability)),
//         TimeUnitSerializer::serialize(unit),
//     )
// }
//
// const LOCALDATE_DTYPE: &str = "localdate";
//
// pub fn localdate(width: IntWidth, nullability: Nullability) -> DType {
//     DType::Composite(
//         Arc::new(LOCALDATE_DTYPE.to_string()),
//         Box::new(DType::Int(width, Signedness::Signed, nullability)),
//         vec![],
//     )
// }
//
// const INSTANT_DTYPE: &str = "instant";
//
// pub fn instant(unit: TimeUnit, nullability: Nullability) -> DType {
//     DType::Composite(
//         Arc::new(INSTANT_DTYPE.to_string()),
//         Box::new(DType::Int(IntWidth::_64, Signedness::Signed, nullability)),
//         TimeUnitSerializer::serialize(unit),
//     )
// }
//
// const ZONEDDATETIME_DTYPE: &str = "zoneddatetime";
//
// pub fn zoneddatetime(unit: TimeUnit, nullability: Nullability) -> DType {
//     DType::Composite(
//         Arc::new(ZONEDDATETIME_DTYPE.to_string()),
//         Box::new(DType::Struct(
//             vec![
//                 Arc::new("instant".to_string()),
//                 Arc::new("timezone".to_string()),
//             ],
//             vec![
//                 DType::Int(IntWidth::_64, Signedness::Signed, nullability),
//                 DType::Utf8(nullability),
//             ],
//         )),
//         TimeUnitSerializer::serialize(unit),
//     )
// }
