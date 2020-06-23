// /// Helper macro for creating boxed query parameters ergonomically.
// ///
// /// To transform from Sled to Postgres, you will often have to do the
// /// following kind of transformation.
// /// ```rust
// /// |key: &[u8], value: &[u8]| {
// ///     vec![
// ///         Box::new(decode(&*key)) as Box<dyn ToSql + Send + Sync>,
// ///         Box::new(decode(&*value)) as Box<dyn ToSql + Send + Sync>,
// ///     ]
// /// }
// /// ```
// /// This is really bothersome! You can use `params!` to make your experience
// /// a bit more pleasant.
// /// ```rust
// /// |key: &[u8], value: &[u8]| params![decode(&*key), decode(&*value)]
// /// ```
// #[macro_export]
// macro_rules! params {
//     ($($item:expr),*) => {
//         vec![$(
//             Box::new($item) as Box<dyn $crate::ToSql + Send + Sync>,
//         )*]
//     };
// }
