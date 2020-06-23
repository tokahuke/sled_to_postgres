#![allow(non_snake_case, unused_variables)]

use tokio_postgres::types::ToSql;

pub trait SqlTuple {
    type ArrayTuple: SqlArrayTuple;
    fn update(self, array_tuple: &mut Self::ArrayTuple);
}

pub trait SqlArrayTuple {
    fn init() -> Self;
    fn into_params(self) -> Vec<Box<dyn ToSql + Sync + Send>>;
}

macro_rules! impl_sql_tuple {
    ($($var:tt : $type:ident),*) => {
        impl<$($type: 'static + ToSql + Sync + Send,)*> SqlTuple for ($($type,)*) {
            type ArrayTuple = ($(Vec<$type>,)*);
            fn update(self, array_tuple: &mut Self::ArrayTuple) {
                $(
                    array_tuple.$var.push(self.$var);
                )*
            }
        }

        impl<$($type: 'static + ToSql + Sync + Send,)*> SqlArrayTuple for ($(Vec<$type>,)*) {
            fn init() -> Self {
                $(
                    let $type = vec![];
                )*

                ($($type,)*)
            }
            fn into_params(self) -> Vec<Box<dyn ToSql + Sync + Send>> {
                let ($($type,)*) = self;
                vec![$(
                    Box::new($type) as Box<dyn ToSql + Sync + Send>,
                )*]
            }
        }
    };
}

impl_sql_tuple! {}
impl_sql_tuple! { 0: A }
impl_sql_tuple! { 0: A, 1: B }
impl_sql_tuple! { 0: A, 1: B , 2: C}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q, 17: R}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q, 17: R, 18: S}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q, 17: R, 18: S, 19: T}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q, 17: R, 18: S, 19: T, 20: U}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q, 17: R, 18: S, 19: T, 20: U, 21: V}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q, 17: R, 18: S, 19: T, 20: U, 21: V, 22: W}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q, 17: R, 18: S, 19: T, 20: U, 21: V, 22: W, 23: X}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q, 17: R, 18: S, 19: T, 20: U, 21: V, 22: W, 23: X, 24: Y}
impl_sql_tuple! { 0: A, 1: B , 2: C, 3: D, 4: E, 5: F, 6: G, 7: H, 8: I, 9: J, 10: K, 11: L, 12: M, 13: N, 14: O, 15: P, 16: Q, 17: R, 18: S, 19: T, 20: U, 21: V, 22: W, 23: X, 24: Y, 25: Z}
