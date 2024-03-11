use std::str::FromStr;

use pgrx::*;
use pgrx::IntoDatum;
use pgrx::pg_sys::{Datum, Oid};

pub(super) trait SerdeList {
    unsafe fn serialize_to_list(state: PgBox<Self>, mut ctx: PgMemoryContexts) -> *mut pg_sys::List
        where
            Self: Sized,
    {
        let mut old_ctx = ctx.set_as_current();

        let mut ret = PgList::new();
        let val = state.into_pg() as i64;
        let cst = pg_sys::makeConst(
            pg_sys::INT8OID,
            -1,
            pg_sys::InvalidOid,
            8,
            val.into_datum().unwrap(),
            false,
            true,
        );
        ret.push(cst);
        let ret = ret.into_pg();

        old_ctx.set_as_current();

        ret
    }

    unsafe fn deserialize_from_list(list: *mut pg_sys::List) -> PgBox<Self>
        where
            Self: Sized,
    {
        let list = PgList::<pg_sys::Const>::from_pg(list);
        if list.is_empty() {
            return PgBox::<Self>::null();
        }

        let cst = list.head().unwrap();
        let ptr = i64::from_datum((*cst).constvalue, (*cst).constisnull).unwrap();
        PgBox::<Self>::from_pg(ptr as _)
    }
}

pub fn generate_test_data_for_oid(oid: Oid) -> Option<Datum> {

    // log!("{oid}");
    match oid {
        INT2OID => 10i16.into_datum(),
        INT4OID => 100i32.into_datum(),
        INT8OID => 1000i64.into_datum(),
        FLOAT4ARRAYOID => {
            let mut values = Vec::new();
            for i in 0..10 {
                values.push(i as f32)
            }
            values.into_datum()
        }
        FLOAT8ARRAYOID => {
            None
        }
        BOOLOID => (0).into_datum(),
        //CHAROID => Some(3.into()),
        FLOAT4OID => 2.0f32
            .into_datum(),
        FLOAT8OID => 8.8f64
            .into_datum(),

        NUMERICOID => AnyNumeric::try_from(
            1.23f32
        )
            .unwrap_or_default()
            .into_datum(),

        TEXTOID => String::from("hello")
            .into_datum(),

        VARCHAROID => String::from("hello")
            .into_datum(),

        BPCHAROID => String::from("hello")
            .into_datum(),

        DATEOID => {
            Date::from_str("1934-01-03").unwrap().into_datum()
        }
        TIMEOID => {
            Time::from_str("12:23:00").unwrap().into_datum()
        }
        TIMESTAMPOID => {
            None
        }
        INTERVALOID => {
            pgrx::Interval::new(0,
                                3,
                                400000).into_datum()
        }
        UUIDOID => {
            None
        }

        INETOID => {
            None
        }
        POINTOID => {
            None
        }
        BOXOID => {
            None
        }
        CIRCLEOID => {
            None
        }
        JSONOID => {
            None
        }
        XMLOID => None,
        _ => None,
    }
}

