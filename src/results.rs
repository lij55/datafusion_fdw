use std::slice::{Iter, IterMut};
use std::vec::IntoIter;
use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::Int32Type;
use pgrx::IntoDatum;
use pgrx::pg_sys::Datum;

fn transpose2<T>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    assert!(!v.is_empty());
    let len = v[0].len();
    let mut iters: Vec<_> = v.into_iter().map(|n| n.into_iter()).collect();
    (0..len)
        .map(|_| {
            iters
                .iter_mut()
                .map(|n| n.next().unwrap())
                .collect::<Vec<T>>()
        })
        .collect()
}

pub fn transpose_recordbatch(batch: &RecordBatch) -> Vec<Vec<Datum>> {
    let ret = batch.columns().iter().map(|c| {
        c.as_primitive::<Int32Type>().iter().map(|v| v.unwrap_or_default().into_datum().unwrap()).collect()
    }
    ).collect::<Vec<Vec<Datum>>>();
    transpose2::<Datum>(ret)
}

#[derive(Debug)]
pub struct DFResult {
    // df_batches:  Vec<RecordBatch>,
    // cached_records: Vec<Vec<Datum>>,
    batch_iter:  IntoIter<RecordBatch>,
    record_iter:  IntoIter<Vec<Datum>>,
    done: bool,
}

impl DFResult<> {
    pub fn new(mut record_batches: Vec<RecordBatch> ) -> DFResult<> {
        let mut ret =
            DFResult {
            batch_iter: record_batches.into_iter(), // default iter returns None
            record_iter: Default::default(),
                done: false
        };
        ret.update_cache();
        ret
    }

    pub fn next_record(&mut self) -> Option<Vec<Datum>> {
        let next_cache_item = self.record_iter.next();
        if next_cache_item.is_some() {
            return Some(next_cache_item.unwrap());
        }
        self.update_cache();
        return self.record_iter.next();

    }

    pub fn finished(self) -> bool {self.done}
    fn update_cache(&mut self) {
        let batch_to_cache = self.batch_iter.next();
        if batch_to_cache.is_none() {
            // done
            self.done = true;
            return;
        }
        let cached_records = transpose_recordbatch(&batch_to_cache.unwrap());
        self.record_iter = cached_records.into_iter();

    }
}