use std::os::raw::c_int;
use std::ptr;
use std::ptr::addr_of_mut;

use async_std::task;
use datafusion::arrow::array::RecordBatch;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys::{AsPgCStr, TopMemoryContext};
use pgrx::PgTupleDesc;
use pgrx::prelude::*;

use crate::utils::{generate_test_data_for_oid, run_df_sql, SerdeList};

#[pg_guard]
pub extern "C" fn datafusion_get_foreign_rel_size(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    debug2!("---> get_foreign_rel_size");
    // it is the first called fdw function.
    // init private state here
    unsafe {
        let ctx_name = format!("datafusion_fdw");

        let ctx = pg_sys::AllocSetContextCreateExtended(
            TopMemoryContext,
            ctx_name.as_str().as_pg_cstr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );
        let ctx = PgMemoryContexts::For(ctx);


        let mut my_fdw_state = DataFusionFdwStat::new(ctx);

        my_fdw_state.current = 0;
        my_fdw_state.total = 100;


        let ctx = my_fdw_state.self_ctx.value();


        (*baserel).fdw_private = PgMemoryContexts::For(ctx).leak_and_drop_on_delete(my_fdw_state) as _;
    }
    // get estimate row count and mean row width
    // let (rows, width) = state.get_rel_size().report_unwrap();
    // (*baserel).rows = rows as f64;
    // (*(*baserel).reltarget).width = width;
}

#[pg_guard]
pub extern "C" fn datafusion_get_foreign_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    debug2!("---> get_foreign_paths");
    unsafe {
        // create a ForeignPath node and add it as the only possible path
        let path = pg_sys::create_foreignscan_path(
            root,
            baserel,
            ptr::null_mut(), // default pathtarget
            (*baserel).rows,
            0.0,
            0.0,
            ptr::null_mut(), // no pathkeys
            ptr::null_mut(), // no outer rel either
            ptr::null_mut(), // no extra plan
            ptr::null_mut(), // no fdw_private data
        );
        pg_sys::add_path(baserel, &mut ((*path).path));
    }
}

#[pg_guard]
pub extern "C" fn datafusion_get_foreign_plan(
    _root: *mut pgrx::prelude::pg_sys::PlannerInfo,
    baserel: *mut pgrx::prelude::pg_sys::RelOptInfo,
    _foreigntableid: pgrx::prelude::pg_sys::Oid,
    _best_path: *mut pgrx::prelude::pg_sys::ForeignPath,
    tlist: *mut pgrx::prelude::pg_sys::List,
    scan_clauses: *mut pgrx::prelude::pg_sys::List,
    outer_plan: *mut pgrx::prelude::pg_sys::Plan,
) -> *mut pgrx::prelude::pg_sys::ForeignScan {
    debug2!("---> get_foreign_plan");

    unsafe {
        let state = PgBox::<DataFusionFdwStat>::from_pg((*baserel).fdw_private as _);

        let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);

        let ctx = PgMemoryContexts::For(state.self_ctx.value());

        let fdw_private = DataFusionFdwStat::serialize_to_list(state, ctx);

        pg_sys::make_foreignscan(
            tlist,
            scan_clauses,
            (*baserel).relid,
            ptr::null_mut(),
            fdw_private,
            ptr::null_mut(),
            ptr::null_mut(),
            outer_plan,
        )
    }
}


struct DataFusionFdwStat {
    pub self_ctx: PgMemoryContexts,
    pub current: u64,
    pub total: u64,
    // query conditions
    pub quals: Vec<String>,
    pub df_result: Option<Vec<RecordBatch>>,
}

impl SerdeList for DataFusionFdwStat {}

impl DataFusionFdwStat {
    unsafe fn new(self_ctx: PgMemoryContexts) -> Self {
        Self {
            current: 0,
            total: 0,
            quals: Vec::new(),
            self_ctx,
            df_result: None,
        }
    }
}

#[pg_guard]
pub extern "C" fn datafusion_begin_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    eflags: c_int,
) {
    debug2!("---> begin_foreign_scan");
    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as c_int > 0 {
        log!("explain only, do nothing");
        return;
    }
    unsafe {
        let scan_state = (*node).ss;
        let plan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
        let mut state = DataFusionFdwStat::deserialize_from_list((*plan).fdw_private as _);
        assert!(!state.is_null());


        // deparse, where, join, sort and limit
        // run remote query
        state.df_result = match run_df_sql() {
            Ok(v) => match (task::block_on(v.collect())) {
                Ok(v) => Some(v),
                _ => None
            }
            _ => None
        };

        (*node).fdw_state = state.into_pg() as _;
    }
}

#[pg_guard]
pub extern "C" fn datafusion_re_scan_foreign_scan(_node: *mut pg_sys::ForeignScanState) {
    debug2!("---> re_scan_foreign_scan");
}

#[pg_guard]
pub extern "C" fn datafusion_end_foreign_scan(_node: *mut pg_sys::ForeignScanState) {
    debug2!("---> end_foreign_scan");
    // unsafe {
    // let mut my_fdw_stat =
    //     PgBox::<RandomFdwStat>::from_pg((*node).fdw_state as *mut RandomFdwStat);
    // // debug2!("{:?}", my_fdw_stat.opts);
    // }
}

#[pg_guard]
pub extern "C" fn datafusion_iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    //debug2!("---> iterate_foreign_scan");
    unsafe {
        let mut state = PgBox::<DataFusionFdwStat>::from_pg((*node).fdw_state as _);
        let slot = (*node).ss.ss_ScanTupleSlot;

        // clear slot
        if let Some(clear) = (*(*slot).tts_ops).clear {
            clear(slot);
        }
        if state.current < state.total {
            state.current += 1;
        } else {
            debug2!("---> iterate_foreign_scan done");
            return slot;
        }

        let tup_desc = (*slot).tts_tupleDescriptor;

        let tuple_desc = PgTupleDesc::from_pg_copy(tup_desc);

        for (col_index, attr) in tuple_desc.iter().enumerate() {
            let tts_isnull = (*slot).tts_isnull.add(col_index);
            let tts_value = (*slot).tts_values.add(col_index);

            match generate_test_data_for_oid(attr.atttypid) {
                Some(v) => *tts_value = v,
                None => *tts_isnull = true,
            }
        }


        pgrx::prelude::pg_sys::ExecStoreVirtualTuple(slot);

        slot
    }
}

pub static mut DATAFUSION_FDW_ROUTINE: pg_sys::FdwRoutine = pg_sys::FdwRoutine {
    type_: pg_sys::NodeTag::T_FdwRoutine,
    BeginForeignScan: Some(datafusion_begin_foreign_scan),
    IterateForeignScan: Some(datafusion_iterate_foreign_scan),
    ReScanForeignScan: Some(datafusion_re_scan_foreign_scan),
    EndForeignScan: Some(datafusion_end_foreign_scan),
    GetForeignJoinPaths: None,
    GetForeignUpperPaths: None,
    AddForeignUpdateTargets: None,
    PlanForeignModify: None,
    BeginForeignModify: None,
    ExecForeignInsert: None,
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    ExecForeignBatchInsert: None,
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    GetForeignModifyBatchSize: None,
    ExecForeignUpdate: None,
    ExecForeignDelete: None,
    EndForeignModify: None,
    BeginForeignInsert: None,
    EndForeignInsert: None,
    IsForeignRelUpdatable: None,
    PlanDirectModify: None,
    BeginDirectModify: None,
    IterateDirectModify: None,
    EndDirectModify: None,
    GetForeignRowMarkType: None,
    RefetchForeignRow: None,
    GetForeignRelSize: Some(datafusion_get_foreign_rel_size),
    GetForeignPaths: Some(datafusion_get_foreign_paths),
    GetForeignPlan: Some(datafusion_get_foreign_plan),
    ExplainForeignScan: None,
    ExplainForeignModify: None,
    ExplainDirectModify: None,
    AnalyzeForeignTable: None,
    ImportForeignSchema: None,
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    ExecForeignTruncate: None,
    IsForeignScanParallelSafe: None,
    EstimateDSMForeignScan: None,
    InitializeDSMForeignScan: None,
    ReInitializeDSMForeignScan: None,
    InitializeWorkerForeignScan: None,
    ShutdownForeignScan: None,
    ReparameterizeForeignPathByChild: None,
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    IsForeignPathAsyncCapable: None,
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    ForeignAsyncRequest: None,
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    ForeignAsyncConfigureWait: None,
    RecheckForeignScan: None,
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    ForeignAsyncNotify: None,
};

#[pg_guard]
#[no_mangle]
extern "C" fn pg_finfo_datafusion_fdw_handler() -> &'static pg_sys::Pg_finfo_record {
    const V1_API: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}


#[no_mangle]
#[pg_guard]
extern "C" fn datafusion_fdw_handler(_fcinfo: pg_sys::FunctionCallInfo) -> *mut pg_sys::FdwRoutine {
    unsafe { addr_of_mut!(DATAFUSION_FDW_ROUTINE) }
}

