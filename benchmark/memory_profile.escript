#!/usr/bin/env escript
%%! -pa _build/default/lib/cozodb/ebin -pa _build/default/lib/thoas/ebin -pa _build/default/lib/telemetry/ebin

%% Memory Profiling Script for CozoDB
%%
%% Usage:
%%   MALLOC_CONF="prof:true,lg_prof_interval:30" ./benchmark/memory_profile.escript
%%
%% This script:
%% 1. Opens a RocksDB database
%% 2. Captures baseline memory stats
%% 3. Runs workload (creates tables, inserts data)
%% 4. Captures memory stats during and after workload
%% 5. Dumps heap profiles for jemalloc analysis
%% 6. Closes database and captures final stats

-mode(compile).

main(_Args) ->
    io:format("~n========================================~n"),
    io:format("CozoDB Memory Profiling~n"),
    io:format("========================================~n~n"),

    %% Configuration
    NumTables = 10,
    RowsPerTable = 10000,
    ValueSize = 1024,
    DbPath = "/tmp/cozodb_memory_profile",
    ProfileDir = "/tmp/cozodb_heap_profiles",

    io:format("Configuration:~n"),
    io:format("  Tables: ~p~n", [NumTables]),
    io:format("  Rows per table: ~p~n", [RowsPerTable]),
    io:format("  Value size: ~p bytes~n", [ValueSize]),
    io:format("  DB path: ~s~n", [DbPath]),
    io:format("  Profile dir: ~s~n~n", [ProfileDir]),

    %% Clean up previous runs
    os:cmd("rm -rf " ++ DbPath),
    os:cmd("mkdir -p " ++ ProfileDir),

    %% Get OS PID for RSS monitoring
    OsPid = os:getpid(),
    io:format("OS PID: ~s~n~n", [OsPid]),

    %% Phase 1: Baseline
    io:format("~n--- Phase 1: Baseline (before opening DB) ---~n"),
    capture_memory_stats("baseline", ProfileDir, 1),

    %% Phase 2: Open database
    io:format("~n--- Phase 2: Opening RocksDB database ---~n"),
    {ok, Db} = cozodb:open(rocksdb, DbPath),
    timer:sleep(1000),  % Let things settle
    capture_memory_stats("after_open", ProfileDir, 2),
    capture_rocksdb_stats(Db, "after_open"),

    %% Phase 3: Create tables
    io:format("~n--- Phase 3: Creating ~p tables ---~n", [NumTables]),
    Tables = create_tables(Db, NumTables),
    timer:sleep(1000),
    capture_memory_stats("after_create_tables", ProfileDir, 3),
    capture_rocksdb_stats(Db, "after_create_tables"),

    %% Phase 4: Insert data
    io:format("~n--- Phase 4: Inserting ~p rows per table ---~n", [RowsPerTable]),
    Value = list_to_binary(lists:duplicate(ValueSize, $x)),
    insert_data(Db, Tables, RowsPerTable, Value),
    timer:sleep(2000),
    capture_memory_stats("after_insert", ProfileDir, 4),
    capture_rocksdb_stats(Db, "after_insert"),

    %% Phase 5: Flush memtables
    io:format("~n--- Phase 5: Flushing memtables ---~n"),
    ok = cozodb:flush_memtables(Db),
    timer:sleep(2000),
    capture_memory_stats("after_flush", ProfileDir, 5),
    capture_rocksdb_stats(Db, "after_flush"),

    %% Phase 6: Read workload
    io:format("~n--- Phase 6: Running read workload (1000 queries) ---~n"),
    run_read_workload(Db, Tables, 1000),
    capture_memory_stats("after_reads", ProfileDir, 6),
    capture_rocksdb_stats(Db, "after_reads"),

    %% Phase 7: More writes
    io:format("~n--- Phase 7: Running more writes (1000 per table) ---~n"),
    insert_data(Db, Tables, 1000, Value),
    capture_memory_stats("after_more_writes", ProfileDir, 7),
    capture_rocksdb_stats(Db, "after_more_writes"),

    %% Phase 8: Flush again
    io:format("~n--- Phase 8: Final flush ---~n"),
    ok = cozodb:flush_memtables(Db),
    timer:sleep(2000),
    capture_memory_stats("after_final_flush", ProfileDir, 8),
    capture_rocksdb_stats(Db, "after_final_flush"),

    %% Phase 9: Force GC and wait
    io:format("~n--- Phase 9: Erlang GC and wait ---~n"),
    erlang:garbage_collect(),
    timer:sleep(5000),
    capture_memory_stats("after_gc", ProfileDir, 9),
    capture_rocksdb_stats(Db, "after_gc"),

    %% Phase 10: Close database
    io:format("~n--- Phase 10: Closing database ---~n"),
    ok = cozodb:close(Db),
    timer:sleep(2000),
    capture_memory_stats("after_close", ProfileDir, 10),

    io:format("~n========================================~n"),
    io:format("Profiling Complete~n"),
    io:format("========================================~n"),
    io:format("~nHeap profiles saved to: ~s~n", [ProfileDir]),
    io:format("~nTo analyze with jeprof:~n"),
    io:format("  jeprof --svg $(which beam.smp) ~s/heap_*.prof > heap.svg~n", [ProfileDir]),
    io:format("  open heap.svg~n~n"),

    ok.

capture_memory_stats(Label, ProfileDir, Seq) ->
    io:format("~n[~s] Memory Stats:~n", [Label]),

    %% Erlang memory
    ErlMem = erlang:memory(),
    ErlTotal = proplists:get_value(total, ErlMem),
    ErlProcs = proplists:get_value(processes, ErlMem),
    ErlBin = proplists:get_value(binary, ErlMem),
    ErlEts = proplists:get_value(ets, ErlMem),
    io:format("  Erlang Total:    ~s~n", [format_bytes(ErlTotal)]),
    io:format("  Erlang Procs:    ~s~n", [format_bytes(ErlProcs)]),
    io:format("  Erlang Binary:   ~s~n", [format_bytes(ErlBin)]),
    io:format("  Erlang ETS:      ~s~n", [format_bytes(ErlEts)]),

    %% jemalloc stats
    case cozodb:memory_stats() of
        {ok, JemallocStats} ->
            Allocator = maps:get(<<"allocator">>, JemallocStats, <<"unknown">>),
            io:format("  Allocator:       ~s~n", [Allocator]),
            case Allocator of
                <<"jemalloc">> ->
                    Allocated = maps:get(<<"allocated">>, JemallocStats, 0),
                    Active = maps:get(<<"active">>, JemallocStats, 0),
                    Resident = maps:get(<<"resident">>, JemallocStats, 0),
                    Mapped = maps:get(<<"mapped">>, JemallocStats, 0),
                    Retained = maps:get(<<"retained">>, JemallocStats, 0),
                    io:format("  jemalloc allocated: ~s~n", [format_bytes(Allocated)]),
                    io:format("  jemalloc active:    ~s~n", [format_bytes(Active)]),
                    io:format("  jemalloc resident:  ~s~n", [format_bytes(Resident)]),
                    io:format("  jemalloc mapped:    ~s~n", [format_bytes(Mapped)]),
                    io:format("  jemalloc retained:  ~s~n", [format_bytes(Retained)]);
                _ ->
                    ok
            end;
        {error, Reason} ->
            io:format("  jemalloc stats error: ~p~n", [Reason])
    end,

    %% OS RSS
    OsPid = os:getpid(),
    RssCmd = io_lib:format("ps -o rss= -p ~s", [OsPid]),
    RssStr = string:trim(os:cmd(RssCmd)),
    case string:to_integer(RssStr) of
        {RssKb, _} when is_integer(RssKb) ->
            io:format("  OS RSS:          ~s~n", [format_bytes(RssKb * 1024)]);
        _ ->
            io:format("  OS RSS:          (unable to read)~n")
    end,

    %% Dump heap profile if jemalloc profiling is enabled
    ProfilePath = io_lib:format("~s/heap_~2..0B_~s.prof", [ProfileDir, Seq, Label]),
    case cozodb:dump_heap_profile(ProfilePath) of
        {ok, Path} ->
            io:format("  Heap profile:    ~s~n", [Path]);
        {error, profiling_not_enabled} ->
            io:format("  Heap profile:    (profiling not enabled - run with MALLOC_CONF=\"prof:true\")~n");
        {error, Reason2} ->
            io:format("  Heap profile:    error: ~p~n", [Reason2])
    end,
    ok.

capture_rocksdb_stats(Db, Label) ->
    case cozodb:rocksdb_memory_stats(Db) of
        {ok, Stats} ->
            MemtableSize = maps:get(<<"memtable_size">>, Stats, 0),
            BlockCacheUsage = maps:get(<<"block_cache_usage">>, Stats, 0),
            BlockCachePinned = maps:get(<<"block_cache_pinned">>, Stats, 0),
            TableReadersMem = maps:get(<<"table_readers_mem">>, Stats, 0),
            Total = maps:get(<<"total">>, Stats, 0),
            io:format("  [RocksDB ~s]~n", [Label]),
            io:format("    Memtable:      ~s~n", [format_bytes(MemtableSize)]),
            io:format("    Block Cache:   ~s~n", [format_bytes(BlockCacheUsage)]),
            io:format("    Pinned:        ~s~n", [format_bytes(BlockCachePinned)]),
            io:format("    Table Readers: ~s~n", [format_bytes(TableReadersMem)]),
            io:format("    Total Tracked: ~s~n", [format_bytes(Total)]);
        {error, _} ->
            ok
    end.

create_tables(Db, NumTables) ->
    Tables = [list_to_binary("profile_table_" ++ integer_to_list(I))
              || I <- lists:seq(1, NumTables)],
    lists:foreach(
        fun(Table) ->
            ok = cozodb:create_relation(Db, Table, #{
                keys => [{id, int}],
                columns => [{value, string}, {counter, int}]
            })
        end,
        Tables
    ),
    Tables.

insert_data(Db, Tables, RowsPerTable, Value) ->
    lists:foreach(
        fun(Table) ->
            %% Insert in batches of 1000
            BatchSize = 1000,
            NumBatches = (RowsPerTable + BatchSize - 1) div BatchSize,
            lists:foreach(
                fun(BatchNum) ->
                    Start = (BatchNum - 1) * BatchSize + 1,
                    End = min(BatchNum * BatchSize, RowsPerTable),
                    Rows = [[I, Value, 0] || I <- lists:seq(Start, End)],
                    Query = iolist_to_binary([
                        "?[id, value, counter] <- $rows\n",
                        ":put ", Table, " {id => value, counter}"
                    ]),
                    {ok, _} = cozodb:run(Db, Query, #{parameters => #{<<"rows">> => Rows}})
                end,
                lists:seq(1, NumBatches)
            ),
            io:format("  Inserted ~p rows into ~s~n", [RowsPerTable, Table])
        end,
        Tables
    ).

run_read_workload(Db, Tables, NumQueries) ->
    lists:foreach(
        fun(_) ->
            Table = lists:nth(rand:uniform(length(Tables)), Tables),
            Id = rand:uniform(10000),
            Query = iolist_to_binary([
                "?[id, value, counter] := *", Table, "{id, value, counter}, id = $id"
            ]),
            {ok, _} = cozodb:run(Db, Query, #{parameters => #{<<"id">> => Id}})
        end,
        lists:seq(1, NumQueries)
    ),
    io:format("  Completed ~p read queries~n", [NumQueries]).

format_bytes(Bytes) when Bytes < 1024 ->
    io_lib:format("~p B", [Bytes]);
format_bytes(Bytes) when Bytes < 1024 * 1024 ->
    io_lib:format("~.2f KB", [Bytes / 1024]);
format_bytes(Bytes) when Bytes < 1024 * 1024 * 1024 ->
    io_lib:format("~.2f MB", [Bytes / (1024 * 1024)]);
format_bytes(Bytes) ->
    io_lib:format("~.2f GB", [Bytes / (1024 * 1024 * 1024)]).
