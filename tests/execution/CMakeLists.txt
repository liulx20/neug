add_neug_test(
	execution_test
	test_stream.cc
	test_morsel_execution.cc
	test_hash_join.cc
	test_parallel_group_by.cc
	test_parallel_dedup.cc
	test_task_scheduler.cc
	test_logical_expr.cc
	test_plan_parser.cc
	test_query_cache.cc
	test_value.cc
	test_value_column.cc
	test_runtime_column.cc)

# Optional diagnostic: state-kernel timings without the scheduler or storage.
if(UNIX)
    add_executable(partition_phases EXCLUDE_FROM_ALL
        ../../tools/benchmarks/partition_phases.cc)
    target_link_libraries(partition_phases PRIVATE neug ${Protobuf_LIBRARIES}
        ${GLOG_LIBRARIES} ${ARROW_LIB} ${YAML_CPP_LIBRARIES})
endif()

if(UNIX)
    add_executable(pk_join_pipeline EXCLUDE_FROM_ALL
        ../../tools/benchmarks/pk_join_pipeline.cc)
    target_link_libraries(pk_join_pipeline PRIVATE neug ${Protobuf_LIBRARIES}
        ${GLOG_LIBRARIES} ${ARROW_LIB} ${YAML_CPP_LIBRARIES})
endif()
