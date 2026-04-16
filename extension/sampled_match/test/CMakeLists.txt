# End-to-end smoke test for the sampled_match extension.
# The test loads the built extension via LOAD (no INSTALL / no network),
# so it depends on the extension library being built first.

add_executable(test_sampled_match test_sampled_match.cc)
target_include_directories(test_sampled_match PRIVATE
    ${CMAKE_SOURCE_DIR}/extension/sampled_match/include
    ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_sampled_match PRIVATE neug)
add_dependencies(test_sampled_match neug_sampled_match_extension)
add_test(NAME test_sampled_match COMMAND test_sampled_match)
