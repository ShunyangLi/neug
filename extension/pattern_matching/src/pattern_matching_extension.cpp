/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/compiler/extension/extension_api.h"
#include "neug/utils/exception/exception.h"

#include <memory>
#include <mutex>
#include <utility>

#include "pattern_matching_functions.h"

extern "C" {

// Entry point invoked by the extension loader. Registers the
// pattern_matching table functions with the catalog and publishes the
// extension metadata so SQL callers can invoke them.
void Init() {
  try {
    // The extension DSO is process-wide, but registration and cache ownership
    // are catalog-local. Serialize concurrent LOADs, then use the final
    // function as the completion marker so repeated LOADs preserve the warm
    // cache while a new database catalog still receives a fresh one.
    static std::mutex init_mutex;
    std::lock_guard<std::mutex> lock(init_mutex);
    auto* catalog = neug::main::MetadataRegistry::getCatalog();
    if (catalog->containsFunction(
            &neug::transaction::DUMMY_TRANSACTION,
            neug::pattern_matching::SaveSampledmatchCheckpointFunction::name,
            false)) {
      return;
    }

    auto cache = std::make_shared<neug::pattern_matching::GraphDataCache>();
    auto register_function =
        [catalog](const char* name, neug::function::function_set function_set) {
          if (catalog->containsFunction(&neug::transaction::DUMMY_TRANSACTION,
                                        name, false)) {
            return;
          }
          catalog->addFunctionWithSignature(
              &neug::transaction::DUMMY_TRANSACTION,
              neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY, name,
              std::move(function_set), false);
        };

    // Graph cache bootstrap: loads / prepares the in-memory data graph.
    register_function(
        neug::pattern_matching::InitializeGraphFunction::name,
        neug::pattern_matching::InitializeGraphFunction::getFunctionSet(cache));

    // Unified subgraph matching entry: PATTERN_MATCH(cypher) runs exact
    // matching over all matches; PATTERN_MATCH(cypher, size, is_sampled)
    // runs sampled matching (FaSTest, is_sampled=true) or exact matching with
    // early termination after `size` matches (is_sampled=false).
    register_function(
        neug::pattern_matching::PatternMatchFunction::name,
        neug::pattern_matching::PatternMatchFunction::getFunctionSet(cache));

    // Vertex property lookup for matched vertices.
    register_function(
        neug::pattern_matching::GetVertexPropertyFunction::name,
        neug::pattern_matching::GetVertexPropertyFunction::getFunctionSet(
            cache));

    // Edge property lookup for matched edges.
    register_function(
        neug::pattern_matching::GetEdgePropertyFunction::name,
        neug::pattern_matching::GetEdgePropertyFunction::getFunctionSet(cache));

    // Persists the prepared graph cache to disk for faster restarts.
    register_function(
        neug::pattern_matching::SaveSampledmatchCheckpointFunction::name,
        neug::pattern_matching::SaveSampledmatchCheckpointFunction::
            getFunctionSet(cache));

    neug::extension::ExtensionAPI::registerExtension(
        neug::extension::ExtensionInfo{
            "pattern_matching",
            "Provides subgraph matching and property access functions. "
            "Functions: CALL INITIALIZE([checkpoint_dir]) - initializes graph "
            "data cache, "
            "CALL SAVE_SAMPLEDMATCH_CHECKPOINT(checkpoint_dir) - saves graph "
            "cache to files, "
            "CALL PATTERN_MATCH(cypher_text_or_file[, size, is_sampled]) - "
            "exact "
            "matching over all matches when size/is_sampled are omitted; with "
            "size (>= 1) and is_sampled, runs sampled matching "
            "(is_sampled=true) "
            "or exact matching that early-terminates after size matches "
            "(is_sampled=false), "
            "CALL GET_VERTEX_PROPERTY(vertex_ids_json, vertex_label, "
            "prop_names_json), "
            "CALL GET_EDGE_PROPERTY(edge_keys_json, edge_label, "
            "prop_names_json). "
            "PATTERN_MATCH accepts Cypher pattern text, Cypher pattern "
            "files, JSON text, or JSON pattern files."});
  } catch (const std::exception& e) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[pattern_matching extension] registration failed: " +
        std::string(e.what()));
  } catch (...) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[pattern_matching extension] registration failed: unknown exception");
  }
}

// Display name surfaced by the extension loader.
const char* Name() { return "PATTERN_MATCHING"; }

}  // extern "C"
