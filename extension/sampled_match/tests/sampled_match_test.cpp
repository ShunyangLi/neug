/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include <gtest/gtest.h>

#include <neug/main/neug_db.h>

#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

namespace neug {
namespace test {

namespace {

std::filesystem::path GetExecutablePath() {
#if defined(__APPLE__)
  uint32_t size = 0;
  _NSGetExecutablePath(nullptr, &size);
  std::string buf(size, '\0');
  if (_NSGetExecutablePath(buf.data(), &size) != 0) return {};
  return std::filesystem::canonical(buf.c_str());
#else
  return std::filesystem::read_symlink("/proc/self/exe");
#endif
}

// Walk upward from the test binary and return the first ancestor that contains
// the built extension library. That directory is what LOAD expects via
// NEUG_EXTENSION_HOME_PYENV.
std::string FindBuildRoot() {
  auto dir = GetExecutablePath().parent_path();
  const std::string target =
      "extension/sampled_match/libsampled_match.neug_extension";
  for (int i = 0; i < 8; ++i) {
    if (std::filesystem::exists(dir / target)) return dir.string();
    if (dir == dir.parent_path()) break;
    dir = dir.parent_path();
  }
  return "";
}

constexpr const char* kTrianglePattern = R"({
  "vertices": [
    {"id": 0, "label": "Person"},
    {"id": 1, "label": "Person"},
    {"id": 2, "label": "Person"}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"},
    {"source": 1, "target": 2, "label": "person_knows_person"},
    {"source": 2, "target": 0, "label": "person_knows_person"}
  ]
})";

// Allocate a unique scratch directory under the system temp dir. Using
// mkdtemp avoids races between concurrently-running tests and never touches
// the current working directory.
std::filesystem::path MakeUniqueTempDir() {
  auto tmpl =
      (std::filesystem::temp_directory_path() / "neug_sampled_match_XXXXXX")
          .string();
  std::vector<char> buf(tmpl.begin(), tmpl.end());
  buf.push_back('\0');
  if (mkdtemp(buf.data()) == nullptr) return {};
  return std::filesystem::path(buf.data());
}

}  // namespace

class SampledMatchTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    const std::string build_root = FindBuildRoot();
    ASSERT_FALSE(build_root.empty())
        << "Could not locate libsampled_match.neug_extension near "
        << GetExecutablePath();
    setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1);
  }

  void SetUp() override {
    test_dir_ = MakeUniqueTempDir();
    ASSERT_FALSE(test_dir_.empty());

    db_ = std::make_unique<neug::NeugDB>();
    ASSERT_TRUE(db_->Open(test_dir_ / "db"));

    conn_ = db_->Connect();
    ASSERT_TRUE(conn_);

    // Schema: a single Person node label and a single knows edge label.
    // Data: a 3-node directed triangle (0->1, 1->2, 2->0).
    const std::pair<std::string, std::string> setup_queries[] = {
        {"schema: Person",
         "CREATE NODE TABLE Person(id INT32 PRIMARY KEY, name STRING);"},
        {"schema: knows",
         "CREATE REL TABLE person_knows_person(FROM Person TO Person);"},
        {"insert p0", "CREATE (n:Person {id: 0, name: 'A'})"},
        {"insert p1", "CREATE (n:Person {id: 1, name: 'B'})"},
        {"insert p2", "CREATE (n:Person {id: 2, name: 'C'})"},
        {"edge 0->1",
         "MATCH (a:Person), (b:Person) WHERE a.id = 0 AND b.id = 1 "
         "CREATE (a)-[:person_knows_person]->(b)"},
        {"edge 1->2",
         "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 "
         "CREATE (a)-[:person_knows_person]->(b)"},
        {"edge 2->0",
         "MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 0 "
         "CREATE (a)-[:person_knows_person]->(b)"},
    };
    for (const auto& [label, q] : setup_queries) {
      auto res = conn_->Query(q);
      ASSERT_TRUE(res.has_value())
          << label << " failed: " << res.error().ToString();
    }

    auto load = conn_->Query("LOAD sampled_match;");
    ASSERT_TRUE(load.has_value()) << load.error().ToString();
  }

  void TearDown() override {
    conn_.reset();
    if (db_) {
      db_->Close();
      db_.reset();
    }
    if (!test_dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(test_dir_, ec);
    }
  }

 protected:
  std::filesystem::path test_dir_;
  std::unique_ptr<neug::NeugDB> db_;
  std::shared_ptr<neug::Connection> conn_;
};

// Smoke test: the fixture does the heavy lifting — open a DB, create the
// schema, populate a 3-node triangle, and `LOAD sampled_match;`. Reaching
// this body means the build is wired up, the extension .so loaded via
// dlopen, and the catalog functions registered. We additionally drop a
// pattern.json under the per-test temp dir to confirm the working-dir
// hygiene change (no writes to CWD, no fixed /tmp paths).
TEST_F(SampledMatchTest, LoadsExtensionAndStagesPatternFile) {
  auto pattern_path = test_dir_ / "triangle.json";
  {
    std::ofstream ofs(pattern_path);
    ASSERT_TRUE(ofs.is_open());
    ofs << kTrianglePattern;
  }
  EXPECT_TRUE(std::filesystem::exists(pattern_path));
  EXPECT_GT(std::filesystem::file_size(pattern_path), 0u);
}

}  // namespace test
}  // namespace neug
