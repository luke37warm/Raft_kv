#pragma once
// Tiny dependency-free test helper (no network access to fetch gtest here).

#include <cstdio>

inline int& failures() {
  static int f = 0;
  return f;
}

#define CHECK(cond)                                                   \
  do {                                                                \
    if (!(cond)) {                                                    \
      std::fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
      ++failures();                                                   \
    }                                                                 \
  } while (0)

struct TestCase {
  const char* name;
  void (*fn)();
};

inline int run_tests(const TestCase* tests, int n) {
  for (int i = 0; i < n; ++i) {
    int before = failures();
    tests[i].fn();
    std::printf("[%s] %s\n", failures() == before ? " ok " : "FAIL", tests[i].name);
  }
  std::printf("%d failure(s)\n", failures());
  return failures() == 0 ? 0 : 1;
}
