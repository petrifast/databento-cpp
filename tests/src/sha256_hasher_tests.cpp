#include <gtest/gtest.h>

#include <cstddef>

#include "databento/detail/sha256_hasher.hpp"

namespace databento::detail::tests {
TEST(Sha256HasherTests, SanityCheck) {
  ASSERT_EQ(Sha256Hash("123DBN\n"),
            // obtained with `echo 123DBN | sha256sum`
            "f15e88dd823646fb031d871850a352bf081d0686933dee4c5d9ff3c376f15ea7");
}

TEST(Sha256HasherTests, Equivalence) {
  const auto one_shot = Sha256Hash("1234567890");
  Sha256Hasher hasher;
  auto update = [&hasher](std::string_view s) {
    hasher.Update(reinterpret_cast<const std::byte*>(s.data()), s.length());
  };
  update("123");
  update("4567");
  update("890");
  ASSERT_EQ(hasher.Finalize(), one_shot);
}
}  // namespace databento::detail::tests
