#pragma once

#include <cstddef>  // byte, size_t
#include <memory>   // unique_ptr
#include <string>
#include <string_view>

// Forward declaration
struct evp_md_ctx_st;
using EVP_MD_CTX = evp_md_ctx_st;

namespace databento::detail {
// One-off hash
std::string Sha256Hash(std::string_view data);

class Sha256Hasher {
 public:
  Sha256Hasher();

  void Update(const std::byte* buffer, std::size_t length);
  std::string Finalize();

 private:
  std::unique_ptr<::EVP_MD_CTX, void (*)(::EVP_MD_CTX*)> ctx_;
};
}  // namespace databento::detail
