#include "databento/detail/sha256_hasher.hpp"

#include <openssl/evp.h>

#include <ios>  // hex, setw, setfill
#include <sstream>

#include "databento/exceptions.hpp"

std::string databento::detail::Sha256Hash(std::string_view data) {
  Sha256Hasher hasher{};
  hasher.Update(reinterpret_cast<const std::byte*>(data.data()), data.length());
  return hasher.Finalize();
}

using databento::detail::Sha256Hasher;

Sha256Hasher::Sha256Hasher() : ctx_{::EVP_MD_CTX_new(), &::EVP_MD_CTX_free} {
  ::EVP_DigestInit_ex(ctx_.get(), ::EVP_sha256(), NULL);
}

void Sha256Hasher::Update(const std::byte* buffer, std::size_t length) {
  if (!::EVP_DigestUpdate(ctx_.get(), buffer, length)) {
    throw databento::Exception{"Failed to update SHA256 digest"};
  }
}

std::string Sha256Hasher::Finalize() {
  std::array<unsigned char, EVP_MAX_MD_SIZE> hash{};
  unsigned int hash_length = 0;
  if (!::EVP_DigestFinal_ex(ctx_.get(), hash.data(), &hash_length)) {
    throw databento::Exception{"Failed to finalize SHA256 digest"};
  }

  std::ostringstream hash_hex_stream;
  for (size_t i = 0; i < hash_length; ++i) {
    hash_hex_stream << std::hex << std::setw(2) << std::setfill('0') << +hash[i];
  }
  return hash_hex_stream.str();
}
