// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_RECOVERY_RECORD_HEADER_H_
#define USTORE_RECOVERY_RECORD_HEADER_H_

#include <string>

namespace ustore {
namespace recovery {


/*
 * @brief Introduction to RecordHeader
 * */
class RecordHeader {
 public:
  // TODO(yaochang): replace magic number
  static constexpr int16_t kLogMagicNumber = static_cast<int16_t>(0xFFFF);
  static constexpr int kRecordHeaderLength = sizeof(UStoreRecordHeader);

  RecordHeader();
  ~RecordHeader();

  std::string ToString() const;
  uint64_t ToString(char* buf, const uint64_t len) const;
  /*
   * Set the magic number
   * */
  inline void setMagicNumber(int16_t magic = kMagicNumber) { magic_ = magic; }
  /*
   * set checksum of the header
   * */
  inline void setHeaderChecksum();
  /*
   * check the checksum of the header
   * */
  bool CheckHeaderChecksum() const;
  /*
   * check the magic number of the header
   * */
  bool CheckMagicNumber(int16_t magic = kMagicNumber) const;
  /*
   * check the compressed data length is correct or not
   * */
  bool CheckDataLengthCompressed(uint32_t compressed_length) const;
  /*
   * check the data length is correct or not
   * */
  bool CheckDataLength(uint32_t data_length) const;
  /*
   * check whether the header is compressed or not
   * */
  bool IsCompressed() const;
  /*
   * check the checksum of the header
   * */
  bool CheckHeaderChecksum(const char* buf, uint64_t length) const;

 private:
  int16_t  magic_;
  uint16_t header_length_;
  uint16_t version_;
  int16_t  header_checksum_;
  uint32_t data_length_;
  uint32_t data_length_compressed_;
  int64_t  data_checksum_;
};  // RecordHeader

/*
 * check record after being read
 * @param string buffer to store the bytes string
 * @param total length of record and record header
 * @param magic number
 * */
static int CheckRecord(const char* buf, uint64_t length, int16_t magic);
/*
 * check the record after reading the header
 * @param record header
 * @param string buffer after the record header
 * @param length the of buffer
 * @param magic number that is used to check the correctness
 * */
static int CheckRecord(const RecordHeader& record_header,
                       const char* payload_buf, uint64_t payload_length,
                       int16_t magic);
/*
 * check the record buffer and do the extraction
 * @param raw data buffer address
 * @param size of the raw data buffer
 * @param magic number that is used to do the checking
 * @param [out] record header
 * @param [out] content address
 * @param [out] content size
 * */
static int CheckRecord(const char* rawdata, uint64_t rawdata_size,
                         int16_t magic, RecordHeader* header,
                         const char** payload_ptr, uint64_t* payload_size);
/*
 * do not check and directly extract header and payload address
 * @param raw data buffer address
 * @param size of the raw data buffer
 * @param [out] record header
 * @param [out] content address
 * @param [out] content size
 * */
static int GetRecordHeader(const char* rawdata, uint64_t rawdata_size,
                             RecordHeader* header, const char** payload_ptr,
                             uint64_t* payload_size);
}  // namespace recovery
}  // namespace ustore

#endif  // USTORE_RECOVERY_RECORD_HEADER_H_
