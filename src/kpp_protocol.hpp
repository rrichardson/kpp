
#include <kpp_endian.hpp>
#include <kpp_variant.hpp>
#include <vector>
#include <iostream>
#include <tuple>
#include <cstdint>

/**
RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32

RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
  ApiKey => int16
  ApiVersion => int16
  CorrelationId => int32
  ClientId => string
  RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest

Response => CorrelationId ResponseMessage
  CorrelationId => int32
  ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse

MessageSet => [Offset MessageSize Message]
  Offset => int64
  MessageSize => int32

Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes

MetadataResponse => [Broker][TopicMetadata]
  Broker => NodeId Host Port
  NodeId => int32
  Host => string
  Port => int32
  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
  TopicErrorCode => int16
  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
  PartitionErrorCode => int16
  PartitionId => int32
  Leader => int32
  Replicas => [int32]
  Isr => [int32]

ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
  RequiredAcks => int16
  Timeout => int32
  Partition => int32
  MessageSetSize => int32

ProduceResponse => [TopicName [Partition ErrorCode Offset]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
  Offset => int64

FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
  ReplicaId => int32
  MaxWaitTime => int32
  MinBytes => int32
  TopicName => string
  Partition => int32
  FetchOffset => int64
  MaxBytes => int32

FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
  HighwaterMarkOffset => int64
  MessageSetSize => int32

OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
  ReplicaId => int32
  TopicName => string
  Partition => int32
  Time => int64
  MaxNumberOfOffsets => int32

OffsetResponse => [TopicName [PartitionOffsets]]
  PartitionOffsets => Partition ErrorCode [Offset]
  Partition => int32
  ErrorCode => int16
  Offset => int64

ConsumerMetadataRequest => ConsumerGroup
  ConsumerGroup => string

ConsumerMetadataResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
  ErrorCode => int16
  CoordinatorId => int32
  CoordinatorHost => string
  CoordinatorPort => int32

OffsetCommitRequest => ConsumerGroup [TopicName [Partition Offset TimeStamp Metadata]]
  ConsumerGroup => string
  TopicName => string
  Partition => int32
  Offset => int64
  TimeStamp => int64
  Metadata => string

OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
  TopicName => string
  Partition => int32
  ErrorCode => int16

OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
  ConsumerGroup => string
  TopicName => string
  Partition => int32

OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
  TopicName => string
  Partition => int32
  Offset => int64
  Metadata => string
  ErrorCode => int16

*/

namespace kpp {
using namespace variant;

namespace ApiKey {
  using Type = int16_t;
  constexpr Type ProduceRequest = 0, 
                 FetchRequest   = 1,
                 OffsetRequest  = 2,
                 MetadataRequest = 3,
                 OffsetCommitRequest = 8,
                 OffsetFetchRequest  = 9,
                 ConsumerMetadataRequest = 10;
}

namespace Error {
  using Type = int16_t;
  constexpr Type NoError = 0,
                 Unknown = -1,
                 OffsetOutOfRange = 1,
                 InvalidMessage = 2,
                 UnknownTopicOrPartition = 3,
                 InvalidMessageSize = 4,
                 LeaderNotAvailable = 5,
                 NotLeaderForPartition = 6,
                 RequestTimedOut = 7,
                 BrokerNotAvailable = 8,
                 Unused             = 9,
                 MessageSizeTooLarge = 10,
                 StaleControllerEpochCode = 11,
                 OffsetMetadataTooLargeCode = 12,
                 OffsetLoadInProgressCode = 14,
                 ConsumerCoordinatorNotAvailableCode = 15,
                 NotCoordinatorForConsumerCode = 16;
}

template<typename INT>
struct BE {
  INT value;
};
template <typename charT, typename traits, typename INT>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, const BE<INT> & benum )
{
  INT val = endian::hton(benum.value);
  oStream.write(reinterpret_cast<charT*>(&val), sizeof(INT));
  return oStream;
}
template <typename charT, typename traits, typename INT>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, BE<INT>& benum)
{
  INT val;
  iStream.read(reinterpret_cast<charT*>(&val), sizeof val);
  benum.value = endian::ntoh(val);
  return iStream ;
}

struct String {
  std::vector<uint8_t> bytes;
};
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, const String & str )
{
  BE<int16_t> sz = {str.bytes.size()};
  oStream << sz;
  oStream.write(reinterpret_cast<charT*>(str.bytes.data()), str.bytes.size());
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, String & str)
{
  BE<int16_t> sz; 
  iStream >> sz;
  str.bytes.resize(sz.value);
  iStream.read(reinterpret_cast<charT*>(str.bytes.data()), sz.value);
  return iStream ;
}

struct Bytes {
  std::vector<uint8_t> bytes;
};
template <typename charT, typename traits>
std::basic_ostream<charT, traits> & operator << (std::basic_ostream<charT,traits> & oStream, const Bytes & str )
{
  BE<int32_t> sz = {str.bytes.size()};
  oStream << sz;
  oStream.write(reinterpret_cast<charT*>(str.bytes.data()), str.bytes.size());
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, Bytes & str)
{
  BE<int32_t> sz; 
  iStream >> sz;
  str.bytes.resize(sz.value);
  iStream.read(reinterpret_cast<charT*>(str.bytes.data()), sz.value);
  return iStream ;
}

template <typename T>
struct Array {
  std::vector<T> contents;
};
template <typename charT, typename traits, typename ArrayT>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, const Array<ArrayT> & arr )
{
  BE<int32_t> sz = {arr.contents.size()};
  oStream << sz;
  for(auto itor = arr.contents.begin(); itor != arr.contents.end(); ++itor)
  {
    oStream << *itor;
  }
  return oStream;
}
template <typename charT, typename traits, typename ArrayT>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, Array<ArrayT> & arr)
{
  BE<int32_t> sz;
  iStream >> sz;
  arr.contents.resize(sz.value);
  for(int i = 0; i < sz.value; ++i)
  {
    arr.contents.push_back();
    iStream >> arr.contents.back();
  }
  return iStream;
}

struct OffsetFetchResponse { 
   struct PartitionsT {
     BE<int32_t> Partition;
     BE<int64_t> Offset;
     String  Metadata;
     BE<Error::Type> ErrorCode; 
   };
   struct TopicsT {
     String TopicName;
     Array<PartitionsT> Partitions;
   };
   
   Array<TopicsT> Topics;
};
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetFetchResponse & ofr)
{
  oStream << ofr.Topics;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetFetchResponse & ofr)
{
  iStream >> ofr.Topics;
  return iStream;
}
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetFetchResponse::TopicsT & t)
{
  oStream << t.TopicName;
  oStream << t.Partitions;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetFetchResponse::TopicsT & t)
{
  iStream >> t.TopicName;
  iStream >> t.Partitions;
  return iStream;
}
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetFetchResponse::PartitionsT & p)
{
  oStream << p.Partition;
  oStream << p.Offset;
  oStream << p.Metadata;
  oStream << p.ErrorCode; 
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetFetchResponse::PartitionsT & p)
{
  iStream >> p.Partition;
  iStream >> p.Offset;
  iStream >> p.Metadata;
  iStream >> p.ErrorCode;
  return iStream;
}

struct OffsetFetchRequest {
  struct TopicsT {
    String TopicName;
    Array<BE<int32_t>> Partitions;
  };
  String ConsumerGroup;
  Array<TopicsT> Topics; 
};
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetFetchRequest & ofr)
{
  oStream << ofr.ConsumerGroup;
  oStream << ofr.Topics;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetFetchRequest & ofr)
{
  iStream >> ofr.ConsumerGroup;
  iStream >> ofr.Topics;
  return iStream;
}
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetFetchRequest::TopicsT & t)
{
  oStream << t.TopicName;
  oStream << t.Partitions;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetFetchRequest::TopicsT & t)
{
  iStream >> t.TopicName;
  iStream >> t.Partitions;
  return iStream;
}



struct OffsetCommitResponse {
  struct PartitionsT {
    BE<int32_t> Partition;
    BE<Error::Type> ErrorCode;
  };
   struct TopicsT {
    String TopicName;
    Array<PartitionsT> Partitions;
  };
  Array<TopicsT> Topics;  
};
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetCommitResponse & ocr)
{
  oStream << ocr.Topics;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetCommitResponse & ocr)
{
  iStream >> ocr.Topics;
  return iStream;
}
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetCommitResponse::TopicsT & t)
{
  oStream << t.TopicName;
  oStream << t.Partitions;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetCommitResponse::TopicsT & t)
{
  iStream >> t.TopicName;
  iStream >> t.Partitions;
  return iStream;
}
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetCommitResponse::PartitionsT & p)
{
  oStream << p.Partition;
  oStream << p.ErrorCode;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetCommitResponse::PartitionsT & p)
{
  iStream >> p.Partition;
  iStream >> p.ErrorCode;
  return iStream;
}


struct OffsetCommitRequest {
  struct PartitionsT {
    BE<int32_t> Partition;
    BE<int64_t> Offset;
    BE<int64_t> Timestamp;
    String Metadata;
  };
  struct TopicsT {
    String TopicName;
    Array<PartitionsT> Partitions;
  };
  String ConsumerGroup;
  Array<TopicsT> Topics;
};
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetCommitRequest & ocr)
{
  oStream << ocr.ConsumerGroup;
  oStream << ocr.Topics;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetCommitRequest & ocr)
{
  iStream >> ocr.ConsumerGroup;
  iStream >> ocr.Topics;
  return iStream;
}
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetCommitRequest::TopicsT & t)
{
  oStream << t.TopicName;
  oStream << t.Partitions;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetCommitRequest::TopicsT & t)
{
  iStream >> t.TopicName;
  iStream >> t.Partitions;
  return iStream;
}
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetCommitRequest::PartitionsT & p)
{
  oStream << p.Partition;
  oStream << p.Offset;
  oStream << p.Timestamp;
  oStream << p.Metadata;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetCommitRequest::PartitionsT & p)
{
  iStream >> p.Partition;
  iStream >> p.Offset;
  iStream >> p.Timestamp;
  iStream >> p.Metadata;
  return iStream;
}


struct ConsumerMetadataResponse {
  BE<Error::Type> ErrorCode;
  BE<int32_t> CoordinatorId;
  String CoordinatorHost;
  BE<int32_t> CoordinatorPort;
};
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const ConsumerMetadataResponse & cmr) 
{
  oStream << cmr.ErrorCode; 
  oStream << cmr.CoordinatorId;
  oStream << cmr.CoordinatorHost;
  oStream << cmr.CoordinatorPort;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                ConsumerMetadataResponse & cmr)
{
  iStream >> cmr.ErrorCode;
  iStream >> cmr.CoordinatorId;
  iStream >> cmr.CoordinatorHost;
  iStream >> cmr.CoordinatorPort;
  return iStream;
}



struct ConsumerMetadataRequest {
  String ConsumerGroup;
};
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const ConsumerMetadataRequest & cmr) 
{
  oStream << cmr.ConsumerGroup;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                ConsumerMetadataRequest & cmr)
{
  iStream >> cmr.ConsumerGroup;
  return iStream;
}


struct OffsetResponse {

  struct PartitionOffsetT {
    BE<int32_t> Partition;
    BE<Error::Type> ErrorCode;
    Array<BE<int64_t>> Offset;
  };

  struct TopicsT {
    String TopicName;
    Array<PartitionOffsetT> Partitions;
  };
  Array<PartitionOffsetT> Topics; 
};
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetResponse & ocr)
{
  oStream << ocr.Topics;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetResponse & ocr)
{
  iStream >> ocr.Topics;
  return iStream;
}
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetResponse::TopicsT & t)
{
  oStream << t.TopicName;
  oStream << t.Partitions;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetResponse::TopicsT & t)
{
  iStream >> t.TopicName;
  iStream >> t.Partitions;
  return iStream;
}
template <typename charT, typename traits>
std::basic_ostream<charT,traits> & operator << (std::basic_ostream<charT,traits> & oStream, 
                                                const OffsetResponse::PartitionOffsetT & p)
{
  oStream << p.Partition;
  oStream << p.ErrorCode;
  oStream << p.Offset;
  return oStream;
}
template <typename charT, typename traits>
std::basic_istream<charT,traits> & operator >> (std::basic_istream<charT,traits> & iStream, 
                                                OffsetResponse::PartitionOffsetT & p)
{
  iStream >> p.Partition;
  iStream >> p.ErrorCode;
  iStream >> p.Offset;
  return iStream;
}




}
