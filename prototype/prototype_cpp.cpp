#include <memory>
#include <stdexcept>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/ipc/reader.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/function.h>
#include <nanobind/stl/string.h>

using namespace arrow;
namespace nb = nanobind;

// Wrapper for ArrowArrayStream with RAII cleanup
// Ensures the stream is properly released when the handle goes out of scope.
struct ArrayStreamHandle {
  ArrowArrayStream stream{};
  ~ArrayStreamHandle() {
    if (stream.release) {
      stream.release(&stream);
    }
  }
};

// Simple function to illustrate usage of nanobind
int get_arrow_version() { return ARROW_VERSION_MAJOR; }

// Custom Listener class that handles decoded Arrow RecordBatches.
// Converts each batch to a C Data Interface format and passes it to a Python callback.
class Listener : public arrow::ipc::Listener {
private:
  std::function<void(uintptr_t)> batch_callback_;
  std::function<void(uintptr_t)> schema_callback_;

public:
  Status OnSchemaDecoded(std::shared_ptr<Schema> schema) override {
    if (schema_callback_) {
      struct ArrowSchema c_schema;
      ARROW_RETURN_NOT_OK(arrow::ExportSchema(*schema, &c_schema));

      schema_callback_(reinterpret_cast<uintptr_t>(&c_schema));
    }
    return Status::OK();
  }
  Status OnRecordBatchDecoded(std::shared_ptr<RecordBatch> batch) override {
    if (!batch) {
      return Status::Invalid("Received null RecordBatch");
    }

    ArrayStreamHandle stream;
    auto schema = batch->schema();

    ARROW_ASSIGN_OR_RAISE(auto reader,
                         arrow::RecordBatchReader::Make({batch}, schema));
    ARROW_RETURN_NOT_OK(arrow::ExportRecordBatchReader(reader, &stream.stream));

    batch_callback_(reinterpret_cast<uintptr_t>(&stream.stream));

    return Status::OK();
  }

  void SetSchemaCallback(std::function<void(uintptr_t)> callback) {
    this->schema_callback_ = callback;
  }

  void SetBatchCallback(std::function<void(uintptr_t)> callback) {
    this->batch_callback_ = callback;
  }
};

class StreamDecoderWrapper {
private:
  std::unique_ptr<arrow::ipc::StreamDecoder> decoder;
  std::shared_ptr<Listener> listener;
  Status last_status_;

public:
  StreamDecoderWrapper() {
    listener = std::make_shared<Listener>();
    decoder = std::make_unique<arrow::ipc::StreamDecoder>(listener);
  }

  // Consume a buffer of bytes and feed them to the Arrow StreamDecoder
  // @param data Pointer to buffer containing bytes to consume
  // @param length Number of bytes to consume
  // @return Number of bytes consumed
  // @throws std::runtime_error if the decoder encounters an error
  size_t ConsumeBytes(const uint8_t* data, size_t length) {
    last_status_ = decoder->Consume(data, length);
    if (!last_status_.ok()) {
      throw std::runtime_error(last_status_.ToString());
    }
    return length;
  }
  
  // Set the callback function that will be called when a complete batch is received.
  // @param callback Function taking a uintptr_t representing a pointer to an ArrowArrayStream
  void SetBatchCallback(std::function<void(uintptr_t)> callback) {
    listener->SetBatchCallback(callback);
  }

  void SetSchemaCallback(std::function<void(uintptr_t)> callback) {
      listener->SetSchemaCallback(callback);
    }
};

NB_MODULE(prototype_cpp, m) {
  m.doc() = "Module for processing Arrow streams over HTTP";
  m.def("arrow_version", &get_arrow_version,
        "Returns the major version of Arrow");
  nb::class_<StreamDecoderWrapper>(m, "StreamDecoderWrapper")
      .def(nb::init<>())
      .def("set_batch_callback", &StreamDecoderWrapper::SetBatchCallback,
           "Set the callback for processing Arrow batches")
      .def("set_schema_callback", &StreamDecoderWrapper::SetSchemaCallback,
          "Set the callback for receiving the Arrow schema")
      // Bytes as input
      .def("consume_bytes", 
           [](StreamDecoderWrapper& self, const nb::bytes& data) {
               return self.ConsumeBytes(
                   reinterpret_cast<const uint8_t*>(data.c_str()),
                   data.size()
               );
           })
      // Bytearray as input
      .def("consume_bytes",
           [](StreamDecoderWrapper& self, const nb::bytearray& data) {
               return self.ConsumeBytes(
                   reinterpret_cast<const uint8_t*>(data.data()),
                   data.size()
               );
           });
}
