#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/ipc/reader.h>
#include <chrono>
#include <curl/curl.h>
#include <memory>
#include <nanobind/nanobind.h>
#include <nanobind/stl/function.h>
#include <nanobind/stl/string.h>
#include <stdexcept>

using namespace arrow;
namespace nb = nanobind;

struct CurlHandle {
  CURL *handle{};
  ~CurlHandle() {
    if (handle) {
      curl_easy_cleanup(handle);
    }
  }
};

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

// Custom listener that processes Arrow RecordBatches and forwards them to
// Python
class Listener : public arrow::ipc::Listener {
private:
  std::function<void(uintptr_t)> callback_;

public:
  Status OnRecordBatchDecoded(std::shared_ptr<RecordBatch> batch) override {
    if (!batch) {
      return Status::Invalid("Received null RecordBatch");
    }

    ArrayStreamHandle stream;
    auto schema = batch->schema();

    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::RecordBatchReader::Make({batch}, schema));
    ARROW_RETURN_NOT_OK(arrow::ExportRecordBatchReader(reader, &stream.stream));

    callback_(reinterpret_cast<uintptr_t>(&stream.stream));

    return Status::OK();
  }

  void SetCallback(std::function<void(uintptr_t)> callback) {
    this->callback_ = callback;
  }
};

class StreamDecoderWrapper {
  // Wrapper class for arrow::ipc::StreamDecoder with CURL integration
private:
  std::unique_ptr<arrow::ipc::StreamDecoder> decoder;
  std::shared_ptr<Listener> listener;
  Status last_status_;

  static size_t WriteFunction(void *contents, size_t size, size_t nmemb,
                              void *userp) {
    size_t real_size = size * nmemb;
    auto decoder_wrapper = static_cast<StreamDecoderWrapper *>(userp);
    decoder_wrapper->last_status_ = decoder_wrapper->decoder->Consume(
        static_cast<const uint8_t *>(contents), real_size);
    return decoder_wrapper->last_status_.ok() ? real_size : 0;
  }

public:
  StreamDecoderWrapper() {
    listener = std::make_shared<Listener>();
    decoder = std::make_unique<arrow::ipc::StreamDecoder>(listener);
  }

  size_t ProcessStream(std::string url,
                       std::function<void(uintptr_t)> callback) {

    CurlHandle curl_handle;

    curl_global_init(CURL_GLOBAL_ALL);
    curl_handle.handle = curl_easy_init();

    listener->SetCallback(callback);

    curl_easy_setopt(curl_handle.handle, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_handle.handle, CURLOPT_WRITEFUNCTION, WriteFunction);
    curl_easy_setopt(curl_handle.handle, CURLOPT_WRITEDATA, this);
    curl_easy_setopt(curl_handle.handle, CURLOPT_FOLLOWLOCATION, true);

    CURLcode res = curl_easy_perform(curl_handle.handle);
    if (res != 0 && !last_status_.ok()) {
      throw std::runtime_error(last_status_.ToString());
    } else {
      throw std::runtime_error(std::string("curl error : ") +
                               curl_easy_strerror(res));
    }

    curl_easy_cleanup(curl_handle.handle);
    curl_handle.handle = nullptr;
    // The global cleanup should possibly be separated from this via a separate
    // function.
    curl_global_cleanup();

    return 0;
  }
};

int process_stream(std::string url, std::function<void(uintptr_t)> callback) {
  // Main entry point for processing arrow streams from URLs
  try {
    StreamDecoderWrapper decoderwrapper;
    return decoderwrapper.ProcessStream(url, std::move(callback));
  } catch (const std::exception &e) {
    fprintf(stderr, "Error processing stream: %s\n", e.what());
    return -1;
  }
}

NB_MODULE(prototype_cpp, m) {
  m.doc() = "Module for processing Arrow streams over HTTP";
  m.def("arrow_version", &get_arrow_version,
        "Returns the major version of Arrow");
  m.def("process_stream", &process_stream, nb::arg("url"), nb::arg("callback"),
        "Runs a stream decoder test");
}
