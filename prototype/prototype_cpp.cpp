#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/function.h>
#include <curl/curl.h>
#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/c/bridge.h>
#include <chrono>
#include <stdexcept>
#include <memory>

using namespace arrow;
namespace nb = nanobind;

// Simple function to illustrate usage of nanobind
int get_arrow_version() {
  return ARROW_VERSION_MAJOR;
}

// Custom listener that processes Arrow RecordBatches and forwards them to Python
class Listener : public arrow::ipc::Listener {
private:
  std::function<void(uintptr_t)> callback_;
public:
  Status OnRecordBatchDecoded(std::shared_ptr<RecordBatch> batch) override {
    if (!batch) {
      return Status::Invalid("Received null RecordBatch");
    }
    ArrowArrayStream* stream = new ArrowArrayStream();
    auto schema = batch->schema();

    auto reader = arrow::RecordBatchReader::Make({batch}, schema);
    if (!reader.ok()) {
      return Status::Invalid("Failed to create RecordBatchReader: ", reader.status().ToString());
    }

    auto status = arrow::ExportRecordBatchReader(reader.ValueOrDie(), stream);
    if (!status.ok()) {
      delete stream;
      return status;
    }

    try {
      callback_(reinterpret_cast<uintptr_t>(stream));
    } catch(...) {
      delete stream;
      return Status::Invalid("Callback execution failed");
    }

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

  static size_t WriteFunction(void *contents, size_t size, size_t nmemb, void *userp) {
      size_t real_size = size * nmemb;
      auto decoder = static_cast<arrow::ipc::StreamDecoder*>(userp);

      auto status = decoder->Consume(static_cast<const uint8_t*>(contents), real_size);
      return status.ok() ? real_size : 0;
  }
public:
  StreamDecoderWrapper() {
    listener = std::make_shared<Listener>();
    decoder = std::make_unique<arrow::ipc::StreamDecoder>(listener);
  }


  size_t ProcessStream (std::string url, std::function<void(uintptr_t)> callback) {

    CURL *curl_handle;
    CURLcode res;

    curl_global_init(CURL_GLOBAL_ALL);
    curl_handle = curl_easy_init();

    listener->SetCallback(callback);

    curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteFunction);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, this->decoder.get());
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, true);
  
    // Perform request and measure time duration of request
    auto start_time = std::chrono::steady_clock::now();

    res = curl_easy_perform(curl_handle);

    auto end_time = std::chrono::steady_clock::now();
    auto time_duration = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time);

    if (res != 0) {
      throw std::runtime_error(std::string("Curl error: ") + curl_easy_strerror(res));
    }

    printf("%.2f seconds elapsed\n", time_duration.count());

    curl_easy_cleanup(curl_handle);
    curl_global_cleanup();

    return 0;
    }

};

int process_stream(std::string url, std::function<void(uintptr_t)> callback) {
  // Main entry point for processing arrow streams from URLs
  try {
      StreamDecoderWrapper decoderwrapper;
      return decoderwrapper.ProcessStream(url, callback);
  } catch (const std::exception& e) {
    fprintf(stderr, "Error processing stream: %s\n", e.what());
    return -1;
  }
}

NB_MODULE(prototype_cpp, m) {
  m.doc() = "Module for processing Arrow streams over HTTP";
  m.def("arrow_version", &get_arrow_version,
          "Returns the major version of Arrow");
  m.def("process_stream", &process_stream, 
        nb::arg("url"), nb::arg("callback"),
        "Runs a stream decoder test");
}
