#include <vector>
#include <string>
#include <cstring>
#include <stdint.h>

template <typename T>
class DAG {
public:
  DAG(): data_(), offsets_({0}) { };

  T new_node(const std::vector<T>& depends_on, const std::string& payload) {
    size_t payload_bytes = payload.size();
    size_t payload_size = payload_bytes / sizeof(T) + (payload_bytes % sizeof(T) != 0);

    data_.reserve(data_.size() + 1 + depends_on.size() + 1 + payload_size);

    data_.push_back(depends_on.size());
    data_.insert(data_.end(), depends_on.begin(), depends_on.end());

    data_.push_back(payload_bytes);
    size_t payload_start = data_.size();
    data_.resize(payload_start + payload_size, 0);
    T* buffer = &data_.data()[payload_start];
    memcpy(
      reinterpret_cast<void*>(buffer),
      reinterpret_cast<const void*>(payload.c_str()),
      payload_bytes
    );

    offsets_.push_back(data_.size());
    return offsets_.size() - 2;
  }

  std::vector<T> node_depends_on(T node_index) {
    T data_index = offsets_[node_index];
    T depends_on_size = data_[data_index];

    return std::vector<T>(
      data_.begin() + data_index + 1,
      data_.begin() + data_index + 1 + depends_on_size
    );
  }

  std::string node_payload(T node_index) {
    T data_index = offsets_[node_index];
    T depends_on_size = data_[data_index];
    T payload_size = data_[data_index + 1 + depends_on_size];
    T* buffer = &data_.data()[data_index + 1 + depends_on_size + 1];
    return std::string(reinterpret_cast<const char*>(buffer), payload_size);
  }

  size_t nbytes() {
    return offsets_[offsets_.size() - 1] * sizeof(T);
  }

  std::vector<std::vector<T>> upstream_map() {
    std::vector<std::vector<T>> out;
    for (T i = 0;  i < offsets_.size() - 1;  i++) {
      out.push_back(node_depends_on(i));
    }
    return std::move(out);
  }

  std::vector<std::vector<T>> downstream_map() {
    std::vector<std::vector<T>> out;
    for (T i = 0;  i < offsets_.size() - 1;  i++) {
      out.push_back(std::vector<T>());
    }
    for (T i = 0;  i < offsets_.size() - 1;  i++) {
      for (T j : node_depends_on(i)) {
        out[j].push_back(i);
      }
    }
    return std::move(out);
  }

private:
  std::vector<T> data_;
  std::vector<T> offsets_;
};

///////////////////////////////////////////////////////////////

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

template <typename T>
py::class_<DAG<T>>
make_DAG(const py::handle& m, const char* name) {
  return py::class_<DAG<T>>(m, name)
    .def(py::init<>())
    .def("new_node", &DAG<T>::new_node)
    .def("node_depends_on", &DAG<T>::node_depends_on)
    .def("node_payload", [](DAG<T>& self, T node_index) {
      return py::bytes(self.node_payload(node_index));
    })
    .def_property_readonly("nbytes", &DAG<T>::nbytes)
    .def_property_readonly("upstream_map", &DAG<T>::upstream_map)
    .def_property_readonly("downstream_map", &DAG<T>::downstream_map)
  ;
};

PYBIND11_MODULE(_core, m) {
  make_DAG<uint32_t>(m, "DAG32");
  make_DAG<uint64_t>(m, "DAG64");
}
