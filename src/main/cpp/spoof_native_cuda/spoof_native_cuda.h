
#include <cstdint>
#include <map>
#include <string>

#include "jitify.hpp"

struct SpoofOperator {
  enum AggType { NO_AGG, ROW_AGG, COL_AGG, FULL_AGG };

  jitify::Program program;
  AggType agg_type;
};

class SpoofCudaContext {

  jitify::JitCache kernel_cache;
  std::map<const std::string, SpoofOperator> ops;

public:
  static long initialize_cuda(uint32_t device_id);

  static void destroy_cuda(SpoofCudaContext *ctx, uint32_t device_id);

  bool compile_cuda(const std::string &src, const std::string &name);

  //  template <typename T>
  //  bool execute_kernel(const std::string &name, T *in_ptr, long side_ptr,
  //                      long out_ptr, long scalars_ptr, long m, long n,
  //                      long grix);

  template <typename T>
  T execute_kernel(const std::string &name, T **in_ptrs, int num_inputs,
                   T **side_ptrs, int num_sides, T *out_ptr, T *scalars_ptr,
                   int num_scalars, int m, int n, int grix) {

    using jitify::reflection::type_of;

    double result = 0.0;

    auto o = ops.find(name);
    if (o != ops.end()) {
      SpoofOperator *op = &(o->second);
      std::cout << "launching kernel " << name << std::endl;

      // Todo: proper cta config
      dim3 grid(1);
      dim3 block(m, n);
      std::cout << "launching " << block.x << "x" << block.y
                << "==" << block.x * block.y << " threads" << std::endl;

      T *d_scalars;
      size_t dev_buf_size;
      if (op->agg_type == SpoofOperator::FULL_AGG)
        dev_buf_size = sizeof(T) * num_scalars + 1;
      else
        dev_buf_size = sizeof(T) * num_scalars;

      cudaMalloc((void **)&d_scalars, dev_buf_size);
      cudaMemcpy(d_scalars, scalars_ptr, dev_buf_size, cudaMemcpyHostToDevice);

      // ToDo: fix type handling
      // ToDo: copy pointer array
      op->program.kernel(name)
          .instantiate(type_of(result))
          .configure(grid, block)
          .launch(in_ptrs[0], side_ptrs, out_ptr, d_scalars, m, n, grix, 0, 0);
      cudaDeviceSynchronize();
      if (op->agg_type == SpoofOperator::FULL_AGG)
        cudaMemcpy(&result, (scalars_ptr + num_scalars * sizeof(T)), sizeof(T),
                   cudaMemcpyDeviceToHost);
      cudaFree(d_scalars);
    } else {
      std::cout << "kernel " << name << " not found." << std::endl;
      return result;
    }
    return result;
  }
};
