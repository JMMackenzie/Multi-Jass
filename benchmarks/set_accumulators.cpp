#include <cstdlib>
#include <vector>
#include <random>
#include <chrono>
#include <fstream>
#include <iostream>


/* A basic benchmark for iterating and zero'ing the accumulators.*/

int main(void) {

  std::vector<uint16_t> H_accumulators; // Accumulator table, one per doc..
  std::chrono::time_point<std::chrono::steady_clock, std::chrono::nanoseconds> start, end;
  std::chrono::nanoseconds total;
 
  std::random_device rd;     // only used once to initialise (seed) engine
  std::mt19937 rng(rd());    // random-number engine used (Mersenne-Twister in this case)
  std::uniform_int_distribution<int> uni(0, 1000);

  // 12B sized vector
  H_accumulators.resize(52000000);


  for (size_t c = 0; c < 100; ++c) {
  for (size_t i = 0; i < H_accumulators.size(); ++i)
    H_accumulators[i] = uni(rng);

  // Zero it
  start = std::chrono::steady_clock::now();
  std::fill(H_accumulators.begin(), H_accumulators.end(), 0);
  end = std::chrono::steady_clock::now();
  total = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start); 
  std::cerr << total.count()/1000000.0 << std::endl;
  }
  return 0;

}
