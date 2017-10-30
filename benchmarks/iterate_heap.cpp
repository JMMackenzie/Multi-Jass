#include <vector>
#include <algorithm>
#include <queue>
#include <chrono>
#include <ctime>
#include <cstdlib>
#include <iostream>

/* Basic benchmark for iterating the heap and returning top-k docs.*/

struct doc_score {
  uint32_t score;
  uint32_t docid;
  bool operator>(const doc_score& rhs) const {
    if(score == rhs.score)
      return docid>rhs.docid;
    return score>rhs.score;
  }
  doc_score() {};
  doc_score(uint32_t s, uint32_t id) : score(s), docid(id) {};
};
  
std::vector<doc_score> accumulators;
std::priority_queue<doc_score, std::vector<doc_score>, std::greater<doc_score>> heap;

const size_t MAX = 52000000; // Around CW12B
   
int main(int argc, char**argv) {
  using clock = std::chrono::high_resolution_clock;

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " k\n";
    return -1;
  }
 
  size_t CI_top_k = std::atoi(argv[1]);
  std::cerr << "K = " << CI_top_k << "\n";

  std::srand(std::time(0));

  accumulators.resize(MAX);

  for(size_t runs = 0; runs < 100; ++runs) {

  // Populate our fake accumulator table...
  for(size_t i = 0; i < MAX; ++i) {
    accumulators[i].docid = i;
    accumulators[i].score = std::rand() % 100;
  }

  auto qry_start = clock::now();

  for(size_t i = 0; i < accumulators.size(); ++i) {
    if (heap.size() < CI_top_k)
      heap.push(accumulators[i]);
    else if(heap.top().score < accumulators[i].score) {
      heap.pop();
      heap.push(accumulators[i]);
    } 
  }
  
  uint32_t current;
  std::vector<doc_score> results;
  results.resize(heap.size());
  for (current = 0; current < results.size(); ++current) {
    doc_score min = heap.top();
    heap.pop();
    results[results.size()-1-current] = min;
  }
  auto qry_stop = clock::now();

  auto query_time = std::chrono::duration_cast<std::chrono::microseconds>(qry_stop-qry_start);
      std::cout 
                << query_time.count() / 1000.0 
                << std::endl;

  }
  return 0;

}
 
