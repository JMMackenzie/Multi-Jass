#define PRINT_PER_QUERY_STATS 1
#define ANYTIME 1

#include <thread>
#include <vector>
#include <chrono>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _MSC_VER
#include <intrin.h>
#include <windows.h>
#else
#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/mach_time.h>
#endif
#include <unistd.h>
#include <glob.h>
#include <sys/times.h>
#include <dlfcn.h>
#endif
#ifdef __linux__
#include <sys/time.h>
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <emmintrin.h>
#include <smmintrin.h>

#include "CI.h"
#include "compress_qmx.h"
#include "compress_qmx_d4.h"
#include "compress_simple8b.h"
#include "process_postings.h"
#include <queue>
#include <atomic>

using namespace std;
 
void (*process_postings_list)(uint8_t *doclist, uint8_t *end, uint16_t impact, uint32_t integers, size_t offset);
 
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
  
std::priority_queue<doc_score, std::vector<doc_score>, std::greater<doc_score>> heap;

uint16_t **CI_accumulator_pointers;     // an array of pointers into the accumulators (used to avoid computing docIDs)
uint32_t CI_top_k;                      // the number of results to find (top-k)
uint32_t CI_results_list_length;        // the number of results we found (at most top-k)

ANT_heap<uint16_t *, add_rsv_compare> *CI_heap;
std::vector<uint16_t> H_accumulators; // Accumulator table, one per doc...
std::vector<std::atomic_flag> H_spinlock; // Spinlock per accumulator


char **CI_documentlist;                 // the list of document IDs (TREC document IDs)

CI_vocab_heap *CI_dictionary;           // the vocab array
uint32_t CI_unique_terms;
uint32_t CI_unique_documents;

#define MAX_TERMS_PER_QUERY 20
#define MAX_QUERIES 10000
#define MAX_QUANTUM 0xFF

const size_t threads_to_use = 40; // Set THREADS

#ifdef _MSC_VER
#define atoll(x) _atoi64(x)
double log2(double n) {return log(n) / log(2.0);}
#endif

void print_os_time(void) {
#ifdef __APPLE__
  struct tms tmsbuf;
  long clock_speed = sysconf(_SC_CLK_TCK);

  if (times(&tmsbuf) > 0) {
    printf("OS reports kernel time: %.3f seconds\n", (double)tmsbuf.tms_stime / clock_speed);
    printf("OS reports user time  : %.3f seconds\n", (double)tmsbuf.tms_utime / clock_speed);
  }
#endif
}

void trec_dump_results_heap(uint32_t topic_id, FILE *out, std::vector<doc_score>& results) {
 size_t current;
 for(current = 0; current < results.size(); ++current)
    fprintf(out, "%d Q0 %s %d %d Jass\n", topic_id, CI_documentlist[results[current].docid], current+1, results[current].score);
}

char *read_entire_file(const char *filename, uint64_t *length = NULL) {
  char *block = NULL;
  FILE *fp;
  struct stat details;

  if (filename == NULL) {
    return NULL;
  }

  if ((fp = fopen(filename, "rb")) == NULL) {
    return NULL;
  }

  if (fstat(fileno(fp), &details) == 0) {
    if (details.st_size != 0) {
      if ((block = new char [(size_t)(details.st_size + 1)]) != NULL) { // +1 for the '\0' on the end
        if (length != NULL) {
          *length = details.st_size;
        }

        if (fread(block, details.st_size, 1, fp) == 1) {
          block[details.st_size] = '\0';
        } else {
          delete [] block;
          block = NULL;
        }
      }
    }
  }
  fclose(fp);

  return block;
}

class CI_quantum_header {
public:
  uint16_t impact;
  uint64_t offset;
  uint64_t end;
  uint32_t quantum_frequency;
}__attribute__((packed));

int quantum_compare(const void *a, const void *b) {
  CI_quantum_header *lhs = (CI_quantum_header *) ((*(uint64_t *) a) + postings);
  CI_quantum_header *rhs = (CI_quantum_header *) ((*(uint64_t *) b) + postings);

  /*
   sort from highest to lowest impact, but break ties by placing the lowest quantum-frequency first and the highest quantum-drequency last
   */
  return lhs->impact > rhs->impact ? -1 : lhs->impact < rhs->impact ? 1 :
         lhs->quantum_frequency > rhs->quantum_frequency ? 1 :
         lhs->quantum_frequency == rhs->quantum_frequency ? 0 : -1;
}

void print_postings_list(CI_vocab_heap *postings_list) {
  CI_quantum_header *header;

  uint32_t current;
  uint64_t *data;

  printf("\n\noffset:0x%llX\n", postings_list->offset);
  fflush(stdout);
  printf("impacts:%llu\n", postings_list->impacts);
  for (uint32_t x = 0; x < 64; x++) {
    printf("%02X ", *(postings + postings_list->offset + x));
  }
  puts("");

  data = (uint64_t *) (postings + postings_list->offset);
  for (current = 0; current < postings_list->impacts; current++) {
    header = (CI_quantum_header *) (postings + (data[current]));
    printf("OFFSET:%llx Impact:%hx Offset:%llx End:%llx docids:%u\n", data[current], header->impact,
        header->offset, header->end, header->quantum_frequency);

    for (uint8_t *byte = postings + header->offset; byte < postings + header->end; byte++) {
      printf("0x%02X, ", *byte);
      if (*byte & 0x80) {
        printf("\n");
      }
    }
    puts("");
  }
}

void read_doclist(void) {
  char *doclist;
  uint64_t total_docs;
  uint64_t length;
  uint64_t *offset_base;

  printf("Loading doclist... ");
  fflush(stdout);
  if ((doclist = read_entire_file("CIdoclist.bin", &length)) == 0) {
    exit(printf("Can't read CIdoclist.bin"));
  }

  CI_unique_documents = total_docs = *((uint64_t *) (doclist + length - sizeof(uint64_t)));
  CI_documentlist = new char * [total_docs];


  offset_base = (uint64_t *) (doclist + length - (total_docs * sizeof(uint64_t) + sizeof(uint64_t)));
  for (uint64_t id = 0; id < total_docs; id++) {
    CI_documentlist[id] = doclist + offset_base[id];
  }

  puts("done");
  fflush(stdout);
}

void read_vocab(void) {
  char *vocab, *vocab_terms;
  uint64_t total_terms;
  uint64_t length;
  uint64_t *base;
  uint64_t term;

  printf("Loading vocab... ");
  fflush(stdout);
  if ((vocab = read_entire_file("CIvocab.bin", &length)) == NULL) {
    exit(printf("Can't read CIvocab.bin"));
  }
  if ((vocab_terms = read_entire_file("CIvocab_terms.bin")) == NULL) {
    exit(printf("Can't read CIvocab_terms.bin"));
  }

  CI_unique_terms = total_terms = length / (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t));

  CI_dictionary = new CI_vocab_heap[total_terms];
  for (term = 0; term < total_terms; term++) {
    base = (uint64_t *) (vocab + (3 * sizeof(uint64_t)) * term);

    CI_dictionary[term].term = vocab_terms + base[0];
    CI_dictionary[term].offset = base[1];
    CI_dictionary[term].impacts = base[2];
  }

  puts("done");
  fflush(stdout);
}

void process_blocks(size_t no_quantums, size_t jump, size_t local_rho, size_t thread_id, uint64_t *quantum_order, size_t buffer_offset) {
  size_t current_quantum;
  size_t postings_processed = 0;
  CI_quantum_header *current_header;
  for(current_quantum = thread_id; current_quantum < no_quantums; current_quantum += jump) {
    current_header = (CI_quantum_header *) (postings + quantum_order[current_quantum]);
    if (postings_processed + current_header->quantum_frequency > local_rho) {
      return;
    }
    size_t offset = thread_id * buffer_offset; 
    (*process_postings_list)(postings + current_header->offset, postings + current_header->end,
      current_header->impact, current_header->quantum_frequency, offset); 
      postings_processed += current_header->quantum_frequency;
  }
}
    
size_t calculate_buffer_offset(size_t raw_buffer_size) {
  while (raw_buffer_size % 64 != 0)
    ++raw_buffer_size;
  return raw_buffer_size;
}

int main(int argc, char *argv[]) {
  chrono::time_point<chrono::steady_clock, chrono::nanoseconds> full_query_timer = chrono::steady_clock::now();
  chrono::time_point<chrono::steady_clock, chrono::nanoseconds> start_timer, end_timer, full_query_without_io_timer;
  chrono::nanoseconds stats_total_time_to_search;
  chrono::nanoseconds stats_total_time_to_search_without_io;
  chrono::nanoseconds stats_tmp;

  static char buffer[1024];
  const char *SEPERATORS = " \t\r\n";
  int i;
  FILE *fp, *out;
  char *term, *id;
  uint64_t query_id;
  CI_vocab_heap *postings_list;

  uint32_t postings_to_process;
  uint32_t postings_processed = 0;
  uint64_t total_number_of_topics;
  uint64_t stats_quantum_check_count, stats_quantum_count, stats_early_terminations;
  uint64_t experimental_repeat = 0, times_to_repeat_experiment = 3;
  uint64_t *quantum_order, *current_quantum;
  uint64_t max_remaining_impact;
  uint16_t **quantum_check_pointers;
  uint64_t early_terminate;
  uint16_t **partial_rsv;
  CI_quantum_header *current_header;
  //void (*process_postings_list)(uint8_t *doclist, uint8_t *end, uint16_t impact, uint32_t integers, size_t offset, size_t thread_id);
  uint32_t parameter, decompress_then_process;

  uint64_t queries_id[MAX_QUERIES];
  uint32_t queries_num_postings[MAX_QUERIES];
  uint64_t queries_latency[MAX_QUERIES];

  std::thread m_threads[threads_to_use];


  /*
   Parameter parsing
   */
  CI_top_k = CI_unique_documents + 1;
  decompress_then_process = false;

  if (argc < 1 || argc > 5) {
    exit(printf("Usage:%s <queryfile> [<top-k-number>] [<num-postings-to-proces>] [-d<ecompress then process>]\n", argv[0]));
  }

  for (parameter = 2; parameter < argc; parameter++) {
    if (strcmp(argv[parameter], "-d") == 0) {
      decompress_then_process = true;
    } else if (parameter == 2) {
      CI_top_k = atoll(argv[parameter]);
    } else if (parameter == 3) {
      postings_to_process = atoi(argv[parameter]);
//    Uncomment this for aggressive per-thread processing  
//    postings_to_process = (postings_to_process/threads_to_use)+1000; // unsure non-zero
      fprintf(stderr, "Each thread will process %zu postings.\n", postings_to_process);
    }
  }

  printf("Ranking mode: anytime\n");
  printf("Settings: top k = %d, postings to process = %d", CI_top_k, postings_to_process);
  if (decompress_then_process) {
    printf(", decompress then process (non-interleaved)\n");
  } else {
    printf(", interleaving decompression and postings processing\n");
  }

  printf("Loading postings... ");
  fflush(stdout);
  if ((postings = (uint8_t *) read_entire_file("CIpostings.bin")) == NULL) {
    exit(printf("Cannot open postings file 'CIpostings.bin'\n"));
  }
  puts("done");
  fflush(stdout);

  if ((fp = fopen(argv[1], "r")) == NULL) {
    exit(printf("Can't open query file:%s\n", argv[1]));
  }

  if ((out = fopen("ranking.txt", "w")) == NULL) {
    exit(printf("Can't open output file.\n"));
  }

  /*
   Sort out how to decode the postings (either compressed or not)
   */
  if (*postings == 's') {
    puts("Uncompressed Index");
    process_postings_list = CIt_process_list_not_compressed;
  } else if (*postings == 'c') {
    puts("Variable Byte Compressed Index");
    if (decompress_then_process) {
      process_postings_list = CIt_process_list_decompress_then_process;
    } else {
      process_postings_list = CIt_process_list_compressed_vbyte;
    }
  } else if (*postings == '8') {
    puts("Simple-8b Compressed Index");
    if (decompress_then_process) {
      process_postings_list = CIt_process_list_compressed_simple8b_ATIRE;
    } else {
      process_postings_list = CIt_process_list_compressed_simple8b;
    }
  } else if (*postings == 'q') {
    puts("QMX Compressed Index");
    if (!decompress_then_process) {
      exit(printf("Cannot interleave QMX\n"));
    }
    process_postings_list = CIt_process_list_compressed_qmx;
  } else if (*postings == 'Q') {
    puts("QMX-D4 Compressed Index");
    if (!decompress_then_process) {
      exit(printf("Cannot interleave QMX\n"));
    }
    process_postings_list = CIt_process_list_compressed_qmx_d4;
  } 
  else if (*postings == 'R') {
    puts("QMX-D0 Compressed Index");
    if (!decompress_then_process) {
      exit(printf("Cannot interleave QMX\n"));
    }
    process_postings_list = CIt_process_list_compressed_qmx_d0;
  } else {
    exit(printf("This index appears to be invalid as it is neither compressed nor not compressed!\n"));
  }

  read_doclist();
  read_vocab();

  // First calculate the buffer size such that we are 64 bit aligned
  size_t buffer_offset = calculate_buffer_offset(CI_unique_documents + 1024);

  //Create a buffer to store the decompressed postings lists
  CI_decompressed_postings = new uint32_t[buffer_offset * threads_to_use];	// we add because some decompressors are allowed to overflow ehis buffer
  H_accumulators.resize(CI_unique_documents);
  std::vector<std::atomic_flag> H_spinlock_hack(CI_unique_documents);
  H_spinlock.swap(H_spinlock_hack);

  /*
   Now prime the search engine
   */
  //CI_accumulators = new uint16_t[accumulators_needed];
  CI_accumulator_pointers = new uint16_t * [CI_unique_documents];

  /*
   For QaaT early termination we need K+1 elements in the heap so that we can check that nothing else can get into the top-k.
   */
  CI_top_k++;

  /*
   Allocate the quantum at a time table
   */
  quantum_order = new uint64_t[MAX_TERMS_PER_QUERY * MAX_QUANTUM];
  ///quantum_check_pointers = new uint16_t * [accumulators_needed];

  /*
   Now start searching
   */
  while (experimental_repeat < times_to_repeat_experiment) {
    experimental_repeat++;

    stats_total_time_to_search = chrono::nanoseconds(0);
    stats_total_time_to_search_without_io = chrono::nanoseconds(0);

    total_number_of_topics = 0;
    stats_quantum_check_count = 0;
    stats_quantum_count = 0;
    stats_early_terminations = 0;

    rewind(fp);					// the query file
    rewind(out);				// the TREC run file

    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
      full_query_without_io_timer = chrono::steady_clock::now();
      if ((id = strtok(buffer, SEPERATORS)) == NULL) {
        continue;
      }

      total_number_of_topics++;
      CI_results_list_length = 0;

      query_id = atoll(id);

      // Reset the accumulator table
      std::fill(H_accumulators.begin(), H_accumulators.end(), 0);

      max_remaining_impact = 0;
      current_quantum = quantum_order;
      early_terminate = false;

      while ((term = strtok(NULL, SEPERATORS)) != NULL) {
        postings_list = (CI_vocab_heap *) bsearch(term, CI_dictionary, CI_unique_terms, sizeof(*CI_dictionary), CI_vocab_heap::compare_string);

        if (postings_list != NULL) {
          // Copy this term's pointers to the segments list
          memcpy(current_quantum, postings + postings_list->offset, postings_list->impacts * sizeof(*quantum_order));

          // Compute the maximum possibe impact score (that is, assume one document has the maximum impact for each term)
          max_remaining_impact += ((CI_quantum_header *) (postings + *current_quantum))->impact;

          // Advance to the place we want to place the next set of segments
          current_quantum += postings_list->impacts;
        }
      }
      /*
       NULL termainate the list of quantums
       */
      *current_quantum = 0;

      /*
       Sort the quantum list from highest to lowest
       */
      size_t quantum_total = current_quantum - quantum_order;
      qsort(quantum_order, quantum_total, sizeof(*quantum_order), quantum_compare);

      /*
       Now process each quantum, one at a time
       */

      // Loop to fire off threads
      for(size_t thread_id = 0; thread_id < threads_to_use; ++thread_id) {
        m_threads[thread_id] = std::thread(process_blocks, quantum_total, threads_to_use, postings_to_process, thread_id, quantum_order, buffer_offset);
      }

      for(size_t thread_id = 0; thread_id < threads_to_use; ++thread_id) {
        m_threads[thread_id].join();
      }

    for(uint32_t i = 0; i < H_accumulators.size(); ++i) {
      if (heap.size() < CI_top_k-1)
        heap.push({H_accumulators[i], i});
      else if(heap.top().score < H_accumulators[i]) {
        heap.pop();
        heap.push({H_accumulators[i], i});
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

    end_timer = chrono::steady_clock::now();
    stats_tmp = chrono::duration_cast<chrono::nanoseconds>(end_timer - full_query_without_io_timer);
    stats_total_time_to_search_without_io += stats_tmp;

#ifdef PRINT_PER_QUERY_STATS
      queries_id[total_number_of_topics-1] = query_id;
      queries_num_postings[total_number_of_topics-1] = postings_processed;
      queries_latency[total_number_of_topics-1] = stats_tmp.count();
#endif

      // Dump TREC output
     trec_dump_results_heap(query_id, out, results);	// Subtract 1 from top_k because we added 1 for the early termination checks.
    }
  }

  fclose(out);
  fclose(fp);

  end_timer = chrono::steady_clock::now();
  stats_total_time_to_search += chrono::duration_cast<chrono::nanoseconds>(end_timer - full_query_timer);

#ifdef PRINT_PER_QUERY_STATS
  for (i=0; i<total_number_of_topics; i++) {
    printf("%llu,%.3f,%llu\n", queries_id[i], (double)queries_latency[i]/1000000.0, queries_num_postings[i]);
  }
#endif

  return 0;
}
