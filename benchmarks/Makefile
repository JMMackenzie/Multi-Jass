CI_FLAGS = -x c++ -DCI_FORCEINLINE -msse4 -std=c++11 -O3 -pthread 

all : benchmark_zero_acc benchmark_heapiter

benchmark_zero_acc: set_accumulators.cpp
	g++ $(CI_FLAGS) set_accumulators.cpp -o benchmark_accumulators

benchmark_heapiter: iterate_heap.cpp
	g++ $(CI_FLAGS) iterate_heap.cpp -o iterate_heap

clean:
	rm iterate_heap benchmark_accumulators

