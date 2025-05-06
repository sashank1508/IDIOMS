#include <iostream>
#include <string>

// Function declared in adaptive_benchmark.cpp
void run_benchmark();

int main(int argc, char **argv)
{
    std::cout << "===== IDIOMS Adaptive Query Distribution Demo =====" << std::endl;
    std::cout << "This program demonstrates and benchmarks the adaptive metadata" << std::endl;
    std::cout << "distribution enhancement for IDIOMS." << std::endl
              << std::endl;

    run_benchmark();

    std::cout << "\nBenchmark complete! To visualize results, run:" << std::endl;
    std::cout << "  python3 visualization/benchmark_visualizer.py" << std::endl;

    return 0;
}