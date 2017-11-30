Multi Jass
=========

This codebase is used in the ADCS 2017 paper "Early Termination Heuristics for Score-at-a-Time Index Traversal".
If you use this codebase in your own work, please cite this work:

Joel Mackenzie, Falk Scholer, and J. Shane Culpepper,
**Early Termination Heuristics for Score-at-a-Time Index Traversal**
In proceedings of the 22nd Australasian Document Computing Symposium (ADCS 2017).

Acknowledgements
----------------
This code depends on [ATIRE](http://atire.org/).

The original Jass implementation can be found [here](http://github.com/lintool/jass).

Currently, A new version of Jass is under development This can be found [here](https://github.com/andrewtrotman/JASSv2).

Licence
-------
The licence is provided, see the LICENCE file.

Usage
=====
You first need to build ATIRE, and then point to the ATIRE directory in the GNUMakefile.
The invocation is the same as with the regular Jass program, but you may wish to specify
the number of threads to use when building the code. 

```cpp
const size_t threads_to_use = 40; // Set THREADS
```

