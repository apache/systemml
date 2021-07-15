Advanced Common Subexpression Elimination
==================================

Common Subexpression Elimination (CSE) is a commonly used compiler optimization technique minimizing code by detecting already calculated (and therefore duplicate) expressions and replacing them with a single variable if needed, which holds the computed value. Improving CSE can drastically improve performance in terms of speed and memory requirements.

Current State
----
The current implementation of CSE in SystemDS detects and eliminates common subexpressions in individual DAGs in two stages:

1. merging all leaf nodes identified by their name
2. bottom-up merging inner nodes by comparing parent nodes

Note that there is a strict separation between DAGs and the above steps are only executed on individual DAGs.

Improvements
------------
Our improved CSE will allow for nodes from multiple DAGs to be merged, allowing deeper optimization. While this sounds trivial, there are some considerations to be taken into account. If nodes from multiple DAGs are merged, the DAGs themselves are merged, possibly posing a problem for decentralized execution. If too many DAGs are being merged with only minor parts of the DAGs being merged together. Therefore it might be necessary to take the cost of merging into consideration and find a good balance.

