//
// Created by Evyatar on 10/04/2022.
//
#include "MapReduceClient.h"

class MapReduceClient {
 public:
  // gets a single pair (K1, V1) and calls emit2(K2,V2, context) any
  // number of times to output (K2, V2) pairs.
  virtual void map(const K1* key, const V1* value, void* context) const = 0;

  // gets a single K2 key and a vector of all its respective V2 values
  // calls emit3(K3, V3, context) any number of times (usually once)
  // to output (K3, V3) pairs.
  virtual void reduce(const IntermediateVec* pairs, void* context) const = 0;
};