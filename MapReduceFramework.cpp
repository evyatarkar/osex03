//
// Created by evyat on 25/04/2022.
//

#include "MapReduceFramework.h"

#include "Barrier/Barrier.h"
#include <pthread.h>
#include <cstdio>
#include <atomic>
#include <iostream>
#include <algorithm>

typedef void *(*action_func) (void *);
void *action (void *);
class JobContext;

void lock_mutex (pthread_mutex_t *mutex)
{
  if (pthread_mutex_lock (mutex) != 0)
    {
      std::cerr << "Could not lock mutex" << std::endl;
      exit (1);
    }
}

void unlock_mutex (pthread_mutex_t *mutex)
{
  if (pthread_mutex_unlock (mutex) != 0)
    {
      std::cerr << "Could not lock mutex" << std::endl;
      exit (1);
    }
}

//class KChar : public K2, public K3 {
// public:
//  KChar (char c) : c (c)
//  {}
//
//  virtual bool operator< (const K2 &other) const
//  {
//    return c < static_cast<const KChar &>(other).c;
//  }
//
//  virtual bool operator< (const K3 &other) const
//  {
//    return c < static_cast<const KChar &>(other).c;
//  }
//
//  char c;
//};
//
//class VCount : public V2, public V3 {
// public:
//  VCount (unsigned int count) : count (count)
//  {}
//
//  unsigned int count;
//};

bool compare_keys (IntermediatePair i1, IntermediatePair i2)
{
  return (*i1.first < *i2.first);
}

class ThreadContext {
 public:
  int threadID;
  Barrier *barrier;
  pthread_mutex_t *input_mutex;
  JobContext *job;
  IntermediateVec *intermediate_pairs;
  int already_joined;

  ThreadContext (int id, Barrier *barrier,
                 pthread_mutex_t *input_mutex_,
                 JobContext *job_, IntermediateVec *intermediate_vec_) :
      threadID (id), barrier (barrier),
      input_mutex (input_mutex_),
      job (job_), intermediate_pairs (intermediate_vec_), already_joined (0)
  {}

  ~ThreadContext ()
  {
    delete intermediate_pairs;
  }

  void sort ()
  {
    std::cout << "thread - " << threadID << " started sorting" << std::endl;
    std::sort (intermediate_pairs->begin (),
               intermediate_pairs->end (),
               compare_keys);
    std::cout << "thread - " << threadID << " finished sorting" << std::endl;
  }
};

class JobContext {
 public:
  stage_t current_stage;
  const MapReduceClient *client;
  const InputVec *input_vec;
  std::vector<IntermediateVec *> shuffled_intermediate_elements;
  OutputVec *output_vec;

  Barrier *barrier;
  std::atomic<int> *progress_counter_map;
  std::atomic<int> *progress_counter_shuffle;
  std::atomic<int> *progress_counter_reduce;
  std::atomic<int> *intermediate_element_counter;
  std::atomic<int> *output_element_counter;

  int num_of_threads;
  int size_of_input;
  pthread_t *threads;
  std::vector<ThreadContext *> *contexts;
  pthread_mutex_t input_mutex;
  pthread_mutex_t reduce_mutex;
  pthread_mutex_t waiting_mutex;
  int is_waiting;

  JobContext (const MapReduceClient &client,
              const InputVec &inputVec, OutputVec &outputVec, Barrier *barrier,
              int multiThreadLevel, pthread_t *list_of_threads)
      :
      current_stage (UNDEFINED_STAGE), client (&client),
      input_vec (&inputVec), output_vec (&outputVec), barrier (barrier),
      num_of_threads (multiThreadLevel),
      threads (list_of_threads),
      is_waiting (0)
  {
    if (pthread_mutex_init (&input_mutex, nullptr) != 0)
      {
        std::cerr << "Could not init mutex" << std::endl;
        exit (1);
      }
    if (pthread_mutex_init (&reduce_mutex, nullptr) != 0)
      {
        std::cerr << "Could not init mutex" << std::endl;
        exit (1);
      }
    if (pthread_mutex_init (&waiting_mutex, nullptr) != 0)
      {
        std::cerr << "Could not init mutex" << std::endl;
        exit (1);
      }

    size_of_input = input_vec->size ();
    contexts = new std::vector<ThreadContext *> ();
//    shuffled_intermediate_elements = new std::vector<IntermediateVec *> ();
    progress_counter_map = new std::atomic<int> (0);
    progress_counter_shuffle = new std::atomic<int> (0);
    progress_counter_reduce = new std::atomic<int> (0);
    intermediate_element_counter = new std::atomic<int> (0);
    output_element_counter = new std::atomic<int> (0);
  }

  ~JobContext ()
  {
    if (pthread_mutex_destroy (&input_mutex) != 0)
      {
        std::cerr << "Could not destroy mutex" << std::endl;
        exit (1);
      }
    if (pthread_mutex_destroy (&reduce_mutex) != 0)
      {
        std::cerr << "Could not destroy mutex" << std::endl;
        exit (1);
      }
    if (pthread_mutex_destroy (&waiting_mutex) != 0)
      {
        std::cerr << "Could not destroy mutex" << std::endl;
        exit (1);
      }
    delete progress_counter_map;
    delete progress_counter_shuffle;
    delete progress_counter_reduce;
    delete intermediate_element_counter;
    delete output_element_counter;
    delete barrier;
//    delete shuffled_intermediate_elements;
    for (auto context: *contexts)
      {
        delete context;
      }
    delete contexts;
//    for (auto vec: *shuffled_intermediate_elements)
//      {
//        delete vec;
//      }
    delete[] threads;
  }

  float get_percentage (stage_t stage) const
  {
    if (stage == MAP_STAGE)
      {
        return ((float) *progress_counter_map / (float) size_of_input)
               * 100;
      }
    else if (stage == SHUFFLE_STAGE)
      {
        return ((float) *progress_counter_shuffle
                / (float) *intermediate_element_counter) * 100;
      }
    else if (stage == REDUCE_STAGE)
      {
        return ((float) *progress_counter_reduce
                / (float) *output_element_counter) * 100;
      }

    return 0;
  }

  void init_pairs (IntermediatePair *pairs)
  {
    for (int i = 0; i < num_of_threads; i++)
      {
        IntermediateVec *cur_pairs = contexts->at (i)->intermediate_pairs;
        if (!cur_pairs->empty ())
          {
            pairs[i] = cur_pairs->back ();
            cur_pairs->pop_back ();
          }
      }
  }
};

void shuffle (JobContext *job) //, ThreadContext *tc)
{
  job->current_stage = SHUFFLE_STAGE;
  std::cout << "SHUFFLING BITCHHH" << std::endl;
  K2 *max_key;
  int counters[job->num_of_threads];
  for (int i = 0; i < job->num_of_threads; i++)
    {
      counters[i] =
          (int) job->contexts->at (i)->intermediate_pairs->size () - 1;
    }
  while (1)
    {
      for (int i = 0; i < job->num_of_threads; i++) // find max key for round
        {
          if (counters[i] >= 0)
            {
              if (max_key == nullptr || max_key
                                        < job->contexts->at (i)->intermediate_pairs->at (counters[i]).first)
                {
                  max_key = job->contexts->at (i)->intermediate_pairs->at (counters[i]).first;
                }
            }
        }
      if (max_key == nullptr)
        { break; }

      auto *vec_for_key = new IntermediateVec ();

      for (int i = 0;
           i < job->num_of_threads; i++) // get all pairs with max_key as key
        {
          if (counters[i] >= 0)
            {
              IntermediatePair cur_pair = job->contexts->at (i)->intermediate_pairs->at (counters[i]);
              while (!(*cur_pair.first < *max_key
                       || *max_key < *cur_pair.first))
                {
                  vec_for_key->push_back (cur_pair);
                  counters[i]--;
                  (*(job->progress_counter_shuffle))++;
                  if (counters[i] < 0)
                    { break; }
                  cur_pair = job->contexts->at (i)->intermediate_pairs->at (counters[i]);
                }
            }
        }
      job->shuffled_intermediate_elements.push_back (vec_for_key);
      (*(job->output_element_counter))++;
      max_key = nullptr;
    }
  std::cout << "FINISHED SHUFFLING" << std::endl;
  job->current_stage = REDUCE_STAGE;
}


void emit2 (K2 *key, V2 *value, void *context)
{
  auto *thread_context = (ThreadContext *) context;
  auto new_pair = IntermediatePair (key, value);
  thread_context->intermediate_pairs->push_back (new_pair); // TODO check if added to vec OK

  (*(thread_context->job->intermediate_element_counter))++;
}

void emit3 (K3 *key, V3 *value, void *context)
{
  auto *thread_context = (ThreadContext *) context;
  auto new_pair = OutputPair (key, value);
  thread_context->job->output_vec->push_back (new_pair); // TODO check if added to vec OK

}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
//  pthread_t thread_list[multiThreadLevel];
  auto *thread_list = new pthread_t[multiThreadLevel];

  auto *barrier = new Barrier (multiThreadLevel);
  auto *new_job = new JobContext (client, inputVec, outputVec, barrier,
                                  multiThreadLevel, thread_list);

  for (int tid = 0; tid < multiThreadLevel; tid++)
    {
      IntermediateVec *intermediate_pairs = new IntermediateVec ();
      ThreadContext *new_context = new ThreadContext (tid, barrier, &new_job->input_mutex,
                                                      new_job, intermediate_pairs);
      new_job->contexts->push_back (new_context);
      pthread_create (new_job->threads + tid, nullptr, &action, new_context);
    }
  return (JobHandle) new_job;
}

void getJobState (JobHandle job, JobState *state)
{
  auto *job_ = (JobContext *) job;
  state->stage = job_->current_stage;
  state->percentage = job_->get_percentage (job_->current_stage);
}

void waitForJob (JobHandle job)
{
  auto *job_ = (JobContext *) job;
  lock_mutex (&job_->waiting_mutex);
  if (!job_->is_waiting)
    {
      job_->is_waiting = 1;
      for (int i = 0; i < job_->num_of_threads; i++)
        {
          if (!job_->contexts->at (i)->already_joined)
            {
              job_->contexts->at (i)->already_joined = 1;
              pthread_join (job_->threads[i], nullptr);
            }
        }
    }
  job_->is_waiting = 0;
  unlock_mutex (&job_->waiting_mutex);
}

void closeJobHandle (JobHandle job)
{
  if (job != nullptr)
    {
      auto *job_ = (JobContext *) job;
      if (job_->is_waiting)
        {
          while (job_->is_waiting)
            {}
        }
      else
        {
          waitForJob (job);
        }
      delete job_;
    }
}
void *action (void *thread_context)
{
  auto *tc = (ThreadContext *) thread_context;
  JobContext *job = tc->job;
  while (1)
    {
      lock_mutex (&job->input_mutex);
      if (job->current_stage == UNDEFINED_STAGE)
        {
          std::cout << "thread - " << tc->threadID << " changes state to MAP"
                    << std::endl;
          job->current_stage = MAP_STAGE;
        }
      if (*job->progress_counter_map < job->size_of_input)
        {
          int next_index = (*(job->progress_counter_map))++;
          unlock_mutex (&job->input_mutex);
          std::cout << "Thread " << tc->threadID << " got mutex for element " << next_index << std::endl;
          InputPair element = (job->input_vec)->at (next_index);


          job->client->map (element.first, element.second, tc);
        }
      else
        {
          std::cout << "thread - " << tc->threadID
                    << " unlocking mutex from else in mapping" << std::endl;
          unlock_mutex (&job->input_mutex);
          break;
        }
    }
  printf ("got to sort. thread: %d\n", tc->threadID);

  tc->sort ();
//  std::cout << "thread - " << tc->threadID << " started sorting" << std::endl;
//  std::sort (tc->intermediate_pairs->begin (),
//             tc->intermediate_pairs->end (),
//             compare_keys);
//  std::cout << "thread - " << tc->threadID << " finished sorting" << std::endl;

  printf ("got to first barrier: %d\n", tc->threadID);

  job->barrier->barrier (); // everyone finished sort

//  lock_mutex (&job->input_mutex);
  if (tc->threadID == 0)
    {
      shuffle (job);
    }

//  unlock_mutex (&job->input_mutex);

  printf ("got to last barrier: %d\n", tc->threadID);

  job->barrier->barrier (); // thread 0 finished shuffle and every other thread just waited for him

  printf ("entering reduce after last barrier: %d\n", tc->threadID);

  while (1)
    {
      lock_mutex (&job->reduce_mutex);
      if (job->get_percentage (REDUCE_STAGE) < 100)
        {
          int next_index = (*(job->progress_counter_reduce))++; // TODO if crashes its here
          auto element = job->shuffled_intermediate_elements.at (next_index);
          unlock_mutex (&job->reduce_mutex);
          job->client->reduce (element, tc);
        }
      else
        {
          unlock_mutex (&job->reduce_mutex);
          break;
        }
    }

  return (void *) 1;
}
