//
// Created by evyat on 25/04/2022.
//

#include "MapReduceFramework.h"

#include "Barrier.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <algorithm>

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

bool compare_keys (IntermediatePair i1, IntermediatePair i2)
{
  return (*i1.first < *i2.first);
}

class ThreadContext {
 public:
  int threadID;
  JobContext *job;
  IntermediateVec *intermediate_pairs;
  int already_joined;

  ThreadContext (int id, JobContext *job_, IntermediateVec *intermediate_vec_) :
      threadID (id), job (job_), intermediate_pairs (intermediate_vec_), already_joined (0)
  {}

  ~ThreadContext ()
  {
    delete intermediate_pairs;
  }

  void sort () const
  {
    std::sort (intermediate_pairs->begin (),
               intermediate_pairs->end (),
               compare_keys);
  }
};

class JobContext {
 public:
  stage_t current_stage;
  const MapReduceClient *client;
  const InputVec *input_vec;
  std::vector<IntermediateVec> shuffled_intermediate_elements;
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
    for (auto context: *contexts)
      {
        delete context;
      }
    delete contexts;
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

  void init_pairs (IntermediatePair *pairs) const
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

void shuffle (JobContext *job)
{
  job->current_stage = SHUFFLE_STAGE;
  while (true)
    {
      K2 *max_key = nullptr;
      for (ThreadContext *context : *job->contexts)
        {
          if (context->intermediate_pairs->empty ())
            {
              continue;
            }
          if (max_key == nullptr || *max_key < *context->intermediate_pairs->back ().first)
            {
              max_key = context->intermediate_pairs->back ().first;
            }
        }
      if (max_key == nullptr)
        {
          break;
        }
      IntermediateVec keyVec;
      for (ThreadContext *context : *job->contexts)
        {
          if (!context->intermediate_pairs->empty ())
            {
              IntermediatePair pair = context->intermediate_pairs->back ();
              while (!(*pair.first < *max_key) && !(*max_key < *pair.first))
                {
                  keyVec.push_back (pair);
                  context->intermediate_pairs->pop_back ();
                  (*(job->progress_counter_shuffle))++;
                  if (context->intermediate_pairs->empty ())
                    {
                      break;
                    }
                  pair = context->intermediate_pairs->back ();
                }
            }
        }
      job->shuffled_intermediate_elements.push_back (keyVec);
      (*(job->output_element_counter))++;
      max_key = nullptr;
    }
  job->current_stage = REDUCE_STAGE;
}

void emit2 (K2 *key, V2 *value, void *context)
{
  auto *thread_context = (ThreadContext *) context;
  auto new_pair = IntermediatePair (key, value);
  thread_context->intermediate_pairs->push_back (new_pair);
  (*(thread_context->job->intermediate_element_counter))++;
}

void emit3 (K3 *key, V3 *value, void *context)
{
  auto *thread_context = (ThreadContext *) context;
  lock_mutex (&thread_context->job->input_mutex);
  OutputPair pair = {key, value};
  thread_context->job->output_vec->push_back (pair);
  unlock_mutex (&thread_context->job->input_mutex);
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  auto *thread_list = new pthread_t[multiThreadLevel];
  auto *barrier = new Barrier (multiThreadLevel);
  auto *new_job = new JobContext (client, inputVec, outputVec, barrier,
                                  multiThreadLevel, thread_list);

  for (int tid = 0; tid < multiThreadLevel; tid++)
    {
      auto *intermediate_pairs = new IntermediateVec ();
      auto *new_context = new ThreadContext (tid, new_job, intermediate_pairs);
      new_job->contexts->push_back (new_context);
      pthread_create (new_job->threads + tid, nullptr, &action, new_context);
    }
  return (JobHandle) new_job;
}

void getJobState (JobHandle job, JobState *state)
{
  auto *job_ = (JobContext *) job;
  auto  cur_stage = job_->current_stage;
  state->stage = cur_stage;
  state->percentage = job_->get_percentage (cur_stage);
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

  while (true) // MAP
    {
      lock_mutex (&job->input_mutex);
      if (job->current_stage == UNDEFINED_STAGE)
        {
          job->current_stage = MAP_STAGE;
        }
      if (*job->progress_counter_map < job->size_of_input)
        {
          int next_index = (*(job->progress_counter_map))++;
          unlock_mutex (&job->input_mutex);
          InputPair element = (job->input_vec)->at (next_index);

          job->client->map (element.first, element.second, tc);
        }
      else
        {
          unlock_mutex (&job->input_mutex);
          break;
        }
    }

  tc->sort (); // SORT

  job->barrier->barrier (); // everyone finished sort

  if (tc->threadID == 0)
    {
      shuffle (job); // SHUFFLE
    }

  job->barrier->barrier (); // thread 0 finished shuffle and every other thread just waited for him

  while (true) // REDUCE
    {
      lock_mutex (&job ->reduce_mutex);
      if (*job->progress_counter_reduce < *job->output_element_counter)
        {
          int next_index = (*(job->progress_counter_reduce))++; // TODO if crashes its here
          if (next_index >= *job->output_element_counter)
            {
              unlock_mutex (&job->reduce_mutex);
              return (void *) 1;
            }

          unlock_mutex (&job->reduce_mutex);
          IntermediateVec keyVec = job->shuffled_intermediate_elements.at (next_index);
          job->client->reduce (&keyVec, tc);
        }
      else
        {
          unlock_mutex(&job->reduce_mutex);
          break;
        }
    }
  return (void *) 1;
}
