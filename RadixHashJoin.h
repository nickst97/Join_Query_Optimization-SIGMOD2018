#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <math.h>
#include <sys/sysinfo.h>
#include <errno.h>
#include <pthread.h>

#define n 8
#define MAXSIZE ((1024*1024)/128)
#define MAXSIZE_2 ((128*128)/128)

typedef struct jobs jobs;
struct jobs{
    int start;
    int end;
    int id;
    int fun;
    int rel_id;
    jobs *next;
};

void jobs_insert(jobs **j,int start,int end,int id,int fun,int rel_id);
jobs *jobs_pop(jobs **j);
int jobs_isEmpty(jobs **j);

typedef struct tuple{
    uint64_t key;
    uint64_t payload;
} tuple;

typedef struct relation{
    tuple *tuples;
    uint64_t num_tuples;
} relation;

typedef struct ids{
    uint64_t rowid_R;
    uint64_t rowid_S;
} ids;

typedef struct result result;
struct result{
    ids *id;
    int cur_size;
    result *next;
    ids *old_pos;
};

typedef struct info info;
struct info{
    relation *re_R;
    relation *re_S;

};


void itemization(relation *reCURR,int *sums,int rel_id);
void write_result(result **last,uint64_t R_key,uint64_t S_key);
void write_result_2(result **last,uint64_t R_key,uint64_t S_key,uint64_t R_old,uint64_t S_old);
void write_result_3(result **last,uint64_t R_key,uint64_t S_key,uint64_t R_old,uint64_t S_old);
uint64_t hash_function_1(uint64_t payload);
uint64_t hash_function_2(uint64_t payload,uint64_t next);
result* RadixHashJoin(relation *reIR,relation *reIS);
void HistogramJob(relation *r,jobs *temp);
void PartitionJob(relation *r,jobs *temp);
void JoinJob(jobs *temp);
