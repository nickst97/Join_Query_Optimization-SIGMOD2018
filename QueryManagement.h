#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdbool.h>
#include <math.h>

#include "RadixHashJoin.h"

#define MAXSTATS 40000000
#define MAX_RELATIONS 4

typedef struct node im_node;
struct node{
    result *r;
    int position;
    int im_id;
};

typedef struct pred{
    int flag_0;
    int flag_1;
    char *c_0,*c_1;
    char *side_0,*side_1;
    char *rlt_id_0,*rlt_id_1;
    int compare;
} pred;

result *im_update(relation upd_r,result *res,int pos);

typedef struct input_data input_data;
struct input_data{
    char *input;
    input_data *next;
};

typedef struct statistics{
    uint64_t min;
    uint64_t max;
    int size;
    int distinct;
} statistics;

typedef struct data{
    int num_columns;
    int num_tuples;
    statistics *stats;
    uint64_t **positions;
} data;

void data_storing(data *d,input_data *filepath,int num_files,statistics **copy);
void data_stats(data *d,int num_files,statistics **copy);
void query_reading(data *d,int num_files,statistics **copy);
void statistics_copy(data *d,int num_files,statistics **copy);
void predicates_execution(char *query,data *d, int data_size);
result *predicate_filter(relation *r,uint64_t value,int compare);
result *same_array(relation *r,relation *s,relation *upd,im_node *im,int im_id);
result *bothIM(relation *r,relation *s,relation *upd,im_node *im,int im_id);

void assessment_equal(data *d,int rel,int col,int k);
void assessment_bigger(data *d,int rel,int col,int k);
void assessment_smaller(data *d,int rel,int col,int k);
void assessment_same_rel(data *d,int rel,int col_1,int col_2);
void assessment_same_rel_col(data *d,int rel,int col);
void assessment_join(data *d,int rel_1,int rel_2,int col_1,int col_2);

pred *optimization(data *d,char *rlt_list[],pred *predj,int predj_size);

typedef struct lefttree{
    int pos[MAX_RELATIONS];
    int cost;
}lefttree;

int tree_isEmpty(lefttree lt);
void tree_insert(lefttree *lt,int new_cost,int rel,int pos);
int tree_connected(lefttree lt,int rel,int t);
