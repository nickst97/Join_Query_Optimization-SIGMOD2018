#include "RadixHashJoin.h"
#define perror2(s,e) fprintf(stderr,"%s: %s\n",s,strerror(e))

int stop,stop_2,stopped,num_threads,buckets;
int *totalhist,*psum;
int *pos_R,*pos_S,**hists,**psums;
int times_in;
relation *reIR_new,*reIS_new;
pthread_mutex_t list;
pthread_mutex_t barrier;
pthread_cond_t list_cond;
pthread_cond_t barrier_cond;
jobs *job;
result **all;
int *old_rp,*old_sp;

//threads
void *Jobs(void *renew){
    info *in=(info *)renew;
    int flag=0;
    int err;
    jobs *temp;
    //Scheduling
    while(1){
      if(err=pthread_mutex_lock(&list)){
          perror2("pthread_mutex_lock",err);
          return 0;
      }
      while(jobs_isEmpty(&job)){	//wait for signal if empty
          if((jobs_isEmpty(&job))&&(stop)){   //done
            flag=1;
            break;
          }
          pthread_cond_wait(&list_cond,&list);
      }
      if(flag){   //no more jobs,exit
          if(err=pthread_mutex_unlock(&list)){
              perror2("pthread_mutex_unlock",err);
              return 0;
          }
          break;
      }
      temp=jobs_pop(&job);  //pop a job

      if(err=pthread_mutex_unlock(&list)){
          perror2("pthread_mutex_unlock",err);
          return 0;
      }
      if(temp->fun==0){     //HistogramJob
            if(temp->rel_id==0)
                HistogramJob(in->re_R,temp);
            else
                HistogramJob(in->re_S,temp);
            if(err=pthread_mutex_lock(&barrier)){
                    perror2("pthread_mutex_lock",err);
                    return 0;
            }
            stopped++;    //barrier
            if(stopped==num_threads)
                    pthread_cond_signal(&barrier_cond);
            if(err=pthread_mutex_unlock(&barrier)){
                    perror2("pthread_mutex_lock",err);
                    return 0;
            }
      }
      else if(temp->fun==1){    //PartitionJob
        if(temp->rel_id==0)
            PartitionJob(in->re_R,temp);
        else
            PartitionJob(in->re_S,temp);
            if(err=pthread_mutex_lock(&barrier)){
                    perror2("pthread_mutex_lock",err);
                    return 0;
            }
            stopped++;    //barrier
            if(stopped==num_threads)
                    pthread_cond_signal(&barrier_cond);
            if(err=pthread_mutex_unlock(&barrier)){
                    perror2("pthread_mutex_lock",err);
                    return 0;
            }
      }
      else{     //JoinJob
            JoinJob(temp);

      }
    }   //while(1)
    pthread_exit(NULL);
}

//Histogram-hash_1
void HistogramJob(relation *r,jobs *temp){
  for(int i=0;i<buckets;i++)
      hists[temp->id][i]=0;
  for(int i=temp->start;i<=temp->end;i++)
      hists[temp->id][hash_function_1(r->tuples[i].payload)]++;
}

//Partition
void PartitionJob(relation *r,jobs *temp){
  uint64_t hash_res;
  for(int i=temp->start;i<=temp->end;i++){
        hash_res=hash_function_1(r->tuples[i].payload);
        if(temp->rel_id==0){
            old_rp[psums[temp->id][hash_res]]=i;
            reIR_new->tuples[psums[temp->id][hash_res]].payload=r->tuples[i].payload;
            reIR_new->tuples[psums[temp->id][hash_res]].key=r->tuples[i].key;
        }
        else{
            old_sp[psums[temp->id][hash_res]]=i;
            reIS_new->tuples[psums[temp->id][hash_res]].payload=r->tuples[i].payload;
            reIS_new->tuples[psums[temp->id][hash_res]].key=r->tuples[i].key;
        }
        psums[temp->id][hash_res]++;
  }

}

//results-hash_2
void JoinJob(jobs *temp){
  int flag=0;
  int err;
    //hash_2/result
    int i=temp->id;   //bucket #
    int size_a,size_b;
    result *last;
    all[i]=malloc(sizeof(result)*1); //result #
    last=all[i];
    last->cur_size=0;
    last->next=NULL;
    last->id=(ids*)malloc(MAXSIZE_2*(sizeof(ids)));
    last->old_pos=(ids*)malloc(MAXSIZE_2*(sizeof(ids)));
    int *chain;
    int *bucket;
    //calculate each size of bucket i
    if(i==buckets-1){
      size_a=reIR_new->num_tuples-pos_R[i];
      size_b=reIS_new->num_tuples-pos_S[i];
    }
    else{
      size_a=pos_R[i+1]-pos_R[i];
      size_b=pos_S[i+1]-pos_S[i];
    }
    //if one (or two) of the i-buckets are empty move on to the next job
    if((size_a==0)||(size_b==0))
      return;
    //indexing for the smaller bucket
    if(size_a<=size_b){ //index for R
      //bucket, chain arrays
      chain=(int*)malloc(size_a*sizeof(int));
      bucket=(int*)malloc(size_a*sizeof(int));
      for(int j=0;j<size_a;j++){
        chain[j]=-1;
        bucket[j]=-1;
      }
      uint64_t hash_result;
      int iter=0;
      //for every payload in the i-bucket of R
      //loop from the last one to the first one
      for(int j=pos_R[i]+size_a-1;j>=pos_R[i];j--){
          hash_result=hash_function_2(reIR_new->tuples[j].payload,size_a);
          //plaicing in bucket
          if(bucket[hash_result]==-1){
              iter++;
              bucket[hash_result]=j-pos_R[i];
          }
          else{ //else if it is not the first payload with that hash_result, placing in chain
            int position=bucket[hash_result];
            do{
              if(chain[position]==-1){
                iter++;
                chain[position]=j-pos_R[i];
                break;
              }
              position=chain[position];
            }while(1);
          }
      }
      //for every payload in the i-bucket of S
      for(int j=pos_S[i];j<pos_S[i]+size_b;j++){
          hash_result=hash_function_2(reIS_new->tuples[j].payload,size_a);
          //if that hash_result didn't show up in R move on to the next payload
          if(bucket[hash_result]==-1)
            continue;
          //else check for match, if there is one store it
          if(reIR_new->tuples[bucket[hash_result]+pos_R[i]].payload==reIS_new->tuples[j].payload){
            write_result_2(&last,reIR_new->tuples[bucket[hash_result]+pos_R[i]].key,reIS_new->tuples[j].key,old_rp[bucket[hash_result]+pos_R[i]],old_sp[j]);
          }
          //then check the chain
          int position=bucket[hash_result];
          do{   //for every element in chain with that hash_result
            if(chain[position]==-1)
              break;
            //check for match
            if(reIR_new->tuples[chain[position]+pos_R[i]].payload==reIS_new->tuples[j].payload){
              write_result_2(&last,reIR_new->tuples[chain[position]+pos_R[i]].key,reIS_new->tuples[j].key,old_rp[chain[position]+pos_R[i]],old_sp[j]);
            }
            position=chain[position];
          }while(1);
      }
    }
    else{   //index for S, same as above
        chain=(int*)malloc(size_b*sizeof(int));
        bucket=(int*)malloc(size_b*sizeof(int));
        for(int j=0;j<size_b;j++){
          chain[j]=-1;
          bucket[j]=-1;
        }
        uint64_t hash_result;
        int iter=0;
        for(int j=pos_S[i]+size_b-1;j>=pos_S[i];j--){
          hash_result=hash_function_2(reIS_new->tuples[j].payload,size_b);
              if(bucket[hash_result]==-1){
                iter++;
                  bucket[hash_result]=j-pos_S[i];
              }
              else{
                int position=bucket[hash_result];
                do{
                  if(chain[position]==-1){
                    iter++;
                    chain[position]=j-pos_S[i];
                    break;
                  }
                  position=chain[position];
                }while(1);
              }
          }
          for(int j=pos_R[i];j<pos_R[i]+size_a;j++){
              hash_result=hash_function_2(reIR_new->tuples[j].payload,size_b);
              if(bucket[hash_result]==-1)
                continue;
              if(reIS_new->tuples[bucket[hash_result]+pos_S[i]].payload==reIR_new->tuples[j].payload){
                write_result_2(&last,reIR_new->tuples[j].key,reIS_new->tuples[bucket[hash_result]+pos_S[i]].key,old_rp[j],old_sp[bucket[hash_result]+pos_S[i]]);
              }
              int position=bucket[hash_result];
              do{
                if(chain[position]==-1)
                  break;
                if(reIS_new->tuples[chain[position]+pos_S[i]].payload==reIR_new->tuples[j].payload){
                    write_result_2(&last,reIR_new->tuples[j].key,reIS_new->tuples[chain[position]+pos_S[i]].key,old_rp[j],old_sp[chain[position]+pos_S[i]]);
                }
                  position=chain[position];
              }while(1);
            }
      }
      free(chain);
      free(bucket);
}


void itemization(relation *reCURR,int *sums,int rel_id){
     stop=0;
     stop_2=0;
     stopped=0;
     int err;
     int start_pos=0,end_pos;
     int sum=reCURR->num_tuples/num_threads;
     end_pos=reCURR->num_tuples/num_threads+reCURR->num_tuples%num_threads-1;
     //1st parallel part
     for(int i=0;i<num_threads;i++){
          if(err=pthread_mutex_lock(&list)){
            perror2("pthread_mutex_lock",err);
          }
          jobs_insert(&job,start_pos,end_pos,i,0,rel_id);   //insert
          pthread_cond_signal(&list_cond);	//signal to possibly waiting threads
          if(err=pthread_mutex_unlock(&list)){
            perror2("pthread_mutex_unlock",err);
          }
          start_pos=end_pos+1;
          end_pos+=sum;
      }
      //barrier
      if(err=pthread_mutex_lock(&barrier)){
        perror2("pthread_mutex_lock",err);
      }
      while(stopped<num_threads)
          pthread_cond_wait(&barrier_cond,&barrier);
      if(err=pthread_mutex_unlock(&barrier)){
        perror2("pthread_mutex_lock",err);
      }
      //total histogram-psum (used for duplicates in join)
      totalhist=malloc(sizeof(int)*buckets);
      for(int i=0;i<buckets;i++){
          totalhist[i]=0;
          for(int j=0;j<num_threads;j++)
            totalhist[i]+=hists[j][i];
      }
      stopped=0;
      psum=malloc(sizeof(int)*buckets);
      psum[0]=0;
      sums[0]=0;
      for(int i=0;i<buckets;i++){
          psum[i]=psum[i-1]+totalhist[i-1];
          sums[i]=psum[i];
      }
      int start;
      for(int i=0;i<buckets;i++){
          psums[0][i]=psum[i];
          for(int j=1;j<num_threads;j++){
            psums[j][i]=hists[j-1][i]+psums[j-1][i];
          }
      }
      //
      //2nd parallel part
      end_pos=reCURR->num_tuples/num_threads+reCURR->num_tuples%num_threads-1;
      start_pos=0;
      for(int i=0;i<num_threads;i++){
           if(err=pthread_mutex_lock(&list)){
             perror2("pthread_mutex_lock",err);
           }
           jobs_insert(&job,start_pos,end_pos,i,1,rel_id);
           pthread_cond_signal(&list_cond);	//signal to possibly waiting threads
           if(err=pthread_mutex_unlock(&list)){
             perror2("pthread_mutex_unlock",err);
           }
           start_pos=end_pos+1;
           end_pos+=sum;
       }
       //barrier
       if(err=pthread_mutex_lock(&barrier)){
         perror2("pthread_mutex_lock",err);
       }
       while(stopped<num_threads)
           pthread_cond_wait(&barrier_cond,&barrier);
       if(err=pthread_mutex_unlock(&barrier)){
         perror2("pthread_mutex_lock",err);
       }
       totalhist=malloc(sizeof(int)*buckets);
       for(int i=0;i<buckets;i++){
           totalhist[i]=0;
           for(int j=0;j<num_threads;j++)
             totalhist[i]+=hists[j][i];
       }
    free(psum);
    free(totalhist);
}

result* RadixHashJoin(relation *reIR,relation *reIS){
    times_in=0;
    //two arrays for updating intermediate
    old_rp=malloc(sizeof(int)*reIR->num_tuples);
    old_sp=malloc(sizeof(int)*reIS->num_tuples);
    //no of buckets
    buckets=pow(2,n);
    // R',S'
    reIR_new=malloc(sizeof(relation));
    reIS_new=malloc(sizeof(relation));
    reIR_new->tuples=malloc(sizeof(tuple)*reIR->num_tuples);
    reIR_new->num_tuples=reIR->num_tuples;
    reIS_new->tuples=malloc(sizeof(tuple)*reIS->num_tuples);
    reIS_new->num_tuples=reIS->num_tuples;
    //init of mutexes,condition variables
    num_threads=get_nprocs()*5;
    pthread_mutex_init(&list,NULL);
    pthread_mutex_init(&barrier,NULL);
    pthread_cond_init(&list_cond,NULL);
    pthread_cond_init(&barrier_cond,NULL);
    //result storing
    all=(result **)malloc(sizeof(result *)*buckets);

    //duplicates of psum
    pos_R=(int*)malloc(buckets*sizeof(int));
    pos_S=(int*)malloc(buckets*sizeof(int));
    hists=(int **)malloc(num_threads*sizeof(int *));
    psums=(int **)malloc(num_threads*sizeof(int *));
    //
    for(int i=0;i<num_threads;i++){
        hists[i]=(int *)malloc(buckets*sizeof(int));
        psums[i]=(int *)malloc(buckets*sizeof(int));
    }
    //threads
    pthread_t *tids;
    info in;
    in.re_R=reIR;
    in.re_S=reIS;
    int err;
    if((tids=malloc(num_threads*sizeof(pthread_t)))==NULL){
        perror("malloc");
    }
    for(int i=0;i<num_threads;i++){	//create threads
		    if(err=pthread_create(tids+i,NULL,Jobs,(void *)&in)){
			       perror2("pthread_join",err);
		    }
	   }
    //itemization
	  job=NULL;
    itemization(reIR,pos_R,0);
	  job=NULL;
    stopped=0;
    times_in=1;
    itemization(reIS,pos_S,1);
	  job=NULL;
    //parallel bucket
    stop=0;
     //3rd parallel part
     for(int i=0;i<buckets;i++){
          if(err=pthread_mutex_lock(&list)){
            perror2("pthread_mutex_lock",err);
          }
          jobs_insert(&job,i,i,i,2,0);
          pthread_cond_signal(&list_cond);	//signal to possibly waiting threads
          if(err=pthread_mutex_unlock(&list)){
            perror2("pthread_mutex_unlock",err);
          }
      }
      //signal for threads to exit
      if(err=pthread_mutex_lock(&list)){
          perror2("pthread_mutex_lock",err);
      }
      stop=1;
      pthread_cond_broadcast(&list_cond);
      if(err=pthread_mutex_unlock(&list)){
        perror2("pthread_mutex_lock",err);
      }
      //wait for threads to exit
      for(int i=0;i<num_threads;i++){
		      if(err=pthread_join(*(tids+i),NULL)){
			         perror2("pthread_join", err);
		      }
	    }
      //free and destroyes
      free(tids);
      if(err=pthread_cond_destroy(&barrier_cond)){
                  perror2("pthread_cond_destroy",err);
          }
      if(err=pthread_mutex_destroy(&list)){
                  perror2("pthread_mutex_destroy",err);
      }
      if(err=pthread_mutex_destroy(&barrier)){
                  perror2("pthread_mutex_destroy",err);
      }
      if(err=pthread_cond_destroy(&list_cond)){
                  perror2("pthread_cond_destroy",err);
      }
    //merge results
    //for each bucket
    result *start;
    result *last;
    start=(result*)malloc(1*sizeof(result));
    last=start;
    last->cur_size=0;
    last->next=NULL;
    last->id=(ids*)malloc(MAXSIZE*(sizeof(ids)));
    last->old_pos=(ids*)malloc(MAXSIZE*(sizeof(ids)));
    for(int i=0;i<buckets;i++){
        result *temp=all[i];
        while(temp!=NULL){
            for(int j=0;j<temp->cur_size;j++)
                write_result_3(&last,temp->id[j].rowid_R,temp->id[j].rowid_S,temp->old_pos[j].rowid_R,temp->old_pos[j].rowid_S);
            temp=temp->next;
        }
    }
    //free
    
    free(pos_R);
    free(pos_S);
    for(int i=0;i<num_threads;i++){
        free(hists[i]);
        free(psums[i]);
    }
    free(hists);
    free(psums);

    //
    return start;
}

//result 1024MB
void write_result(result **last,uint64_t R_key,uint64_t S_key){
    if((*last)->cur_size==MAXSIZE-1){   //if max allocate a new array
      (*last)->next=(result*)malloc(1*sizeof(result));
      (*last)=(*last)->next;
      (*last)->next=NULL;
      (*last)->cur_size=0;
      (*last)->id=(ids*)malloc(MAXSIZE*sizeof(ids));
    }
    (*last)->id[(*last)->cur_size].rowid_R=R_key;
    (*last)->id[(*last)->cur_size].rowid_S=S_key;
    (*last)->cur_size++;

}

//result 128KB
void write_result_2(result **last,uint64_t R_key,uint64_t S_key,uint64_t R_old,uint64_t S_old){
    if((*last)->cur_size==MAXSIZE_2-1){   //if max allocate a new array
      (*last)->next=(result*)malloc(1*sizeof(result));
      (*last)=(*last)->next;
      (*last)->next=NULL;
      (*last)->cur_size=0;
      (*last)->id=(ids*)malloc(MAXSIZE_2*sizeof(ids));
      (*last)->old_pos=(ids*)malloc(MAXSIZE_2*sizeof(ids));
    }
    (*last)->id[(*last)->cur_size].rowid_R=R_key;
    (*last)->id[(*last)->cur_size].rowid_S=S_key;
    (*last)->old_pos[(*last)->cur_size].rowid_R=R_old;
    (*last)->old_pos[(*last)->cur_size].rowid_S=S_old;
    (*last)->cur_size++;

}

//result 1024MB+used for intermediate update
void write_result_3(result **last,uint64_t R_key,uint64_t S_key,uint64_t R_old,uint64_t S_old){
    if((*last)->cur_size==MAXSIZE-1){   //if max allocate a new array
      (*last)->next=(result*)malloc(1*sizeof(result));
      (*last)=(*last)->next;
      (*last)->next=NULL;
      (*last)->cur_size=0;
      (*last)->id=(ids*)malloc(MAXSIZE*sizeof(ids));
      (*last)->old_pos=(ids*)malloc(MAXSIZE*sizeof(ids));
    }
    (*last)->id[(*last)->cur_size].rowid_R=R_key;
    (*last)->id[(*last)->cur_size].rowid_S=S_key;
    (*last)->old_pos[(*last)->cur_size].rowid_R=R_old;
    (*last)->old_pos[(*last)->cur_size].rowid_S=S_old;
    (*last)->cur_size++;

}


uint64_t hash_function_1(uint64_t payload){
    return (payload & 0xFF);
}

uint64_t hash_function_2(uint64_t payload,uint64_t next){
    return (payload % next);
}
