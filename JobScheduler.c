#include "RadixHashJoin.h"

//insert job
void jobs_insert(jobs **j,int start,int end,int id,int fun,int rel_id){
    jobs *temp;
    temp=malloc(sizeof(jobs)*1);
    temp->start=start;
    temp->end=end;
    temp->id=id;
    temp->fun=fun;
    temp->rel_id=rel_id;
    temp->next=NULL;
    if((*j)==NULL){
        (*j)=temp;
    }
    else{
        jobs *k=*j;
        while(k->next!=NULL)
            k=k->next;
        k->next=temp;
    }
}

//pop job
jobs *jobs_pop(jobs **j){
    if((*j)==NULL)
        return NULL;
    jobs *temp=*j;
    (*j)=(*j)->next;
    return temp;
}

//check empty
int jobs_isEmpty(jobs **j){
    return ((*j)==NULL);
}
