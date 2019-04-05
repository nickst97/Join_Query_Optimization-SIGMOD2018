#include "QueryManagement.h"

//filter R.A=k
void assessment_equal(data *d,int rel,int col,int k){
    //R.A
    int exists=0;
    int old;
    for(int i=0;i<d[rel].num_tuples;i++){
        if(d[rel].positions[col][i]==k){
            exists=1;
            break;
        }
    }
    d[rel].stats[col].min=k;
    d[rel].stats[col].max=k;
    old=d[rel].stats[col].size;
    if((exists)&&(d[rel].stats[col].distinct)){
        d[rel].stats[col].size=d[rel].stats[col].size/d[rel].stats[col].distinct;
        d[rel].stats[col].distinct=1;
    }
    else{
        d[rel].stats[col].size=0;
        d[rel].stats[col].distinct=0;
    }
    //R.C,C!=A
    for(int i=0;i<d[rel].num_columns;i++){
        if(i==col)
            continue;
        if((d[rel].stats[i].distinct)&&(old)){
            int val_1=1-d[rel].stats[col].size/old;
            int val_2=d[rel].stats[i].size/d[rel].stats[i].distinct;
            int val_3=pow(val_1,val_2);
            d[rel].stats[i].distinct=d[rel].stats[i].distinct*(1-val_3);
        }
        else{
            d[rel].stats[i].distinct=0;
        }
        d[rel].stats[i].size=d[rel].stats[col].size;
    }
}

//filter R.A>k
void assessment_bigger(data *d,int rel,int col,int k){
    //R.A
    int exists=0;
    int old;
    if(k<d[rel].stats[col].min)
        k=d[rel].stats[col].min;
    d[rel].stats[col].min=k;
    old=d[rel].stats[col].size;
    if(d[rel].stats[col].min!=d[rel].stats[col].max){
        d[rel].stats[col].size=d[rel].stats[col].size*((d[rel].stats[col].max-k)/(d[rel].stats[col].max-d[rel].stats[col].min));
        d[rel].stats[col].distinct=d[rel].stats[col].distinct*((d[rel].stats[col].max-k)/(d[rel].stats[col].max-d[rel].stats[col].min));
    }
    else{
        d[rel].stats[col].size=0;
        d[rel].stats[col].distinct=0;
    }
    //R.C,C!=A
    for(int i=0;i<d[rel].num_columns;i++){
        if(i==col)
            continue;
        if((d[rel].stats[i].distinct)&&(old)){
            int val_1=1-d[rel].stats[col].size/old;
            int val_2=d[rel].stats[i].size/d[rel].stats[i].distinct;
            int val_3=pow(val_1,val_2);
            d[rel].stats[i].distinct=d[rel].stats[i].distinct*(1-val_3);
        }
        else{
            d[rel].stats[i].distinct=0;
        }
        d[rel].stats[i].size=d[rel].stats[col].size;
    }
}

//filter R.A<k
void assessment_smaller(data *d,int rel,int col,int k){
    //R.A
    int exists=0;
    int old;
    if(k>d[rel].stats[col].max)
        k=d[rel].stats[col].max;
    d[rel].stats[col].max=k;
    old=d[rel].stats[col].size;
    if(d[rel].stats[col].min!=d[rel].stats[col].max){
        d[rel].stats[col].size=d[rel].stats[col].size*((k-d[rel].stats[col].min)/(d[rel].stats[col].max-d[rel].stats[col].min));
        d[rel].stats[col].distinct=d[rel].stats[col].distinct*((k-d[rel].stats[col].min)/(d[rel].stats[col].max-d[rel].stats[col].min));
    }
    else{
        d[rel].stats[col].size=0;
        d[rel].stats[col].distinct=0;
    }
    //R.C,C!=A
    for(int i=0;i<d[rel].num_columns;i++){
        if(i==col)
            continue;
        if((d[rel].stats[i].distinct)&&(old)){
            int val_1=1-d[rel].stats[col].size/old;
            int val_2=d[rel].stats[i].size/d[rel].stats[i].distinct;
            int val_3=pow(val_1,val_2);
            d[rel].stats[i].distinct=d[rel].stats[i].distinct*(1-val_3);
        }
        else{
            d[rel].stats[i].distinct=0;
        }
        d[rel].stats[i].size=d[rel].stats[col].size;
    }
}

//join R.A=R.B
void assessment_same_rel(data *d,int rel,int col_1,int col_2){
    //R.A,R.B
    int exists=0;
    int old,old_2;
    if(d[rel].stats[col_1].min<d[rel].stats[col_2].min){
        d[rel].stats[col_1].min=d[rel].stats[col_2].min;
        d[rel].stats[col_2].min=d[rel].stats[col_2].min;
    }
    else{
      d[rel].stats[col_1].min=d[rel].stats[col_1].min;
      d[rel].stats[col_2].min=d[rel].stats[col_1].min;
    }
    if(d[rel].stats[col_1].max<d[rel].stats[col_2].max){
        d[rel].stats[col_1].max=d[rel].stats[col_1].max;
        d[rel].stats[col_2].max=d[rel].stats[col_1].max;
    }
    else{
      d[rel].stats[col_1].max=d[rel].stats[col_1].max;
      d[rel].stats[col_2].max=d[rel].stats[col_1].max;
    }
    old=d[rel].stats[col_1].size;
    old_2=d[rel].stats[col_1].distinct;
    uint64_t nn=d[rel].stats[col_2].max-d[rel].stats[col_2].min+1;
    d[rel].stats[col_1].size=d[rel].stats[col_1].size/nn;
    d[rel].stats[col_2].size=d[rel].stats[col_1].size;
    if((d[rel].stats[col_1].distinct)&&(old)&&(old_2)){
        int val_1=1-d[rel].stats[col_1].size/old;
        int val_2=old/old_2;
        int val_3=pow(val_1,val_2);
        d[rel].stats[col_1].distinct=d[rel].stats[col_1].distinct*(1-val_3);
    }
    else{
        d[rel].stats[col_1].distinct=0;
    }
    d[rel].stats[col_2].distinct=d[rel].stats[col_1].distinct;
    ////R.C,C!=A and C!=B
    for(int i=0;i<d[rel].num_columns;i++){
        if((i==col_1)||(i==col_2))
            continue;
        if((d[rel].stats[i].distinct)&&(old)){
            int val_1=1-d[rel].stats[col_1].size/old;
            int val_2=d[rel].stats[i].size/d[rel].stats[i].distinct;
            int val_3=pow(val_1,val_2);
            d[rel].stats[i].distinct=d[rel].stats[i].distinct*(1-val_3);
        }
        else{
            d[rel].stats[i].distinct=0;
        }
        d[rel].stats[i].size=d[rel].stats[col_1].size;
    }
}

//join R.A=R.A
void assessment_same_rel_col(data *d,int rel,int col){
    //R.A
    int exists=0;
    int old,old_2;
    int nn=d[rel].stats[col].max-d[rel].stats[col].min+1;
    d[rel].stats[col].size=d[rel].stats[col].size*d[rel].stats[col].size/nn;
    //R.C,C!=A
    for(int i=0;i<d[rel].num_columns;i++){
        if(i==col)
            continue;
        d[rel].stats[i].size=d[rel].stats[col].size;
    }
}

//join R.A=S.B
void assessment_join(data *d,int rel_1,int rel_2,int col_1,int col_2){
    //R.A,S.B
    int val_1,val_2;
    if(d[rel_1].stats[col_1].min>d[rel_2].stats[col_2].min){
          if(d[rel_1].stats[col_1].min>d[rel_2].stats[col_2].max)
              val_1=d[rel_2].stats[col_2].min;
          else
              val_1=d[rel_1].stats[col_1].min;
    }
    else{
      if(d[rel_2].stats[col_2].min>d[rel_1].stats[col_1].max)
          val_1=d[rel_1].stats[col_1].min;
      else
          val_1=d[rel_2].stats[col_2].min;
    }
    if(d[rel_1].stats[col_1].max<d[rel_2].stats[col_2].max){
          if(d[rel_1].stats[col_1].max<d[rel_2].stats[col_2].min)
              val_2=d[rel_2].stats[col_2].max;
          else
              val_2=d[rel_1].stats[col_1].max;
    }
    else{
      if(d[rel_2].stats[col_2].max<d[rel_1].stats[col_1].min)
          val_2=d[rel_1].stats[col_1].max;
      else
          val_2=d[rel_2].stats[col_2].max;
    }
    assessment_bigger(d,rel_1,col_1,val_1);
    assessment_bigger(d,rel_2,col_2,val_1);
    assessment_smaller(d,rel_1,col_1,val_2);
    assessment_smaller(d,rel_2,col_2,val_2);
    uint64_t nn=d[rel_1].stats[col_1].max-d[rel_1].stats[col_1].min+1;
    int old=d[rel_1].stats[col_1].distinct;
    int old_2=d[rel_2].stats[col_2].distinct;
    d[rel_1].stats[col_1].size=d[rel_1].stats[col_1].size*d[rel_2].stats[col_2].size/nn;
    d[rel_2].stats[col_2].size=d[rel_1].stats[col_1].size;
    d[rel_1].stats[col_1].distinct=d[rel_1].stats[col_1].distinct*d[rel_2].stats[col_2].distinct/nn;
    d[rel_2].stats[col_2].distinct=d[rel_1].stats[col_1].distinct;
    //R.C,C!=A
    for(int i=0;i<d[rel_1].num_columns;i++){
        if(i==col_1)
            continue;
        if((d[rel_1].stats[i].distinct)&&(old)){
            int val_1=1-d[rel_1].stats[col_1].distinct/old;
            int val_2=d[rel_1].stats[i].size/d[rel_1].stats[i].distinct;
            int val_3=pow(val_1,val_2);
            d[rel_1].stats[i].distinct=d[rel_1].stats[i].distinct*(1-val_3);
        }
        else{
            d[rel_1].stats[i].distinct=0;
        }
        d[rel_1].stats[i].size=d[rel_1].stats[col_1].size;
    }
    //S.C,C!=B
    for(int i=0;i<d[rel_2].num_columns;i++){
        if(i==col_2)
            continue;
        if((d[rel_2].stats[i].distinct)&&(old_2)){
            int val_1=1-d[rel_2].stats[col_2].distinct/old_2;
            int val_2=d[rel_2].stats[i].size/d[rel_2].stats[i].distinct;
            int val_3=pow(val_1,val_2);
            d[rel_2].stats[i].distinct=d[rel_2].stats[i].distinct*(1-val_3);
        }
        else{
            d[rel_2].stats[i].distinct=0;
        }
        d[rel_2].stats[i].size=d[rel_2].stats[col_2].size;
    }

}
