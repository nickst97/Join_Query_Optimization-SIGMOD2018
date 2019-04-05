#include "QueryManagement.h"


//query optimization
//returns join predicates in optimized order
pred *optimization(data *d,char *rlt_list[],pred *predj,int predj_size){
    lefttree lt;
    lt.cost=-1;
    int rel_num=MAX_RELATIONS;
    for(int i=0;i<MAX_RELATIONS;i++){
        lt.pos[i]=-1;
    }
    //A,B,C,D
    for(int i=0;i<MAX_RELATIONS;i++){
        //if we have less than 4 relations
        if(rlt_list[i]==NULL){
            rel_num=i;
            break;
        }
        int rel=atoi(rlt_list[i]);    //A
        int new_cost=d[rel].stats[0].size;
        //if empty()
        if(tree_isEmpty(lt)){
            tree_insert(&lt,new_cost,rel,0);
            continue;
        }
        //if new_cost<tree(cost)
        if(new_cost<lt.cost)
            tree_insert(&lt,new_cost,rel,0);

    }
    //AB,AC...
    lt.cost=-1;
    int dupl=0;
    for(int i=0;i<predj_size;i++){
        int rel_0=atoi(rlt_list[atoi(predj[i].rlt_id_0)]);  //A
        int rel_1=atoi(rlt_list[atoi(predj[i].rlt_id_1)]);  //B
        int col_0=atoi(predj[i].c_0);                       //.0
        int col_1=atoi(predj[i].c_1);                       //.1
        int conn=-1;
        if(tree_connected(lt,rel_0,1))
            conn=0;
        else if(tree_connected(lt,rel_1,1))
            conn=1;
        if(conn==-1)      //if(!connected())
            continue;
        if(rel_0==rel_1){   //A==B
            if(col_0==col_1){ // .0,0
                assessment_same_rel_col(d,rel_0,col_0);
            }
            else{             // .0,.1
                assessment_same_rel(d,rel_0,col_0,col_1);
            }
        }
        else{           // A!=B
            assessment_join(d,rel_0,rel_1,col_0,col_1);
        }
        // calculate new cost
        int new_cost=d[rel_0].stats[col_0].size+d[rel_1].stats[col_1].size;
        //insert
        if(tree_isEmpty(lt)){
            if(conn==0)
                tree_insert(&lt,new_cost,rel_1,1);
            else
                tree_insert(&lt,new_cost,rel_0,1);
            continue;
        }
        if(new_cost<lt.cost){
            if(conn==0)
                tree_insert(&lt,new_cost,rel_1,1);
            else
                tree_insert(&lt,new_cost,rel_0,1);
        }
    }
    //
    if(rlt_list[2]!=NULL){ //if we have more than 2 predicates
      lt.cost=-1;
      //same as above
      for(int i=0;i<predj_size;i++){
          int rel_0=atoi(rlt_list[atoi(predj[i].rlt_id_0)]);
          int rel_1=atoi(rlt_list[atoi(predj[i].rlt_id_1)]);
          int col_0=atoi(predj[i].c_0);
          int col_1=atoi(predj[i].c_1);
          int conn=-1;
          int both_conn=0;
          if(tree_connected(lt,rel_0,2)){
              both_conn++;
              conn=0;
          }
          if(tree_connected(lt,rel_1,2)){
              both_conn++;
              conn=1;
          }
          if((both_conn==2)&&(rel_0==rel_1))
              both_conn=0;
          //if !connected()
          if(conn==-1)
              continue;
          if(both_conn==2){
              dupl++;
              continue;
          }
          //same as above
          if(rel_0==rel_1){
              if(col_0==col_1){
                  assessment_same_rel_col(d,rel_0,col_0);
              }
              else{
                  assessment_same_rel(d,rel_0,col_0,col_1);
              }
          }
          else{
              assessment_join(d,rel_0,rel_1,col_0,col_1);
          }
          //calculate cost
          int new_cost;
          if(conn==0)
              new_cost=2*d[rel_0].stats[col_0].size+d[rel_1].stats[col_1].size;
          else
              new_cost=d[rel_0].stats[col_0].size+2*d[rel_1].stats[col_1].size;
          if(tree_isEmpty(lt)){
              if(conn==0)
                  tree_insert(&lt,new_cost,rel_1,2);
              else
                  tree_insert(&lt,new_cost,rel_0,2);
              continue;
          }
          if(new_cost<lt.cost){
              if(conn==0)
                  tree_insert(&lt,new_cost,rel_1,2);
              else
                  tree_insert(&lt,new_cost,rel_0,2);
          }
      }//for
    //
  }//if we have 4 predicates find the value of the 4th relation
    if(rlt_list[3]!=NULL){
        int found=0,j;
        for(int i=0;i<MAX_RELATIONS;i++){
            found=0;
            for(j=0;j<MAX_RELATIONS;j++){
                  if(atoi(rlt_list[i])==lt.pos[j]){
                      found=1;
                      break;
                  }
            }
            if(!found){
                lt.pos[3]=atoi(rlt_list[i]);
                break;
            }
        }
    }
    // order the predicates
    // ex. (((AxB)xC)xD)
    pred *new_pred;
    new_pred=malloc(sizeof(pred)*predj_size);
    int first_pos=0,double_pos=dupl,triple_pos=predj_size-1;
    for(int t=0;t<predj_size;t++){
      int rel_0=atoi(rlt_list[atoi(predj[t].rlt_id_0)]);
      int rel_1=atoi(rlt_list[atoi(predj[t].rlt_id_1)]);
      int write_pos;
      //(AxB)
      if(((rel_0==lt.pos[0])&&(rel_1==lt.pos[1]))||((rel_0==lt.pos[1])&&(rel_1==lt.pos[0]))){
        write_pos=first_pos;
        first_pos++;
      } //(AxC)
      else if(((rel_0==lt.pos[0])&&(rel_1==lt.pos[2]))||((rel_0==lt.pos[2])&&(rel_1==lt.pos[0]))){
        write_pos=double_pos;
        double_pos++;
      } //(BxC)
      else if(((rel_0==lt.pos[1])&&(rel_1==lt.pos[2]))||((rel_0==lt.pos[2])&&(rel_1==lt.pos[1]))){
        write_pos=double_pos;
        double_pos++;
      } // (AxD)||(BxD)||(CxD)
      else{
        write_pos=triple_pos;
        triple_pos--;
      }
      new_pred[write_pos].flag_0=predj[t].flag_0;
      new_pred[write_pos].flag_1=predj[t].flag_1;
      new_pred[write_pos].c_0=predj[t].c_0;
      new_pred[write_pos].c_1=predj[t].c_1;
      new_pred[write_pos].side_0=predj[t].side_0;
      new_pred[write_pos].side_1=predj[t].side_1;
      new_pred[write_pos].rlt_id_0=predj[t].rlt_id_0;
      new_pred[write_pos].rlt_id_1=predj[t].rlt_id_1;
      new_pred[write_pos].compare=predj[t].compare;
    }
    return new_pred;
}

//empty()
int tree_isEmpty(lefttree lt){
    if(lt.cost==-1)
        return 1;
    return 0;
}

//insert()
void tree_insert(lefttree *lt,int new_cost,int rel,int pos){
    lt->pos[pos]=rel;
    lt->cost=new_cost;
}

//connected()
int tree_connected(lefttree lt,int rel,int t){
    for(int i=0;i<t;i++){
        if(lt.pos[i]==rel)
            return 1;
    }
    return 0;
}
