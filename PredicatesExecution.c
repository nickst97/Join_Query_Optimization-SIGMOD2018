#include "QueryManagement.h"

void predicates_execution(char *query,data *d, int data_size){
    /*
    In this function, the arrays that have two cells,
    cell "0" stands for left side of a predicate
    and cell "1" for the right side

    IM stands for intermediate
    */
    char *q_part_0,*q_part_1,*q_part_2;
    char *rlt_list[MAX_RELATIONS];  //variables for the parts of the predicates
    char *curr_prd, *tmp, *tmp2;    //variables for saving temporarily other values
    int tpl, im_pos_a, im_pos_b, compare, i, both_im;
    int flag[2], rlt_number_a, rlt_number_b;
    uint64_t sum;
    result *res;
    im_node im[MAX_RELATIONS];
    result *curr_im;

    //initialising the im array
    for(i=0; i<MAX_RELATIONS; i++){
        im[i].position = -1;
        im[i].im_id=i-5;
    }
    //reading the three parts of the query
    tmp = query;
    q_part_0  = strtok_r(tmp, "|", &tmp);  //getting the relations
    q_part_1  = strtok_r(tmp, "|", &tmp);  //getting the predicates
    q_part_2  = strtok_r(tmp, "|", &tmp);  //getting the projects

    //getting each relation from the first part
    tmp = q_part_0;
    for(i=0; i<MAX_RELATIONS; i++){
        rlt_list[i] = strtok_r(tmp, " ", &tmp);
        //check if atoi(rlt_list[i]<data_size)?
        if((rlt_list[i])==NULL) //rlt_list has the relations that this query uses
            break;
    }
    if(i==2)
        rlt_list[3]=NULL;
    //predicate management
    char *dupl=strdup(q_part_1);
    tmp = dupl;
    int pred_count=0;
    //number of joins && filters
    while((curr_prd = strtok_r(tmp, "&", &tmp))!= NULL){
        pred_count++;
    }
    pred preds[pred_count];
    dupl=strdup(q_part_1);
    tmp = dupl;
    int t=0;
    //management
    while((curr_prd = strtok_r(tmp, "&", &tmp))!= NULL){
      if(strchr(curr_prd, '='))
          preds[t].compare = 0;
      else if(strchr(curr_prd, '>'))
          preds[t].compare = 1;
      else if(strchr(curr_prd, '<'))
          preds[t].compare = 2;
      //getting left and right is side of current predicate
      tmp2 = curr_prd;
      //left side
      preds[t].side_0 = strtok_r(tmp2, "=><", &tmp2);
      if(strchr(preds[t].side_0, '.')){
          preds[t].rlt_id_0 = strtok(preds[t].side_0, ".");
          preds[t].c_0 = strtok(NULL, ".");
          preds[t].flag_0 = 1;
      }
      else{
          preds[t].flag_0 = 0;
      }

      //right side
      preds[t].side_1 = strtok_r(tmp2, "=><", &tmp2);
      if(strchr(preds[t].side_1, '.')){
          preds[t].rlt_id_1 = strtok(preds[t].side_1, ".");
          preds[t].c_1= strtok(NULL, ".");
          preds[t].flag_1 = 1;
        }
        else{
          preds[t].flag_1 = 0;
        }
        t++;
    }
    //number of filters
    int filter_count=0;
    for(t=0;t<pred_count;t++){
        if(preds[t].flag_1+preds[t].flag_0==1)
            filter_count++;
    }
    //number of joins
    int join_count=pred_count-filter_count;
    pred predf[filter_count],predj[join_count];
    int fc=0,jc=0;
    //store filters -> predf
    //store joins -> predj
    for(t=0;t<pred_count;t++){
        if(preds[t].flag_1+preds[t].flag_0==1){
          predf[fc].flag_0=preds[t].flag_0;
          predf[fc].flag_1=preds[t].flag_1;
          predf[fc].c_0=preds[t].c_0;
          predf[fc].c_1=preds[t].c_1;
          predf[fc].side_0=preds[t].side_0;
          predf[fc].side_1=preds[t].side_1;
          predf[fc].rlt_id_0=preds[t].rlt_id_0;
          predf[fc].rlt_id_1=preds[t].rlt_id_1;
          predf[fc].compare=preds[t].compare;
          fc++;
        }
        else{
          predj[jc].flag_0=preds[t].flag_0;
          predj[jc].flag_1=preds[t].flag_1;
          predj[jc].c_0=preds[t].c_0;
          predj[jc].c_1=preds[t].c_1;
          predj[jc].side_0=preds[t].side_0;
          predj[jc].side_1=preds[t].side_1;
          predj[jc].rlt_id_0=preds[t].rlt_id_0;
          predj[jc].rlt_id_1=preds[t].rlt_id_1;
          predj[jc].compare=preds[t].compare;
          jc++;
        }
    }
    flag[0]=0; //check if left part has a value
    flag[1]=0; //check if right part has a value
    int im_id=0;
    pred *ord_pred;
    // l==0 filters
    // l==1 joins
    for(int l=0;l<2;l++){
        if(l==0){
            pred_count=filter_count;
        }
        else{
            pred_count=join_count;
            ord_pred=optimization(d,rlt_list,predj,join_count); //optimized order
        }
        for(t=0;t<pred_count;t++){
            im_pos_a=-1;
            im_pos_b=-1;
            both_im=0;
            //
            char *c_0,*c_1;
            char *side_0,*side_1;
            char *rlt_id_0,*rlt_id_1;
            //get data
            if(l==0){ //filters
                flag[0]=predf[t].flag_0;
                flag[1]=predf[t].flag_1;
                c_0=predf[t].c_0;
                c_1=predf[t].c_1;
                side_0=predf[t].side_0;
                side_1=predf[t].side_1;
                rlt_id_0=predf[t].rlt_id_0;
                rlt_id_1=predf[t].rlt_id_1;
                compare=predf[t].compare;
            }
            else{ //joins
                flag[0]=ord_pred[t].flag_0;
                flag[1]=ord_pred[t].flag_1;
                c_0=ord_pred[t].c_0;
                c_1=ord_pred[t].c_1;
                side_0=ord_pred[t].side_0;
                side_1=ord_pred[t].side_1;
                rlt_id_0=ord_pred[t].rlt_id_0;
                rlt_id_1=ord_pred[t].rlt_id_1;
                compare=ord_pred[t].compare;
            }
            if(((flag[1]+flag[0])==2)&&(l==0))
                continue;
            if(((flag[1]+flag[0])==1)&&(l==1))
                continue;
            //for left side
            relation rlt_0,rlt_1;
            int recs_0=0,recs_1=0;
            if(flag[0]){
                im_pos_a = atoi(rlt_id_0)%4;
                rlt_number_a = atoi(rlt_list[atoi(rlt_id_0)]);
                if(im[im_pos_a].position==-1){
                    rlt_0.tuples = (tuple*)malloc(d[rlt_number_a].num_tuples*sizeof(tuple));
                    rlt_0.num_tuples = d[rlt_number_a].num_tuples;
                    for(tpl=0; tpl<d[rlt_number_a].num_tuples; tpl++){
                        rlt_0.tuples[tpl].key = tpl; //checked
                        rlt_0.tuples[tpl].payload = d[rlt_number_a].positions[atoi(c_0)][tpl];
                    }
                }
                else{
                    both_im++;  //in im
                    int recs=0;
                    curr_im = im[im_pos_a].r;
                    while(curr_im!=NULL){
                      recs+=curr_im->cur_size;
                      curr_im=curr_im->next;
                    }
                    recs_0=recs;
                    rlt_0.tuples = (tuple*)malloc(recs*sizeof(tuple));
                    rlt_0.num_tuples = 0;
                    curr_im = im[im_pos_a].r;
                    tpl = 0;
                    while(curr_im!=NULL){
                        for(i=0;i<curr_im->cur_size;i++){
                            int id;
                            if(im[im_pos_a].position==0)
                                id=curr_im->id[i].rowid_R;
                            else
                                id=curr_im->id[i].rowid_S;
                            rlt_0.tuples[tpl].key = id;
                            rlt_0.tuples[tpl].payload = d[rlt_number_a].positions[atoi(c_0)][id];
                            rlt_0.num_tuples++;
                            tpl++;
                        }
                        curr_im = curr_im->next;
                    }

                }
            }
            //for right side
            if(flag[1]){
                im_pos_b = atoi(rlt_id_1)%4;
                rlt_number_b = atoi(rlt_list[atoi(rlt_id_1)]);
                if(im[im_pos_b].position==-1){
                    rlt_1.tuples = (tuple*)malloc(d[rlt_number_b].num_tuples*sizeof(tuple));
                    rlt_1.num_tuples = d[rlt_number_b].num_tuples;
                    for(tpl=0; tpl<d[rlt_number_b].num_tuples; tpl++){
                        rlt_1.tuples[tpl].key = tpl;
                        rlt_1.tuples[tpl].payload = d[rlt_number_b].positions[atoi(c_1)][tpl];
                    }
                }
                else{
                    both_im++;  //in im
                    int recs=0;
                    curr_im = im[im_pos_b].r;
                    while(curr_im!=NULL){
                      recs+=curr_im->cur_size;
                      curr_im=curr_im->next;
                    }
                    recs_1=recs;
                    rlt_1.tuples = (tuple*)malloc(recs*sizeof(tuple));
                    rlt_1.num_tuples = 0;
                    curr_im = im[im_pos_b].r;
                    tpl = 0;
                    while(curr_im!=NULL){
                        for(i=0;i<curr_im->cur_size;i++){
                            int id;
                            if(im[im_pos_b].position==0)
                                id=curr_im->id[i].rowid_R;
                            else
                                id=curr_im->id[i].rowid_S;
                            rlt_1.tuples[tpl].key = id;
                            rlt_1.tuples[tpl].payload = d[rlt_number_b].positions[atoi(c_1)][id];
                            rlt_1.num_tuples++;
                            tpl++;
                        }
                        curr_im = curr_im -> next;
                    }
                }
            }
            int update;
            relation temp_rel[MAX_RELATIONS];
            int rel_count=0;
            //relations for intermediate update in cases of 1: both in intermediate 2: same relation join
            if(l){
            for(i=0;i<MAX_RELATIONS;i++){
                if((i==im_pos_a)||(im[i].im_id!=im[im_pos_a].im_id)){
                    temp_rel[i].num_tuples=-1;
                    continue;
                }
                if((both_im==2)&&(im[im_pos_a].im_id==im[im_pos_b].im_id)&&(i==im_pos_b)){
                    temp_rel[i].num_tuples=-1;
                    continue;
                }
                int recs=0;
                curr_im = im[i].r;
                while(curr_im!=NULL){
                  recs+=curr_im->cur_size;
                  curr_im=curr_im->next;
                }
                temp_rel[i].tuples = (tuple*)malloc(recs*sizeof(tuple));
                temp_rel[i].num_tuples = 0;
                curr_im = im[i].r;
                tpl = 0;
                while(curr_im!=NULL){
                    for(int j=0;j<curr_im->cur_size;j++){
                        int id;
                        if(im[i].position==0)
                            id=curr_im->id[j].rowid_R;
                        else
                            id=curr_im->id[j].rowid_S;
                        temp_rel[i].tuples[tpl].key = id;
                        temp_rel[i].tuples[tpl].payload = d[rlt_number_a].positions[atoi(c_1)][id];
                        temp_rel[i].num_tuples++;
                        tpl++;
                    }
                    curr_im = curr_im -> next;
                }
                rel_count++;
            }//for MAX_RELATIONS
          }//l
            //getting the results
            if(im_pos_a==im_pos_b){ // ex 0.0=0.0
                update=1;
                res=same_array(&rlt_0, &rlt_1,temp_rel,im,im_id);
                im[im_pos_a].im_id=im_id;
                im[im_pos_a].r=res;
                im[im_pos_a].position=0;
                im_id++;
                flag[1]=0;
            }
            else if((both_im==2)&&(im[im_pos_a].im_id==im[im_pos_b].im_id)){    // both in im
                update=2;
                res=bothIM(&rlt_0,&rlt_1,temp_rel,im,im_id);
                im[im_pos_a].im_id=im_id;
                im[im_pos_a].r=res;
                im[im_pos_a].position=0;
                im[im_pos_b].im_id=im_id;
                im[im_pos_b].r=res;
                im[im_pos_b].position=1;
                im_id++;
            }
            else if((flag[0])&&(flag[1])){    //join
                update=3;
                res = RadixHashJoin(&rlt_0, &rlt_1);
            }
            else{ //filter
                update=0;
                if(flag[0]){
                    if(compare==0)
                        assessment_equal(d,rlt_number_a,atoi(c_0),atoi(side_1));
                    else if(compare==1)
                        assessment_bigger(d,rlt_number_a,atoi(c_0),atoi(side_1));
                    else
                        assessment_smaller(d,rlt_number_a,atoi(c_0),atoi(side_1));
                    res = predicate_filter(&rlt_0, atoi(side_1), compare);
                    im[im_pos_a].r=res;
                    im[im_pos_a].position=0;
                    im[im_pos_a].im_id=im_id;
                }
                else if(flag[1]){
                  if(compare==0)
                      assessment_equal(d,rlt_number_b,atoi(c_1),atoi(side_0));
                  else if(compare==1)
                      assessment_bigger(d,rlt_number_b,atoi(c_1),atoi(side_0));
                  else
                      assessment_smaller(d,rlt_number_b,atoi(c_1),atoi(side_0));
                    res = predicate_filter(&rlt_1, atoi(side_0), compare);
                    im[im_pos_b].r=res;
                    im[im_pos_b].position=0;
                    im[im_pos_b].im_id=im_id;
                }
                im_id++;
            }
            //update im
            if(update==3){
                for(i=0;i<MAX_RELATIONS;i++){ //update the side of 0
                    if((i==im_pos_a)||(im[i].im_id!=im[im_pos_a].im_id))
                        continue;
                    int recs=0;
                    relation upd_r;
                    curr_im = im[i].r;
                    while(curr_im!=NULL){
                        recs+=curr_im->cur_size;
                        curr_im=curr_im->next;
                    }
                    upd_r.tuples = (tuple*)malloc(recs*sizeof(tuple));
                    upd_r.num_tuples = 0;
                    curr_im = im[i].r;
                    tpl = 0;
                    while(curr_im!=NULL){
                        for(int j=0;j<curr_im->cur_size;j++){
                              int id;
                              if(im[i].position==0)
                                  id=curr_im->id[j].rowid_R;
                              else
                                  id=curr_im->id[j].rowid_S;
                              upd_r.tuples[tpl].key = id;
                              upd_r.tuples[tpl].payload = d[rlt_number_a].positions[atoi(c_1)][id];
                              upd_r.num_tuples++;
                              tpl++;
                        }
                        curr_im = curr_im -> next;
                    }
                    im[i].r=im_update(upd_r,res,0);
                    im[i].position=0;
                    im[i].im_id=im_id;
                }
                im[im_pos_a].im_id=im_id;
                im[im_pos_a].r=res;
                im[im_pos_a].position=0;
                for(i=0;i<MAX_RELATIONS;i++){   //update the side of 1
                    if((i==im_pos_b)||(im[i].im_id!=im[im_pos_b].im_id))
                        continue;
                    int recs=0;
                    relation upd_r;
                    curr_im = im[i].r;
                    while(curr_im!=NULL){
                        recs+=curr_im->cur_size;
                        curr_im=curr_im->next;
                    }
                    upd_r.tuples = (tuple*)malloc(recs*sizeof(tuple));
                    upd_r.num_tuples = 0;
                    curr_im = im[i].r;
                    tpl = 0;
                    while(curr_im!=NULL){
                        for(int j=0;j<curr_im->cur_size;j++){
                              int id;
                              if(im[i].position==0)
                                  id=curr_im->id[j].rowid_R;
                              else
                                  id=curr_im->id[j].rowid_S;
                              upd_r.tuples[tpl].key = id;
                              upd_r.tuples[tpl].payload = d[rlt_number_a].positions[atoi(c_1)][id];
                              upd_r.num_tuples++;
                              tpl++;
                         }
                        curr_im = curr_im -> next;
                    }
                    im[i].r=im_update(upd_r,res,1);
                    im[i].position=0;
                    im[i].im_id=im_id;
                }
                im[im_pos_b].im_id=im_id;
                im[im_pos_b].r=res;
                im[im_pos_b].position=1;
                im_id++;

            }
        }   //while
    }   //l=2

    //projecting the requested sums

    //here "0" does not stand for left side, is generic
    char *rlt_id_0,*clmn_id_0;
    tmp = q_part_2;
    while((curr_prd = strtok_r(tmp, " ", &tmp))!= NULL){
        //getting the relation id and column id of the current project
        tmp2 = curr_prd;
        rlt_id_0 = strtok(tmp2, ".");
        clmn_id_0= strtok(NULL, ".");
        sum = 0;
        im_pos_a = atoi(rlt_id_0)%4;
        rlt_number_a = atoi(rlt_list[atoi(rlt_id_0)]);
        curr_im = im[im_pos_a].r;

            while(curr_im!= NULL){
                for(i=0;i<curr_im->cur_size;i++){
                    int id;
                    if(im[im_pos_a].position==0)
                        id=curr_im->id[i].rowid_R;
                    else
                        id=curr_im->id[i].rowid_S;
                    sum = sum + d[rlt_number_a].positions[atoi(clmn_id_0)][id];
                }
                curr_im = curr_im->next;
            }
            if(sum==0){
                printf("NULL ");
                continue;
            }
            printf("%ld ", sum);

    }
    printf("\n");
}

//compare: 0 for '=',1 for '>',2 for '<'
result *predicate_filter(relation *r,uint64_t value,int compare){
      //result storing
      result *start;
      result *last;
      start=(result*)malloc(1*sizeof(result));
      last=start;
      last->cur_size=0;
      last->next=NULL;
      last->id=(ids*)malloc(MAXSIZE*(sizeof(ids)));
      for(unsigned int i=0;i<r->num_tuples;i++){
          switch(compare){
              case 0:
                if(r->tuples[i].payload==value)
                  write_result(&last,r->tuples[i].key, r->tuples[i].key);
                break;
              case 1:
                if(r->tuples[i].payload>value)
                  write_result(&last,r->tuples[i].key, r->tuples[i].key);
                break;
              case 2:
                if(r->tuples[i].payload<value)
                  write_result(&last,r->tuples[i].key, r->tuples[i].key);
                break;
          }
      }
      return start;
}

//ex 0.0=0.1 and update the rest of the intermediate
result *same_array(relation *r,relation *s,relation *upd,im_node *im,int im_id){
    result *start;
    result *last;
    start=(result*)malloc(1*sizeof(result));
    last=start;
    last->cur_size=0;
    last->next=NULL;
    last->id=(ids*)malloc(MAXSIZE*(sizeof(ids)));
    result *last_upd[MAX_RELATIONS],*start_upd[MAX_RELATIONS];
    for(int i=0;i<MAX_RELATIONS;i++){
        start_upd[i]=(result *)malloc(1*sizeof(result));
        last_upd[i]=start_upd[i];
        last_upd[i]->next=NULL;
        last_upd[i]->cur_size=0;
        last_upd[i]->id=(ids*)malloc(MAXSIZE*(sizeof(ids)));
    }
    for(unsigned i=0;i<r->num_tuples;i++){
        if(r->tuples[i].payload==s->tuples[i].payload){
            write_result(&last,r->tuples[i].key,s->tuples[i].key);
            for(int j=0;j<MAX_RELATIONS;j++){
              if(upd[j].num_tuples==-1)
                  continue;
              write_result(&last_upd[j],upd[j].tuples[i].key,upd[j].tuples[i].key);
            }
        }
    }
    for(int i=0;i<MAX_RELATIONS;i++){
        if(upd[i].num_tuples==-1)
            continue;
        im[i].r=start_upd[i];
        im[i].position=0;
        im[i].im_id=im_id;
    }
    return start;
}

//both in intermediate and update the rest of intermediate
result *bothIM(relation *r,relation *s,relation *upd,im_node *im,int im_id){
    result *start;
    result *last;
    start=(result*)malloc(1*sizeof(result));
    last=start;
    last->cur_size=0;
    last->next=NULL;
    last->id=(ids*)malloc(MAXSIZE*(sizeof(ids)));
    result *last_upd[MAX_RELATIONS],*start_upd[MAX_RELATIONS];
    for(int i=0;i<MAX_RELATIONS;i++){
        start_upd[i]=(result *)malloc(1*sizeof(result));
        last_upd[i]=start_upd[i];
        last_upd[i]->next=NULL;
        last_upd[i]->cur_size=0;
        last_upd[i]->id=(ids*)malloc(MAXSIZE*(sizeof(ids)));
    }
    for(unsigned int i=0;i<r->num_tuples;i++){
        if(r->tuples[i].payload==s->tuples[i].payload){ //check for '='
            write_result(&last,r->tuples[i].key,s->tuples[i].key);
            for(int j=0;j<MAX_RELATIONS;j++){
              if(upd[j].num_tuples==-1)
                  continue;
              write_result(&last_upd[j],upd[j].tuples[i].key,upd[j].tuples[i].key);
            }
        }
    }
    for(int i=0;i<MAX_RELATIONS;i++){
        if(upd[i].num_tuples==-1)
            continue;
        im[i].r=start_upd[i];
        im[i].position=0;
        im[i].im_id=im_id;
    }
    return start;
}

//update intermediate in case of RadixHashJoin
result *im_update(relation upd_r,result *res,int pos){
    result *start;
    result *last;
    start=(result*)malloc(1*sizeof(result));
    last=start;
    last->cur_size=0;
    last->next=NULL;
    last->id=(ids*)malloc(MAXSIZE*(sizeof(ids)));
    result *temp=res;
    //for every result of join
    //write the row_id in the same position of the other relation
    //by using old_pos which is stored in the result for every row_id
    while(temp!=NULL){
        for(int i=0;i<temp->cur_size;i++){
            if(pos==0){
                write_result(&last,upd_r.tuples[temp->old_pos[i].rowid_R].key,upd_r.tuples[temp->old_pos[i].rowid_R].key);
            }
            else{
                write_result(&last,upd_r.tuples[temp->old_pos[i].rowid_S].key,upd_r.tuples[temp->old_pos[i].rowid_S].key);
            }
        }
        temp=temp->next;
    }
    return start;
}
