#include "QueryManagement.h"

int main(){
    //reading file names
    input_data *filepath,*start,*temp;
    int num_files=0;
    size_t size=0,chars;
    filepath=(input_data*)malloc(1*sizeof(input_data));
    filepath->input=NULL;
    filepath->next=NULL;
    start=filepath;
    while(1){
        chars=getline(&filepath->input,&size,stdin);
        filepath->input[chars-1]='\0';
        if(!strcmp(filepath->input,"Done"))
          break;
        num_files++;
        filepath->next=(input_data*)malloc(1*sizeof(input_data));
        filepath=filepath->next;
        filepath->input=0;
        filepath->next=NULL;
    }
    //
    temp=start;
    data *d;
    statistics **copy;
    d=(data*)malloc(num_files*(sizeof(data)));
    copy=(statistics**)malloc(num_files*(sizeof(statistics *)));
    //reading data
    data_storing(d,temp,num_files,copy);
    //statistics calculation
    data_stats(d,num_files,copy);
    //reading queries
    query_reading(d,num_files,copy);

    //free filepaths
    while(start!=NULL){
        temp=start;
        start=start->next;
        free(temp->input);
        free(temp);
    }
    //free data-statistics
    for(int i=0;i<num_files;i++){
        free(d[i].positions);
        free(d[i].stats);
        free(copy[i]);
    }
    free(copy);
    free(d);
    //
    return 1;
}
