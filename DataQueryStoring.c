#include "QueryManagement.h"

//opening files,reading and storing data
void data_storing(data *d,input_data *start,int num_files,statistics **copy){
    //for every file
    for(int i=0;i<num_files;i++){
        //open file
        int fd=open(start->input, O_RDONLY);
        if(fd==-1){
            perror("file opening");
            return;
        }
        //size
        struct stat sb;
        if(fstat(fd,&sb)==-1){
            perror("fstat");
            return;
        }
        //mmap()
        char *addr=(char *)(mmap(NULL,sb.st_size,PROT_READ,MAP_PRIVATE,fd,0));
        if(addr==MAP_FAILED){
            perror("mmap");
            return;
        }
        if(sb.st_size<16){
            printf("Not a valid header in %s\n",start->input);
            return;
        }
        //header
        //tuples number
        d[i].num_tuples=*(uint64_t*)(addr);
        addr+=sizeof(uint64_t);
        // columns number
        d[i].num_columns=*(uint64_t*)(addr);
        addr+=sizeof(uint64_t);
        //array of pointers to start of each column
        d[i].positions=(uint64_t **)malloc(sizeof(uint64_t*)*d[i].num_columns);
        copy[i]=malloc(sizeof(statistics)*d[i].num_columns);
        d[i].stats=malloc(sizeof(statistics)*d[i].num_columns);
        //array for storing the data
        uint64_t *storing=(uint64_t*)malloc(sizeof(uint64_t)*d[i].num_columns*d[i].num_tuples);
        //pointer 0 points to column 0,1 to column 1 etc
        //column 0 starts at 0,column 1 at 0+num_tuples,2 at 0+2*num_tuples
        for(int j=0;j<d[i].num_columns;j++){
            d[i].positions[j]=&storing[j*d[i].num_tuples];
        }
        //storing data
        for(int j=0;j<d[i].num_columns*d[i].num_tuples;j++){
                storing[j]=*(uint64_t*)(addr);
                addr+=sizeof(uint64_t);
        }
        //next file
        start=start->next;
    }
}

//statistics calculation
void data_stats(data *d,int num_files,statistics **copy){
    for(int i=0;i<num_files;i++){   //for every file
        for(int j=0;j<d[i].num_columns;j++){  //for every column
            //min,max,size calculation
            copy[i][j].size=d[i].num_tuples;
            copy[i][j].min=d[i].positions[j][0];
            copy[i][j].max=d[i].positions[j][0];
            for(int k=1;k<d[i].num_tuples;k++){
                if(d[i].positions[j][k]<copy[i][j].min)    //new min
                    copy[i][j].min=d[i].positions[j][k];
                if(d[i].positions[j][k]>copy[i][j].max)    //new max
                    copy[i][j].max=d[i].positions[j][k];
            }
            //boolean array
            bool *temp;
            int size=copy[i][j].max-copy[i][j].min+1;
            bool flag=false;
            if(size>MAXSTATS){
                flag=true;
                size=MAXSTATS;
            }
            temp=malloc(sizeof(bool)*(size));
            for(int k=0;k<size;k++)
                temp[k]=false;
            for(int k=0;k<d[i].num_tuples;k++){
                if(flag){
                    temp[(d[i].positions[j][k]-copy[i][j].min)%size]=true;
                    break;
                }
                temp[d[i].positions[j][k]-copy[i][j].min]=true;
            }
            copy[i][j].distinct=0;
            for(int k=0;k<size;k++){
                if(temp[k])
                    copy[i][j].distinct++;
            }
            free(temp);
        }
    }
}

//query reading
void query_reading(data *d,int num_files,statistics **copy){
    //while not EOF
    while(1){
        //read queries
        input_data *query,*start,*temp;
        size_t size=0;
        int chars;
        query=(input_data*)malloc(1*sizeof(input_data));
        query->input=NULL;
        query->next=NULL;
        start=query;
        //while not 'F' or EOF
        while(1){
            //read queries
            chars=getline(&query->input,&size,stdin);
            if(chars==-1)
                break;
            query->input[chars-1]='\0';
            if(!strcmp(query->input,"F"))
                    break;
            query->next=(input_data*)malloc(1*sizeof(input_data));
            query=query->next;
            query->input=0;
            query->next=NULL;
        }
        temp=start;
        //for every query of current set
        while(temp->next!=NULL){
                statistics_copy(d,num_files,copy);
                predicates_execution(temp->input,d,num_files);
                temp=temp->next;
        }
        //free
        while(start!=NULL){
            temp=start;
            start=start->next;
            free(temp->input);
            free(temp);
        }
        if(chars==-1)
            break;
        //next set of queries
    }
}

//copy statistics for every query
void statistics_copy(data *d,int num_files,statistics **copy){
  for(int i=0;i<num_files;i++){
      for(int j=0;j<d[i].num_columns;j++){
          d[i].stats[j].min=copy[i][j].min;
          d[i].stats[j].max=copy[i][j].max;
          d[i].stats[j].size=copy[i][j].size;
          d[i].stats[j].distinct=copy[i][j].distinct;
      }
  }


}
