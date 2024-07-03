import grpc
import bruh_pb2
import bruh_pb2_grpc
from concurrent import futures
import time
import threading
import random
import os

#persistent variables 
node_num=3
term=0
voted_for=0
log=[]

#volatile for all servers
commitindex=0
lastApplied=0

#volatile for leaders
nextIndex=[0]*6
matchIndex=[0]*6

data={}
nodes={}
nodes[1]=bruh_pb2_grpc.NodeStub(grpc.insecure_channel('localhost:50048'))
nodes[2]=bruh_pb2_grpc.NodeStub(grpc.insecure_channel('localhost:50046'))
nodes[4]=bruh_pb2_grpc.NodeStub(grpc.insecure_channel('localhost:50049'))
nodes[5]=bruh_pb2_grpc.NodeStub(grpc.insecure_channel('localhost:50050'))

vote_casted=False
role="follower"
prevIndex=0
prevLogterm=0
recv_beat=False
leader=0

#write fucntions to files
def write_to_log(new_log):
    global node_num
    write_to_meta()
    with open(f"node{node_num}/log.txt","a") as file:
        file.write(new_log)

def write_to_dump(new_print):
    write_to_meta()
    with open(f"node{node_num}/dump.txt","a") as file:
        file.write(new_print)

def write_to_meta():
    global commitindex,lastApplied,voted_for,term
   
    with open(f"node{node_num}/meta.txt","w") as file:
        file.write(str(term)+"\n")
        file.write(str(voted_for)+"\n")
        file.write(str(commitindex)+"\n")
        file.write(str(lastApplied)+"\n")
        file.write(f"{log}"+"\n")

# fucntions for intiliasing variables
def make_log():
    global log
    with open(f"node{node_num}/log.txt","r") as file:
        if os.path.getsize(f"node{node_num}/log.txt") == 0:
            log=[]
        else:
            lines=file.readlines()
            for line in lines:
                line=line.split()
                if line[0]=="NO":
                    log.append([(line[0]),(line[1]),int(line[2]),int(line[3])])

                else:
                    log.append([(line[1]),(line[2]),int(line[3]),int(line[4])])
        
def make_meta():
    global term,voted_for,  commitindex,lastApplied,vote_casted
    vote_casted=False
    with open(f"node{node_num}/meta.txt","r") as file:
        if os.path.getsize(f"node{node_num}/meta.txt") == 0:
            pass
        else:
            lines=file.readlines()
            term=int(lines[0].split()[0])
            voted_for=int(lines[1].split()[0])
            commitindex=int(lines[2].split()[0])
            lastApplied=int(lines[3].split()[0])

    print(f"term: {term}\nvoted for: {voted_for}\ncommitindex: {commitindex}\nlastApplied: {lastApplied}")




def lease_timer():
   
    write_to_dump(f"Leader {node_num} failed to renew lease. Stepping Down\n")
    role="follower"
    global t3, vote_casted
    vote_casted=False
    t3.cancel()
    global t1
    t1=threading.Timer(random.uniform(6,10),reqVote)
    # t1.start()
    pass
    
    
acks=0

t4=threading.Timer(2.5,lease_timer)

def heartbeat():
    global hb,acks,commitindex,lastApplied,node_num,nextIndex,matchIndex
    global term
    write_to_dump(f"Leader {node_num} sending heartbeat\n")
    global t3
    global t4
    def heart(node_no):
        global term
        request=bruh_pb2.app_entry()
        request.term=term
        request.leaderId=node_num
        request.entries=""
        request.prevLogTerm=0
      
        n=nextIndex[node_no]
        m=matchIndex[node_no]
        l=lastApplied
        if len(log)>1:
            request.prevLogTerm=log[n-2][2]
        while(n<=l):
            print(n,"<",l)
            if n>len(log):
                break
            else:
                request.entries+=f"{log[n-1][0]},{log[n-1][1]},{log[n-2][2]},{log[n-3][3]} "
                n+=1
        print(node_no,request.entries)
        
        try: 
            global acks
            response=nodes[node_no].append_entry(request)
            with lock3:
                if response.term>term:
                    term=response.term
            if(response.success):
                nextIndex[node_no]=l+1
                matchIndex[node_no]=l
                with lock:
                    acks+=1

            elif not response.success:
                
                nextIndex[node_no]-=1
                print("DEDUCTED",node_no, nextIndex[node_no])
        except Exception as e:
            print(f"{node_no} not reachable: ")
            
    threads=[]
    #sending heartbeat to all servers at once using threads
    for node in nodes.keys():
        thread=threading.Thread(target=heart,args=(node,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    with lock2:
        if not hb:
            hb=True
            lock1.release()
    if acks>=2:# how to get entry for which 
        acks=0
        if commitindex != lastApplied:
            commitindex=lastApplied
            write_to_dump(f"Committed entry {lastApplied}\n")
            write_to_meta()
        
        t3.cancel()
        t3=threading.Timer(1,heartbeat)
        t3.start()
        t4.cancel()
        write_to_dump("Renewing lease\n")
        t4=threading.Timer(2.5,lease_timer)
        t4.start()

    else:
        acks=0
        t3.cancel()
        t3=threading.Timer(1,heartbeat)
        t3.start()

  
def leader_job():
    global term,acks,t4,lastApplied,leader,nextIndex,matchIndex
    leader=node_num
    write_to_dump(f"Node {node_num} waiting for leader's lease time to get over\n")
    lastApplied+=1
    write_to_meta()
    log.append(["NO","OP",term,lastApplied])
    write_to_log(f"NO OP {term} {lastApplied}\n")
    matchIndex=[0]*6
    nextIndex=[lastApplied]*6
    print("nextIndex",nextIndex)
    
    time.sleep(2.5)
     #function for sending heartbeat
    t4=threading.Timer(2.5,lease_timer)
    t4.start()
    heartbeat()



t3=threading.Timer(1,heartbeat)
votes=1 #self vote
lock=threading.Semaphore(value=1)
lock1=threading.Semaphore(value=1)
lock1.acquire()
lock2=threading.Semaphore(value=1)
lock3=threading.Semaphore(value=1)
hb=True
def reqVote():
    global term,vote_casted,role,voted_for,node_num,t1
    if vote_casted:
        t1=threading.Timer(random.uniform(6,10),reqVote)
        t1.start()
    else:    
        term+=1

        role="candidate"
        global votes
        votes=1
        voted_for=node_num
        vote_casted=True
        def req(node_no):

        
            request=bruh_pb2.req_vote()
            request.term=term
            request.candidateId=node_num
            if len(log)==0:
                request.lastLogInd=0
            
            else:
                request.lastLogInd=log[len(log)-1][3]

            if len(log)==0:
                request.lastLogTerm=0
            else:
                request.lastLogTerm=log[len(log)-1][2]
            try:
                print(f"sending request to {node_no} ")
                response=nodes[node_no].request_vote(request)
                print(f"received vote from {node_no} : {response.vote_grant}")
                if response.vote_grant:
                    global votes
                    with lock:
                
                        votes+=1
                        print("num votes: ",votes)

                    

                        # print(f"received vote from {node_no}")
            except Exception as e:
                print("NAHI CHALA: ",e)
                pass

        threads=[]
        for node in nodes.keys():
            thread=threading.Thread(target=req,args=(node,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
        
        print("tot votes received= ",votes)
        if votes>=3:
            # t1.cancel()
            print("I AM LEADERR")
            role="leader"
            # term+=1
            leader_job()
            #send rpc to all other nodes
        
        else:
            
            # global t1
            print("DID NOT RECEIVE ENOUGH VOTES")
            role="follower"
            t1=threading.Timer(random.uniform(6,10),reqVote)
            votes=0
            vote_casted=False
            t1.start()
            
    

        
       
t1=threading.Timer(random.uniform(6,10),reqVote)
class NodeServices(bruh_pb2_grpc.NodeServicer):
    def request_vote(self, request, context):
        global vote_casted,voted_for,recv_beat,t1
        response=bruh_pb2.req_vote_res()
        response.vote_grant=False
        print(f"vote requested from {request.candidateId}")
        if  (not vote_casted) and role=="follower" :
            print("SOME")
            if len(log)==0 and request.lastLogTerm>=0 and request.lastLogInd>=0:
                print("MORE")
                response.term=term
                response.vote_grant=True
                voted_for=request.candidateId
                vote_casted=True
                write_to_dump(f"Granting vote to {request.candidateId}\n")
                print(f"voting for {request.candidateId}")
                t1.cancel()
                t1=threading.Timer(random.uniform(6,11),reqVote)
                t1.start()

            elif request.lastLogTerm>=log[len(log)-1][2] and request.lastLogInd>=log[len(log)-1][3]:
                print("MOST")
                response.term=term
                response.vote_grant=True
                voted_for=request.candidateId
                vote_casted=True

                write_to_dump(f"Granting vote to {request.candidateId}\n")
                print(f"voting for {request.candidateId}")
                t1.cancel()
                t1=threading.Timer(random.uniform(6,11),reqVote)
                t1.start()

        else:
            write_to_dump(f"Rejecting to vote for {request.candidateId}\n")
        return response
    
    def append_entry(self, request, context):
        global t1,term,lastApplied,leader,recv_beat,vote_casted,commitindex
        vote_casted=False
        leader=request.leaderId
        t1.cancel()
        response=bruh_pb2.app_entry_res()
        response.term=term
        response.success=False
        recv_beat=False
        if request.term>term:
            term=request.term
            response.term=term
        
        # code to check log adn append new logs
        
        li=lastApplied
        if request.entries=="" or len(log)==0 or request.prevLogTerm==log[len(log)-1][2]:
            response.success=True
            recv_beat=True
            if request.entries!="":
                print(request.entries.split())
                for l in request.entries.split():
                    log_split=l.split(",")
                    # if int(log_split[3])>lastApplied:
                    key=log_split[0]
                    value=log_split[1]
                    term=int(log_split[2])
                    ind=int(log_split[3])
                    data[key]=value
                    f=True
                    print(l)
                    # for i in log:
                    #     if i[3]==ind:
                    #         f=False
                    #         break
                    if f:
                        log.append([key,value,term,ind])
                        lastApplied+=1
                        if(key=="NO"):
                            write_to_log(f"{key} {value} {term} {ind}\n")

                        else:
                            write_to_log(f"SET {key} {value} {term} {ind}\n")
                        
                        
                        
                commitindex=li   
                write_to_dump(f"Follower {node_num} commited entry {commitindex}\n")
                write_to_dump(f"Follower {node_num} accepted append entries from leader {leader}\n")
        else:
            recv_beat=False
            write_to_dump(f"Follower {node_num} rejected append entries from leader {leader}\n")
        
           
        write_to_meta()
        response.term=term
        t1=threading.Timer(random.uniform(6,10),reqVote)
        t1.start()
        return response
    


    def findLead(self, request, context):
        print("findleader req")
        response=bruh_pb2.res_findLead()
        response.success=False
        if role=="leader":
            response.success=True
            
        response.node=leader
        print(leader)
        return response
    
    def sett(self, request, context):
        global lastApplied,leader,hb,data,node_num
        write_to_dump(f"Leader {node_num} received SET request frok client\n")
        response=bruh_pb2.res_set()
        response.success=False
        if leader==node_num:
            data[request.key]=request.val
            lastApplied+=1
            log.append([request.key,request.val,term,lastApplied])
            write_to_log(f"SET {request.key} {request.val} {term} {lastApplied}\n")
            hb=False
            global acks
            lock1.acquire()
            if acks>=2:
                write_to_dump(f"Committed entry {lastApplied}\n")
                response.success=True
            else:
                write_to_dump(f"Failed to commit entry {lastApplied}\n")
        write_to_meta()  
        return response
    
    def get(self, request, context):
        write_to_dump(f"Leader {node_num} received GET request frok client\n")

        response=bruh_pb2.res_get()
        response.success=False
        if request.key in data and leader==node_num:
            response.success=True
            response.value=data[request.key]
        return response 
    

def serve():
    server=grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    bruh_pb2_grpc.add_NodeServicer_to_server(NodeServices(),server)
    print('Starting server. Listening on port 50047')
    server.add_insecure_port('[::]:50047')
    server.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__=="__main__":
    make_log()
    make_meta()
    t2= threading.Thread(target=serve)
    t2.start()
    t1.start()
    t2.join()
    t1.join()


        

        






    
    

        








        
    
        