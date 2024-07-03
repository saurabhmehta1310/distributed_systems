import grpc
import bruh_pb2
import bruh_pb2_grpc
from concurrent import futures
import time
import threading
import random

nodes={}
nodes[1]=bruh_pb2_grpc.NodeStub(grpc.insecure_channel('localhost:50048'))
nodes[2]=bruh_pb2_grpc.NodeStub(grpc.insecure_channel('localhost:50046'))
nodes[3]=bruh_pb2_grpc.NodeStub(grpc.insecure_channel('localhost:50047'))
nodes[4]=bruh_pb2_grpc.NodeStub(grpc.insecure_channel('localhost:50049'))
nodes[5]=bruh_pb2_grpc.NodeStub(grpc.insecure_channel('localhost:50050'))

leaderId=3
def findlead():
    global leaderId
    flag=False
    try:
        while not flag:
            # print("going")
            request=bruh_pb2.client_findLead()
            response=nodes[leaderId].findLead(request)
            if response.success:
                print("FOUND LEADER")
                leaderId=response.node
                break
            else:
                print("NOT THE LEADER")
                leaderId=response.node

    except Exception as e:
        print(f"client error: LEADER IS DEAD",leaderId)
        leaderId=random.randint(1,5)
        findlead()

    return leaderId
    
    # return leaderId




def sett(key,val):
    print("Sending SET request")
    request=bruh_pb2.client_set()
    request.key=key
    request.val=val

    try:
        response=nodes[leaderId].sett(request)

        if response.success:
            print("SET SUCCESSFUL")
        else:
            # print("FAILURE")
            pass
    
    except Exception as e:
        print(f"client error: {e}")


def get(key):
    request=bruh_pb2.client_get()
    request.key=key
    
    response=nodes[leaderId].get(request)
    if response.success:
        print(f"value of {key}: {response.value}")
    else:
        print("FAILURE")



    


def menu():
    while True:
        x=int(input("SELECT QUERY:\n1)SET\n2)GET\n"))
        if x==1:
            key=input("Enter key:\n")
            val=input("Enter value:\n")
            findlead()
            sett(key,val)

        elif x==2:
            key=input("Enter key:\n")
            findlead()
            get(key)


if __name__=="__main__":
    menu()
    

    



