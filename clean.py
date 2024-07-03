for i in range(1,6):
    try:
        with open(f"node{i}/dump.txt","w") as file:
            file.write("")
        with open(f"node{i}/log.txt","w") as file:
            file.write("")
        with open(f"node{i}/meta.txt","w") as file:
            file.write("")
    except:
        pass