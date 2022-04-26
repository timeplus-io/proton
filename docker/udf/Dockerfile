FROM timeplus/proton:prestage

RUN apt update -y &&\
    apt install software-properties-common -y &&\ 
    add-apt-repository ppa:deadsnakes/ppa &&\
    apt update -y &&\
    apt install python3.9 python3-pip -y

RUN pip3 install requests

CMD ["python3"]
