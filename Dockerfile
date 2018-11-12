FROM python:3.5 
WORKDIR /k2r
COPY k2r.py ./
COPY example.avsc ./
RUN pip3 install six 
RUN pip3 install redis
RUN pip3 install confluent-kafka
RUN pip3 install avro-python3
RUN pip3 install rejson
CMD python3 k2r.py
