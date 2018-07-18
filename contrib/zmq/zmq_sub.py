#!/usr/bin/env python
import binascii
import zmq
import struct
import csv
import time
import initialstate
import sys
from multiprocessing import Process, Lock

from collections import deque

port = 28332

mutex = Lock()


def write_csv(tstamp, type, value, sequence):
    with open("../../../messages.csv", 'ab') as mlog_file:
        msg_logger = csv.writer(mlog_file, delimiter=',')
        msg_logger.writerow([tstamp, type, value, sequence])

    return True


def listener():
    print("Creating Queue")
    q = deque()

    print("Starting ZMQ Consumer...")
    worker_1 = Process(target=zmq_tx_consumer, args=(q,))
    worker_1.start()

    print("Starting Queue Consumer/ InitialState Submitter")
    worker_2 = Process(target=submit_is, args=(q,))
    worker_2.start()


def submit_is(msg_queue):

    while True:
        print("Printing queue output")

        count = len(list(msg_queue))
        print("Queue Size: {}".format(count))

        if initialstate.send_log({"last_10_secs": count}):
            print("Submitted the tx count for the last 10 seconds")

        # Clear Queue
        msg_queue.clear()
        print("Cleared Queue")

        time.sleep(10)


def zmq_tx_consumer(msg_queue):
    zmqContext = zmq.Context()
    zmqSubSocket = zmqContext.socket(zmq.SUB)
    # zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"hashblock")
    zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"hashtx")
    # zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"rawgovernanceobject")
    # zmqSubSocket.setsockopt(zmq.SUBSCRIBE, b"rawgovernancevote")
    zmqSubSocket.connect("tcp://127.0.0.1:%i" % port)

    try:
        while True:
            msg = zmqSubSocket.recv_multipart()
            topic = str(msg[0].decode("utf-8"))
            body = msg[1]
            sequence = "Unknown"

            if len(msg[-1]) == 4:
                msgSequence = struct.unpack('<I', msg[-1])[-1]
                sequence = str(msgSequence)
            if topic == "hashblock":
                print('- HASH BLOCK ('+sequence+') -')
                hashblock = binascii.hexlify(body).decode("utf-8")
                write_csv(time.time(), 'hashblock', hashblock, sequence)
                print(hashblock)
                # initialstate.send_log({"block_count": "{}".format(sequence)})
            elif topic == "rawgovernanceobject":
                print('- RAW GOVERNANCE OBJECT ('+sequence+') -')
                governance_object = binascii.hexlify(body).decode("utf-8")
                write_csv(time.time(), 'rawgovernanceobject', governance_object, sequence)
                print(governance_object)
                # initialstate.send_log({"hash": governance_object, "tx_count": sequence})
            elif topic == "rawgovernancevote":
                print('- RAW GOVERNANCE VOTE ('+sequence+') -')
                governance_vote = binascii.hexlify(body).decode("utf-8")
                write_csv(time.time(), 'rawgovernancevote', governance_vote, sequence)
                print(governance_vote)
                # initialstate.send_log({"hash": governance_vote, "tx_count": sequence})

            elif topic == "hashtx":
                print('- HASH TX ('+sequence+') -')
                tx_hash = binascii.hexlify(body).decode("utf-8")
                # initialstate.send_log({"hash": tx_hash, "tx_count": sequence})
                print(binascii.hexlify(body).decode("utf-8"))

                msg_queue.append(tx_hash)

            elif topic == "hashtxlock":
                print('- HASH TX LOCK ('+sequence+') -')
                print(binascii.hexlify(body).decode("utf-8"))
            elif topic == "rawblock":
                print('- RAW BLOCK HEADER ('+sequence+') -')
                print(binascii.hexlify(body[:80]).decode("utf-8"))
            elif topic == "rawtx":
                print('- RAW TX ('+sequence+') -')
                print(binascii.hexlify(body).decode("utf-8"))
            elif topic == "rawtxlock":
                print('- RAW TX LOCK ('+sequence+') -')
                print(binascii.hexlify(body).decode("utf-8"))

    except Exception as e:
        print(e)
        zmqContext.destroy()
        msg_queue.join()


if __name__ == '__main__':
    listener()
