#!/usr/bin/env python
import binascii
import zmq
import struct
import csv
import time
import initialstate
from multiprocessing import Process, Queue, Lock
from collections import deque

port = 28332


def write_csv(tstamp, type, value, sequence):
    with open("../../../messages.csv", 'ab') as mlog_file:
        msg_logger = csv.writer(mlog_file, delimiter=',')
        msg_logger.writerow([tstamp, type, value, sequence])

    return True


def thread_manager():
    lock_ten = Lock()
    lock_sixty = Lock()

    print("Creating Queue")
    short_queue = Queue()
    long_queue = Queue()

    print("Starting ZMQ Consumer...")
    worker_1 = Process(target=zmq_tx_consumer, args=(short_queue, lock_ten))
    worker_1.start()

    print("Starting Queue Consumer/ InitialState Submitter")
    worker_2 = Process(target=ten_seconds, args=(short_queue, long_queue, lock_ten, lock_sixty))
    worker_2.start()

    print("Starting 60 Second SMA Consumer/ InititalState Submitter")
    worker_3 = Process(target=sixty_seconds, args=(long_queue, lock_sixty))
    worker_3.start()


def sixty_seconds(sma_queue, lock):

    while True:
        lock.acquire()
        try:
            count = sma_queue.qsize()

            if initialstate.send_log({"10_seconds_sma": (count/6)}):
                print("Submitted the tx count for the last 10 seconds")

            # Clear Queue
            while not sma_queue.empty():
                sma_queue.get()
            print("Cleared Queue")

        finally:
            lock.release()

        time.sleep(60)


def ten_seconds(short_queue, lock_ten):

    while True:
        lock_ten.acquire()
        try:
            count = short_queue.qsize()

            if initialstate.send_log({"last_10_secs": count}):
                print("Submitted the tx count for the last 10 seconds")

            # Clear Queue
            while not short_queue.empty():
                short_queue.get()
            print("Cleared Queue")

        finally:
            lock.release()

        time.sleep(10)


def zmq_tx_consumer(short_queue, long_queue, lock_ten, lock_sixty):
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

            if topic == "hashtx":
                print('- HASH TX ('+sequence+') -')
                tx_hash = binascii.hexlify(body).decode("utf-8")
                print(binascii.hexlify(body).decode("utf-8"))

                lock_ten.acquire()

                try:
                    short_queue.put(tx_hash)
                finally:
                    lock_ten.release()


                lock_sixty.acquire()

                try:
                    long_queue.put(tx_hash)
                finally:
                    lock_sixty.release()


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

            elif topic == "hashblock":
                print('- HASH BLOCK ('+sequence+') -')
                hashblock = binascii.hexlify(body).decode("utf-8")
                write_csv(time.time(), 'hashblock', hashblock, sequence)
                print(hashblock)
                # initialstate.send_log({"block_count": "{}".format(sequence)})

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
    thread_manager()
