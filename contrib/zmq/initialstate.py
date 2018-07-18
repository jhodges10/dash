from ISStreamer.Streamer import Streamer

# Create Streamer Instance
streamer = Streamer(bucket_name="Transaction Count",
                    bucket_key="MX46QWNFVXXL",
                    access_key="CvQBfas9GOuwa5Vd3MicSAeNKYch7l1W")


def send_log(info_dict):

    # Send some data
    for key, value in info_dict.items():
        streamer.log(key, value)

    streamer.close()

    print("Finished Submitting to Ze Cloud")

    return True