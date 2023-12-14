import re
import pytest
import main

activity_sending_texts = [
    ( "READS.RANGE_READ", "Sending READS.RANGE_READ message to /172.25.225.5, size=3783 bytes" ), # From trace.txt
    ( "MUTATION_REQ", "Sending MUTATION_REQ message to /172.25.225.5:7000 message size 3783 bytes [Messaging-EventLoop-3-2]" ), # Cassandra 4.0.11/4.1.3/5.0-beta1 without hostname
    ( "MUTATION_REQ", "Sending MUTATION_REQ message to cassandra-2/172.25.225.5:7000 message size=3783 bytes") # Cassandra 4.0.11/4.1.3/5.0-beta1 with hostname
]

@pytest.mark.parametrize("type, text" , activity_sending_texts)
def test_sending_regexp(type, text):
    search_result = re.search( main.sending_regexp, text)
    assert search_result.group(1) == type
    assert search_result.group(2) == "172.25.225.5"
    assert search_result.group(3) == "3783"


activity_receiving_texts = [
    ( "READS.RANGE_READ", "READS.RANGE_READ message received from /172.25.146.4 [CoreThread-9]" ), # From trace.txt
    ( "MUTATION_REQ","MUTATION_REQ message received from /172.25.146.4:7000 [Messaging-EventLoop-3-2] " ), # Cassandra 4.0.11/4.1.3/5.0-beta1 without hostname
    ( "MUTATION_REQ","MUTATION_REQ message received from cassandra-2/172.25.146.4:7000 [Messaging-EventLoop-3-5]" ) # Cassandra 4.0.11/4.1.3/5.0-beta1 with hostname
]
@pytest.mark.parametrize("type, text" , activity_receiving_texts)
def test_receiving_regexp(type, text):
    search_result = re.search( main.receiving_regexp, text)
    assert search_result.group(1) == type
    assert search_result.group(2) == "172.25.146.4"

