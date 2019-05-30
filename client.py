import paho.mqtt.client as mqtt
import time, sys, statistics

# Data type for our userdata
class Object(object):
    pass


userdata = Object()
userdata.messages = [[], [], []]
userdata.lastTime = [0, 0, 0]
userdata.gaps = [[], [], []]
userdata.activeClients = []
userdata.currentHeap = []
userdata.messagesReceived = []
userdata.messagesSent = []
userdata.recv = [0, 0, 0]
userdata.loss = [0, 0, 0]
userdata.dupe = [0, 0, 0]
userdata.ooo = [0, 0, 0]
userdata.gap = [0, 0, 0]
userdata.gvar = [0, 0, 0]


# Called when the client is connected to the server
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected OK " + str(rc))
    else:
        print("Failed to connect with return code " + str(rc))
    # client.subscribe("$SYS/#", 0);


# Called when a message is received
def on_message(client, userdata, msg):
    # print("QoS " + str(msg.qos) + " " + "message on topic " + msg.topic + ": " + msg.payload.decode("utf-8"))

    # update the inter-message-gap
    def update_gap(qos, num):
        userdata.messages[qos].append(num)
        if userdata.lastTime[qos] == 0:
            userdata.lastTime[qos] = time.time()
        elif num == userdata.messages[qos][-2] + 1:  # only store gaps for consecutive numbers
            userdata.gaps[qos].append(time.time() - userdata.lastTime[qos])
            userdata.lastTime[qos] = time.time()

    payload = msg.payload.decode("utf-8")
    if payload != "close":
        if msg.topic.startswith("counter/") & str.isdecimal(payload):
                num = int(payload)
                update_gap(msg.qos, num)
        elif msg.topic.startswith("$SYS/broker/"):
            if msg.topic == "$SYS/broker/clients/connected":
                userdata.activeClients.append(int(payload))
            elif msg.topic == "$SYS/broker/heap/current":
                userdata.currentHeap.append(int(payload))
            elif msg.topic == "$SYS/broker/load/messages/received/1min":
                userdata.messagesReceived.append(float(payload))
            elif msg.topic == "$SYS/broker/load/messages/sent/1min":
                userdata.messagesSent.append(float(payload))
            client.unsubscribe(msg.topic)


def on_publish(client, userdata, mid):
    pass


def on_subscribe(client, userdata, mid, granted_qos):
    pass


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection " + str(rc))
    else:
        print("Disconnected OK " + str(rc))


def on_unsubscribe(client, userdata, mid):
    pass

def ignore_messages(client, userdata, msg):
    pass

# Construct the client
client = mqtt.Client(client_id="3310-u6381103", clean_session=True, userdata=userdata, transport="tcp")

# Assign event callbacks
client.on_message = on_message
client.on_connect = on_connect
client.on_publish = on_publish
client.on_subscribe = on_subscribe
client.on_disconnect = on_disconnect
client.on_unsubscribe = on_unsubscribe
client.enable_logger()
# Connect
client.username_pw_set("students", "33106331")
client.connect("comp3310.ddns.net")

userdata.startTime = time.time()
userdata.statTime = userdata.startTime

# Subscribe to topics
client.subscribe("counter/fast/q0", 0)
client.subscribe("counter/fast/q1", 1)
client.subscribe("counter/fast/q2", 2)

client.subscribe("$SYS/broker/clients/connected", 0)
client.subscribe("$SYS/broker/heap/current", 0)
client.subscribe("$SYS/broker/load/messages/received/1min", 0)
client.subscribe("$SYS/broker/load/messages/sent/1min", 0)

# start the network loop
client.loop_start()

# Continue the network loop
while True:
    try:
        if time.time() - userdata.statTime > 60:  # subscribe to the stats every minute
            userdata.statTime = time.time()
            client.subscribe("$SYS/broker/clients/connected", 0)
            client.subscribe("$SYS/broker/heap/current", 0)
            client.subscribe("$SYS/broker/load/messages/received/1min", 0)
            client.subscribe("$SYS/broker/load/messages/sent/1min", 0)

        if time.time() - userdata.startTime > 300:  # stop after 5 mins
            client.unsubscribe("counter/fast/q0")
            client.unsubscribe("counter/fast/q1")
            client.unsubscribe("counter/fast/q2")
            client.on_message = ignore_messages   # ignore other incoming messages

            userdata.endTime = time.time()
            duration = userdata.endTime - userdata.startTime

            print("Time elapsed: {:4f}".format(duration))

            def print_stats(qos):
                print("")
                print("QoS " + str(qos))
                messages = userdata.messages[qos]
                message_set = set(messages)
                gaps = userdata.gaps[qos]

                def print_receive_rate(msg, t):
                    userdata.recv[qos] = len(msg)/t
                    print("Rate of messages received = {:.4f} per second".format(userdata.recv[qos]))

                def print_loss_rate(set):
                    set_range = max(set) - min(set) + 1
                    userdata.loss[qos] = (set_range - len(set)) / set_range
                    print("Rate of message loss = {:.2%}".format(userdata.loss[qos]))

                def print_duplicate_rate(msg, set):
                    userdata.dupe[qos] = (len(msg) - len(set)) / len(msg)
                    print("Rate of duplicated messages = {:.2%}".format(userdata.dupe[qos]))

                def print_ooo_rate(msg):
                    ooo = 0
                    for i, value in enumerate(msg, start=1):
                        if msg[i - 1] > value:
                            ooo += 1
                    userdata.ooo[qos] = ooo / len(msg)
                    print("Rate of out-of-order messages = {:.2%}".format(userdata.ooo[qos]))

                def print_gaps(gaps):
                    userdata.gap[qos] = statistics.mean(gaps) * 1000
                    userdata.gvar[qos] = statistics.pstdev(gaps) * 1000
                    print("Mean inter-message-gap and gap-variation (ms) = {:6f}, {:6f}".format(
                        userdata.gap[qos],
                        userdata.gvar[qos]))

                def print_everything(messages, duration, message_set, gaps):
                    print_receive_rate(messages, duration)
                    print_loss_rate(message_set)
                    print_duplicate_rate(messages, message_set)
                    print_ooo_rate(messages)
                    print_gaps(gaps)

                def print_sys_stats(clients, heap, received, sent):
                    print("Average active clients: {:.3f}".format(statistics.mean(clients)))
                    print("Average heap size: {:.3f}".format(statistics.mean(heap)))
                    print("Average messages received: {:.3f}".format(statistics.mean(received)))
                    print("Average messages sent: {:.3f}".format(statistics.mean(sent)))

                print("Total messages received: " + str(len(messages)))
                print_everything(messages, duration, message_set, gaps)
                print_sys_stats(userdata.activeClients, userdata.currentHeap, userdata.messagesReceived,
                                userdata.messagesSent)

                # Partitioning the data for further inspection

                # if qos != 0:
                #     partitions = 2
                #
                #     # Reference: https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length
                #     def split(a, n):
                #         k, m = divmod(len(a), n)
                #         return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))
                #
                #     messages_split = list(split(messages, partitions))
                #     sets_split = list(map(set, messages_split))
                #     gaps_split = list(split(gaps, partitions))
                #     clients_split = list(split(userdata.activeClients, partitions))
                #     heap_split = list(split(userdata.currentHeap, partitions))
                #     received_split = list(split(userdata.messagesReceived, partitions))
                #     sent_split = list(split(userdata.messagesSent, partitions))
                #
                #     print(str(len(messages_split[0])) + " messages in each partition")
                #     print(str(len(userdata.activeClients)) + " clients values")
                #     for i in range(partitions):
                #         print("Part " + str(i + 1) + ": ")
                #         print_everything(messages_split[i], duration / partitions, sets_split[i], gaps_split[i])
                #         print_sys_stats(clients_split[i], heap_split[i], received_split[i], sent_split[i])


            print_stats(0)
            print_stats(1)
            print_stats(2)

            client.publish("studentreport/u6381103/language", payload="Python, paho-mqtt", qos=2, retain=True)
            client.publish("studentreport/u6381103/network", payload="ANU Ethernet in labs", qos=2, retain=True)
            for i in range(3):
                client.publish("studentreport/u6381103/slow/" + str(i) + "/recv", payload=userdata.recv[i], qos=2,
                               retain=True)
                client.publish("studentreport/u6381103/slow/" + str(i) + "/loss", payload=userdata.loss[i], qos=2,
                               retain=True)
                client.publish("studentreport/u6381103/slow/" + str(i) + "/dupe", payload=userdata.dupe[i], qos=2,
                               retain=True)
                client.publish("studentreport/u6381103/slow/" + str(i) + "/ooo", payload=userdata.ooo[i], qos=2,
                               retain=True)
                client.publish("studentreport/u6381103/slow/" + str(i) + "/gap", payload=userdata.gap[i], qos=2,
                               retain=True)
                client.publish("studentreport/u6381103/slow/" + str(i) + "/gvar", payload=userdata.gvar[i], qos=2,
                               retain=True)


            client.loop_stop()
            client.disconnect()
            exit(0)

    except (KeyboardInterrupt, SystemExit):
        userdata.endTime = time.time()
        client.loop_stop()
        client.disconnect()
        exit(0)
