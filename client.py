import paho.mqtt.client as mqtt
import time, sys, statistics


# todo https://wattlecourses.anu.edu.au/mod/forum/discuss.php?d=537042#p1592070

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


# Define event callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected OK return code " + str(rc))
    else:
        print("Failed to connect with return code " + str(rc))
    # client.subscribe("$SYS/#", 0);


def on_message(client, userdata, msg):
    # print("QoS " + str(msg.qos) + " " + "message on topic " + msg.topic + ": " + msg.payload.decode("utf-8"))

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
            print("received sys message at " + str(time.time() - userdata.startTime))
            if msg.topic == "$SYS/broker/clients/connected":
                userdata.activeClients.append(int(payload))
            elif msg.topic == "$SYS/broker/heap/current":
                userdata.currentHeap.append(int(payload))
            elif msg.topic == "$SYS/broker/load/messages/received/1min":
                userdata.messagesReceived.append(float(payload))
            elif msg.topic == "$SYS/broker/load/messages/sent/1min":
                userdata.messagesSent.append(float(payload))
            # client.unsubscribe(msg.topic)


def on_publish(client, userdata, mid):
    print("message id: " + str(mid))


def on_subscribe(client, userdata, mid, granted_qos):
    pass
    # print("Subscribed on " + str(mid) + " " + str(granted_qos))
    # if userdata.starttime[granted_qos[0]] == sys.maxsize:
    #    userdata.starttime[granted_qos[0]] = time.time()


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection " + str(rc))
    else:
        print("Disconnected OK " + str(rc))
        duration = userdata.endTime - userdata.startTime
        print("Time elapsed: {:4f}".format(duration))

        def print_stats(qos):
            print("")
            print("QoS " + str(qos))
            messages = userdata.messages[qos]
            message_set = set(messages)
            gaps = userdata.gaps[qos]

            def print_receive_rate(msg, t):
                print("Rate of messages received = {:4f} per second".format(len(msg) / t))

            def print_loss_rate(set):
                set_range = max(set) - min(set) + 1
                print("Rate of message loss = {:.2%}".format((set_range - len(set)) / set_range))

            def print_duplicate_rate(msg, set):
                print("Rate of duplicated messages = {:.2%}".format((len(msg) - len(set)) / len(msg)))

            def print_ooo_rate(msg):
                ooo = 0
                for i, value in enumerate(msg, start=1):
                    if msg[i - 1] > value:
                        ooo += 1
                print("Rate of out-of-order messages = {:.2%}".format(ooo / len(msg)))

            def print_gaps(gaps):
                print("Mean inter-message-gap and gap-variation = {:6f}, {:6f}".format(statistics.mean(gaps) * 1000,
                                                                                       statistics.stdev(gaps) * 1000))

            def print_everything(messages, duration, message_set, gaps):
                print_receive_rate(messages, duration)
                print_loss_rate(message_set)
                print_duplicate_rate(messages, message_set)
                print_ooo_rate(messages)
                print_gaps(gaps)

            def print_sys_stats(clients, heap, received, sent):
                print("Average active clients: {:2f}".format(statistics.mean(clients)))
                print("Average heap size: {:2f}".format(statistics.mean(heap)))
                print("Average messages received: {:2f}".format(statistics.mean(received)))
                print("Average messages sent: {:2f}".format(statistics.mean(sent)))

            print("Total: ")
            print_everything(messages, duration, message_set, gaps)
            print_sys_stats(userdata.activeClients, userdata.currentHeap, userdata.messagesReceived,
                            userdata.messagesSent)

            if qos != 0:
                # reference: https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length
                def split(a, n):
                    k, m = divmod(len(a), n)
                    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

                messages_split = list(split(messages, 5))
                sets_split = list(map(set, messages_split))
                gaps_split = list(split(gaps, 5))
                clients_split = list(split(userdata.activeClients, 5))
                heap_split = list(split(userdata.currentHeap, 5))
                received_split = list(split(userdata.messagesReceived, 5))
                sent_split = list(split(userdata.messagesSent, 5))

                print(str(len(messages_split[0])) + " messages in each partition")
                print(str(len(userdata.activeClients))+ " clients values")
                for i in range(5):
                    print("Part " + str(i+1) + ": ")
                    print_everything(messages_split[i], duration / 5, sets_split[i], gaps_split[i])
                    print_sys_stats(clients_split[i], heap_split[i], received_split[i], sent_split[i])

        print_stats(0)
        print_stats(1)
        print_stats(2)


def on_unsubscribe(client, userdata, mid):
    # print("Unsubbed " + str(mid) + " at " + str(time.time() - userdata.startTime))
    pass


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

client.loop_start()

# Publish a message
# client.publish(topic, "my message")

# Continue the network loop, exit when an error occurs
while True:
    try:
        if time.time() - userdata.statTime > 60:  # record stats every minute
            userdata.statTime = time.time()
            client.subscribe("$SYS/broker/clients/connected", 0)
            client.subscribe("$SYS/broker/heap/current", 0)
            client.subscribe("$SYS/broker/load/messages/received/1min", 0)
            client.subscribe("$SYS/broker/load/messages/sent/1min", 0)
            # print("Subbed at " + str(time.time() - userdata.startTime))

        if time.time() - userdata.startTime > 90:  # stop after 5 mins
            userdata.endTime = time.time()
            client.loop_stop()
            client.disconnect()
            exit(0)

    except (KeyboardInterrupt, SystemExit):
        userdata.endTime = time.time()
        client.loop_stop()
        client.disconnect()
        exit(0)
