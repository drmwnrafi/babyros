import argparse
import sys
import time
from babyrosbag.bag import Bag, Time
from loguru import logger

def discover_topics(timeout: float = 2.0):
    """Discover all active topics across the Zenoh network using liveliness queries."""
    try:
        import babyros
        session = babyros.node.SessionManager.get_session()
        discovered_topics = set()
        replies = session.liveliness().get("**/__liveliness__", timeout=timeout)
        for reply in replies:
            if reply.ok:
                key_expr = str(reply.ok.key_expr)
                if "/__liveliness__" in key_expr:
                    topic = key_expr.split("/__liveliness__")[0]
                    discovered_topics.add(topic)
        return list(discovered_topics)
    
    except Exception as e:
        logger.error(f"Error discovering topics: {e}")
        return []

def cmd_record(args):
    try:
        import babyros
    except ImportError:
        print("Error: babyros module not found. Cannot record topics.")
        sys.exit(1)

    print(f"Recording to {args.output}...")
    topics = args.topics if args.topics else []
    
    # If no topics specified, attempt to discover all active topics
    if not topics:
        print("No topics specified. Discovering active topics...")
        topics = discover_topics(timeout=2.0)
        
        if not topics:
            print("❌ No active topics discovered. Please specify topics manually, e.g.:")
            print("  babyrosbag record -O my_bag.bag imu")
            sys.exit(1)
        else:
            print(f"✓ Discovered {len(topics)} active topic(s): {', '.join(topics)}")

    with Bag(args.output, 'w') as bag:
        def make_callback(topic):
            def callback(msg):
                bag.write(topic, msg)
            return callback

        subscribers = []
        for t in topics:
            sub = babyros.node.Subscriber(t, make_callback(t))
            subscribers.append(sub)
            print(f"  Subscribed to: {t}")
                
        print("\nRecording... Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("\nStopped recording.")
            # Clean up subscribers
            for sub in subscribers:
                sub.delete()

def cmd_play(args):
    try:
        import babyros
    except ImportError:
        print("Error: babyros module not found. Cannot play topics.")
        sys.exit(1)

    print(f"Playing {args.input} at {args.rate}x speed...")
    publishers = {}
    
    with Bag(args.input, 'r') as bag:
        start_play_time = None
        first_msg_time = None
        
        for topic, msg, t in bag.read_messages():
            if topic not in publishers:
                publishers[topic] = babyros.node.Publisher(topic=topic)
                
            if first_msg_time is None:
                first_msg_time = t.to_sec()
                start_play_time = time.time()
                
            msg_elapsed = (t.to_sec() - first_msg_time) / args.rate
            play_elapsed = time.time() - start_play_time
            
            if msg_elapsed > play_elapsed:
                time.sleep(msg_elapsed - play_elapsed)
                
            publishers[topic].publish(msg)
            
    print("Playback finished.")

def cmd_info(args):
    try:
        with Bag(args.input, 'r') as bag:
            info = bag.get_type_and_topic_info()
            print(f"path:   {args.input}")
            print(f"version: 2.0 (babyrosbag format)")
            
            duration = 0.0
            if bag.start_time and bag.end_time:
                duration = bag.end_time.to_sec() - bag.start_time.to_sec()
                
            print(f"duration: {duration:.2f}s")
            print(f"start:    {bag.start_time.to_sec() if bag.start_time else 0.0:.2f} [0.00]")
            print(f"end:      {bag.end_time.to_sec() if bag.end_time else 0.0:.2f} [{duration:.2f}]")
            print(f"topics:   {len(info.topics)}")
            
            for topic, t_info in info.topics.items():
                print(f"  {topic:<25} {t_info.message_count:>6} msgs  : {t_info.msg_type}")
    except Exception as e:
        print(f"Error reading bag: {e}")

def cmd_filter(args):
    print(f"Filtering {args.input} to {args.output}...")
    print(f"Expression: {args.expression}")
    
    with Bag(args.input, 'r') as in_bag, Bag(args.output, 'w') as out_bag:
        count = 0
        for topic, msg, t in in_bag.read_messages():
            try:
                if eval(args.expression):
                    out_bag.write(topic, msg, t)
                    count += 1
            except Exception as e:
                print(f"Error evaluating expression: {e}")
                sys.exit(1)
    print(f"Filtering complete. Wrote {count} messages.")

def main():
    parser = argparse.ArgumentParser(prog='babyrosbag', description='BabyROS bag file utilities')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Record
    parser_record = subparsers.add_parser('record', help='Record topics to a bag file')
    parser_record.add_argument('topics', nargs='*', help='Topics to record (if empty, records all active topics)')
    parser_record.add_argument('-O', '--output', default='output.bag', help='Output bag file')

    # Play
    parser_play = subparsers.add_parser('play', help='Play a bag file')
    parser_play.add_argument('input', help='Input bag file')
    parser_play.add_argument('-r', '--rate', type=float, default=1.0, help='Playback rate multiplier')

    # Info
    parser_info = subparsers.add_parser('info', help='Print information about a bag file')
    parser_info.add_argument('input', help='Input bag file')

    # Filter
    parser_filter = subparsers.add_parser('filter', help='Filter messages from a bag file')
    parser_filter.add_argument('input', help='Input bag file')
    parser_filter.add_argument('output', help='Output bag file')
    parser_filter.add_argument('expression', help='Python expression (e.g., "topic == \'hand_gestures\'")')

    args = parser.parse_args()

    if args.command == 'record':
        cmd_record(args)
    elif args.command == 'play':
        cmd_play(args)
    elif args.command == 'info':
        cmd_info(args)
    elif args.command == 'filter':
        cmd_filter(args)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()