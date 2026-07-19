# cli.py
import argparse
import sys
import time
from babyrosbag.bag import Bag, Time
from loguru import logger
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn
from rich.console import Console


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

    console = Console()
    mode = 'a' if args.append else 'w'
    console.print(f"Recording to {args.output} ({'append' if mode == 'a' else 'overwrite'} mode)...")
    topics = args.topics if args.topics else []
    logger.disable("babyros")

    if not topics:
        console.print("No topics specified. Discovering active topics...")
        topics = discover_topics(timeout=2.0)
        if not topics:
            console.print("No active topics discovered. Please specify topics manually, e.g.:")
            console.print("  babyrosbag record -O my_bag.bag imu")
            sys.exit(1)
        else:
            console.print(f"Discovered {len(topics)} active topic(s): {', '.join(topics)}")

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TextColumn("[bold]{task.fields[msg_count]}"),
        TextColumn("msgs"),
        console=console,
        refresh_per_second=10,
    )

    with Bag(args.output, mode) as bag:
        subscribers = []
        for t in topics:
            console.print(f"  Subscribed to: {t}")

        task_id = progress.add_task(
            "Recording",
            total=None, 
            msg_count=0
        )
        def make_callback(topic):
            def callback(msg):
                bag.write(topic, msg)
                progress.update(task_id, advance=1, msg_count=progress.tasks[task_id].completed)
            return callback

        for t in topics:
            sub = babyros.node.Subscriber(t, make_callback(t))
            subscribers.append(sub)

        console.print("\nRecording... Press Ctrl+C to stop.")
        with progress:
            try:
                while True:
                    time.sleep(0.1)
            except KeyboardInterrupt:
                pass
            finally:
                for sub in subscribers:
                    sub.delete()
        
        logger.enable("babyros")
        final_count = int(progress.tasks[task_id].completed)
        console.print(f"\nStopped recording. {final_count} messages written to {args.output}", style="green")

def cmd_play(args):
    try:
        import babyros
    except ImportError:
        print("Error: babyros module not found. Cannot play topics.")
        sys.exit(1)

    console = Console()
    console.print(f"Playing {args.input} at {args.rate}x speed...")
    publishers = {}
    logger.disable("babyros")
    console.print("Opening bag file...", style="dim")

    t_start = time.time()
    with Bag(args.input, 'r') as bag:
        t_loaded = time.time()
        load_time = t_loaded - t_start
        
        if getattr(args, 'verbose', False):
            console.print(f"Bag loaded in {load_time:.3f} seconds.", style="dim")
        else:
            console.print("Bag opened.", style="dim")

        info = bag.get_type_and_topic_info()
        total_messages = sum(t_info.message_count for t_info in info.topics.values())
        console.print(f"Found {total_messages} messages.", style="dim")
        
        total_duration = 0.0
        if bag.start_time and bag.end_time:
            total_duration = bag.end_time.to_sec() - bag.start_time.to_sec()

        start_play_time = None
        first_msg_time = None
        msg_count = 0
        last_topic = ""
        interrupted = False

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            TextColumn("msgs"),
            TimeElapsedColumn(),
            TextColumn("<"),
            TimeRemainingColumn(),
            console=console,
            refresh_per_second=10,
        )
        
        with progress:
            task_id = progress.add_task(
                "Playing",
                total=total_messages,
                topic=""
            )
            try:
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
                    msg_count += 1
                    last_topic = topic
                    progress.update(task_id, advance=1, topic=f"[dim]{last_topic[:30]}")

            except KeyboardInterrupt:
                interrupted = True

        logger.enable("babyros")
        if interrupted:
            console.print("\nPlayback interrupted.", style="yellow")
            return
            
    console.print(f"Playback finished. {msg_count} messages published.", style="green")

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

    parser_record = subparsers.add_parser('record', help='Record topics to a bag file')
    parser_record.add_argument('topics', nargs='*', help='Topics to record')
    parser_record.add_argument('-O', '--output', default='output.bag', help='Output bag file')
    parser_record.add_argument('-w', '--overwrite', action='store_true', default=True, help='Overwrite existing file (default)')
    parser_record.add_argument('-a', '--append', action='store_true', help='Append to existing file instead of overwriting')

    parser_play = subparsers.add_parser('play', help='Play a bag file')
    parser_play.add_argument('input', help='Input bag file')
    parser_play.add_argument('-r', '--rate', type=float, default=1.0, help='Playback rate multiplier')
    parser_play.add_argument('-v', '--verbose', action='store_true', help='Print detailed timing information')

    parser_info = subparsers.add_parser('info', help='Print information about a bag file')
    parser_info.add_argument('input', help='Input bag file')

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